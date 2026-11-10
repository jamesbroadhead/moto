"""Background polling for Lambda event source mappings (MOTO_LAMBDA_EVENT_SOURCE_POLLING).

As in Lambda, each enabled mapping reads its SQS queue, Kinesis stream or DynamoDB stream in its own thread and invokes
the function synchronously with a batch of records; the producer's call returns at once.
"""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from moto.core.utils import iso_8601_datetime_with_milliseconds, unix_time, utcnow

if TYPE_CHECKING:
    from moto.awslambda.models import EventSourceMapping

logger = logging.getLogger(__name__)

# Sent with each invocation that an event source mapping makes to MOTO_LAMBDA_INVOKE_ENDPOINT, so that the executor
# can tell them from direct invocations.
EVENT_SOURCE_MAPPING_HEADER = "X-Moto-Event-Source-Mapping-Arn"

# Concurrent batches per SQS mapping without a ScalingConfig.MaximumConcurrency
DEFAULT_SQS_CONCURRENCY = 4


def dynamodb_stream_records(
    items: Iterable[Any], source: str, region: str
) -> list[dict[str, Any]]:
    """DynamoDB StreamRecords as Lambda delivers them."""
    records = []
    for item in items:
        record = item.to_json()
        dynamodb = dict(record["dynamodb"])
        if isinstance(dynamodb.get("ApproximateCreationDateTime"), datetime):
            dynamodb["ApproximateCreationDateTime"] = unix_time(
                dynamodb["ApproximateCreationDateTime"]
            )
        records.append(
            {
                "eventID": record["eventID"],
                "eventName": record["eventName"],
                "eventVersion": record["eventVersion"],
                "eventSource": record["eventSource"],
                "awsRegion": region,
                "dynamodb": dynamodb,
                "eventSourceARN": source,
            }
        )
    return records


def sqs_record(message: Any, queue_arn: str, region: str) -> dict[str, Any]:
    attributes = {
        "ApproximateReceiveCount": str(message.approximate_receive_count),
        "SentTimestamp": str(message.sent_timestamp),
        "SenderId": message.sender_id,
        "ApproximateFirstReceiveTimestamp": str(
            message.approximate_first_receive_timestamp
        ),
    }
    if message.group_id:
        attributes["MessageGroupId"] = message.group_id
    if message.deduplication_id:
        attributes["MessageDeduplicationId"] = message.deduplication_id
    if message.sequence_number:
        attributes["SequenceNumber"] = message.sequence_number
    trace = (message.system_attributes or {}).get("AWSTraceHeader")
    if trace:
        attributes["AWSTraceHeader"] = trace.get("StringValue")

    def binary(value: Any) -> Any:
        return base64.b64encode(value).decode() if isinstance(value, bytes) else value

    message_attributes = {
        name: {
            "stringValue": value.get("StringValue"),
            "binaryValue": binary(value.get("BinaryValue")),
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": value["DataType"],
        }
        for name, value in message.message_attributes.items()
    }
    return {
        "messageId": message.id,
        "receiptHandle": message.receipt_handle,
        "body": message.body,
        "attributes": attributes,
        "messageAttributes": message_attributes,
        "md5OfMessageAttributes": message.attribute_md5
        if message.message_attributes
        else None,
        "md5OfBody": message.body_md5,
        "eventSource": "aws:sqs",
        "eventSourceARN": queue_arn,
        "awsRegion": region,
    }


def kinesis_record(
    record: Any, shard_id: str, stream_arn: str, region: str, role: str
) -> dict[str, Any]:
    return {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": record.partition_key,
            "sequenceNumber": str(record.sequence_number),
            "data": record.data,
            "approximateArrivalTimestamp": round(record.created_at, 3),
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": f"{shard_id}:{record.sequence_number}",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": role,
        "awsRegion": region,
        "eventSourceARN": stream_arn,
    }


def _json_or_none(value: Any) -> Any:
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


class EventSourceMappingPoller:
    """Reads one mapping's event source and invokes its function until stopped."""

    # Tests shorten these
    POLL_INTERVAL = 0.2
    RETRY_DELAY = 1.0

    def __init__(self, esm: EventSourceMapping):
        self.esm = esm
        self.stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"esm-{esm.uuid}", daemon=True
        )

    @property
    def source_account(self) -> str:
        return self.esm.event_source_arn.split(":")[4]

    @property
    def source_region(self) -> str:
        return self.esm.event_source_arn.split(":")[3]

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.stopped.set()

    def _run(self) -> None:
        while not self.stopped.is_set():
            busy = False
            if self.esm.enabled:
                try:
                    busy = self.poll()
                except Exception as e:  # the source may not exist yet, or was deleted
                    logger.warning("Event source mapping %s: %r", self.esm.arn, e)
                    self.stopped.wait(self.RETRY_DELAY)
            if not busy:
                self.stopped.wait(self.POLL_INTERVAL)

    def poll(self) -> bool:
        """Processes what the source has; returns whether there was anything."""
        raise NotImplementedError

    def filter_subject(self, record: dict[str, Any]) -> dict[str, Any]:
        """What FilterCriteria patterns are matched against."""
        return record

    def matches(self, record: dict[str, Any]) -> bool:
        from moto.events.models import EventPattern

        filters = (self.esm.filter_criteria or {}).get("Filters") or []
        if not filters:
            return True
        subject = self.filter_subject(record)
        return any(
            EventPattern.load(f.get("Pattern")).matches_event(subject) for f in filters
        )

    def invoke(self, records: list[dict[str, Any]]) -> tuple[bool, Any]:
        """Invokes the function with a batch; returns whether it succeeded and its parsed response."""
        from moto.awslambda.utils import get_backend

        esm = self.esm
        function_region = esm.function_arn.split(":")[3]
        function_account = esm.function_arn.split(":")[4]
        request_headers = {
            "x-amz-invocation-type": "RequestResponse",
            EVENT_SOURCE_MAPPING_HEADER.lower(): esm.arn,
        }
        response_headers: dict[str, Any] = {}
        try:
            payload = get_backend(function_account, function_region).invoke(
                function_name=esm.function_arn,
                qualifier=None,
                body=json.dumps({"Records": records}),
                headers=request_headers,
                response_headers=response_headers,
            )
        except Exception as e:
            logger.warning("Event source mapping %s: invoke failed: %r", esm.arn, e)
            esm.last_processing_result = "PROBLEM: Function call failed"
            return False, None
        ok = "x-amz-function-error" not in response_headers
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        esm.last_processing_result = "OK" if ok else "PROBLEM: Function call failed"
        if not ok:
            logger.warning(
                "Event source mapping %s: function error: %s", esm.arn, payload
            )
        return ok, _json_or_none(payload)

    def failed_items(self, response: Any, identifiers: set[str]) -> set[str] | None:
        """The partial batch response's failed item identifiers, or None if the whole batch failed.

        Without ReportBatchItemFailures a successful invocation processed every record; Lambda treats a response
        that names an unknown or empty identifier as a failure of the whole batch.
        """
        if "ReportBatchItemFailures" not in (self.esm.function_response_types or []):
            return set()
        if not isinstance(response, dict):
            return set()
        failures = response.get("batchItemFailures") or []
        failed = set()
        for failure in failures:
            identifier = (failure or {}).get("itemIdentifier")
            if not identifier or str(identifier) not in identifiers:
                return None
            failed.add(str(identifier))
        return failed


class SqsPoller(EventSourceMappingPoller):
    LONG_POLL_SECONDS = 1

    def __init__(self, esm: EventSourceMapping):
        super().__init__(esm)
        self.queue_name = esm.event_source_arn.split(":")[5]
        self.fifo = self.queue_name.endswith(".fifo")
        concurrency = (esm.scaling_config or {}).get("MaximumConcurrency")
        workers = 1 if self.fifo else int(concurrency or DEFAULT_SQS_CONCURRENCY)
        # A received batch is hidden only for the visibility timeout: receive one only when a worker is free
        self.free = threading.Semaphore(workers)
        self.pool = ThreadPoolExecutor(workers, thread_name_prefix=f"esm-{esm.uuid}")

    @property
    def sqs(self) -> Any:
        from moto.sqs.models import sqs_backends

        return sqs_backends[self.source_account][self.source_region]

    def stop(self) -> None:
        super().stop()
        self.pool.shutdown(wait=False)

    def filter_subject(self, record: dict[str, Any]) -> dict[str, Any]:
        body = _json_or_none(record["body"])
        return {**record, "body": body} if isinstance(body, (dict, list)) else record

    @property
    def long_polls(self) -> bool:
        # Lambda long-polls, so a message is delivered as soon as it is sent. Not with a batching window, which
        # receives for its whole length anyway, nor FIFO queues: moto's long poll spins while a visible message
        # waits behind its group's in-flight messages.
        return not self.fifo and not float(self.esm.max_batch_window or 0)

    def receive(self) -> list[Any]:
        queue = self.sqs.get_queue(self.queue_name)
        batch_size = self.esm.batch_size
        window = float(self.esm.max_batch_window or 0)
        deadline = time.monotonic() + window
        messages: list[Any] = []
        while True:
            wait = self.LONG_POLL_SECONDS if self.long_polls else 0
            messages += self.sqs.receive_message(
                self.queue_name,
                batch_size - len(messages),
                wait,
                queue.visibility_timeout,
                message_attribute_names=["All"],
            )
            if len(messages) >= batch_size or time.monotonic() >= deadline:
                return messages
            if self.stopped.wait(min(self.POLL_INTERVAL, window)):
                return messages

    def poll(self) -> bool:
        if not self.free.acquire(timeout=self.POLL_INTERVAL):
            return True
        try:
            messages = self.receive()
        except Exception:
            self.free.release()
            raise
        if self.stopped.is_set():
            # Stopped during a long poll: leave the messages to whatever reads the queue next
            with contextlib.suppress(Exception):  # after a reset the queue is gone
                for message in messages:
                    self.sqs.change_message_visibility(
                        self.queue_name, message.receipt_handle, 0
                    )
            self.free.release()
            return False
        if not messages:
            self.free.release()
            return self.long_polls  # a long poll has already waited
        future = self.pool.submit(self.handle, messages)
        future.add_done_callback(lambda _: self.free.release())
        return True

    def delete(self, messages: list[Any]) -> None:
        for message in messages:
            self.sqs.delete_message(self.queue_name, message.receipt_handle)

    def handle(self, messages: list[Any]) -> None:
        region = self.source_region
        records = [
            (m, sqs_record(m, self.esm.event_source_arn, region)) for m in messages
        ]
        # Lambda deletes messages that match no filter without invoking the function
        self.delete([m for m, r in records if not self.matches(r)])
        records = [(m, r) for m, r in records if self.matches(r)]
        if not records:
            return
        ok, response = self.invoke([r for _, r in records])
        if not ok:
            return  # the messages become visible again after the visibility timeout
        failed = self.failed_items(response, {m.id for m, _ in records})
        if failed is None:
            return
        if self.fifo and failed:
            # A failed message holds back the rest of its message group
            held_groups: set[str | None] = set()
            done = []
            for m, _ in records:
                if m.id in failed or m.group_id in held_groups:
                    held_groups.add(m.group_id)
                else:
                    done.append(m)
        else:
            done = [m for m, _ in records if m.id not in failed]
        self.delete(done)


class StreamPoller(EventSourceMappingPoller):
    """Kinesis and DynamoDB Streams: one batch per shard at a time, in order, retried until it succeeds."""

    BATCH_INFO_KEY = ""

    def __init__(self, esm: EventSourceMapping):
        super().__init__(esm)
        # Per shard: the position after the last record processed
        self.positions: dict[str, Any] = {}
        self.waiting_since: dict[str, float] = {}
        self.started = False

    def shards(self) -> dict[str, Any]:
        raise NotImplementedError

    def initial_position(self, shard: Any, at_start: bool) -> Any:
        """Where to start reading a shard: at StartingPosition for the shards there when the mapping starts."""
        raise NotImplementedError

    def read(self, shard: Any, position: Any, limit: int) -> tuple[list[Any], Any]:
        """Up to limit records after position, and the position after them."""
        raise NotImplementedError

    def to_records(self, shard_id: str, items: list[Any]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def sequence_number(self, record: dict[str, Any]) -> str:
        raise NotImplementedError

    def arrival(self, record: dict[str, Any]) -> float:
        raise NotImplementedError

    def start(self) -> None:
        # Fix LATEST now: records written once the mapping exists are delivered
        try:
            self.track_shards()
        except Exception:  # the stream may not exist yet; poll() tries again
            pass
        super().start()

    def track_shards(self) -> dict[str, Any]:
        shards = self.shards()
        for shard_id, shard in shards.items():
            if shard_id not in self.positions:
                self.positions[shard_id] = self.initial_position(
                    shard, at_start=not self.started
                )
        self.started = True
        return shards

    def poll(self) -> bool:
        shards = self.track_shards()
        busy = False
        for shard_id, shard in shards.items():
            if self.stopped.is_set():
                break
            batch_size = self.esm.batch_size
            items, after = self.read(shard, self.positions[shard_id], batch_size)
            if not items:
                continue
            window = float(self.esm.max_batch_window or 0)
            if len(items) < batch_size and window:
                since = self.waiting_since.setdefault(shard_id, time.monotonic())
                if time.monotonic() - since < window:
                    continue
            self.waiting_since.pop(shard_id, None)
            busy = True
            records = self.to_records(shard_id, items)
            # Lambda skips records that match no filter
            records = [r for r in records if self.matches(r)]
            if self.process(shard_id, records):
                self.positions[shard_id] = after
        return busy

    def process(self, shard_id: str, records: list[dict[str, Any]]) -> bool:
        """Invokes until the batch succeeds or is discarded; returns False if stopped first."""
        esm = self.esm
        max_attempts = esm.max_retry_attempts
        max_age = esm.max_record_age
        attempts = 0
        while records:
            ok, response = self.invoke(records)
            if self.stopped.is_set():
                return False
            failed = (
                self.failed_items(response, {self.sequence_number(r) for r in records})
                if ok
                else None
            )
            if failed is not None and not failed:
                return True
            if failed:
                # Like Lambda, retry from the lowest failed sequence number
                first = min(int(s) for s in failed)
                records = [r for r in records if int(self.sequence_number(r)) >= first]
            attempts += 1
            if max_attempts is not None and int(max_attempts) != -1:
                if attempts > int(max_attempts):
                    self.on_failure(
                        shard_id, records, "RetryAttemptsExhausted", attempts
                    )
                    return True
            if max_age is not None and int(max_age) != -1:
                if time.time() - self.arrival(records[0]) > int(max_age):
                    self.on_failure(shard_id, records, "RecordAgeExceeded", attempts)
                    return True
            if self.stopped.wait(self.RETRY_DELAY):
                return False
        return True

    def on_failure(
        self,
        shard_id: str,
        records: list[dict[str, Any]],
        condition: str,
        attempts: int,
    ) -> None:
        """Sends a discarded batch's details to the mapping's OnFailure SQS destination, if it has one."""
        destination = (
            (self.esm.destination_config or {}).get("OnFailure", {}).get("Destination")
        )
        logger.warning(
            "Event source mapping %s: discarding %d records of %s (%s)",
            self.esm.arn,
            len(records),
            shard_id,
            condition,
        )
        if not destination or destination.split(":")[2] != "sqs":
            return
        from moto.moto_api._internal import mock_random
        from moto.sqs.models import sqs_backends

        def iso(epoch: float) -> str:
            return iso_8601_datetime_with_milliseconds(
                datetime.fromtimestamp(epoch, timezone.utc).replace(tzinfo=None)
            )

        message = {
            "requestContext": {
                "requestId": str(mock_random.uuid4()),
                "functionArn": self.esm.function_arn,
                "condition": condition,
                "approximateInvokeCount": attempts,
            },
            "responseContext": {
                "statusCode": 200,
                "executedVersion": "$LATEST",
                "functionError": "Unhandled",
            },
            "version": "1.0",
            "timestamp": iso_8601_datetime_with_milliseconds(utcnow()),
            self.BATCH_INFO_KEY: {
                "shardId": shard_id,
                "startSequenceNumber": self.sequence_number(records[0]),
                "endSequenceNumber": self.sequence_number(records[-1]),
                "approximateArrivalOfFirstRecord": iso(self.arrival(records[0])),
                "approximateArrivalOfLastRecord": iso(self.arrival(records[-1])),
                "batchSize": len(records),
                "streamArn": self.esm.event_source_arn,
            },
        }
        _, _, _, region, account, name = destination.split(":")
        sqs_backends[account][region].send_message(name, json.dumps(message))


class KinesisPoller(StreamPoller):
    BATCH_INFO_KEY = "KinesisBatchInfo"

    def stream(self) -> Any:
        from moto.kinesis.models import kinesis_backends

        backend = kinesis_backends[self.source_account][self.source_region]
        for stream in backend.streams.values():
            if stream.arn == self.esm.event_source_arn:
                return stream
        raise LookupError(f"No stream {self.esm.event_source_arn}")

    def shards(self) -> dict[str, Any]:
        return {s.shard_id: s for s in self.stream().shards.values()}

    def initial_position(self, shard: Any, at_start: bool) -> int:
        if not at_start:
            return 0  # a shard that resharding created: read it from the start
        position = self.esm.starting_position
        if position == "LATEST":
            return shard.get_max_sequence_number()
        if position == "AT_TIMESTAMP" and self.esm.starting_position_timestamp:
            timestamp = self.esm.starting_position_timestamp
            if isinstance(timestamp, datetime):
                timestamp = unix_time(timestamp)
            return shard.get_sequence_number_at(float(timestamp))
        return 0

    def read(self, shard: Any, position: int, limit: int) -> tuple[list[Any], int]:
        records, last, _ = shard.get_records(str(position), limit)
        return records, last

    def to_records(self, shard_id: str, items: list[Any]) -> list[dict[str, Any]]:
        from moto.awslambda.utils import get_backend

        fn = get_backend(
            self.esm.function_arn.split(":")[4], self.esm.function_arn.split(":")[3]
        ).get_function(self.esm.function_arn)
        return [
            kinesis_record(
                r, shard_id, self.esm.event_source_arn, self.source_region, fn.role
            )
            for r in items
        ]

    def filter_subject(self, record: dict[str, Any]) -> dict[str, Any]:
        # Kinesis filters match the record's metadata, and its data as JSON
        subject = dict(record["kinesis"])
        data = _json_or_none(base64.b64decode(subject["data"]))
        if data is None:
            del subject["data"]
        else:
            subject["data"] = data
        return subject

    def sequence_number(self, record: dict[str, Any]) -> str:
        return record["kinesis"]["sequenceNumber"]

    def arrival(self, record: dict[str, Any]) -> float:
        return record["kinesis"]["approximateArrivalTimestamp"]


class DynamoDBStreamPoller(StreamPoller):
    BATCH_INFO_KEY = "DDBStreamBatchInfo"

    def shards(self) -> dict[str, Any]:
        from moto.dynamodbstreams.models import dynamodbstreams_backends

        backend = dynamodbstreams_backends[self.source_account][self.source_region]
        table = backend._get_table_from_arn(self.esm.event_source_arn)
        shard = table.stream_shard
        if shard is None:
            return {}
        # Re-enabling a table's stream starts a new shard
        return {f"{shard.id}/{id(shard)}": shard}

    def initial_position(self, shard: Any, at_start: bool) -> int:
        if at_start and self.esm.starting_position == "LATEST":
            return len(shard.items)
        return 0

    def read(self, shard: Any, position: int, limit: int) -> tuple[list[Any], int]:
        items = shard.items[position : position + limit]
        return items, position + len(items)

    def to_records(self, shard_id: str, items: list[Any]) -> list[dict[str, Any]]:
        return dynamodb_stream_records(
            items, self.esm.event_source_arn, self.source_region
        )

    def sequence_number(self, record: dict[str, Any]) -> str:
        return record["dynamodb"]["SequenceNumber"]

    def arrival(self, record: dict[str, Any]) -> float:
        return float(record["dynamodb"]["ApproximateCreationDateTime"])


_pollers: dict[str, EventSourceMappingPoller] = {}
_pollers_lock = threading.Lock()


def start_polling(esm: EventSourceMapping) -> None:
    service = esm.event_source_arn.split(":")[2]
    poller_class = {
        "sqs": SqsPoller,
        "kinesis": KinesisPoller,
        "dynamodb": DynamoDBStreamPoller,
    }.get(service)
    if poller_class is None:
        return
    with _pollers_lock:
        if esm.arn in _pollers:
            return
        poller = _pollers[esm.arn] = poller_class(esm)
    poller.start()


def stop_polling(esm_arn: str) -> None:
    with _pollers_lock:
        poller = _pollers.pop(esm_arn, None)
    if poller is not None:
        poller.stop()
