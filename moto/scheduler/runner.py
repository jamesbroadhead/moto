"""Runs EventBridge Scheduler schedules and scheduled EventBridge rules when MOTO_SCHEDULER_FIRE is set.

One daemon thread per process looks at every schedule and rule with a ScheduleExpression each
MOTO_SCHEDULER_TICK_SECONDS and invokes the targets of those that are due.
"""

import calendar
import datetime
import json
import logging
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo

from moto import settings
from moto.moto_api._internal import mock_random

logger = logging.getLogger(__name__)

UTC = datetime.timezone.utc
RATE = re.compile(r"^rate\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days)\s*\)$")
AT = re.compile(r"^at\(\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s*\)$")
CRON = re.compile(r"^cron\((.*)\)$")
UNIT_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}
MONTHS = {m.upper(): i for i, m in enumerate(calendar.month_abbr) if m}
WEEKDAYS = {"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}


class Cron:
    """An EventBridge cron expression: minutes hours day-of-month month day-of-week year.

    Supports lists, ranges, steps, month and weekday names, `?`, and `L`, `W` and `#`.
    """

    def __init__(self, expression: str):
        fields = expression.split()
        if len(fields) != 6:
            raise ValueError(f"cron({expression}) needs 6 fields")
        minutes, hours, self.dom, months, self.dow, years = fields
        self.minutes = sorted(self._values(minutes, 0, 59))
        self.hours = sorted(self._values(hours, 0, 23))
        self.months = self._values(months, 1, 12, MONTHS)
        self.years = self._values(years, 1970, 2199)

    @staticmethod
    def _values(
        field: str, low: int, high: int, names: dict[str, int] | None = None
    ) -> set[int]:
        def number(text: str) -> int:
            return (names or {}).get(text.upper()) or int(text)

        values: set[int] = set()
        for part in field.split(","):
            span, _, step = part.partition("/")
            if span in ("*", "?"):
                start, end = low, high
            elif "-" in span:
                first, last = span.split("-")
                start, end = number(first), number(last)
            else:
                start = number(span)
                end = high if step else start
            values.update(range(start, end + 1, int(step) if step else 1))
        return values

    def _day_matches(self, day: datetime.date) -> bool:
        if day.month not in self.months or day.year not in self.years:
            return False
        last_day = calendar.monthrange(day.year, day.month)[1]
        weekday = (day.weekday() + 1) % 7 + 1  # 1 = Sunday, as in EventBridge
        dom, dow = self.dom.upper(), self.dow.upper()
        if dom not in ("*", "?"):
            if dom == "L":
                matches = day.day == last_day
            elif dom.endswith("W"):
                target = last_day if dom == "LW" else int(dom[:-1])
                matches = day.day == _nearest_weekday(day, min(target, last_day))
            else:
                matches = day.day in self._values(dom, 1, 31)
            if not matches:
                return False
        if dow not in ("*", "?"):
            if "#" in dow:
                name, nth = dow.split("#")
                wanted = WEEKDAYS.get(name) or int(name)
                return weekday == wanted and (day.day - 1) // 7 + 1 == int(nth)
            if dow.endswith("L") and len(dow) > 1:
                wanted = WEEKDAYS.get(dow[:-1]) or int(dow[:-1])
                return weekday == wanted and day.day + 7 > last_day
            return weekday in self._values(dow, 1, 7, WEEKDAYS)
        return True

    def next_after(self, after: datetime.datetime) -> datetime.datetime | None:
        """The first time, in `after`'s timezone, strictly later than `after`."""
        tz = after.tzinfo
        day = after.date()
        for _ in range(366 * 10):
            if self._day_matches(day):
                for hour in self.hours:
                    for minute in self.minutes:
                        candidate = datetime.datetime(
                            day.year, day.month, day.day, hour, minute, tzinfo=tz
                        )
                        if candidate > after:
                            return candidate
            day += datetime.timedelta(days=1)
        return None


def _nearest_weekday(day: datetime.date, target: int) -> int:
    candidate = day.replace(day=target)
    if (
        candidate.weekday() == 5
    ):  # Saturday: the Friday before, unless that is last month
        return target - 1 if target > 1 else target + 2
    if candidate.weekday() == 6:  # Sunday: the Monday after, unless that is next month
        last_day = calendar.monthrange(day.year, day.month)[1]
        return target + 1 if target < last_day else target - 2
    return target


def to_utc(value: Any) -> datetime.datetime | None:
    """A StartDate or EndDate as the API (epoch seconds) or CloudFormation (ISO 8601) gives it."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.datetime.fromtimestamp(float(value), tz=UTC)
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass
class Job:
    """Something with a schedule expression: a Scheduler schedule or an EventBridge rule."""

    key: str
    expression: str
    timezone: str
    anchor: datetime.datetime
    start: datetime.datetime | None
    end: datetime.datetime | None
    enabled: bool
    fingerprint: tuple[Any, ...]
    fire: Callable[[datetime.datetime], None]
    complete: Callable[[], None]

    def first_due(self, now: datetime.datetime) -> datetime.datetime | None:
        at = AT.match(self.expression)
        if at:
            due = datetime.datetime.fromisoformat(at[1]).replace(
                tzinfo=ZoneInfo(self.timezone)
            )
            return due.astimezone(UTC)
        return self.due_after(now)

    def due_after(self, after: datetime.datetime) -> datetime.datetime | None:
        rate = RATE.match(self.expression)
        if rate:
            period = datetime.timedelta(
                seconds=int(rate[1]) * UNIT_SECONDS[rate[2].rstrip("s")]
            )
            anchor = self.start or self.anchor
            if after < anchor:
                return anchor
            return anchor + period * ((after - anchor) // period + 1)
        cron = CRON.match(self.expression)
        if cron:
            after = max(after, self.start or after)
            local = Cron(cron[1]).next_after(
                after.astimezone(ZoneInfo(self.timezone)).replace(
                    second=0, microsecond=0
                )
            )
            return local.astimezone(UTC) if local else None
        return None


@dataclass
class _State:
    fingerprint: tuple[Any, ...]
    due: datetime.datetime | None


class ScheduleRunner:
    def __init__(self, clock: Callable[[], datetime.datetime] | None = None):
        self.clock = clock or (lambda: datetime.datetime.now(UTC))
        self._states: dict[str, _State] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def ensure_started(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run, name="moto-scheduler", daemon=True
                )
                self._thread.start()

    def _run(self) -> None:
        while settings.scheduler_fire():
            try:
                for fire in self.tick():
                    threading.Thread(target=fire, daemon=True).start()
            except Exception:
                logger.exception("Schedule runner tick failed")
            time.sleep(settings.scheduler_tick_seconds())

    def tick(self, now: datetime.datetime | None = None) -> list[Callable[[], None]]:
        """Advances every job to `now` and returns the invocations that are due, oldest first."""
        now = now or self.clock()
        fires: list[Callable[[], None]] = []
        seen = set()
        with self._lock:
            for job in list(_jobs()):
                seen.add(job.key)
                try:
                    self._advance(job, now, fires)
                except Exception:
                    logger.exception("Cannot run the schedule of %s", job.key)
            for key in set(self._states) - seen:
                del self._states[key]
        return fires

    def _advance(
        self, job: Job, now: datetime.datetime, fires: list[Callable[[], None]]
    ) -> None:
        state = self._states.get(job.key)
        if state is None or state.fingerprint != job.fingerprint:
            due = job.first_due(now) if job.enabled else None
            state = self._states[job.key] = _State(job.fingerprint, due)
        if state.due is None or now < state.due:
            return
        scheduled = state.due
        state.due = None
        if (job.start and scheduled < job.start) or (job.end and scheduled > job.end):
            fires.append(_logged(job.key, job.complete))
            return
        fires.append(_logged(job.key, lambda: job.fire(scheduled)))
        following = None if AT.match(job.expression) else job.due_after(now)
        if following is None or (job.end and following > job.end):
            fires.append(_logged(job.key, job.complete))
        else:
            state.due = following

    def run_due(self, now: datetime.datetime | None = None) -> None:
        """Invokes whatever is due at `now` in this thread."""
        for fire in self.tick(now):
            fire()


def _logged(key: str, call: Callable[[], None]) -> Callable[[], None]:
    def run() -> None:
        try:
            call()
        except Exception:
            logger.exception("Cannot invoke the target of %s", key)

    return run


def _jobs() -> Iterator[Job]:
    yield from _schedule_jobs()
    yield from _rule_jobs()


def _schedule_jobs() -> Iterator[Job]:
    from .models import scheduler_backends

    for account_id, regions in list(scheduler_backends.items()):
        for region, backend in list(regions.items()):
            for group in list(backend.schedule_groups.values()):
                for schedule in list(group.schedules.values()):
                    yield Job(
                        key=schedule.arn,
                        expression=schedule.schedule_expression,
                        timezone=schedule.schedule_expression_timezone,
                        anchor=to_utc(schedule.creation_date),  # type: ignore[arg-type]
                        start=to_utc(schedule.start_date),
                        end=to_utc(schedule.end_date),
                        enabled=schedule.state == "ENABLED",
                        fingerprint=(
                            schedule.schedule_expression,
                            schedule.schedule_expression_timezone,
                            schedule.start_date,
                            schedule.end_date,
                            schedule.state,
                            schedule.creation_date,
                        ),
                        fire=partial(
                            invoke_schedule_target, schedule, account_id, region
                        ),
                        complete=partial(_complete_schedule, schedule, group),
                    )


def _complete_schedule(schedule: Any, group: Any) -> None:
    if schedule.action_after_completion == "DELETE":
        if group.schedules.get(schedule.name) is schedule:
            del group.schedules[schedule.name]


def _rule_jobs() -> Iterator[Job]:
    from moto.events.models import events_backends

    for account_id, regions in list(events_backends.items()):
        for region, backend in list(regions.items()):
            for bus in list(backend.event_buses.values()):
                for rule in list(bus.rules.values()):
                    if not rule.scheduled_expression:
                        continue
                    yield Job(
                        key=rule.arn,
                        expression=rule.scheduled_expression,
                        timezone="UTC",
                        anchor=rule.created,
                        start=None,
                        end=None,
                        enabled=rule.state != "DISABLED",
                        fingerprint=(rule.scheduled_expression, rule.state, id(rule)),
                        fire=partial(_fire_rule, rule, account_id, region),
                        complete=_nothing,
                    )


def _nothing() -> None:
    pass


def _fire_rule(rule: Any, account_id: str, region: str, at: datetime.datetime) -> None:
    rule.send_scheduled_event(
        {
            "version": "0",
            "id": str(mock_random.uuid4()),
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "account": account_id,
            "time": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "region": region,
            "resources": [rule.arn],
            "detail": {},
            "ingestion-time": time.time(),
        }
    )


def schedule_input(schedule: Any, at: datetime.datetime) -> str:
    """The target's Input with the schedule's context attributes filled in.

    Without an Input, targets get an empty JSON object.
    """
    text = schedule.target.get("Input") or "{}"
    context = {
        "aws.scheduler.schedule-arn": schedule.arn,
        "aws.scheduler.scheduled-time": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "aws.scheduler.execution-id": str(mock_random.uuid4()),
        "aws.scheduler.attempt-number": "1",
    }
    for name, value in context.items():
        text = text.replace(f"<{name}>", value)
    return text


def invoke_schedule_target(
    schedule: Any, account_id: str, region: str, at: datetime.datetime
) -> None:
    """Invokes a schedule's templated or universal target. RoleArn is not checked."""
    from moto.events.models import Rule, events_backends

    target = schedule.target
    body = schedule_input(schedule, at)
    parts = target["Arn"].split(":")
    service, target_region, target_account = parts[2], parts[3], parts[4]
    if service == "scheduler" and parts[5] == "aws-sdk":
        _call_universal_target(parts[6], parts[7], body, account_id, region)
        return
    target_region = target_region or region
    target_account = target_account or account_id
    if service == "events" and parts[5].startswith("event-bus/"):
        parameters = target.get("EventBridgeParameters") or {}
        events_backends[target_account][target_region].put_events(
            [
                {
                    "Source": parameters.get("Source"),
                    "DetailType": parameters.get("DetailType"),
                    "Detail": body,
                    "EventBusName": target["Arn"],
                }
            ]
        )
        return
    # Templated targets are delivered as EventBridge delivers to the same kind of target
    rule_target = {"Id": schedule.name, "Arn": target["Arn"], "Input": body}
    if target.get("SqsParameters"):
        rule_target["SqsParameters"] = target["SqsParameters"]
    Rule(
        name=schedule.name,
        account_id=target_account,
        region_name=target_region,
        description=None,
        event_pattern=None,
        schedule_exp=None,
        role_arn=None,
        event_bus_name="default",
        state="ENABLED",
        targets=[rule_target],
    ).send_scheduled_event({})


def _call_universal_target(
    service: str, action: str, body: str, account_id: str, region: str
) -> None:
    """Calls `service:action` with the Input as its request, against this moto process."""
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from botocore.session import Session

    from moto.core.botocore_stubber import BotocoreStubber

    stubber = BotocoreStubber()
    stubber.enabled = True

    def send(request: Any, **kwargs: Any) -> Any:
        request.headers["x-moto-account-id"] = account_id
        response = stubber("before-send", request)
        if response is None:
            raise RuntimeError(f"moto does not implement {request.url}")
        return response

    client = boto3.session.Session(botocore_session=Session()).client(
        service,  # type: ignore[call-overload]
        region_name=region,
        aws_access_key_id="scheduler",
        aws_secret_access_key="scheduler",
        config=Config(retries={"max_attempts": 1}),
    )
    client.meta.events.register_first("before-send", send)
    operation = client.meta.method_to_api_mapping
    method = next(
        (name for name, api in operation.items() if api.lower() == action.lower()),
        None,
    )
    if method is None:
        raise ValueError(f"{service} has no API action {action}")
    try:
        getattr(client, method)(**json.loads(body))
    except ClientError as e:
        logger.warning("Schedule target %s:%s failed: %s", service, action, e)


runner = ScheduleRunner()


def start_if_enabled() -> None:
    if settings.scheduler_fire():
        runner.ensure_started()
