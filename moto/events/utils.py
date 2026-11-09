import json
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from typing_extensions import Any, Required


# NOTE: Typing is based on the following document https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-event-patterns.html
EventMessageType = TypedDict(
    "EventMessageType",
    {
        "version": str,
        "id": str,
        "detail-type": "Required[str | list[str]]",
        "source": "Required[str | list[str]]",
        "account": str,
        # support float type for internal use of moto.
        "time": "str | float",
        "replay-name": str,
        "region": str,
        "resources": list[str],
        "detail": "Required[dict[str, Any]]",
        "ingestion-time": float,
    },
    total=False,
)

PAGINATION_MODEL = {
    "list_rules": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 50,
        "unique_attribute": "arn",
        "fail_on_invalid_token": False,
    },
    "list_rule_names_by_target": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 50,
        "unique_attribute": "arn",
        "fail_on_invalid_token": False,
    },
    "list_targets_by_rule": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 50,
        "unique_attribute": "Arn",
        "fail_on_invalid_token": False,
    },
    "list_event_buses": {
        "input_token": "next_token",
        "limit_key": "limit",
        "limit_default": 50,
        "unique_attribute": "arn",
        "fail_on_invalid_token": False,
    },
}

_BASE_EVENT_MESSAGE: EventMessageType = {
    "version": "0",
    "id": "17793124-05d4-b198-2fde-7ededc63b103",
    "detail-type": "",
    "source": "",
    "account": "",
    "time": "",
    "region": "",
    "resources": [],
    "detail": {},
}


class EventTemplateParser:
    DEFAULT_EVENT_INPUT_TEMPLATE = '{"version": "0", "id": "<id>", "detail-type": "<detail-type>", "source": "<source>", "account": "<account>", "time": <time>, "region": "<region>", "resources": <resources>, "detail": <detail>}'
    DEFAULT_EVENT_INPUT_PATHS_MAP = {
        "account": "$.account",
        "detail": "$.detail",
        "detail-type": "$.detail-type",
        "source": "$.source",
        "id": "$.id",
        "region": "$.region",
        "resources": "$.resources",
        "time": "$.time",
    }

    @staticmethod
    def _stringify(result: Any) -> str:  # type: ignore[misc]
        if isinstance(result, dict):
            result = json.dumps(result)
        elif isinstance(result, list):
            result = json.dumps([EventTemplateParser._stringify(x) for x in result])
        elif isinstance(result, (int, float)):
            result = str(result)
        return result

    @staticmethod
    def parse(  # type: ignore[misc]
        input_template: str, input_paths_map: dict[str, Any], event: EventMessageType
    ) -> dict[str, Any]:
        from jsonpath_ng.ext import parse

        template_to_use = (
            input_template or EventTemplateParser.DEFAULT_EVENT_INPUT_TEMPLATE
        )
        input_paths_map = (
            input_paths_map or EventTemplateParser.DEFAULT_EVENT_INPUT_PATHS_MAP
        )
        values: dict[str, Any] = {}
        for input_path in input_paths_map:
            input_expr = parse(input_paths_map[input_path])  # type: ignore[no-untyped-call]
            matches = input_expr.find(event)
            values[input_path] = matches[0].value if matches else None
        values["aws.events.event.json"] = event
        values["aws.events.event.ingestion-time"] = event["ingestion-time"]

        def text_of(value: Any) -> str:
            if value is None:
                return ""
            return (
                value
                if isinstance(value, str)
                else EventTemplateParser._stringify(value)
            )

        template_is_json = template_to_use.lstrip().startswith(("{", "["))
        out: list[str] = []
        in_string = False
        i = 0
        while i < len(template_to_use):
            char = template_to_use[i]
            if char == "<":
                end = template_to_use.find(">", i)
                name = template_to_use[i + 1 : end] if end != -1 else ""
                if name in values:
                    value = values[name]
                    if not template_is_json:
                        out.append(text_of(value))
                    elif in_string:
                        # Inside a JSON string the value is spliced in as text; a missing value is empty.
                        out.append(json.dumps(text_of(value))[1:-1])
                    else:
                        # A bare placeholder takes the value as JSON; a missing value is null.
                        out.append(json.dumps(value))
                    i = end + 1
                    continue
            elif char == "\\" and in_string:
                out.append(template_to_use[i : i + 2])
                i += 2
                continue
            elif char == '"':
                in_string = not in_string
            out.append(char)
            i += 1
        template_to_use = "".join(out)

        return json.loads(template_to_use)
