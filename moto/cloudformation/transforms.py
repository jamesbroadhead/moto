"""Transforms that CloudFormation applies to a template before it creates any resource.

CloudFormation processes the snippet-level `Fn::Transform` macros first, from the most deeply nested outward, and
then the template-level `Transform` section in the order it lists them:
https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-macros.html#template-macros-evaluation
"""

import copy
import json
import logging
from functools import cache
from typing import Any

import yaml

from moto.s3.utils import bucket_and_name_from_url
from moto.utilities.utils import get_partition

from .exceptions import ValidationError
from .utils import yaml_tag_constructor

logger = logging.getLogger(__name__)

INCLUDE = "AWS::Include"
LANGUAGE_EXTENSIONS = "AWS::LanguageExtensions"
SERVERLESS = "AWS::Serverless-2016-10-31"

_NO_VALUE = object()
_UNKNOWN = object()


def process_transforms(
    template: dict[str, Any],
    parameters: dict[str, Any],
    account_id: str,
    region_name: str,
) -> dict[str, Any]:
    if not isinstance(template, dict) or not _has_transforms(template):
        return template
    template = _include(copy.deepcopy(template), account_id, region_name)
    declared = template.get("Transform", [])
    for name in declared if isinstance(declared, list) else [declared]:
        if name == LANGUAGE_EXTENSIONS:
            template = LanguageExtensions(
                template, parameters, account_id, region_name
            ).process()
        elif name == SERVERLESS:
            template = _serverless(template, parameters, account_id, region_name)
        else:
            logger.warning("Moto does not process the transform %s", name)
    return template


def _has_transforms(o: Any) -> bool:
    if isinstance(o, dict):
        return (
            "Transform" in o
            or "Fn::Transform" in o
            or any(map(_has_transforms, o.values()))
        )
    if isinstance(o, list):
        return any(map(_has_transforms, o))
    return False


def _include(o: Any, account_id: str, region_name: str) -> Any:
    """Replaces each `Fn::Transform` of AWS::Include with the snippet at its S3 location."""
    if isinstance(o, list):
        return [_include(x, account_id, region_name) for x in o]
    if not isinstance(o, dict):
        return o
    o = {k: _include(v, account_id, region_name) for k, v in o.items()}
    transform = o.get("Fn::Transform")
    if not (isinstance(transform, dict) and transform.get("Name") == INCLUDE):
        return o
    location = (transform.get("Parameters") or {}).get("Location")
    bucket_name, key_name = bucket_and_name_from_url(location or "")
    if not bucket_name or not key_name:
        raise ValidationError(
            message=f"Transform {INCLUDE} failed with: Location must be an S3 URI, got {location}"
        )
    from moto.s3.models import s3_backends

    backend = s3_backends[account_id][get_partition(region_name)]
    try:
        body = backend.get_object(bucket_name, key_name).value.decode("utf-8")  # type: ignore[union-attr]
    except Exception:
        raise ValidationError(
            message=f"Transform {INCLUDE} failed with: S3 object {location} does not exist"
        )
    yaml.add_multi_constructor("", yaml_tag_constructor)
    snippet = yaml.load(body, Loader=yaml.Loader)
    # Keys next to Fn::Transform are merged with the snippet
    rest = {k: v for k, v in o.items() if k != "Fn::Transform"}
    return {**snippet, **rest} if rest and isinstance(snippet, dict) else snippet


def _cfn_string(v: Any) -> str:
    """The string CloudFormation compares for a template value, e.g. `true` for a YAML boolean."""
    return str(v).lower() if isinstance(v, bool) else str(v)


def _parameter_values(
    template: dict[str, Any],
    parameters: dict[str, Any],
    account_id: str,
    region_name: str,
) -> dict[str, Any]:
    values = {
        "AWS::AccountId": account_id,
        "AWS::Region": region_name,
        "AWS::Partition": get_partition(region_name),
        "AWS::URLSuffix": "amazonaws.com",
    }
    for name, declared in (template.get("Parameters") or {}).items():
        if name in parameters:
            values[name] = parameters[name]
        elif "Default" in declared:
            values[name] = declared["Default"]
    return values


class LanguageExtensions:
    """The AWS::LanguageExtensions transform.

    It resolves Fn::FindInMap with a DefaultValue, Fn::Length, intrinsic functions in DeletionPolicy and
    UpdateReplacePolicy, and Fn::If whose condition follows from the parameters, in Resources and Outputs.
    What depends on a resource stays for CloudFormation to resolve at create time.
    https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/transform-aws-languageextensions.html
    """

    def __init__(
        self,
        template: dict[str, Any],
        parameters: dict[str, Any],
        account_id: str,
        region_name: str,
    ):
        self.template = template
        self.values = _parameter_values(template, parameters, account_id, region_name)
        self.conditions = template.get("Conditions") or {}
        self.mappings = template.get("Mappings") or {}
        self.known_conditions: dict[str, Any] = {}

    def process(self) -> dict[str, Any]:
        t = self.template
        for section in ("Resources", "Outputs"):
            if section in t:
                t[section] = {
                    k: v
                    for k, v in ((k, self.resolve(v)) for k, v in t[section].items())
                    if v is not _NO_VALUE
                }
        transforms = (
            t["Transform"] if isinstance(t["Transform"], list) else [t["Transform"]]
        )
        transforms = [x for x in transforms if x != LANGUAGE_EXTENSIONS]
        if transforms:
            t["Transform"] = transforms
        else:
            del t["Transform"]
        return t

    def scalar(self, v: Any) -> Any:
        """The string value of `v`, or _UNKNOWN when it depends on more than the parameters."""
        if isinstance(v, dict) and set(v) == {"Ref"}:
            v = self.values.get(v["Ref"], _UNKNOWN)
        else:
            v = self.resolve(v)
        return _cfn_string(v) if isinstance(v, (str, bool, int, float)) else _UNKNOWN

    def condition(self, name: str, seen: tuple[str, ...] = ()) -> Any:
        if name not in self.known_conditions:
            if name in seen or name not in self.conditions:
                return _UNKNOWN
            self.known_conditions[name] = self.evaluate(
                self.conditions[name], (*seen, name)
            )
        return self.known_conditions[name]

    def evaluate(self, c: Any, seen: tuple[str, ...] = ()) -> Any:
        if isinstance(c, bool):
            return c
        if not isinstance(c, dict) or len(c) != 1:
            return _UNKNOWN
        op, args = next(iter(c.items()))
        if op == "Condition":
            return self.condition(args, seen)
        if op == "Fn::Equals":
            a, b = self.scalar(args[0]), self.scalar(args[1])
            return _UNKNOWN if _UNKNOWN in (a, b) else a == b
        values = [self.evaluate(x, seen) for x in args]
        if op == "Fn::Not":
            return _UNKNOWN if values[0] is _UNKNOWN else not values[0]
        if op == "Fn::And":
            return (
                False if False in values else (_UNKNOWN if _UNKNOWN in values else True)
            )
        if op == "Fn::Or":
            return (
                True if True in values else (_UNKNOWN if _UNKNOWN in values else False)
            )
        return _UNKNOWN

    def resolve(self, o: Any) -> Any:
        if isinstance(o, list):
            return [x for x in map(self.resolve, o) if x is not _NO_VALUE]
        if not isinstance(o, dict):
            return o
        if o == {"Ref": "AWS::NoValue"}:
            return _NO_VALUE
        if len(o) == 1 and "Fn::If" in o:
            name, if_true, if_false = o["Fn::If"]
            known = self.condition(name)
            if known is not _UNKNOWN:
                return self.resolve(if_true if known else if_false)
            # CloudFormation decides at create time
            branches = [self.resolve(x) for x in (if_true, if_false)]
            return {
                "Fn::If": [
                    name,
                    *(
                        {"Ref": "AWS::NoValue"} if b is _NO_VALUE else b
                        for b in branches
                    ),
                ]
            }
        if len(o) == 1 and "Fn::FindInMap" in o:
            return self.find_in_map(o["Fn::FindInMap"])
        if len(o) == 1 and "Fn::Length" in o:
            return self.length(o["Fn::Length"])
        out = {k: self.resolve(v) for k, v in o.items()}
        return {k: v for k, v in out.items() if v is not _NO_VALUE}

    def find_in_map(self, args: list[Any]) -> Any:
        args = [self.resolve(x) for x in args]
        options = args[3] if len(args) > 3 else {}
        has_default = isinstance(options, dict) and "DefaultValue" in options
        keys = [self.scalar(x) for x in args[:3]]
        if _UNKNOWN in keys:
            # A key that depends on a resource: CloudFormation looks it up at create time
            return {"Fn::FindInMap": args}
        level: Any = self.mappings
        for key in keys:
            matches = (
                [v for k, v in level.items() if _cfn_string(k) == key]
                if isinstance(level, dict)
                else []
            )
            if not matches or matches[0] is None:
                if has_default:
                    return self.resolve(options["DefaultValue"])
                return {"Fn::FindInMap": args[:3]}
            level = matches[0]
        return level

    def length(self, arg: Any) -> Any:
        if isinstance(arg, dict) and set(arg) == {"Ref"} and arg["Ref"] in self.values:
            value = self.values[arg["Ref"]]
            return len(value if isinstance(value, list) else str(value).split(","))
        arg = self.resolve(arg)
        if isinstance(arg, list):
            return len(arg)
        return {"Fn::Length": arg}


def _serverless(
    template: dict[str, Any],
    parameters: dict[str, Any],
    account_id: str,
    region_name: str,
) -> dict[str, Any]:
    """The AWS::Serverless transform, with the AWS SAM translator that CloudFormation runs."""
    try:
        import boto3
        from samtranslator.model.exceptions import InvalidDocumentException
        from samtranslator.parser.parser import Parser
        from samtranslator.translator.translator import Translator
    except ImportError:
        logger.warning(
            "Install aws-sam-translator to create the resources of the %s transform",
            SERVERLESS,
        )
        return template

    partition = get_partition(region_name)

    @cache
    def managed_policies() -> dict[str, str]:
        from moto.iam.aws_managed_policies import aws_managed_policies_data

        return {
            name: f"arn:{partition}:iam::aws:policy{policy['Path']}{name}"
            for name, policy in json.loads(aws_managed_policies_data).items()
        }

    values = _parameter_values(template, parameters, account_id, region_name)
    translator = Translator(
        None, Parser(), boto_session=boto3.session.Session(region_name=region_name)
    )
    try:
        return translator.translate(
            template,
            parameter_values=values,
            get_managed_policy_map=managed_policies,
        )
    except InvalidDocumentException as e:
        raise ValidationError(
            message=f"Transform {SERVERLESS} failed with: {e.message}"
        )
