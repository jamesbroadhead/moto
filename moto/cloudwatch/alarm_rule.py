import json
import re
from collections.abc import Callable
from typing import Any


class AlarmRule:
    def __init__(self, expression: str):
        self.tokens = re.findall(r'"(?:\\.|[^"\\])*"|[()]|[^\s()]+', expression)
        self.position = 0
        self.references: set[str] = set()
        self.tree = self._or()
        if self.position != len(self.tokens):
            raise ValueError("Unexpected token in AlarmRule")

    def _take(self, token: str) -> bool:
        if self.position < len(self.tokens) and self.tokens[self.position] == token:
            self.position += 1
            return True
        return False

    def _or(self) -> Any:
        tree = self._and()
        while self._take("OR"):
            tree = ("OR", tree, self._and())
        return tree

    def _and(self) -> Any:
        tree = self._atom()
        while self._take("AND"):
            tree = ("AND", tree, self._atom())
        return tree

    def _atom(self) -> Any:
        if self._take("NOT"):
            return ("NOT", self._atom())
        if self._take("("):
            tree = self._or()
            if not self._take(")"):
                raise ValueError("Missing closing parenthesis in AlarmRule")
            return tree
        for constant in ("TRUE", "FALSE"):
            if self._take(constant):
                return (constant,)
        for state in ("ALARM", "OK", "INSUFFICIENT_DATA"):
            if self._take(state):
                if not self._take("(") or self.position == len(self.tokens):
                    raise ValueError("Missing alarm name in AlarmRule")
                name = self.tokens[self.position]
                self.position += 1
                if name.startswith('"'):
                    name = json.loads(name)
                if not name or name in ("(", ")") or not self._take(")"):
                    raise ValueError("Invalid alarm reference in AlarmRule")
                self.references.add(name)
                return ("STATE", state, name)
        raise ValueError("Expected a state expression in AlarmRule")

    def evaluate(self, get_state: Callable[[str], str | None]) -> bool | None:
        def visit(tree: Any) -> bool | None:
            operator = tree[0]
            if operator in ("TRUE", "FALSE"):
                return operator == "TRUE"
            if operator == "STATE":
                state = get_state(tree[2])
                return None if state is None else state == tree[1]
            left = visit(tree[1])
            if operator == "NOT":
                return None if left is None else not left
            right = visit(tree[2])
            if left is None or right is None:
                return None
            return left and right if operator == "AND" else left or right

        return visit(self.tree)
