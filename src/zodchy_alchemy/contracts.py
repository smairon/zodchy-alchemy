import collections.abc
import enum
import typing

import sqlalchemy
import sqlalchemy.ext.asyncio
import zodchy


class EngineContract(sqlalchemy.ext.asyncio.AsyncEngine):
    pass


class ConnectionContract(sqlalchemy.ext.asyncio.AsyncConnection):
    pass


class ReadConnectionContract(ConnectionContract):
    pass


class WriteConnectionContract(ConnectionContract):
    pass


class WriteTransactionContract(sqlalchemy.ext.asyncio.AsyncTransaction):
    pass


class ReadTransactionContract(sqlalchemy.ext.asyncio.AsyncTransaction):
    pass


class Logic(str, enum.Enum):
    AND = "AND"
    OR = "OR"


class Clause:
    def __init__(
        self,
        column: sqlalchemy.Column,
        operation: zodchy.codex.operator.ClauseBit,
        *conditions: sqlalchemy.BinaryExpression | sqlalchemy.Table,
    ):
        self.column = column
        self.operation = operation
        self.conditions = conditions

    def __and__(self, other: typing.Self | "ClauseExpression") -> typing.Self | "ClauseExpression":
        self_obj = Clause(self.column, self.operation, *self.conditions)
        if isinstance(other, Clause):
            return ClauseExpression(other, self_obj, Logic.AND)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(self_obj, *other, Logic.AND)

    def __or__(self, other: typing.Self | "ClauseExpression") -> typing.Self | "ClauseExpression":
        self_obj = Clause(self.column, self.operation, *self.conditions)
        if isinstance(other, Clause):
            return ClauseExpression(other, self_obj, Logic.OR)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(self_obj, *other, Logic.OR)

    def clone(self) -> typing.Self:
        return type(self)(self.column, self.operation, *self.conditions)

    def dump(self) -> dict:
        return {"column": self.column, "operation": self.operation, "conditions": self.conditions}


class ClauseExpression:
    def __init__(self, *clauses: Clause | Logic):
        self._clauses = list(self._assure_filter_clause(clauses))

    def __iter__(self) -> collections.abc.Generator[Clause | Logic, None, None]:
        yield from self._clauses

    def __and__(self, other: typing.Self | Clause) -> typing.Self:
        if isinstance(other, Clause):
            return type(self)(other, *self._clauses, Logic.AND)
        elif isinstance(other, ClauseExpression):
            return type(self)(*self._clauses, *other, Logic.AND)

    def __or__(self, other: typing.Self | Clause) -> typing.Self:
        if isinstance(other, Clause):
            return type(self)(other, *self._clauses, Logic.OR)
        elif isinstance(other, ClauseExpression):
            return type(self)(*self._clauses, *other, Logic.OR)

    @staticmethod
    def _assure_filter_clause(
        clauses: collections.abc.Iterable[Clause | Logic],
    ) -> collections.abc.Generator[Clause | Logic, None, None]:
        for c in clauses:
            if c is Logic.AND or c is Logic.OR or zodchy.codex.operator.FilterBit in c.operation.__class__.__mro__:
                yield c
            else:
                raise ValueError(f"Expected a filter clause, got {c.operation}")
