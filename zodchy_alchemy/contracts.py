import typing
import collections.abc

import sqlalchemy
import zodchy

AND = typing.NewType('AND', object)
OR = typing.NewType('OR', object)


class Clause:
    def __init__(
        self,
        column: sqlalchemy.Column,
        operation: zodchy.codex.query.ClauseBit,
        *conditions: sqlalchemy.BinaryExpression | sqlalchemy.Table
    ):
        self.column = column
        self.operation = operation
        self.conditions = conditions

    def __and__(self, other: typing.Self | 'ClauseExpression'):
        if self.operation is None:
            return other
        self_obj = Clause(self.column, self.operation, *self.conditions)
        if isinstance(other, Clause):
            if other.operation is None:
                return self
            else:
                return ClauseExpression(other, self_obj, AND)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(self_obj, *other, AND)

    def __or__(self, other: typing.Self | 'ClauseExpression'):
        if self.operation is None:
            return other
        self_obj = Clause(self.column, self.operation, *self.conditions)
        if isinstance(other, Clause):
            if other.operation is None:
                return self
            else:
                return ClauseExpression(other, self_obj, OR)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(self_obj, *other, OR)

    def clone(self) -> typing.Self:
        return Clause(self.column, self.operation, *self.conditions)

    def dump(self) -> dict:
        return dict(
            column=self.column,
            operation=self.operation,
            conditions=self.conditions
        )


class ClauseExpression:
    def __init__(self, *clauses: Clause | AND | OR):
        self._clauses = list(self._assure_filter_clause(clauses))

    def __iter__(self):
        yield from self._clauses

    def __and__(self, other: typing.Self | Clause):
        if isinstance(other, Clause):
            if other.operation is None:
                return self
            else:
                return ClauseExpression(other, *self._clauses, AND)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(*self._clauses, *other, AND)

    def __or__(self, other: typing.Self | Clause):
        if isinstance(other, Clause):
            if other.operation is None:
                return self
            else:
                return ClauseExpression(other, *self._clauses, OR)
        elif isinstance(other, ClauseExpression):
            return ClauseExpression(*self._clauses, *other, OR)

    @staticmethod
    def _assure_filter_clause(
        clauses: collections.abc.Iterable[Clause]
    ) -> collections.abc.Generator[None, None, Clause]:
        for c in clauses:
            if c is AND or c is OR or zodchy.codex.query.FilterBit in c.operation.__class__.__mro__:
                yield c
            else:
                raise ValueError(f'Expected a filter clause, got {c.operation}')
