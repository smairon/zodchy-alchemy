import operator
import typing
from collections import deque
from collections.abc import Callable

import sqlalchemy  # type: ignore[import-not-found]
import zodchy

from ..contracts import Clause, ClauseExpression, Logic

OperatorType: typing.TypeAlias = Callable


class FilterAssembler:
    def __call__(self, clause: Clause | ClauseExpression) -> sqlalchemy.ColumnElement:
        return self._assemble(clause)

    def _assemble(self, clause: Clause | ClauseExpression) -> sqlalchemy.ColumnElement:
        expression = ClauseExpression(clause) if isinstance(clause, Clause) else clause
        _op_map = {Logic.AND: sqlalchemy.and_, Logic.OR: sqlalchemy.or_}
        operations_stack: deque = deque(
            [
                e
                for e in expression or ()
                if e is Logic.AND or e is Logic.OR or zodchy.codex.operator.FilterBit in e.operation.__class__.__mro__
            ]
        )
        buffer: typing.Any = deque()
        while operations_stack:
            element = operations_stack.popleft()
            if element is Logic.AND or element is Logic.OR:
                operands = []
                i = 0
                while buffer and i < 2:
                    operands.append(buffer.popleft())
                    i += 1
                if operands:
                    buffer.appendleft(_op_map[element](*operands) if len(operands) > 1 else operands[0])
            else:
                buffer.appendleft(self._assemble_element(element))
        return buffer.popleft()

    def _assemble_element(
        self,
        element: sqlalchemy.ColumnElement | Clause,
    ) -> sqlalchemy.ColumnElement | Clause:
        if isinstance(element, Clause):
            if (operation := self._operations[type(element.operation)](element)) is not None:
                element = operation
        return element

    @property
    def _operations(self) -> dict[type, typing.Callable[[Clause], sqlalchemy.ColumnElement | None]]:
        return {
            zodchy.codex.operator.EQ: self._simple_clause(operator.eq),
            zodchy.codex.operator.NE: self._simple_clause(operator.ne),
            zodchy.codex.operator.LE: self._simple_clause(operator.le),
            zodchy.codex.operator.LT: self._simple_clause(operator.lt),
            zodchy.codex.operator.GE: self._simple_clause(operator.ge),
            zodchy.codex.operator.GT: self._simple_clause(operator.gt),
            zodchy.codex.operator.IS: lambda v: v.column.is_(v.operation.value),
            zodchy.codex.operator.LIKE: self._like_clause,
            zodchy.codex.operator.NOT: self._not_clause,
            zodchy.codex.operator.SET: self._set_clause,
            zodchy.codex.operator.RANGE: self._range_clause,
        }

    def _not_clause(self, clause: Clause) -> typing.Any:
        if isinstance(clause.operation, zodchy.codex.operator.IS):
            return clause.column.isnot(clause.operation.value)
        elif isinstance(clause.operation, zodchy.codex.operator.EQ):
            return operator.ne(clause.column, clause.operation.value)
        elif isinstance(clause.operation, zodchy.codex.operator.LIKE):
            return self._like_clause(clause.clone(), inversion=True)
        elif isinstance(clause.operation, zodchy.codex.operator.SET):
            return self._set_clause(clause.clone(), inversion=True)
        else:
            return (sqlalchemy.not_(self._assemble(clause)),)  # type: ignore[return-value, unused-ignore]

    @staticmethod
    def _simple_clause(op: OperatorType) -> typing.Callable[[Clause], sqlalchemy.ColumnElement]:
        return lambda v: op(v.column, v.operation.value)

    def _logic_clause(self, op: OperatorType) -> typing.Callable[[typing.Iterable[Clause]], sqlalchemy.ColumnElement]:
        return lambda v: op(self._assemble(u) for u in v)

    @staticmethod
    def _like_clause(clause: Clause, inversion: bool = False) -> sqlalchemy.ColumnElement:
        column = clause.column
        operation = clause.operation
        value = f"%{operation.value}%"
        if hasattr(operation, "case_sensitive") and operation.case_sensitive:  # type: ignore[attr-defined, unused-ignore]
            return column.notlike(value) if inversion else column.like(value)
        else:
            return column.notilike(value) if inversion else column.ilike(value)

    @staticmethod
    def _set_clause(clause: Clause, inversion: bool = False) -> sqlalchemy.ColumnElement:
        column = clause.column
        value = list(clause.operation.value)
        if inversion:
            return column.notin_(value)
        else:
            return column.in_(value)

    def _range_clause(self, clause: Clause) -> sqlalchemy.ColumnElement | None:
        params = [
            Clause(clause.column, condition, *clause.conditions)
            for condition in clause.operation.value
            if condition is not None
        ]
        if len(params) > 1:
            return self._logic_clause(sqlalchemy.and_)(params)
        elif len(params) == 1:
            return self._assemble(params[0])
        return None
