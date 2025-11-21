import operator
import typing
from collections import deque
from collections.abc import Callable, Iterable

import sqlalchemy
import zodchy
from sqlalchemy.sql.elements import ColumnElement

from ..contracts import Clause, ClauseExpression, Logic

ClauseElement: typing.TypeAlias = ColumnElement[typing.Any]
OperatorType = Callable[..., typing.Any]


class FilterAssembler:
    def __call__(self, clause: Clause | ClauseExpression) -> ClauseElement:
        return self._assemble(clause)

    def _assemble(self, clause: Clause | ClauseExpression) -> ClauseElement:
        expression = ClauseExpression(clause) if isinstance(clause, Clause) else clause
        _op_map: dict[Logic, Callable[..., ClauseElement]] = {
            Logic.AND: typing.cast(Callable[..., ClauseElement], sqlalchemy.and_),
            Logic.OR: typing.cast(Callable[..., ClauseElement], sqlalchemy.or_),
        }
        operations_stack: deque[Clause | Logic] = deque(
            [
                element
                for element in expression or ()
                if element is Logic.AND
                or element is Logic.OR
                or zodchy.codex.operator.FilterBit in element.operation.__class__.__mro__
            ]
        )
        buffer: deque[ClauseElement] = deque()
        while operations_stack:
            element = operations_stack.popleft()
            if element is Logic.AND or element is Logic.OR:
                operands: list[ClauseElement] = []
                i = 0
                while buffer and i < 2:
                    operands.append(buffer.popleft())
                    i += 1
                if operands:
                    buffer.appendleft(_op_map[element](*operands) if len(operands) > 1 else operands[0])
            else:
                buffer.appendleft(self._assemble_element(element))

        if not buffer:
            raise ValueError("Failed to assemble filter expression")

        return buffer.popleft()

    def _assemble_element(self, element: ClauseElement | Clause) -> ClauseElement:
        if isinstance(element, Clause):
            operation_factory = self._operations.get(type(element.operation))
            if operation_factory is None:
                raise ValueError(f"Unexpected operation: {type(element.operation)!r}")
            result = operation_factory(element)
            if result is None:
                return typing.cast(ClauseElement, sqlalchemy.true())
            return result
        return element

    @property
    def _operations(self) -> dict[type, Callable[[Clause], ClauseElement | None]]:
        return {
            zodchy.codex.operator.EQ: self._simple_clause(operator.eq),
            zodchy.codex.operator.NE: self._simple_clause(operator.ne),
            zodchy.codex.operator.LE: self._simple_clause(operator.le),
            zodchy.codex.operator.LT: self._simple_clause(operator.lt),
            zodchy.codex.operator.GE: self._simple_clause(operator.ge),
            zodchy.codex.operator.GT: self._simple_clause(operator.gt),
            zodchy.codex.operator.IS: lambda v: typing.cast(ClauseElement, v.column.is_(v.operation.value)),
            zodchy.codex.operator.LIKE: self._like_clause,
            zodchy.codex.operator.NOT: self._not_clause,
            zodchy.codex.operator.SET: self._set_clause,
            zodchy.codex.operator.RANGE: self._range_clause,
        }

    def _not_clause(self, clause: Clause) -> ClauseElement:
        operation = clause.operation.value
        inner_clause = Clause(clause.column, operation, *clause.conditions)
        if isinstance(operation, zodchy.codex.operator.IS):
            return typing.cast(ClauseElement, clause.column.isnot(operation.value))
        if isinstance(operation, zodchy.codex.operator.EQ):
            return typing.cast(ClauseElement, operator.ne(clause.column, operation.value))
        if isinstance(operation, zodchy.codex.operator.LIKE):
            return self._like_clause(inner_clause, inversion=True)
        if isinstance(operation, zodchy.codex.operator.SET):
            return self._set_clause(inner_clause, inversion=True)
        return sqlalchemy.not_(self._assemble(inner_clause))

    @staticmethod
    def _simple_clause(op: OperatorType) -> Callable[[Clause], ClauseElement]:
        def _wrapper(value: Clause) -> ClauseElement:
            return typing.cast(ClauseElement, op(value.column, value.operation.value))

        return _wrapper

    def _logic_clause(self, op: OperatorType) -> Callable[[Iterable[Clause]], ClauseElement]:
        def _wrapper(clauses: Iterable[Clause]) -> ClauseElement:
            assembled = tuple(self._assemble(clause) for clause in clauses)
            return typing.cast(ClauseElement, op(*assembled))

        return _wrapper

    @staticmethod
    def _like_clause(clause: Clause, inversion: bool = False) -> ClauseElement:
        column = clause.column
        operation = clause.operation
        value = f"%{operation.value}%"
        if isinstance(operation, zodchy.codex.operator.LIKE) and operation.case_sensitive:
            return typing.cast(ClauseElement, column.notlike(value) if inversion else column.like(value))
        return typing.cast(ClauseElement, column.notilike(value) if inversion else column.ilike(value))

    @staticmethod
    def _set_clause(clause: Clause, inversion: bool = False) -> ClauseElement:
        column = clause.column
        value = list(clause.operation.value)
        if inversion:
            return typing.cast(ClauseElement, column.notin_(value))
        return typing.cast(ClauseElement, column.in_(value))

    def _range_clause(self, clause: Clause) -> ClauseElement | None:
        params = [
            Clause(clause.column, condition, *clause.conditions)
            for condition in clause.operation.value
            if condition is not None
        ]
        if len(params) > 1:
            return self._logic_clause(sqlalchemy.and_)(params)
        if len(params) == 1:
            return self._assemble(params[0])
        return None
