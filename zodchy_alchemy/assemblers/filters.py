import typing
import operator
from collections import deque

import sqlalchemy
import zodchy

from ..contracts import Clause, ClauseExpression, AND, OR


class FilterAssembler:
    def __call__(self, clause: Clause | ClauseExpression):
        return self._assemble(clause)

    def _assemble(self, clause: Clause | ClauseExpression) -> sqlalchemy.ColumnElement:
        expression = ClauseExpression(clause) if isinstance(clause, Clause) else clause
        _op_map = {
            AND: sqlalchemy.and_,
            OR: sqlalchemy.or_
        }
        operations_stack: deque = deque(
            list(
                e for e in expression or ()
                if e is AND or e is OR or zodchy.operators.FilterBit in e.operation.__class__.__mro__
            )
        )
        if operations_stack:
            buffer = deque()
            while operations_stack:
                element = operations_stack.popleft()
                if element is AND or element is OR:
                    operands = []
                    i = 0
                    while buffer and i < 2:
                        operands.append(buffer.popleft())
                        i += 1
                    if operands:
                        buffer.appendleft(
                            _op_map[element](*operands) if len(operands) > 1 else operands[0]
                        )
                else:
                    buffer.appendleft(self._assemble_element(element))
            return buffer.popleft()

    def _assemble_element(
        self,
        element: sqlalchemy.ColumnElement | Clause,
    ):
        if isinstance(element, Clause):
            element = self._operations[type(element.operation)](element)
        return element

    @property
    def _operations(self):
        return {
            zodchy.operators.EQ: self._simple_clause(operator.eq),
            zodchy.operators.NE: self._simple_clause(operator.ne),
            zodchy.operators.LE: self._simple_clause(operator.le),
            zodchy.operators.LT: self._simple_clause(operator.lt),
            zodchy.operators.GE: self._simple_clause(operator.ge),
            zodchy.operators.GT: self._simple_clause(operator.gt),
            zodchy.operators.IS: lambda v: v.column.is_(v.operation.value),
            zodchy.operators.LIKE: self._like_clause,
            zodchy.operators.NOT: self._not_clause,
            zodchy.operators.SET: self._set_clause,
            zodchy.operators.RANGE: self._range_clause,
        }

    def _not_clause(self, clause: Clause) -> typing.Any:
        if isinstance(clause.operation, zodchy.operators.IS):
            return clause.column.isnot(clause.operation.value)
        elif isinstance(clause.operation, zodchy.operators.EQ):
            return operator.ne(clause.column, clause.operation.value)
        elif isinstance(clause.operation, zodchy.operators.LIKE):
            return self._like_clause(
                clause.clone(),
                inversion=True
            )
        elif isinstance(clause.operation, zodchy.operators.SET):
            return self._set_clause(
                clause.clone(),
                inversion=True
            )
        else:
            return sqlalchemy.not_(self._assemble(clause)),

    @staticmethod
    def _simple_clause(op):
        return lambda v: op(v.column, v.operation.value)

    def _logic_clause(self, op):
        return lambda v: op(self._assemble(u) for u in v)

    @staticmethod
    def _like_clause(clause: Clause, inversion: bool = False):
        column = clause.column
        operation = clause.operation
        value = f'%{operation.value}%'
        if hasattr(operation, 'case_sensitive') and operation.case_sensitive:
            return column.notlike(value) if inversion else column.like(value)
        else:
            return column.notilike(value) if inversion else column.ilike(value)

    @staticmethod
    def _set_clause(clause: Clause, inversion: bool = False):
        column = clause.column
        value = list(clause.operation.value)
        if inversion:
            return column.notin_(value)
        else:
            return column.in_(value)

    def _range_clause(self, clause: Clause):
        params = [
            Clause(clause.column, condition, *clause.conditions)
            for condition in clause.operation.value
            if condition is not None
        ]
        if len(params) > 1:
            return self._logic_clause(sqlalchemy.and_)(params)
        elif len(params) == 1:
            return params[0]
