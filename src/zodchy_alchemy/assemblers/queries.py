import collections.abc

import sqlalchemy
import zodchy

from ..contracts import Clause, ClauseExpression
from .filters import FilterAssembler
from .joins import JoinsAssembler
from .orders import OrdersAssembler
from .slices import SlicesAssembler


class QueryAssembler:
    def __init__(self, query: sqlalchemy.Select):
        self._query = query

    def __call__(self, *clauses: Clause | ClauseExpression | zodchy.codex.operator.SliceBit) -> sqlalchemy.Select:
        filters, orders, slices = self._separate(clauses)
        filter_expression = self._build_expression(filters)
        if (filter_expression := self._build_expression(filters)) is not None:
            self._query = JoinsAssembler(self._query)(filter_expression)
        if filter_expression and (filters := FilterAssembler()(filter_expression)) is not None:
            self._query = self._query.where(filters)
        self._query = OrdersAssembler(self._query)(*orders)
        self._query = SlicesAssembler(self._query)(*slices)
        return self._query

    @staticmethod
    def _separate(
        clauses: collections.abc.Iterable[Clause | ClauseExpression | zodchy.codex.operator.SliceBit],
    ) -> tuple:
        filters: list[Clause | ClauseExpression] = []
        orders = []
        slices = []
        for clause in clauses:
            if isinstance(clause, Clause):
                if zodchy.codex.operator.FilterBit in clause.operation.__class__.__mro__:
                    filters.append(clause)
                elif zodchy.codex.operator.OrderBit in clause.operation.__class__.__mro__:
                    orders.append(clause)
                else:
                    raise ValueError(f"Expected a filter, order or slice clause, got {clause.operation}")
            elif isinstance(clause, ClauseExpression):
                filters.append(clause)
            elif isinstance(clause, zodchy.codex.operator.SliceBit):
                slices.append(clause)
        return filters, orders, slices

    @staticmethod
    def _build_expression(clauses: collections.abc.Iterable[Clause | ClauseExpression]) -> ClauseExpression | None:
        expression = None
        for clause in clauses:
            if isinstance(clause, Clause):
                if expression is None:
                    expression = ClauseExpression(clause)
                else:
                    if clause is not None:
                        expression = expression & clause
            elif isinstance(clause, ClauseExpression):
                if expression is None:
                    expression = clause
                else:
                    if clause is not None:
                        expression = expression & clause
        return expression
