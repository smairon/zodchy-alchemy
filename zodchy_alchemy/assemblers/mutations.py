import collections.abc

import sqlalchemy
import zodchy

from .. import contracts
from .filters import FilterAssembler

DataRow = collections.abc.Mapping


class MutationAssembler:
    def __init__(self, table: sqlalchemy.Table):
        self._table = table
        self._filter_assembler = FilterAssembler()

    def __call__(self, *elements: DataRow | contracts.Clause | contracts.ClauseExpression):
        data, filters = self._separate(elements)
        if data and filters:
            return self._update(data, filters)
        elif len(data) > 0 and len(filters) == 0:
            return self._insert(data)
        elif len(data) == 0 and len(filters) > 0:
            return self._delete(filters)

    def _update(self, data: list[DataRow], filters: list[contracts.Clause | contracts.ClauseExpression]):
        if len(data) > 1:
            raise ValueError(f'Expected only one data item, got {len(data)}')
        if len(filters) == 0:
            raise ValueError(f'Expected at least one filter, got {len(filters)}')
        return sqlalchemy.update(self._table).values(**data[0]).where(self._filter_assembler(*filters))

    def _insert(self, data: list[DataRow]):
        return sqlalchemy.insert(self._table).values(data)

    def _delete(self, filters: list[contracts.Clause | contracts.ClauseExpression]):
        return sqlalchemy.delete(self._table).where(self._filter_assembler(*filters))

    @staticmethod
    def _separate(
        elements: collections.abc.Iterable[DataRow | contracts.Clause | contracts.ClauseExpression]
    ):
        data = []
        filters = []

        for element in elements:
            if isinstance(element, contracts.Clause):
                if zodchy.codex.query.FilterBit in element.operation.__class__.__mro__:
                    filters.append(element)
                else:
                    raise ValueError(f'Expected a filter, got {element.operation}')
            elif isinstance(element, contracts.ClauseExpression):
                filters.append(element)
            elif isinstance(element, DataRow):
                data.append(element)

        return data, filters
