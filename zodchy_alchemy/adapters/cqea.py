import collections.abc

import sqlalchemy
import zodchy

from ..contracts import Clause


class QueryAdapter:
    def __init__(
        self,
        names_map: collections.abc.Mapping[str, sqlalchemy.Column | str] | None = None,
        default_table: sqlalchemy.Table | None = None
    ):
        self._names_map = names_map
        self._default_table = default_table

    def __call__(self, query: zodchy.codex.cqea.Query) -> collections.abc.Iterable[Clause]:
        for name, value in query:
            yield Clause(self._names_map[name], value)

    def _build_column(self, field_name: str) -> sqlalchemy.Column:
        if column := self._names_map.get(field_name):
            if isinstance(column, sqlalchemy.Column):
                return column
            elif self._default_table is not None:
                return getattr(self._default_table.c, column)
            else:
                raise ValueError(f'Column {field_name} not found')
        else:
            if self._default_table is not None:
                return getattr(self._default_table.c, field_name)
            else:
                raise ValueError(f'Column {field_name} not found')


