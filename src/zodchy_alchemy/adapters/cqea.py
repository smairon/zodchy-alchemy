import collections.abc
import typing

import sqlalchemy  # type: ignore[import-not-found]
import zodchy

from ..contracts import Clause


class QueryAdapter:
    def __init__(
        self,
        names_map: collections.abc.Mapping[str, sqlalchemy.Column | str] | None = None,
        default_table: sqlalchemy.Table | None = None,
    ):
        self._names_map = names_map
        self._default_table = default_table

    def __call__(
        self, query: zodchy.codex.cqea.Query
    ) -> collections.abc.Iterable[Clause | zodchy.codex.operator.SliceBit]:
        for name, value in query:
            if type(value) is typing.TypeAliasType:
                value = value.__value__
            if value is zodchy.codex.types.Empty:
                continue
            if isinstance(value, zodchy.codex.operator.SliceBit):
                yield value
            else:
                if (column := self._build_column(name)) is not None:
                    yield Clause(column, value)

    def _build_column(self, field_name: str) -> sqlalchemy.Column | None:
        column = self._names_map.get(field_name) if self._names_map else None
        if column is not None:
            if isinstance(column, sqlalchemy.Column):
                return column
            elif column == zodchy.codex.types.Skip:
                return None
            elif self._default_table is not None:
                return getattr(self._default_table.c, column)
            else:
                raise ValueError(f"Column {field_name} not found")
        else:
            if self._default_table is not None:
                return getattr(self._default_table.c, field_name)
            else:
                raise ValueError(f"Column {field_name} not found")
