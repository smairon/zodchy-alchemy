import collections.abc
import typing

import sqlalchemy
import zodchy

from ..contracts import Clause

TypeAliasType = typing.cast(type[typing.Any] | None, getattr(typing, "TypeAliasType", None))


class QueryAdapter:
    def __init__(
        self,
        names_map: (
            collections.abc.Mapping[str, sqlalchemy.Column | str | collections.abc.Callable[[typing.Any], typing.Any]]
            | None
        ) = None,
        default_table: sqlalchemy.Table | None = None,
    ):
        self._names_map = names_map
        self._default_table = default_table

    def __call__(
        self, query: zodchy.codex.cqea.Query
    ) -> collections.abc.Iterable[Clause | zodchy.codex.operator.SliceBit]:
        for name, value in typing.cast(collections.abc.Iterable[tuple[str, typing.Any]], query):
            normalized_value = self._normalize_value(value)
            if normalized_value is zodchy.codex.types.Empty:
                continue
            if isinstance(normalized_value, zodchy.codex.operator.SliceBit):
                yield normalized_value
            else:
                if (column := self._build_column(name)) is not None:
                    yield Clause(column, typing.cast(zodchy.codex.operator.ClauseBit, normalized_value))

    def _build_column(self, field_name: str) -> sqlalchemy.Column | None:
        column = self._names_map.get(field_name) if self._names_map else None
        if column is not None:
            if isinstance(column, sqlalchemy.Column):
                return column
            elif column is zodchy.codex.types.Skip:
                return None
            elif isinstance(column, str):
                if self._default_table is not None:
                    return typing.cast(sqlalchemy.Column, getattr(self._default_table.c, column))
                raise ValueError(f"Column {field_name} not found")
            else:
                raise ValueError(f"Column {field_name} not found")
        else:
            if self._default_table is not None:
                return typing.cast(sqlalchemy.Column, getattr(self._default_table.c, field_name))
            else:
                raise ValueError(f"Column {field_name} not found")

    def _normalize_value(self, value: typing.Any) -> typing.Any:
        if TypeAliasType is not None and isinstance(value, TypeAliasType):
            alias_value = getattr(value, "__value__", None)
            if alias_value is not None:
                return alias_value
        return value
