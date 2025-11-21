import typing

import pytest
from zodchy.codex import cqea, operator, types

from zodchy_alchemy import contracts
from zodchy_alchemy.adapters.cqea import QueryAdapter

from . import schema


TypeAliasType = getattr(typing, "TypeAliasType", None)
VersionFilter = None
if TypeAliasType is not None:
    VersionFilter = TypeAliasType("VersionFilter", operator.EQ("1.0"))


class DummyQuery(cqea.Query):
    def __init__(self, items: list[tuple[str, typing.Any]]):
        self._items = items

    def __iter__(self):
        yield from self._items


@pytest.mark.skipif(VersionFilter is None, reason="TypeAliasType is not available")
def test_query_adapter_maps_columns_and_slices():
    adapter = QueryAdapter(
        names_map=typing.cast(
            typing.Mapping[str, typing.Any],
            {
                "hardware_name": schema.hardware.c.name,
                "version_alias": "version",
                "skip_me": types.Skip,
            },
        ),
        default_table=schema.firmware,
    )
    alias_value = VersionFilter
    query = DummyQuery(
        [
            ("hardware_name", operator.LIKE("edge")),
            ("version_alias", alias_value),
            ("skip_me", operator.EQ("ignored")),
            ("limit", operator.Limit(5)),
        ]
    )

    result = list(adapter(query))
    filters = [item for item in result if isinstance(item, contracts.Clause)]
    slices = [item for item in result if isinstance(item, operator.SliceBit)]

    assert len(filters) == 2
    assert {f.column for f in filters} == {schema.hardware.c.name, schema.firmware.c.version}
    assert len(slices) == 1 and isinstance(slices[0], operator.Limit)


def test_query_adapter_skips_empty_and_uses_default_table():
    adapter = QueryAdapter(default_table=schema.firmware)
    query = DummyQuery(
        [
            ("version", operator.EQ("2.0")),
            ("ignored", types.Empty),
        ]
    )

    clauses = [item for item in adapter(query) if isinstance(item, contracts.Clause)]
    assert len(clauses) == 1
    assert clauses[0].column is schema.firmware.c.version


def test_query_adapter_raises_for_unknown_column():
    adapter = QueryAdapter()
    query = DummyQuery([("missing", operator.EQ("value"))])

    with pytest.raises(ValueError, match="Column missing not found"):
        list(adapter(query))
