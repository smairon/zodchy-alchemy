import uuid

import pytest
from zodchy.codex import operator

import sqlalchemy  # type: ignore[import-not-found]
from zodchy_alchemy import QueryAssembler  # type: ignore[import-not-found]
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_query: sqlalchemy.Select) -> QueryAssembler:
    return QueryAssembler(base_query)


def test_trivial(assembler):
    q = str(assembler())
    assert q == "SELECT firmware.id, firmware.uri, firmware.version \nFROM firmware"


def test_filters(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.GE(uuid.uuid4())),
            contracts.Clause(schema.firmware.c.id, operator.DESC()),
        )
    ).strip()
    assert "WHERE firmware.id >= :id_1 ORDER BY firmware.id DESC" in q


def test_joins(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.hardware.c.revision, operator.EQ("01"), schema.hardware_firmware),
        )
    ).strip()
    assert "hardware_firmware ON firmware.id = hardware_firmware.firmware_id" in q
    assert "hardware ON hardware.id = hardware_firmware.hardware_id" in q
    assert "WHERE hardware.revision = :revision_1" in q


def test_complex(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.hardware.c.revision, operator.EQ("01"), schema.hardware_firmware),
            contracts.Clause(schema.hardware.c.name, operator.DESC()),
            operator.Limit(100),
            operator.Offset(10),
        )
    ).strip()
    assert "hardware_firmware ON firmware.id = hardware_firmware.firmware_id" in q
    assert "hardware ON hardware.id = hardware_firmware.hardware_id" in q
    assert "WHERE hardware.revision = :revision_1" in q
    assert "ORDER BY hardware.name DESC" in q
    assert "LIMIT :param_1" in q
    assert "OFFSET :param_2" in q


def test_invalid_clause_kind_raises(assembler):
    bad_clause = contracts.Clause(schema.firmware.c.id, operator.ClauseBit())
    with pytest.raises(ValueError, match="Expected a filter, order or slice clause"):
        assembler(bad_clause)
