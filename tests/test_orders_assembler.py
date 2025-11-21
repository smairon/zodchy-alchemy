import pytest

from zodchy.codex import operator

from zodchy_alchemy import OrdersAssembler
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_query):
    return OrdersAssembler(base_query)


def test_single_desc(assembler):
    q = str(assembler(contracts.Clause(schema.firmware.c.id, operator.DESC()))).strip()
    assert "ORDER BY firmware.id DESC" in q


def test_single_asc(assembler):
    q = str(assembler(contracts.Clause(schema.firmware.c.id, operator.ASC()))).strip()
    assert "ORDER BY firmware.id ASC" in q


def test_double_desc(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.DESC()),
            contracts.Clause(schema.firmware.c.created_at, operator.DESC()),
        )
    ).strip()
    assert "ORDER BY firmware.id DESC, firmware.created_at DESC" in q


def test_double_asc(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.ASC()),
            contracts.Clause(schema.firmware.c.created_at, operator.ASC()),
        )
    ).strip()
    assert "ORDER BY firmware.id ASC, firmware.created_at ASC" in q


def test_double_bidirect(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.DESC()),
            contracts.Clause(schema.firmware.c.created_at, operator.ASC()),
        )
    ).strip()
    assert "ORDER BY firmware.id DESC, firmware.created_at ASC" in q
