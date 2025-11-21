import uuid
from datetime import datetime

import pytest
from zodchy.codex import operator

from zodchy_alchemy import FilterAssembler
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_query):
    return FilterAssembler()


def test_single(assembler: FilterAssembler) -> None:
    q = str(assembler(contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4())))).strip()
    assert "firmware.id = :id_1" in q


def test_and(assembler: FilterAssembler) -> None:
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            & contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
        )
    ).strip()
    assert "firmware.id = :id_1 AND firmware.created_at >= :created_at_1" in q


def test_or(assembler: FilterAssembler) -> None:
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            | contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
        )
    ).strip()
    assert "firmware.id = :id_1 OR firmware.created_at >= :created_at_1" in q


def test_and_big(assembler: FilterAssembler) -> None:
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            & contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
            & contracts.Clause(schema.firmware.c.version, operator.EQ("1.0"))
        )
    ).strip()
    assert "firmware.id = :id_1 AND firmware.created_at >= :created_at_1 AND firmware.version = :version_1" in q


def test_complex(assembler: FilterAssembler) -> None:
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            & (
                contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
                & contracts.Clause(schema.firmware.c.version, operator.EQ("1.0"))
                | contracts.Clause(schema.firmware.c.created_at, operator.LE(datetime.utcnow()))
                & contracts.Clause(schema.firmware.c.version, operator.EQ("2.0"))
            )
        )
    ).strip()
    assert (
        "(firmware.created_at <= :created_at_1 AND firmware.version = :version_1 OR firmware.created_at >= :created_at_2 AND firmware.version = :version_2) AND firmware.id = :id_1"
        in q
    )


def test_range_clause_single_boundary(assembler: FilterAssembler) -> None:
    boundary = datetime(2024, 1, 1)
    q = str(
        assembler(contracts.Clause(schema.firmware.c.created_at, operator.RANGE(operator.GE(boundary), None)))
    ).strip()
    assert "firmware.created_at >= :created_at_1" in q


def test_range_clause_between(assembler: FilterAssembler) -> None:
    q = str(
        assembler(
            contracts.Clause(
                schema.firmware.c.created_at,
                operator.RANGE(operator.GE(datetime(2024, 1, 1)), operator.LE(datetime(2024, 2, 1))),
            )
        )
    ).strip()
    assert "firmware.created_at >= :created_at_1 AND firmware.created_at <= :created_at_2" in q


def test_range_clause_returns_none_for_empty_bounds(assembler: FilterAssembler) -> None:
    clause = contracts.Clause(schema.firmware.c.created_at, operator.RANGE(None, None))
    assert assembler._range_clause(clause) is None


def test_set_clause_inversion(assembler: FilterAssembler) -> None:
    positive = str(assembler(contracts.Clause(schema.firmware.c.version, operator.SET("1.0", "2.0")))).strip()
    negative = str(
        assembler(contracts.Clause(schema.firmware.c.version, operator.NOT(operator.SET("1.0", "2.0"))))
    ).strip()
    assert "firmware.version IN" in positive
    assert "firmware.version NOT IN" in negative


def test_like_case_sensitivity(assembler: FilterAssembler) -> None:
    case_sensitive = str(
        assembler(contracts.Clause(schema.firmware.c.uri, operator.LIKE("fw", case_sensitive=True)))
    ).strip()
    case_insensitive = str(assembler(contracts.Clause(schema.firmware.c.uri, operator.LIKE("fw")))).strip()
    assert "firmware.uri LIKE" in case_sensitive
    assert "lower(firmware.uri) LIKE" in case_insensitive


def test_not_like_clause(assembler: FilterAssembler) -> None:
    q = str(assembler(contracts.Clause(schema.firmware.c.uri, operator.NOT(operator.LIKE("fw"))))).strip()
    assert "lower(firmware.uri) NOT LIKE" in q


def test_not_is_clause(assembler: FilterAssembler) -> None:
    q = str(assembler(contracts.Clause(schema.firmware.c.payload, operator.NOT(operator.IS(None))))).strip()
    assert "firmware.payload IS NOT NULL" in q
