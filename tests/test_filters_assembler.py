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


def test_single(assembler):
    q = str(assembler(contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4())))).strip()
    assert "firmware.id = :id_1" in q


def test_and(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            & contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
        )
    ).strip()
    assert "firmware.id = :id_1 AND firmware.created_at >= :created_at_1" in q


def test_or(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            | contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
        )
    ).strip()
    assert "firmware.id = :id_1 OR firmware.created_at >= :created_at_1" in q


def test_and_big(assembler):
    q = str(
        assembler(
            contracts.Clause(schema.firmware.c.id, operator.EQ(uuid.uuid4()))
            & contracts.Clause(schema.firmware.c.created_at, operator.GE(datetime.utcnow()))
            & contracts.Clause(schema.firmware.c.version, operator.EQ("1.0"))
        )
    ).strip()
    assert "firmware.id = :id_1 AND firmware.created_at >= :created_at_1 AND firmware.version = :version_1" in q


def test_complex(assembler):
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
