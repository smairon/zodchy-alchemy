import pytest

from zodchy.codex import operator

from zodchy_alchemy import MutationAssembler
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_table):
    return MutationAssembler(base_table)


def test_insertion_single_row(assembler):
    q = str(assembler(dict(id=1, name="test", revision="1.0"))).strip()
    assert "INSERT INTO hardware (id, name, revision) VALUES (:id_m0, :name_m0, :revision_m0)" in q


def test_insertion_multiple_rows(assembler):
    q = str(
        assembler(
            dict(id=1, name="test", revision="1.0"),
            dict(id=2, name="test", revision="1.0"),
        )
    ).strip()
    assert (
        "INSERT INTO hardware (id, name, revision) VALUES (:id_m0, :name_m0, :revision_m0), (:id_m1, :name_m1, :revision_m1)"
        in q
    )


def test_updating(assembler):
    q = str(
        assembler(dict(name="test", revision="1.0"), contracts.Clause(schema.hardware.c.id, operator.EQ(1)))
    ).strip()
    assert "UPDATE hardware SET name=:name, revision=:revision WHERE hardware.id = :id_1" in q


def test_cannot_update_multiple_rows(assembler):
    with pytest.raises(ValueError) as excinfo:
        assembler(
            dict(name="test", revision="1.0"),
            dict(name="test2", revision="2.0"),
            contracts.Clause(schema.hardware.c.id, operator.EQ(1)),
        )
    assert str(excinfo.value) == "Expected only one data item, got 2"


def test_deleting(assembler):
    q = str(assembler(contracts.Clause(schema.hardware.c.id, operator.EQ(1))))
    assert q == "DELETE FROM hardware WHERE hardware.id = :id_1"
