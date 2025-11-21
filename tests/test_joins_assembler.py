import pytest

from zodchy.codex import operator

from zodchy_alchemy import JoinsAssembler
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_query):
    return JoinsAssembler(base_query)


def test_single_join(assembler):
    q = str(assembler(contracts.ClauseExpression(contracts.Clause(schema.tag.c.name, operator.EQ("common"))))).strip()
    assert q.count("JOIN") == 1
    assert "LEFT OUTER JOIN tags ON tags.id = firmware.tag_id" in q


def test_join_through(assembler):
    q = str(
        assembler(
            contracts.ClauseExpression(
                contracts.Clause(schema.hardware.c.revision, operator.EQ("01"), schema.hardware_firmware)
            )
        )
    ).strip()
    assert q.count("JOIN") == 2
    assert "LEFT OUTER JOIN hardware_firmware ON firmware.id = hardware_firmware.firmware_id" in q
    assert "LEFT OUTER JOIN hardware ON hardware.id = hardware_firmware.hardware_id" in q


def test_join_through_condition(assembler):
    q = str(
        assembler(
            contracts.ClauseExpression(
                contracts.Clause(
                    schema.hardware.c.revision,
                    operator.EQ("01"),
                    schema.hardware_firmware.c.firmware_id == schema.firmware.c.id,
                )
            )
        )
    ).strip()
    assert q.count("JOIN") == 2
    assert "LEFT OUTER JOIN hardware_firmware ON hardware_firmware.firmware_id = firmware.id" in q
    assert "LEFT OUTER JOIN hardware ON hardware.id = hardware_firmware.hardware_id" in q
