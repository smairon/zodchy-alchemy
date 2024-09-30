import pytest
import zodchy

from zodchy_alchemy import OrdersAssembler
from zodchy_alchemy import contracts

from . import schema


@pytest.fixture
def assembler(base_query):
    return OrdersAssembler(base_query)


def test_single_desc(assembler):
    q = str(assembler(
        contracts.Clause(
            schema.firmware.c.id,
            zodchy.operators.DESC()
        )
    )).strip()
    assert 'ORDER BY firmware.id DESC' in q

def test_single_asc(assembler):
    q = str(assembler(
        contracts.Clause(
            schema.firmware.c.id,
            zodchy.operators.ASC()
        )
    )).strip()
    assert 'ORDER BY firmware.id ASC' in q

def test_double_desc(assembler):
    q = str(assembler(
        contracts.Clause(
            schema.firmware.c.id,
            zodchy.operators.DESC()
        ),
        contracts.Clause(
            schema.firmware.c.created_at,
            zodchy.operators.DESC()
        )
    )).strip()
    assert 'ORDER BY firmware.id DESC, firmware.created_at DESC' in q

def test_double_asc(assembler):
    q = str(assembler(
        contracts.Clause(
            schema.firmware.c.id,
            zodchy.operators.ASC()
        ),
        contracts.Clause(
            schema.firmware.c.created_at,
            zodchy.operators.ASC()
        )
    )).strip()
    assert 'ORDER BY firmware.id ASC, firmware.created_at ASC' in q

def test_double_bidirect(assembler):
    q = str(assembler(
        contracts.Clause(
            schema.firmware.c.id,
            zodchy.operators.DESC()
        ),
        contracts.Clause(
            schema.firmware.c.created_at,
            zodchy.operators.ASC()
        )
    )).strip()
    assert 'ORDER BY firmware.id DESC, firmware.created_at ASC' in q