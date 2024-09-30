import pytest
import zodchy

from zodchy_alchemy import SlicesAssembler


@pytest.fixture
def assembler(base_query):
    return SlicesAssembler(base_query)


def test_limit(assembler):
    q = str(assembler(
            zodchy.operators.Limit(100)
    )).strip()
    assert 'LIMIT :param_1' in q

def test_offset(assembler):
    q = str(assembler(
            zodchy.operators.Offset(100)
    )).strip()
    assert 'OFFSET :param_1' in q

def test_pagination(assembler):
    q = str(assembler(
            zodchy.operators.Offset(100),
            zodchy.operators.Limit(10),
    )).strip()
    assert 'LIMIT :param_1 OFFSET :param_2' in q