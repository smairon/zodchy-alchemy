import pytest
import sqlalchemy

from . import schema


@pytest.fixture(scope='session')
def base_query():
    return sqlalchemy.select(
        schema.firmware.c.id,
        schema.firmware.c.uri,
        schema.firmware.c.version,
    )


@pytest.fixture(scope='session')
def base_table():
    return schema.hardware
