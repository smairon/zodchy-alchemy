import typing
import uuid
import warnings
from functools import singledispatch

from sqlalchemy import Row


def to_dict(data: Row) -> dict[str, typing.Any]:
    result = {}
    for k, v in data._mapping.items():
        result[k] = field_serializer(v)
    return result


@singledispatch
def field_serializer(value):
    return value


try:
    import asyncpg.pgproto.pgproto


    @field_serializer.register
    def _(value: asyncpg.pgproto.pgproto.UUID) -> uuid.UUID:
        return uuid.UUID(bytes=value.bytes)
except Exception as e:
    warnings.warn(f'asyncpg not installed: {e}')
