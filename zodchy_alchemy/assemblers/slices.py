import sqlalchemy
import zodchy


class SlicesAssembler:
    def __init__(self, query: sqlalchemy.Select):
        self._query = query

    def __call__(self, *operations: zodchy.operators.SliceBit) -> sqlalchemy.Select:
        for operation in operations:
            self._assemble(operation)
        return self._query

    def _assemble(self, operation: zodchy.operators.SliceBit):
        if isinstance(operation, zodchy.operators.Limit):
            self._query = self._query.limit(operation.value)
        elif isinstance(operation, zodchy.operators.Offset):
            self._query = self._query.offset(operation.value)
