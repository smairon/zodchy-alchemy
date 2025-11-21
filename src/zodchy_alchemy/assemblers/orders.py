import sqlalchemy
import zodchy

from ..contracts import Clause


class OrdersAssembler:
    def __init__(self, query: sqlalchemy.Select):
        self._query = query

    def __call__(self, *clauses: Clause) -> sqlalchemy.Select:
        for clause in clauses:
            if isinstance(clause, Clause) and isinstance(clause.operation, zodchy.codex.operator.OrderBit):
                self._assemble(clause)
        return self._query

    def _assemble(self, clause: Clause) -> None:
        if isinstance(clause.operation, zodchy.codex.operator.ASC):
            self._query = self._query.order_by(clause.column.asc())
        elif isinstance(clause.operation, zodchy.codex.operator.DESC):
            self._query = self._query.order_by(clause.column.desc())
