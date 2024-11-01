import typing

import zodchy
import sqlalchemy
from sqlalchemy import BinaryExpression, Table

from ..contracts import Clause, ClauseExpression


class JoinsAssembler:
    def __init__(self, query: sqlalchemy.Select):
        self._query = query
        self._joins_digest: set[str] = set()
        self._tables: set[str] = set(t.name for t in query.columns_clause_froms)
        self._foreign_keys: dict[str, sqlalchemy.ForeignKey] = {}
        self._prepare()

    def __call__(self, expression: ClauseExpression) -> sqlalchemy.Select:
        for clause in expression or ():
            if isinstance(clause, Clause) and isinstance(clause.operation, zodchy.operators.FilterBit):
                self._build_link(clause)
        return self._query

    def _build_link(self, clause: Clause):
        column = clause.column
        table_name = column.table.name
        if table_name in self._tables:
            return

        for condition in clause.conditions:
            if isinstance(condition, Table):
                for fk in condition.foreign_keys:
                    if fk.column.table.name in self._tables:
                        condition = fk.column == fk.parent
            if isinstance(condition, BinaryExpression):
                if (joined_table := self._register_join(condition)) is not None:
                    self._query = self._query.join(
                        joined_table,
                        condition,
                        isouter=True
                    )
                    self._register_foreign_keys(joined_table)



        for fk_table_name, fk in self._foreign_keys.items():
            if fk_table_name == table_name:
                if self._register_join(
                    typing.cast(
                        sqlalchemy.BinaryExpression,
                        fk.column == fk.parent
                    )
                ) is not None:
                    self._query = self._query.join(column.table, fk.column == fk.parent, isouter=True)
                break

    def _prepare(self):
        for join in self._query._setup_joins or ():  # todo: should think how to fix this hack
            self._register_join(join[1])

        for entity in self._query.froms:
            if isinstance(entity, sqlalchemy.Table):
                self._register_foreign_keys(entity)

    def _register_join(self, expression: sqlalchemy.BinaryExpression) -> sqlalchemy.Table | None:
        result = None
        if (digest := self._assemble_join_digest(expression)) not in self._joins_digest:
            self._joins_digest.add(digest)
            if expression.left.table.name not in self._tables:
                self._tables.add(expression.left.table.name)
                result = expression.left.table
            if expression.right.table.name not in self._tables:
                self._tables.add(expression.right.table.name)
                result = expression.right.table
        return result

    def _register_foreign_keys(self, table: sqlalchemy.Table):
        for foreign_key in table.foreign_keys:
            table_name = foreign_key.column.table.name
            if table_name not in self._foreign_keys:
                self._foreign_keys[table_name] = foreign_key

    @staticmethod
    def _assemble_join_digest(expression: sqlalchemy.BinaryExpression) -> str | None:
        names = []
        for element in expression.left, expression.right:
            if isinstance(element, sqlalchemy.Column):
                names.append(element.table.name)
            else:
                return
        return "::".join(sorted(names))
