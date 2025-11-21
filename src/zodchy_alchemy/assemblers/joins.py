import typing

import sqlalchemy
import zodchy
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.selectable import FromClause

from ..contracts import Clause, ClauseExpression


class JoinsAssembler:
    def __init__(self, query: sqlalchemy.Select):
        self._query = query
        self._joins_digest: set[str] = set()
        self._tables: set[str] = set()
        for from_clause in query.columns_clause_froms:
            if (name := self._get_table_name(from_clause)) is not None:
                self._tables.add(name)
        self._foreign_keys: dict[str, sqlalchemy.ForeignKey] = {}
        self._prepare()

    def __call__(self, expression: ClauseExpression) -> sqlalchemy.Select:
        for clause in expression or ():
            if isinstance(clause, Clause) and isinstance(clause.operation, zodchy.codex.operator.FilterBit):
                self._build_link(clause)
        return self._query

    def _build_link(self, clause: Clause) -> None:
        column = clause.column
        table = getattr(column, "table", None)
        table_name = self._get_table_name(table)
        if table_name is None or table_name in self._tables:
            return

        for condition in clause.conditions:
            join_condition = self._normalize_condition(condition)
            if join_condition is None:
                continue
            if (joined_table := self._register_join(join_condition)) is not None:
                self._query = self._query.join(joined_table, join_condition, isouter=True)
                if isinstance(joined_table, sqlalchemy.Table):
                    self._register_foreign_keys(joined_table)

        for fk_table_name, fk in self._foreign_keys.items():
            if fk_table_name == table_name:
                fk_condition = typing.cast(BinaryExpression, fk.column == fk.parent)
                if (joined := self._register_join(fk_condition)) is not None:
                    self._query = self._query.join(column.table, fk_condition, isouter=True)
                    if isinstance(joined, sqlalchemy.Table):
                        self._register_foreign_keys(joined)
                break

    def _prepare(self) -> None:
        setup_joins = getattr(self._query, "_setup_joins", None)
        if setup_joins:
            for join in setup_joins:
                join_condition = join[1]
                if isinstance(join_condition, BinaryExpression):
                    self._register_join(join_condition)

        for entity in self._query.froms:
            if isinstance(entity, sqlalchemy.Table):
                self._register_foreign_keys(entity)

    def _register_join(self, expression: BinaryExpression) -> FromClause | None:
        digest = self._assemble_join_digest(expression)
        if digest is None or digest in self._joins_digest:
            return None

        self._joins_digest.add(digest)
        registered_table: FromClause | None = None
        for column in (expression.left, expression.right):
            table = getattr(column, "table", None)
            name = self._get_table_name(table)
            if name is not None and name not in self._tables:
                self._tables.add(name)
                registered_table = typing.cast(FromClause, table)
        return registered_table

    def _register_foreign_keys(self, table: sqlalchemy.Table) -> None:
        for foreign_key in table.foreign_keys:
            table_name = self._get_table_name(foreign_key.column.table)
            if table_name is not None and table_name not in self._foreign_keys:
                self._foreign_keys[table_name] = foreign_key

    @staticmethod
    def _assemble_join_digest(expression: BinaryExpression) -> str | None:
        names = []
        for element in (expression.left, expression.right):
            if (name := JoinsAssembler._get_table_name(getattr(element, "table", None))) is not None:
                names.append(name)
            else:
                return None
        return "::".join(sorted(names))

    def _normalize_condition(self, condition: BinaryExpression | Table) -> BinaryExpression | None:
        if isinstance(condition, Table):
            return self._condition_from_table(condition)
        return condition

    def _condition_from_table(self, table: Table) -> BinaryExpression | None:
        for fk in table.foreign_keys:
            referenced_table = self._get_table_name(getattr(fk.column, "table", None))
            if referenced_table in self._tables:
                return typing.cast(BinaryExpression, fk.column == fk.parent)
        return None

    @staticmethod
    def _get_table_name(table: typing.Any) -> str | None:
        name = getattr(table, "name", None)
        return name if isinstance(name, str) else None
