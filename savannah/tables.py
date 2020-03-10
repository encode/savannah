"""
This module holds the database schema for the migrations table, as well
as functions encapsulating all the database operations that we perform
against the table.
"""
from typing import Set
import sqlalchemy
from databases import Database, DatabaseURL


metadata = sqlalchemy.MetaData()

migrations = sqlalchemy.Table(
    "migrations",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=100), index=True),
)


async def db_load_migrations_table(database: Database) -> Set[str]:
    """
    Load all the migration records from the database.
    """
    has_migrations_table = await _has_table(database, "migrations")
    if not has_migrations_table:
        return set()

    query = sqlalchemy.sql.select([migrations.c.name])
    records = await database.fetch_all(query)
    return set([record["name"] for record in records])


async def db_create_migrations_table_if_not_exists(database: Database) -> None:
    """
    Create the migrations table if needed.
    """
    has_migrations_table = await _has_table(database, "migrations")
    if has_migrations_table:
        return

    dialect = _get_dialect(str(database.url))
    ddl = sqlalchemy.schema.CreateTable(migrations)
    statement = ddl.compile(dialect=dialect).string
    await database.execute(statement)


async def db_apply_migration(database: Database, name: str) -> None:
    """
    Persist a migration record to the database.
    """
    query = migrations.insert()
    await database.execute(query, values={"name": name})


async def db_unapply_migration(database: Database, name: str) -> None:
    """
    Remove a migration record from the database.
    """
    query = migrations.delete().where(migrations.c.name == name)
    await database.execute(query)


async def _has_table(database, table_name):
    if database.url.dialect in ("postgres", "postgresql"):
        statement = (
            f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{table_name}');"
        )
        result = await database.fetch_one(statement)
        return result["exists"]
    elif database.url.dialect == "mysql":
        statement = f"SHOW TABLES LIKE '{table_name}';"
        result = await database.fetch_all(statement)
        return bool(result)
    else:
        statement = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        result = await database.fetch_all(statement)
        return bool(result)


def _get_dialect(url):
    url = DatabaseURL(url)

    if url.dialect in ("postgres", "postgresql"):
        from sqlalchemy.dialects.postgresql import pypostgresql

        return pypostgresql.dialect(paramstyle="pyformat")
    elif url.dialect == "mysql":
        from sqlalchemy.dialects.mysql import pymysql

        return pymysql.dialect(paramstyle="pyformat")
    elif url.dialect == "mysql":
        from sqlalchemy.dialects.sqlite import pysqlite

        pysqlite.dialect(paramstyle="qmark")
