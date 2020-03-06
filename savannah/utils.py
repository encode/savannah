from databases import Database, DatabaseURL
import os


async def database_exists(url):
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ('postgres', 'postgresql'):
        url = url.replace(database='postgres')
    elif url.dialect == 'mysql':
        url = url.replace(database='')

    if url.dialect in ('postgres', 'postgresql'):
        statement = "SELECT 1 FROM pg_database WHERE datname='%s'" % database_name

    elif url.dialect == 'mysql':
        statement = ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                     "WHERE SCHEMA_NAME = '%s'" % database_name)

    elif url.dialect == 'sqlite':
        if database_name == ':memory:' or not database_name:
            return True

        if not os.path.isfile(database_name) or os.path.getsize(database_name) < 100:
            return False

        with open(database_name, 'rb') as file:
            header = file.read(100)

        return header[:16] == b'SQLite format 3\x00'

    async with Database(url) as database:
        return bool(await database.fetch_val(statement))


async def has_table(database, table_name):
    if database.url.dialect in ('postgres', 'postgresql'):
        statement = f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{table_name}');"
        result = await database.fetch_one(statement)
        return result['exists']
    elif database.url.dialect == 'mysql':
        statement = f"SHOW TABLES LIKE '{table_name}';"
        result = await database.fetch_all(statement)
        return bool(result)
    else:
        statement = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        result = await database.fetch_all(statement)
        return bool(result)


def get_dialect(url):
    url = DatabaseURL(url)

    if url.dialect in ('postgres', 'postgresql'):
        from sqlalchemy.dialects.postgresql import pypostgresql
        return pypostgresql.dialect(paramstyle="pyformat")
    elif url.dialect == 'mysql':
        from sqlalchemy.dialects.mysql import pymysql
        return pymysql.dialect(paramstyle="pyformat")
    elif url.dialect == 'mysql':
        from sqlalchemy.dialects.sqlite import pysqlite
        pysqlite.dialect(paramstyle="qmark")
