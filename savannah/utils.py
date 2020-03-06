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
