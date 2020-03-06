from databases import Database, DatabaseURL
import os
from .tables import db_create_migrations_table_if_not_exists, db_load_migrations_table, db_apply_migration, db_unapply_migration
from .loader import load_migrations
from .utils import database_exists
import sqlalchemy


async def make_migration(url: str):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    loader_info = load_migrations(applied, dir_name="migrations")
    final_name = loader_info.leaf_nodes[-1]
    index = int(final_name.split("_")[0])
    index += 1
    with open(f"migrations/{index:04}_auto.py", "w") as fout:
        fout.write(f"""\
import savannah


class Migration(savannah.Migration):
    dependencies = {loader_info.leaf_nodes!r}
    operations = []
""")
    print(f"Created migration '{index:04}_auto'")


async def list_migrations(url: str):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    loader_info = load_migrations(applied, dir_name="migrations")
    return loader_info


async def migrate(url: str, index=None):
    async with Database(url) as database:
        await db_create_migrations_table_if_not_exists(database)
        applied_migrations = await db_load_migrations_table(database)

        # Load the migrations from disk.
        loader_info = load_migrations(applied_migrations, dir_name="migrations")
        migrations = list(loader_info.migrations.values())
        if index is None:
            index = len(migrations) + 1

        async with database.transaction():
            # Unapply migrations.
            for migration in reversed(migrations[index:]):
                if not(migration.is_applied):
                    continue
                await migration.downgrade()
                await db_unapply_migration(database, migration.name)

            # Apply migrations.
            for migration in migrations[:index]:
                if migration.is_applied:
                    continue
                await migration.upgrade()
                await db_apply_migration(database, migration.name)


async def create_database(url, encoding='utf8'):
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ('postgres', 'postgresql'):
        url = url.replace(database='postgres')
    elif url.dialect == 'mysql':
        url = url.replace(database='')

    if url.dialect in ('postgres', 'postgresql'):
        statement = "CREATE DATABASE {0} ENCODING '{1}' TEMPLATE template1".format(
            database_name,
            encoding,
        )
        statements = [statement]

    elif url.dialect == 'mysql':
        statement = "CREATE DATABASE {0} CHARACTER SET = '{1}'".format(
            database_name,
            encoding
        )
        statements = [statement]

    elif url.dialect == 'sqlite':
        if database_name and database_name != ':memory:':
            statements = [
                "CREATE TABLE DB(id int);",
                "DROP TABLE DB;"
            ]
        else:
            statements = []

    async with Database(url) as database:
        for statement in statements:
            await database.execute(statement)


async def drop_database(url):
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ('postgres', 'postgresql'):
        url = url.replace(database='postgres')
    elif url.dialect == 'mysql':
        url = url.replace(database='')

    if url.dialect == 'sqlite':
        if database_name and database_name != ':memory:':
            os.remove(database_name)
        return

    else:
        statement = 'DROP DATABASE {0}'.format(database_name)

    async with Database(url) as database:
        await database.execute(statement)
