from databases import Database, DatabaseURL
import os
from .config import load_config
from .tables import db_create_migrations_table_if_not_exists, db_load_migrations_table, db_apply_migration, db_unapply_migration
from .loader import load_migrations
import sqlalchemy


async def init(dir="migrations"):
    os.mkdir(dir)
    with open(os.path.join(dir, "__init__.py"), "w") as fout:
        fout.write(f"""\
import savannah


config = savannah.Config(metadata="example:metadata")
""")
    with open(os.path.join(dir, "0001_initial.py"), "w") as fout:
        fout.write("""\
import savannah


class Migration(savannah.Migration):
    dependencies = []
    operations = []
""")


async def make_migration(url: str):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    migrations = load_migrations(applied, dir_name="migrations")
    leaf_node_names = [migration.name for migration in migrations if migration.is_leaf]
    final_name = leaf_node_names[-1]
    index = int(final_name.split("_")[0])
    index += 1
    with open(f"migrations/{index:04}_auto.py", "w") as fout:
        fout.write(f"""\
import savannah


class Migration(savannah.Migration):
    dependencies = {leaf_node_names!r}
    operations = []
""")
    print(f"Created migration '{index:04}_auto'")


async def list_migrations(url: str):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    return load_migrations(applied, dir_name="migrations")


async def migrate(url: str, target: str=None):
    async with Database(url) as database:
        await db_create_migrations_table_if_not_exists(database)
        applied_migrations = await db_load_migrations_table(database)

        # Load the migrations from disk.
        migrations = load_migrations(applied_migrations, dir_name="migrations")

        # Determine which migration we are targeting.
        if target is None:
            index = len(migrations) + 1
        elif target.lower() == 'zero':
            index = 0
        else:
            candidates = [
                (index, migration) for index, migration in enumerate(migrations, 1) if migration.name.startswith(target)
            ]
            if len(candidates) > 1:
                raise Exception(f"Target {target!r} matched more than one migration name.")
            elif len(candidates) == 0:
                raise Exception(f"Target {target!r} does not match any migrations.")
            index, migration = candidates[0]

        has_downgrades = any(migration.is_applied for migration in migrations[index:])
        has_upgrades = any(not migration.is_applied for migration in migrations[:index])
        if not has_downgrades and not has_upgrades:
            print("No migrations required.")
            return

        # Apply or unapply migrations.
        async with database.transaction():
            # Unapply migrations.
            if has_downgrades:
                for migration in reversed(migrations[index:]):
                    if not(migration.is_applied):
                        continue
                    await migration.downgrade()
                    await db_unapply_migration(database, migration.name)

            # Apply migrations.
            if has_upgrades:
                for migration in migrations[:index]:
                    if migration.is_applied:
                        continue
                    await migration.upgrade()
                    await db_apply_migration(database, migration.name)


async def create_database(url: str, encoding: str='utf8') -> None:
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


async def drop_database(url: str) -> None:
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


async def database_exists(url: str) -> bool:
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
        return bool(await database.fetch_one(statement))
