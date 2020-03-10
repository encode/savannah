from databases import Database, DatabaseURL
import os
from .config import load_config, Config
from .generators.empty import EmptyGenerator
from .generators.initial import InitialGenerator
from .tables import (
    db_create_migrations_table_if_not_exists,
    db_load_migrations_table,
    db_apply_migration,
    db_unapply_migration,
)
from .loader import load_migrations
import sqlalchemy


async def init(dir="migrations"):
    migration_init_path = os.path.join(dir, "__init__.py")
    migration_0001_path = os.path.join(dir, "0001_initial.py")

    config = Config(metadata="example:metadata")
    from_state = config.get_initial_state()
    to_state = config.get_current_state()
    generator = InitialGenerator(from_state=from_state, to_state=to_state)

    os.mkdir(dir)
    config.write_config_to_disk(path=migration_init_path)
    print(f"Created config in {migration_init_path!r}")
    generator.write_migration_to_disk(path=migration_0001_path)
    print(f"Created migration '0001_initial'")


async def make_migration(url: str, dir: str = "migrations"):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    import os

    print("list dir", os.listdir(dir))
    migrations = load_migrations(applied, dir_name="migrations")
    dependencies = [migration.name for migration in migrations if migration.is_leaf]
    final_name = dependencies[-1]
    index = int(final_name.split("_")[0]) + 1

    migration_000x_path = os.path.join(dir, f"{index:04}_auto.py")
    generator = EmptyGenerator()

    generator.write_migration_to_disk(
        path=migration_000x_path, dependencies=dependencies
    )
    print(f"Created migration '{index:04}_auto'")


async def list_migrations(url: str):
    async with Database(url) as database:
        applied = await db_load_migrations_table(database)

    return load_migrations(applied, dir_name="migrations")


async def migrate(url: str, target: str = None):
    async with Database(url) as database:
        await db_create_migrations_table_if_not_exists(database)
        applied_migrations = await db_load_migrations_table(database)

        #  Load the migrations from disk.
        migrations = load_migrations(applied_migrations, dir_name="migrations")

        # Determine which migration we are targeting.
        if target is None:
            index = len(migrations) + 1
        elif target.lower() == "zero":
            index = 0
        else:
            candidates = [
                (index, migration)
                for index, migration in enumerate(migrations, 1)
                if migration.name.startswith(target)
            ]
            if len(candidates) > 1:
                raise Exception(
                    f"Target {target!r} matched more than one migration name."
                )
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
                    if not (migration.is_applied):
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


async def create_database(url: str, encoding: str = "utf8") -> None:
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ("postgres", "postgresql"):
        url = url.replace(database="postgres")
    elif url.dialect == "mysql":
        url = url.replace(database="")

    if url.dialect in ("postgres", "postgresql"):
        statement = "CREATE DATABASE {0} ENCODING '{1}' TEMPLATE template1".format(
            database_name, encoding,
        )
        statements = [statement]

    elif url.dialect == "mysql":
        statement = "CREATE DATABASE {0} CHARACTER SET = '{1}'".format(
            database_name, encoding
        )
        statements = [statement]

    elif url.dialect == "sqlite":
        if database_name and database_name != ":memory:":
            statements = ["CREATE TABLE DB(id int);", "DROP TABLE DB;"]
        else:
            statements = []

    async with Database(url) as database:
        for statement in statements:
            await database.execute(statement)


async def drop_database(url: str) -> None:
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ("postgres", "postgresql"):
        url = url.replace(database="postgres")
    elif url.dialect == "mysql":
        url = url.replace(database="")

    if url.dialect == "sqlite":
        if database_name and database_name != ":memory:":
            os.remove(database_name)
        return

    else:
        statement = "DROP DATABASE {0}".format(database_name)

    async with Database(url) as database:
        await database.execute(statement)


async def database_exists(url: str) -> bool:
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ("postgres", "postgresql"):
        url = url.replace(database="postgres")
    elif url.dialect == "mysql":
        url = url.replace(database="")

    if url.dialect in ("postgres", "postgresql"):
        statement = "SELECT 1 FROM pg_database WHERE datname='%s'" % database_name

    elif url.dialect == "mysql":
        statement = (
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = '%s'" % database_name
        )

    elif url.dialect == "sqlite":
        if database_name == ":memory:" or not database_name:
            return True

        if not os.path.isfile(database_name) or os.path.getsize(database_name) < 100:
            return False

        with open(database_name, "rb") as file:
            header = file.read(100)

        return header[:16] == b"SQLite format 3\x00"

    async with Database(url) as database:
        return bool(await database.fetch_one(statement))
