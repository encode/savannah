from databases import Database
from .functions import get_dialect, has_table
from .loader import load_migrations
import sqlalchemy
import typing


metadata = sqlalchemy.MetaData()

migrations_table = sqlalchemy.Table(
    "migrations",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=100), index=True),
)


class Migration:
    dependencies = []
    operations = []

    def __init__(self, name, is_applied):
        self.name = name
        self.is_applied = is_applied

    async def upgrade(self):
        print(f'Applying {self.name}')

    async def downgrade(self):
        print(f'Unapplying {self.name}')


async def load_migration_table(url: str) -> typing.Set[str]:
    """
    Load the migration table from the database, returning a set of the
    currently applied migrations.
    """
    async with Database(url) as database:
        has_migrations_table = await has_table(database, "migrations")
        if not has_migrations_table:
            return set()

        query = sqlalchemy.sql.select([migrations_table.c.name])
        records = await database.fetch_all(query)
        return set([record['name'] for record in records])


async def run_migrations(url: str, index=None):
    dialect = get_dialect(url)

    async with Database(url) as database:
        has_migrations_table = await has_table(database, "migrations")
        if not has_migrations_table:
            # Create the migrations table if it doesn't yet exist.
            ddl = sqlalchemy.schema.CreateTable(migrations_table)
            statement = ddl.compile(dialect=dialect).string
            await database.execute(statement)

        # Determine the set of migrations that have been applied.
        query = sqlalchemy.sql.select([migrations_table.c.name])
        records = await database.fetch_all(query)
        applied_migrations = set([record['name'] for record in records])

        # Load the migrations from disk.
        loader_info = load_migrations(applied_migrations, dir_name="migrations")
        migrations = list(loader_info.migrations.values())
        if index is None:
            index = len(migrations) + 1

        # Apply the migrations.
        async with database.transaction():
            for migration in reversed(migrations[index:]):
                if not(migration.is_applied):
                    continue
                await migration.downgrade()
                query = migrations_table.delete().where(migrations_table.c.name==migration.name)
                await database.execute(query)

            for migration in migrations[:index]:
                if migration.is_applied:
                    continue
                await migration.upgrade()
                query = migrations_table.insert()
                await database.execute(query, values={'name': migration.name})
