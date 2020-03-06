import sqlalchemy
import typing
from .utils import has_table
from databases import Database


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

    def __init__(self, name: str, is_applied: bool):
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
