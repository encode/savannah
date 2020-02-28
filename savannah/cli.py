import asyncio
import click
import os
import pkgutil
import sqlalchemy
from databases import Database
from importlib import import_module
from .loader import load_migrations


metadata = sqlalchemy.MetaData()

migrations_table = sqlalchemy.Table(
    "migrations",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(length=100), index=True),
)


async def create_database():
    async with Database('postgresql://localhost/postgres') as database:
        # await database.execute("DROP DATABASE savannah;")
        res = await database.fetch_one("SELECT 1 FROM pg_database WHERE datname='savannah'")
        if not res:
            await database.execute("CREATE DATABASE savannah;")

    async with Database('postgresql://localhost/savannah') as database:
        statement = sqlalchemy.schema.CreateTable(migrations_table)
        await database.execute("DROP TABLE migrations")
        await database.execute(statement)

        # await database.execute("""CREATE TABLE migrations (
        # 	id SERIAL NOT NULL,
        # 	name VARCHAR(100),
        # 	PRIMARY KEY (id)
        # )
        # """)

@click.group()
def cli():
    pass


@click.command()
def init():
    os.mkdir("migrations")
    with open("migrations/0001_initial.py", "w") as fout:
        fout.write("""\
import savannah


class Migration(savannah.Migration):
    dependencies = []
    operations = []
""")


@click.command()
def create():
    loader_info = load_migrations(dir_name="migrations")
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


@click.command()
def migrate():
    loader_info = load_migrations(dir_name="migrations")
    asyncio.run(create_database())


@click.command()
def list():
    loader_info = load_migrations(dir_name="migrations")
    for name, migration in loader_info.migrations.items():
        print(f'[ ] {name}')


cli.add_command(init)
cli.add_command(create)
cli.add_command(migrate)
cli.add_command(list)


if __name__ == '__main__':
    cli()
