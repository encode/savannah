import asyncio
import click
import os
import pkgutil
import sqlalchemy
from databases import Database
from importlib import import_module
from .functions import create_database, drop_database
from .migration import run_migrations, load_migration_table
from .loader import load_migrations





# async def create_database():
#     async with Database('postgresql://localhost/postgres') as database:
#         # await database.execute("DROP DATABASE savannah;")
#         res = await database.fetch_one("SELECT 1 FROM pg_database WHERE datname='savannah'")
#         if not res:
#             await database.execute("CREATE DATABASE savannah;")
#
#     async with Database('postgresql://localhost/savannah') as database:
#         statement = sqlalchemy.schema.CreateTable(migrations_table)
#         await database.execute("DROP TABLE migrations")
#         await database.execute(statement)


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
def makemigration():
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
@click.option('--database', help='Database URL.')
@click.option('--index', type=int, help='Index.')
def migrate(database, index=None):
    #loader_info = load_migrations(dir_name="migrations")
    asyncio.run(run_migrations(database, index=index))


@click.command()
@click.option('--database', help='Database URL.')
def list(database):
    applied = asyncio.run(load_migration_table(database))
    loader_info = load_migrations(applied, dir_name="migrations")
    for name, migration in loader_info.migrations.items():
        if migration.is_applied:
            checkmark = '+'
        else:
            checkmark = ' '
        print(f'[{checkmark}] {name}')


@click.command()
@click.option('--database', help='Database URL.')
def createdb(database):
    asyncio.run(create_database(database))


@click.command()
@click.option('--database', help='Database URL.')
def dropdb(database):
    asyncio.run(drop_database(database))


cli.add_command(init)
cli.add_command(makemigration)
cli.add_command(migrate)
cli.add_command(list)
cli.add_command(createdb)
cli.add_command(dropdb)


if __name__ == '__main__':
    cli()
