import asyncio
import click
import os
import pkgutil
import sqlalchemy
from databases import Database
from importlib import import_module
from .loader import load_migrations
from .migration import load_migration_table
from . import commands



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
@click.option('--database', help='Database URL.')
def make_migration(database):
    applied = asyncio.run(load_migration_table(database))
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


@click.command()
@click.option('--database', help='Database URL.')
def list_migrations(database):
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
@click.option('--index', type=int, help='Index.')
def migrate(database, index=None):
    #loader_info = load_migrations(dir_name="migrations")
    asyncio.run(commands.migrate(database, index=index))


@click.command()
@click.option('--database', help='Database URL.')
def create_database(database):
    asyncio.run(commands.create_database(database))


@click.command()
@click.option('--database', help='Database URL.')
def drop_database(database):
    asyncio.run(commands.drop_database(database))


cli.add_command(init)
cli.add_command(make_migration)
cli.add_command(list_migrations)
cli.add_command(migrate)
cli.add_command(create_database)
cli.add_command(drop_database)


if __name__ == '__main__':
    cli()
