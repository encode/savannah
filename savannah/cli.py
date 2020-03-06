import asyncio
import click
import os
from dotenv import load_dotenv
from . import commands


def load_database_url():
    load_dotenv()
    if 'DATABASE_URL' not in os.environ:
        raise Exception('DATABASE_URL not in environment. You must specify --database.')
    return os.environ['DATABASE_URL']


@click.group()
def cli():
    pass


@click.command()
def init():
    os.mkdir("migrations")
    with open("migrations/__init__.py", "w") as fout:
        fout.write(f"""\
import savannah


config = savannah.Config(metadata="example:metadata")
""")
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
    if database is None:
        database = load_database_url()
    asyncio.run(commands.make_migration(database))


@click.command()
@click.option('--database', help='Database URL.')
def list_migrations(database):
    if database is None:
        database = load_database_url()
    migrations = asyncio.run(commands.list_migrations(database))
    for migration in migrations:
        if migration.is_applied:
            checkmark = '+'
        else:
            checkmark = ' '
        print(f'[{checkmark}] {migration.name}')


@click.command()
@click.option('--database', help='Database URL.')
@click.option('--target', type=str, help='Target.')
def migrate(database, target=None):
    if database is None:
        database = load_database_url()
    asyncio.run(commands.migrate(database, target=target))


@click.command()
@click.option('--database', help='Database URL.')
def create_database(database):
    if database is None:
        database = load_database_url()
    exists = asyncio.run(commands.database_exists(database))
    if not exists:
        asyncio.run(commands.create_database(database))
        print("Created database")
    else:
        print("Database already exists")


@click.command()
@click.option('--database', help='Database URL.')
def drop_database(database):
    if database is None:
        database = load_database_url()
    exists = asyncio.run(commands.database_exists(database))
    if exists:
        asyncio.run(commands.drop_database(database))
        print("Dropped database")
    else:
        print("Database does not exist")


cli.add_command(init)
cli.add_command(make_migration)
cli.add_command(list_migrations)
cli.add_command(migrate)
cli.add_command(create_database)
cli.add_command(drop_database)


if __name__ == '__main__':
    cli()
