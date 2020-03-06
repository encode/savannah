"""
Adapted from SQLAlchemy Utils.

https://github.com/kvesteri/sqlalchemy-utils/blob/master/sqlalchemy_utils/functions/database.py

---

Copyright (c) 2012, Konsta Vesterinen

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* The names of the contributors may not be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from databases import Database, DatabaseURL
import os
from .migration import migrations_table
from .loader import load_migrations
from .utils import database_exists, has_table, get_dialect
import sqlalchemy


async def migrate(url: str, index=None):
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


async def create_database(url, encoding='utf8', template=None):
    url = DatabaseURL(url)
    database_name = url.database

    if url.dialect in ('postgres', 'postgresql'):
        url = url.replace(database='postgres')
    elif url.dialect == 'mysql':
        url = url.replace(database='')

    if url.dialect in ('postgres', 'postgresql'):
        statement = "CREATE DATABASE {0} ENCODING '{1}' TEMPLATE {2}".format(
            database_name,
            encoding,
            template or 'template1'
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
