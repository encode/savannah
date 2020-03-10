import savannah
import pytest
import os


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database_url", ["sqlite:///test.db", "postgresql://127.0.0.1:5432/test_savannah"]
)
async def test_create(database_url):
    assert not await savannah.database_exists(database_url)
    await savannah.create_database(database_url)
    assert await savannah.database_exists(database_url)
    await savannah.drop_database(database_url)
    assert not await savannah.database_exists(database_url)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database_url", ["sqlite:///test.db", "postgresql://127.0.0.1:5432/test_savannah"]
)
async def test_migrate(tmp_path, database_url):
    os.chdir(tmp_path)
    await savannah.create_database(database_url)

    try:
        await savannah.init()
        await savannah.make_migration(database_url)
        await savannah.make_migration(database_url)
        migrations = await savannah.list_migrations(database_url)
        assert len(migrations) == 3
        assert [m.is_applied for m in migrations] == [False, False, False]

        await savannah.migrate(database_url, target="0002")
        migrations = await savannah.list_migrations(database_url)
        assert len(migrations) == 3
        assert [m.is_applied for m in migrations] == [True, True, False]

        await savannah.migrate(database_url)
        migrations = await savannah.list_migrations(database_url)
        assert len(migrations) == 3
        assert [m.is_applied for m in migrations] == [True, True, True]

        await savannah.migrate(database_url, target="0001")
        migrations = await savannah.list_migrations(database_url)
        assert len(migrations) == 3
        assert [m.is_applied for m in migrations] == [True, False, False]

        await savannah.migrate(database_url, target="zero")
        migrations = await savannah.list_migrations(database_url)
        assert len(migrations) == 3
        assert [m.is_applied for m in migrations] == [False, False, False]
    finally:
        await savannah.drop_database(database_url)
