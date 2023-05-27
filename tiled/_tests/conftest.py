import contextlib
import getpass
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..catalog.node import CatalogNodeAdapter
from ..client import context
from ..server.settings import get_settings


@pytest.fixture(autouse=True)
def reset_settings():
    """
    Reset the FastAPI Settings.

    Fast API uses a global singleton for Settings.  It is difficult to get
    around this, so our best option is to reset it.
    """
    get_settings.cache_clear()
    yield


@pytest.fixture(autouse=True)
def set_auth_token_cache_dir(tmpdir):
    """
    Use a tmpdir instead of ~/.cache/tiled/tokens
    """
    original = context.DEFAULT_TOKEN_CACHE
    context.DEFAULT_TOKEN_CACHE = str(tmpdir)
    yield
    context.DEFAULT_TOKEN_CACHE = original


@pytest.fixture
def enter_password(monkeypatch):
    """
    Return a context manager that overrides getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    @contextlib.contextmanager
    def f(password):
        context.PROMPT_FOR_REAUTHENTICATION = True
        original = getpass.getpass
        monkeypatch.setattr("getpass.getpass", lambda: password)
        yield
        monkeypatch.setattr("getpass.getpass", original)
        context.PROMPT_FOR_REAUTHENTICATION = None

    return f


@pytest.fixture(scope="module")
def tmpdir_module(request, tmpdir_factory):
    """A tmpdir fixture for the module scope. Persists throughout the module."""
    # Source: https://stackoverflow.com/a/31889843
    return tmpdir_factory.mktemp(request.module.__name__)


# This can un-commented to debug leaked threads.
# import threading
# import time
#
# def poll_enumerate():
#     while True:
#         time.sleep(1)
#         print("THREAD COUNT", len(threading.enumerate()))
#
# thread = threading.Thread(target=poll_enumerate, daemon=True)
# thread.start()


# To test with postgres, start a container like:
#
# docker run --name tiled-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d docker.io/postgres
# and set this env var like:
#
# TILED_TEST_POSTGRESQL_URI=postgresql+asyncpg://postgres:secret@localhost:5432

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")


@contextlib.asynccontextmanager
async def temp_postgres(uri):
    if uri.endswith("/"):
        uri = uri[:-1]
    # Create a fresh database.
    engine = create_async_engine(uri)
    database_name = f"tiled_test_disposable_{uuid.uuid4().hex}"
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"CREATE DATABASE {database_name};"))
        await connection.commit()
    yield f"{uri}/{database_name}"
    # Drop the database.
    async with engine.connect() as connection:
        await connection.execute(
            text("COMMIT")
        )  # close the automatically-started transaction
        await connection.execute(text(f"DROP DATABASE {database_name};"))
        await connection.commit()


@pytest_asyncio.fixture(params=["sqlite", "postgresql"])
async def adapter(request, tmpdir):
    """
    Adapter instance

    Note that startup() and shutdown() are not called, and must be run
    either manually (as in the fixture 'a') or via the app (as in the fixture 'client').
    """
    if request.param == "sqlite":
        adapter = CatalogNodeAdapter.in_memory(writable_storage=str(tmpdir))
        yield adapter
    elif request.param == "postgresql":
        if not TILED_TEST_POSTGRESQL_URI:
            raise pytest.skip("No TILED_TEST_POSTGRESQL_URI configured")
        # Create temporary database.
        async with temp_postgres(TILED_TEST_POSTGRESQL_URI) as uri_with_database_name:
            # Build an adapter on it.
            adapter = CatalogNodeAdapter.from_uri(
                uri_with_database_name,
                writable_storage=str(tmpdir),
                initialize_database_at_startup=True,
            )
            yield adapter
    else:
        assert False
