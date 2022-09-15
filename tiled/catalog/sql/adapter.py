from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from .base import Base


class SQLAdapter:
    def __init__(self, sessionmaker, parents=None):
        self._sessionmaker = sessionmaker

    @classmethod
    def from_uri(cls, uri, create=False):
        connect_args = {}
        if uri.startswith("sqlite"):
            connect_args.update({"check_same_thread": False})
        engine = create_engine(uri, connect_args=connect_args)
        if create:
            initialize_database(engine)
        sm = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        if uri.startswith("sqlite"):
            # Scope to a session per thread.
            sm = scoped_session(sm)
        return cls(sm)


def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    # Create all tables.
    Base.metadata.create_all(engine)

    # Mark current revision.
    # with temp_alembic_ini(engine.url) as alembic_ini:
    #     alembic_cfg = Config(alembic_ini)
    #     command.stamp(alembic_cfg, "head")
