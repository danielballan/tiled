import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ...structures.core import StructureFamily
from . import orm
from .base import Base


class SQLAdapter:
    def __init__(self, sessionmaker, node, path):
        self._sessionmaker = sessionmaker
        if node is None:
            self.metadata = {}
            self.structure_family = StructureFamily.node
            self.structure = None
            self.specs = []
        else:
            self.metadata = node.metadata_
            self.structure_family = StructureFamily(node.structure_family)
            self.structure = node.structure
            self.specs = node.specs
        self._path = path or ()  # path parts as tuple

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
        return cls(sm, None, None)

    def __repr__(self):
        return f"{type(self).__name__}({self._path})"

    def post_metadata(self, metadata, structure_family, structure, specs):
        key = str(uuid.uuid4())
        # if structure_family == StructureFamily.dataframe:
        #     # Initialize an empty DataFrame with the right columns/types.
        #     meta = deserialize_arrow(structure.micro.meta)
        #     divisions_wrapped_in_df = deserialize_arrow(structure.micro.divisions)
        #     divisions = tuple(divisions_wrapped_in_df["divisions"].values)
        with self._sessionmaker() as db:
            node = orm.Node(
                key=key,
                parent="".join(f"/{segment}" for segment in self._path),
                metadata_=metadata,
                structure_family=structure_family,
                structure=structure,
                specs=specs,
            )
            db.add(node)
            db.commit()
            db.refresh(node)  # Refresh to sync back the auto-generated fields.
        return construct_item(self._sessionmaker, node, self._path + (key,))

    def __getitem__(self, key):
        with self._sessionmaker() as db:
            node = (
                db.query(orm.Node)
                .filter(
                    orm.Node.key == key,
                    orm.Node.parent == "".join(f"/{segment}" for segment in self._path),
                )
                .first(),
            )
        return construct_item(self._sessionmaker, node, self._path + (key,))


class ArrayAdapter:
    def __init__(self, sessionmaker, node, path):
        from ...structures.array import ArrayStructure

        self._sessionmaker = sessionmaker
        self.metadata = node.metadata_
        assert node.structure_family == StructureFamily.array
        self.structure = ArrayStructure.from_json(node.structure)
        self.specs = node.specs
        self._path = path  # path parts as tuple

    def macrostructure(self):
        return self.structure.macro

    def microstructure(self):
        return self.structure.micro


_DISPATCH = {
    StructureFamily.node: SQLAdapter,
    StructureFamily.array: ArrayAdapter,
}


def construct_item(sessionmaker, node, path):
    class_ = _DISPATCH[node.structure_family]
    return class_(
        sessionmaker,
        node,
        path,
    )


def initialize_database(engine):

    # The definitions in .orm alter Base.metadata.
    from . import orm  # noqa: F401

    # Create all tables.
    Base.metadata.create_all(engine)

    # Mark current revision.
    # with temp_alembic_ini(engine.url) as alembic_ini:
    #     alembic_cfg = Config(alembic_ini)
    #     command.stamp(alembic_cfg, "head")
