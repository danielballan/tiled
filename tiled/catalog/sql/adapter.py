import time
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ...adapters.array import slice_and_shape_from_block_and_chunks
from ...structures.core import StructureFamily
from ..utils import Layout
from . import orm
from .base import Base


CACHED_NODE_TTL = 0.1  # seconds


def create(uri, layout=Layout.scalable):
    connect_args = {}
    if uri.startswith("sqlite"):
        connect_args.update({"check_same_thread": False})
    engine = create_engine(uri, connect_args=connect_args)
    initialize_database(engine)
    sm = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    if uri.startswith("sqlite"):
        # Scope to a session per thread.
        sm = scoped_session(sm)
    with sm() as db:
        node = (
            db.query(orm.Node)
            .filter(
                orm.Node.key == "",
                orm.Node.parent == "",
            )
            .first()
        )
    return NodeAdapter(sm, node, ())


def connect(uri):
    connect_args = {}
    if uri.startswith("sqlite"):
        connect_args.update({"check_same_thread": False})
    engine = create_engine(uri, connect_args=connect_args)
    sm = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    if uri.startswith("sqlite"):
        # Scope to a session per thread.
        sm = scoped_session(sm)
    with sm() as db:
        node = (
            db.query(orm.Node)
            .filter(
                orm.Node.key == "",
                orm.Node.parent == "",
            )
            .first()
        )
    return NodeAdapter(sm, node, ())


class BaseAdapter:

    def __init__(self, sessionmaker, node, path):
        self._sessionmaker = sessionmaker
        self._node = node
        self._path = path  # path parts as tuple
        self._parent = path[:-1]
        if not self._path:
            self._key = ""  # special case: root node
        else:
            self._key = path[-1]
        self._deadline = 0

    @property
    def metadata(self):
        return self.node.metadata_

    @property
    def structure_family(self):
        return self.node.structure_family

    @property
    def specs(self):
        return self.node.specs

    @property
    def node(self):
        now = time.monotonic()
        if now > self._deadline:
            with self._sessionmaker() as db:
                node = (
                    db.query(orm.Node)
                    .filter(
                        orm.Node.key == self._key,
                        orm.Node.parent == "".join(f"/{segment}" for segment in self._parent),
                    )
                    .first()
                )
            self._deadline = now + CACHED_NODE_TTL

        return self._node

    def __repr__(self):
        return f"{type(self).__name__}({self._path})"


class NodeAdapter(BaseAdapter):

    def post_metadata(self, metadata, structure_family, structure, specs, data_sources):
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
        adapter = construct_item(self._sessionmaker, node, self._path + (key,))
        adapter.initialize(data_sources)
        return adapter


    def put_data(self, body, block=None):
        # Organize files into subdirectories with the first two
        # characters of the key to avoid one giant directory.
        if block:
            slice_, shape = slice_and_shape_from_block_and_chunks(
                block, self.node.structure.macro.chunks
            )
        else:
            slice_ = numpy.s_[:]
            shape = self.node.structure.macro.shape
        array = numpy.frombuffer(
            body, dtype=self.node.structure.micro.to_numpy_dtype()
        ).reshape(shape)
        self.array[slice_] = array


    def __getitem__(self, key):
        with self._sessionmaker() as db:
            node = (
                db.query(orm.Node)
                .filter(
                    orm.Node.key == key,
                    orm.Node.parent == "".join(f"/{segment}" for segment in self._path),
                )
                .first()
            )
        return construct_item(self._sessionmaker, node, self._path + (key,))


class ArrayAdapter(BaseAdapter):

    def initialize(self, data_sources):
        if not data_sources:
            data_source = {"
        elif len(data_sources) > 1:
            raise ValueError("An Array can only have one data source (but may span multiple assets).")


    def macrostructure(self):
        return self.structure.macro

    def microstructure(self):
        return self.structure.micro


_DISPATCH = {
    StructureFamily.node: NodeAdapter,
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

    # Construct root node.
    sm = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    with sm() as db:
        node = orm.Node(
            key="",
            parent="",
            metadata_={},
            structure_family=StructureFamily.node,
            structure=None,
            specs=[],
        )
        db.add(node)
        db.commit()

    # Mark current revision.
    # with temp_alembic_ini(engine.url) as alembic_ini:
    #     alembic_cfg = Config(alembic_ini)
    #     command.stamp(alembic_cfg, "head")
