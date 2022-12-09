import collections.abc
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ...adapters.array import slice_and_shape_from_block_and_chunks
from ...iterviews import ItemsView, KeysView, ValuesView
from ...structures.core import StructureFamily
from ..utils import Layout
from . import orm
from .base import Base


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
    return NodeAdapter(sm, ())


def connect(uri):
    connect_args = {}
    if uri.startswith("sqlite"):
        connect_args.update({"check_same_thread": False})
    engine = create_engine(uri, connect_args=connect_args)
    sm = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    if uri.startswith("sqlite"):
        # Scope to a session per thread.
        sm = scoped_session(sm)
    return NodeAdapter(sm, ())


class BaseAdapter:
    def __init__(self, sessionmaker, path, node=None):
        self._sessionmaker = sessionmaker
        self._path = path  # path parts as tuple
        # Defer the database lookup because we may just be "passing through"
        # on our way to a child node.
        self._node = node

    @property
    def node(self):
        if self._node is None:
            if self._path:
                *ancestors, key = self._path
            else:
                ancestors, key = None, ""
            with self._sessionmaker() as db:
                self._node = (
                    db.query(orm.Node)
                    .filter(
                        orm.Node.key == key,
                        orm.Node.ancestors == ancestors,
                    )
                    .first()
                )
        return self._node

    @property
    def metadata(self):
        return self.node.metadata_

    @property
    def structure_family(self):
        return self.node.structure_family

    @property
    def specs(self):
        return self.node.specs

    def __repr__(self):
        return f"<{type(self).__name__} {self._path!s} >"


class NodeAdapter(BaseAdapter, collections.abc.Mapping):
    def post_metadata(self, metadata, structure_family, structure, specs, references):
        key = str(uuid.uuid4())
        # if structure_family == StructureFamily.dataframe:
        #     # Initialize an empty DataFrame with the right columns/types.
        #     meta = deserialize_arrow(structure.micro.meta)
        #     divisions_wrapped_in_df = deserialize_arrow(structure.micro.divisions)
        #     divisions = tuple(divisions_wrapped_in_df["divisions"].values)
        with self._sessionmaker() as db:
            node = orm.Node(
                key=key,
                ancestors=self._path,
                metadata_=metadata,
                structure_family=structure_family,
                specs=specs or [],
                references=references or [],
            )
            db.add(node)
            db.commit()
            db.refresh(node)  # Refresh to sync back the auto-generated fields.
        adapter = construct_item(self._sessionmaker, node)
        # adapter.initialize(data_sources)
        return adapter

    def put_data(self, body, block=None):
        import numpy

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
                    orm.Node.ancestors == self._path,
                )
                .first()
            )
        if node is None:
            raise KeyError(key)
        return construct_item(self._sessionmaker, node)

    def __iter__(self):
        with self._sessionmaker() as db:
            rows = db.query(orm.Node.key).filter(
                orm.Node.ancestors == self._path,
            )
            for row in rows:
                yield row[0]

    def __len__(self):
        with self._sessionmaker() as db:
            return (
                db.query(orm.Node.key)
                .filter(
                    orm.Node.ancestors == self._path,
                )
                .count()
            )

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(self, start, stop, direction):
        with self._sessionmaker() as db:
            rows = (
                db.query(orm.Node.key)
                .filter(
                    orm.Node.ancestors == self._path,
                )
                .offset(start)
                .limit(stop)
            )
        keys = [row[0] for row in rows]
        return list(keys)

    def _items_slice(self, start, stop, direction):
        with self._sessionmaker() as db:
            rows = (
                db.query(orm.Node)
                .filter(
                    orm.Node.ancestors == self._path,
                )
                .offset(start)
                .limit(stop)
            )
        items = [(row.key, construct_item(self._sessionmaker, row)) for row in rows]
        return items


class ArrayAdapter(BaseAdapter):

    # def initialize(self, data_sources):
    #     if not data_sources:
    #         data_source = {"
    #     elif len(data_sources) > 1:
    #         raise ValueError("An Array can only have one data source (but may span multiple assets).")

    def macrostructure(self):
        return self.structure.macro

    def microstructure(self):
        return self.structure.micro


_DISPATCH = {
    StructureFamily.node: NodeAdapter,
    StructureFamily.array: ArrayAdapter,
}


def construct_item(sessionmaker, node):
    class_ = _DISPATCH[node.structure_family]
    path = node.ancestors + [node.key]
    return class_(
        sessionmaker,
        path,
        node,
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
            ancestors=None,
            metadata_={},
            structure_family=StructureFamily.node,
            specs=[],
            references=[],
        )
        db.add(node)
        db.commit()

    # Mark current revision.
    # with temp_alembic_ini(engine.url) as alembic_ini:
    #     alembic_cfg = Config(alembic_ini)
    #     command.stamp(alembic_cfg, "head")
