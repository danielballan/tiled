import builtins
import collections.abc
import uuid
from pathlib import Path
from sys import platform

import zarr
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ...adapters.array import slice_and_shape_from_block_and_chunks
from ...iterviews import ItemsView, KeysView, ValuesView
from ...query_registration import QueryTranslationRegistry
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

    @property
    def references(self):
        return self.node.references

    def __repr__(self):
        return f"<{type(self).__name__} {self._path!s} >"


class NodeAdapter(BaseAdapter, collections.abc.Mapping):
    def new_key(self):
        "This is used to generate names for new child nodes."
        # This is broken out into a separate method to make it easy to override.
        return str(uuid.uuid4())

    def post_metadata(
        self,
        externally_managed,
        metadata,
        structure_family,
        structure,
        specs,
        references,
    ):
        key = self.new_key()
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
        # construct_item(self._sessionmaker, node)
        _adapter_class_by_family[structure_family].new(
            sessionmaker,
            node,
            structure,
        )
        return key

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

    @classmethod
    def new(cls, sessionmaker, node, structure):
        return construct_item(sessionmaker, node)

    # def read(self, fields=None):
    #     if fields is not None:
    #         new_mapping = {}
    #         for field in fields:
    #             new_mapping[field] = self._mapping[field]
    #         return self.new_variation(mapping=new_mapping)
    #     return self

    query_registry = QueryTranslationRegistry()

    def search(self, query):
        """
        Return a MongoAdapter with a subset of the mapping.
        """
        return self.query_registry(query, self)


# NodeAdapter.query_registry.register_query(Contains, contains)
# NodeAdapter.query_registry.register_query(Comparison, comparison)
# NodeAdapter.query_registry.register_query(Eq, eq)
# NodeAdapter.query_registry.register_query(NotEq, noteq)
# NodeAdapter.query_registry.register_query(Regex, regex)
# NodeAdapter.query_registry.register_query(In, _in)
# NodeAdapter.query_registry.register_query(NotIn, notin)
# NodeAdapter.query_registry.register_query(FullText, full_text_search)


class ArrayAdapter(BaseAdapter):
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

    @classmethod
    def new(cls, sessionmaker, node, structure):
        if not node.externally_managed:
            # Create a DataSource and Asset(s?).
            with sessionmaker() as db:
                ds = orm.DataSource(
                    node_id=node.id,
                    structure=structure,
                    mimetype="...",
                    parameters={},
                    externally_managed=False,
                )
                db.add(ds)
                db.commit()
                db.refresh(ds)
                data_uri = "..."
                asset = orm.Asset(
                    data_source_id=ds.id,
                    data_uri=data_uri,
                )
                db.add(asset)
                db.commit(asset)
            # Zarr requires evenly-sized chunks within each dimension.
            # Use the first chunk along each dimension.
            chunks = tuple(dim[0] for dim in structure.macro.chunks)
            shape = tuple(dim[0] * len(dim) for dim in structure.macro.chunks)
            storage = zarr.storage.DirectoryStore(str(safe_path(data_uri.path)))
            zarr.storage.init_array(
                storage,
                shape=shape,
                chunks=chunks,
                dtype=structure.micro.to_numpy_dtype(),
            )
            path = node.ancestors + [node.key]
        return cls(sessionmaker, path, node)

    def read(self, slice=None):
        # Trim overflow because Zarr always has equal-sized chunks.
        arr = self.array[
            tuple(builtins.slice(0, dim) for dim in self.node.structure.macro.shape)
        ]
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(self, block, slice=None):
        # Trim overflow because Zarr always has equal-sized chunks.
        slice_, _ = slice_and_shape_from_block_and_chunks(
            block, self.node.structure.macro.chunks
        )
        # Slice the block out of the whole array.
        arr = self.array[slice_]
        # And then maybe slice *within* the block.
        if slice is not None:
            arr = arr[slice]
        return arr

    def macrostructure(self):
        return self.structure.macro

    def microstructure(self):
        return self.structure.micro


def construct_item(sessionmaker, node):
    class_ = _adapter_class_by_family[node.structure_family]
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


_adapter_class_by_family = {
    StructureFamily.node: NodeAdapter,
    StructureFamily.array: ArrayAdapter,
    # StructureFamily.dataframe: DataFrameAdapter,
    # StructureFamily.sparse: COOAdapter,
}


def safe_path(path):
    # TODO Do we need this?
    if platform == "win32" and path[0] == "/":
        path = path[1:]
    return Path(path)
