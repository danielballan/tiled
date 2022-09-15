import json

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    LargeBinary,
    Unicode,
)
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from ..structures.core import StructureFamily
from .base import Base


class Timestamped:
    """
    Mixin for providing timestamps of creation and update time.

    These are not used by application code, but they may be useful for
    forensics.
    """

    time_created = Column(DateTime(timezone=False), server_default=func.now())
    time_updated = Column(
        DateTime(timezone=False), onupdate=func.now()
    )  # null until first update

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            + ", ".join(
                f"{key}={value!r}"
                for key, value in self.__dict__.items()
                if not key.startswith("_")
            )
            + ")"
        )


class JSONList(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded list.

    Usage::

        JSONList(255)

    """

    impl = Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        # Make sure we don't get passed some iterable like a dict.
        if not isinstance(value, list):
            raise ValueError("JSONList must be given a literal `list` type.")
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class JSONDict(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded dict (i.e. object).

    Usage::

        JSONDict(255)

    """

    impl = Unicode
    cache_ok = True

    def process_bind_param(self, value, dialect):
        # Make sure we don't get passed some iterable like a dict.
        if not isinstance(value, dict):
            raise ValueError("JSONDict must be given a literal `dict` type.")
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class Node(Timestamped, Base):
    """
    This describes a single Node and sometimes inlines descriptions of all its children.
    """

    __tablename__ = "nodes"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    key = Column(Unicode(1023), index=True, nullable=False)
    parent = Column(Unicode(1023), index=True, nullable=False)
    structure_family = Column(Enum(StructureFamily), nullable=False)
    structure = Column(JSONDict, nullable=True)
    metadata_ = Column("metadata", JSONDict, nullable=False)
    specs = Column(JSONList, nullable=False)
    stale = Column(Boolean, default=False, nullable=False)

    __table_args__ = (
        UniqueConstraint("key", "parent", name="_key_parent_unique_constraint"),
    )


class DataSource(Timestamped, Base):
    """
    The describes how to open one or more file/blobs to extract data for a Node.

    The mimetype can be used to look up an appropriate Adapter.
    The Adapter will accept the data_uri (which may be a directory in this case)
    and optional parameters.

    The parameters are used to select the data of interest for this DataSource.
    Then, within that, Tiled may use the standard Adapter API to subselect the data
    of interest for a given request.
    """

    __tablename__ = "data_sources"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("node.id"), nullable=False)

    # For 'node' and 'dataframe' structures this tell us which field(s)
    # this DataSource provides.
    fields = Column(Unicode(4095), nullable=True)
    mimetype = Column(Unicode(1023), nullable=False)
    # These are additional parameters passed to the Adapter to guide
    # it to access and arrange the data in the file correctly.
    parameters = Column(JSONDict(1023), nullable=True)

    node = relationship("Node", back_populates="node")


class Asset(Timestamped):
    """
    This tracks individual files/blobs.

    It intended for introspection and forensics. It is not actually used
    when doing routine data access.

    Importantly, it does so without any code execution. For example, it will
    include all the HDF5 subsidiary files for HDF5 files that use external
    links.
    """

    __tablename__ = "assets"

    # This id is internal, never exposed to the user.
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_source.id"), nullable=False)

    # Data is either referenced by a URI (data_uri) or stored in-line as a
    # binary blob (data_blob).
    data_uri = Column(Unicode(1023), nullable=True)
    data_blob = Column(LargeBinary, nullable=True)
    data_mtime = Column(Unicode(1023), nullable=True)
    hash_type = Column(Unicode(63), nullable=True)
    hash_content = Column(Unicode(1023), nullable=True)

    data_source = relationship("DataSource", back_populates="data_source")
