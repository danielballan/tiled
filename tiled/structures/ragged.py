from typing import Tuple

from .array import ArrayStructure, BuiltinDtype, StructDtype


class RaggedStructure(ArrayStructure):
    # None means "variable length".
    shape: Tuple[int | None, ...]

    @classmethod
    def from_array(cls, array, shape=None, chunks=None, dims=None) -> "RaggedStructure":
        # Why would shape ever be different from array.shape, you ask?
        # Some formats (notably Zarr) force shape to be a multiple of
        # a chunk size, such that array.shape may include a margin beyond the
        # actual data.
        if shape is None:
            shape = array.shape
        # if chunks is None:
        #     if hasattr(array, "chunks"):
        #         chunks = array.chunks  # might be None
        #     else:
        #         chunks = None
        #     if chunks is None:
        #         chunks = ("auto",) * len(shape)
        # normalized_chunks = normalize_chunks(
        #     chunks,
        #     shape=shape,
        #     dtype=array.dtype,
        # )
        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)
        return cls(data_type=data_type, shape=shape, chunks=chunks, dims=dims)
