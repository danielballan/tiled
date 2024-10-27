from typing import Tuple

from .pydantic_array import ArrayStructure


class RaggedStructure(ArrayStructure):
    # None means "variable length".
    shape: Tuple[int | None, ...]
