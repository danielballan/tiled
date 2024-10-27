from typing import Tuple

from .array import ArrayStructure


class RaggedStructure(ArrayStructure):
    # None means "variable length".
    shape: Tuple[int | None, ...]
