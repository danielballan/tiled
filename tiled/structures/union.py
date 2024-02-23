import dataclasses
from typing import Any, List, Optional

from .core import StructureFamily


@dataclasses.dataclass
class Item:
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    key: Optional[str]

    @classmethod
    def from_json(cls, item):
        return cls(**item)


@dataclasses.dataclass
class UnionStructure:
    contents: List[Item]

    @classmethod
    def from_json(cls, structure):
        return cls(contents=[Item.from_json(item) for item in structure["contents"]])
