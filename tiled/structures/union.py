import dataclasses
from typing import Any, List, Optional

from .core import StructureFamily


@dataclasses.dataclass
class UnionStructureItem:
    data_source_id: int
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    name: Optional[str]

    @classmethod
    def from_json(cls, item):
        return cls(**item)


@dataclasses.dataclass
class UnionStructure:
    contents: List[UnionStructureItem]
    all_keys: List[str]

    @classmethod
    def from_json(cls, structure):
        return cls(
            contents=[
                UnionStructureItem.from_json(item) for item in structure["contents"]
            ],
            all_keys=structure["all_keys"],
        )
