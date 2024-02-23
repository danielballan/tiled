from typing import Any, List, Optional

import pydantic

from .core import StructureFamily


class Item(pydantic.BaseModel):
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    key: Optional[str]

    @classmethod
    def from_json(cls, item):
        return cls(**item)


class UnionStructure(pydantic.BaseModel):
    contents: List[Item]

    @classmethod
    def from_json(cls, structure):
        return cls(contents=[Item.from_json(item) for item in structure["contents"]])
