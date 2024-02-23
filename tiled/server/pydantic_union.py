from typing import Any, List, Optional

import pydantic

from ..structures.core import StructureFamily


class UnionStructureItem(pydantic.BaseModel):
    data_source_id: Optional[int]
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    key: Optional[str]

    @classmethod
    def from_json(cls, item):
        return cls(**item)


class UnionStructure(pydantic.BaseModel):
    contents: List[UnionStructureItem]

    @classmethod
    def from_json(cls, structure):
        return cls(
            contents=[
                UnionStructureItem.from_json(item) for item in structure["contents"]
            ]
        )
