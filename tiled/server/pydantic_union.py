from typing import Any, List, Optional

import pydantic

from ..structures.core import StructureFamily


class UnionStructureItem(pydantic.BaseModel):
    data_source_id: Optional[int]
    structure_family: StructureFamily
    structure: Any  # Union of Structures, but we do not want to import them...
    name: str

    @classmethod
    def from_json(cls, item):
        return cls(**item)


class UnionStructure(pydantic.BaseModel):
    contents: List[UnionStructureItem]
    all_keys: Optional[List[str]]

    @classmethod
    def from_json(cls, structure):
        return cls(
            contents=[
                UnionStructureItem.from_json(item) for item in structure["contents"]
            ],
            all_keys=structure["all_keys"],
        )
