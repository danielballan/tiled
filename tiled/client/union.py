import copy

from .base import STRUCTURE_TYPES, BaseClient
from .utils import client_for_item


class UnionClient(BaseClient):
    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{key}'" for key in self.structure().all_keys)
            + "}>"
        )

    @property
    def contents(self):
        return UnionContents(self)

    def __getitem__(self, key):
        if key not in self.structure().all_keys:
            raise KeyError(key)
        raise NotImplementedError


class UnionContents:
    def __init__(self, node):
        self.node = node

    def __repr__(self):
        return (
            f"<{type(self).__name__} {{"
            + ", ".join(f"'{item.name}'" for item in self.node.structure().contents)
            + "}>"
        )

    def __getitem__(self, name):
        for index, union_item in enumerate(self.node.structure().contents):
            if union_item.name == name:
                structure_family = union_item.structure_family
                structure_dict = union_item.structure
                break
        else:
            raise KeyError(name)
        item = copy.deepcopy(self.node.item)
        item["attributes"]["structure_family"] = structure_family
        item["attributes"]["structure"] = structure_dict
        item["links"] = item["links"]["contents"][index]
        structure_type = STRUCTURE_TYPES[structure_family]
        structure = structure_type.from_json(structure_dict)
        return client_for_item(
            self.node.context,
            self.node.structure_clients,
            item,
            structure=structure,
            include_data_sources=self.node._include_data_sources,
        )
