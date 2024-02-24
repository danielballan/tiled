from .base import BaseClient


class UnionClient(BaseClient):
    def __repr__(self):
        return (
            f"<{type(self).__name__} "
            f"[{', '.join(item.structure_family for item in self.structure().contents)}]>"
        )
