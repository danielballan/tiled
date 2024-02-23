from .base import BaseClient


class UnionClient(BaseClient):
    def __repr__(self):
        return f"<{type(self).__name__}>"
