from datetime import datetime


class AuthZShim:
    """
    Shim Tiled's new AuthZ API into databroker.mongo_normalized.MongoAdapter.

    Parameters
    ----------
    catalog_tags : list[str]
        Tags placed on the MongoAdapter.
        (On the client-side, that's the CatalogOfBlueskyRuns.)
    default_tags : list[str]
        Tags placed on every BlueskyRun, in addition to dynamically
        determined ones.
    public_interval : tuple[float]
        Given as a tuple of two timestamps, (start, end).
        BlueskyRuns taken in this range are public.
    """

    def __init__(self, catalog_tags, bluesky_run_tags, public_interval=None):
        self.catalog_access_blob = {"tags": catalog_tags}
        self._default_tags = bluesky_run_tags
        self._public_interval = public_interval

    def query_impl(self, query, catalog):
        if query.tags.intersection(self._default_tags):
            return catalog
        query = {"data_session": {"$in": list(query.tags)}}
        if self._public_interval:
            start, end = self._public_interval
            is_in_public_interval = {
                "$and": [{"time": {"$gte": start}}, {"time": {"$lt": end}}]
            }
            query = {"$or": [query, is_in_public_interval]}
        return catalog.apply_mongo_query(query)

    def bluesky_run_access_blob_from_metadata(self, metadata):
        tags = list(self._default_tags)
        if data_session := metadata["start"].get("data_session"):
            tags.append(data_session)
        if self._public_interval:
            start, end = self._public_interval
            if start <= metadata["start"]["time"] < end:
                tags.append("public")
        return {"tags": tags}


bmm_authz_shim = AuthZShim(["_ROOT_NODE_BMM"], ["bmm_beamline"])
chx_authz_shim = AuthZShim(
    ["_ROOT_NODE_CHX"],
    ["chx_beamline"],
    (datetime(2020, 1, 1).timestamp(), datetime(2024, 1, 1).timestamp()),
)
