from functools import partial
from types import SimpleNamespace


def data_session_as_tag(query, catalog):
    return catalog.apply_mongo_query({"data_session": {"$in": list(query.tags)}})


def access_blob_from_metadata(metadata, default=None):
    tags = list(default) or []
    if data_session := metadata["start"].get("data_session"):
        tags.append(data_session)
    return {"tags": tags}


bmm_authz_shim = SimpleNamespace(
    query_impl=data_session_as_tag,
    # The Catalog itself is public, but its contents will be filtered.
    catalog_access_blob={"tags": ["public"]},
    # The BlueskyRun gets default tags plus the 'data_session' from the start doc.
    bluesky_run_access_blob_from_metadata=partial(
        access_blob_from_metadata, default=("bmm_beamline",)
    ),
)
chx_authz_shim = SimpleNamespace(
    query_impl=data_session_as_tag,
    # The Catalog itself is public, but its contents will be filtered.
    catalog_access_blob={"tags": ["public"]},
    # The BlueskyRun gets default tags plus the 'data_session' from the start doc.
    bluesky_run_access_blob_from_metadata=partial(
        access_blob_from_metadata, default=("chx_beamline",)
    ),
)
