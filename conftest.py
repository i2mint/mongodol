"""Pytest configuration for mongodol.

Two responsibilities:

1. Keep the ``mongodol/scrap`` directory out of collection (via the modern
   pytest ``pytest_ignore_collect(collection_path, config)`` hook — the
   ``path`` argument of the pre-pytest-9 signature was removed in pytest 9,
   which turned the old hook into a hard ``PluginValidationError``).

2. Let the suite stay green **without a live MongoDB**. mongodol is a MongoDB
   data-object layer, so nearly every unit test and most module doctests need a
   reachable server. When none is reachable (e.g. CI without a Mongo service)
   the MongoDB-dependent items are *skipped* rather than left to fail; when a
   server is available (local dev, or CI wired to one) they run normally.
"""

import os
import pathlib

import pytest

from mongodol.constants import DFLT_TEST_HOST

#: Directory whose contents must never be collected.
_SCRAP_DIR = pathlib.Path(__file__).parent.resolve() / "mongodol" / "scrap"

#: Package-module stems whose doctests do NOT touch MongoDB and are therefore
#: safe to run without a server. Kept deliberately conservative: any module not
#: listed here has its doctests skipped when no MongoDB is reachable, so a new
#: MongoDB-using doctest can never silently fail CI. Widen only after verifying
#: a module's doctests are genuinely server-free.
_MONGO_FREE_DOCTEST_STEMS = frozenset({"util"})


def pytest_ignore_collect(collection_path, config):
    """Skip the scrap directory during collection.

    Modern (pytest >= 7) signature: ``collection_path`` is a
    :class:`pathlib.Path`. Returning ``True`` ignores the path.
    """
    return str(_SCRAP_DIR) in str(collection_path)


def _mongo_available(*, timeout_ms=500):
    """Return ``True`` iff a MongoDB server answers a ping quickly.

    Probes ``DFLT_TEST_HOST`` by default; set ``MONGODOL_MONGO_PROBE_HOST`` to
    point the probe elsewhere (e.g. to validate the skip behaviour, or to an
    external server) without touching the tests' own connection defaults.
    """
    host = os.environ.get("MONGODOL_MONGO_PROBE_HOST", DFLT_TEST_HOST)
    try:
        from pymongo import MongoClient

        client = MongoClient(host, serverSelectionTimeoutMS=timeout_ms)
        try:
            client.admin.command("ping")
        finally:
            client.close()
        return True
    except Exception:
        return False


def _requires_mongo(item):
    """Whether a collected item needs a live MongoDB to run.

    Every test under ``mongodol/tests`` exercises real collections, and every
    package-module doctest except those in :data:`_MONGO_FREE_DOCTEST_STEMS`
    demonstrates a live store.
    """
    path = pathlib.Path(str(getattr(item, "path", None) or item.fspath))
    if "tests" in path.parts:
        return True
    return path.stem not in _MONGO_FREE_DOCTEST_STEMS


def pytest_collection_modifyitems(config, items):
    """Skip MongoDB-dependent items when no server is reachable."""
    if _mongo_available():
        return
    skip_no_mongo = pytest.mark.skip(
        reason="No live MongoDB reachable; skipping MongoDB-dependent items"
    )
    for item in items:
        if _requires_mongo(item):
            item.add_marker(skip_no_mongo)
