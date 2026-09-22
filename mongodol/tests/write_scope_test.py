"""Writes stay within a store's scope filter; write/delete keys can't carry operators.

These tests use a recording stand-in for the pymongo collection, so they need no
server (but live under ``tests/``, which conftest deselects without one).
"""

import re

import pytest
from bson.regex import Regex
from pymongo import MongoClient

from mongodol.base import MongoCollectionPersister, operator_field_names
from mongodol.stores import MongoCollectionMultipleDocsPersister


class _RecordingCollection:
    """Records every call made to it; no server involved."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def method(*args, **kwargs):
            self.calls.append((name, args, kwargs))

        return method


def _store(cls=MongoCollectionPersister, **kwargs):
    # A real (never connected) collection to construct with, then swap in the recorder.
    unconnected = MongoClient("mongodb://localhost:1", connect=False)["db"]["c"]
    s = cls(unconnected, **kwargs)
    mgc = _RecordingCollection()
    # dol-wrapped classes (e.g. via wrap_kvs) hold the persister in ``.store``
    getattr(s, "store", s).mgc = mgc
    return s, mgc


def test_operator_field_names():
    assert operator_field_names({"a": {"$ne": 1}}) == ["$ne"]
    assert operator_field_names({"$or": [{"a": 1}, {"b": {"$gt": 2}}]}) == [
        "$or",
        "$gt",
    ]
    assert operator_field_names({"a": [1, {"b": 2}]}) == []


@pytest.mark.parametrize(
    "bad_key",
    [
        {"_id": {"$ne": None}},
        {"$or": [{"_id": 1}, {"_id": 2}]},
        {"_id": re.compile(".*")},
        {"name": Regex("")},
    ],
)
def test_write_and_delete_refuse_operator_keys(bad_key):
    s, mgc = _store(filter={"tenant": "a"})
    with pytest.raises(ValueError, match="query operators"):
        s[bad_key] = {"x": 1}
    with pytest.raises(ValueError, match="query operators"):
        del s[bad_key]
    assert mgc.calls == []


@pytest.mark.parametrize("bad_key", [{"_id": {"$exists": True}}, {"g": re.compile("")}])
def test_multiple_docs_setitem_refuses_operator_keys_before_delete_many(bad_key):
    s, mgc = _store(MongoCollectionMultipleDocsPersister, filter={"tenant": "a"})
    with pytest.raises(ValueError, match="query operators"):
        s[bad_key] = [{"x": 1}]
    assert mgc.calls == []


def test_multiple_docs_setitem_validates_values_before_delete_many():
    s, mgc = _store(MongoCollectionMultipleDocsPersister, filter={"tenant": "a"})
    with pytest.raises(ValueError, match="contradicts"):
        s[{"g": 1}] = [{"x": 1}, {"x": 2, "tenant": "b"}]
    assert mgc.calls == []
    s[{"g": 1}] = [{"x": 1}]
    assert [name for name, *_ in mgc.calls] == ["delete_many", "insert_many"]


def test_class_attribute_opt_in():
    class Permissive(MongoCollectionPersister):
        allow_operators_in_write_keys = True

    s, mgc = _store(Permissive)
    del s[{"_id": {"$ne": None}}]
    assert [name for name, *_ in mgc.calls] == ["delete_one"]


def test_operator_keys_allowed_with_explicit_opt_in():
    s, mgc = _store(filter={"tenant": "a"}, allow_operators_in_write_keys=True)
    del s[{"_id": {"$ne": None}}]
    assert [name for name, *_ in mgc.calls] == ["delete_one"]


@pytest.mark.parametrize(
    "k, v",
    [({"_id": 1, "tenant": "b"}, {"x": 1}), ({"_id": 1}, {"x": 1, "tenant": "b"})],
)
def test_contradicting_scope_field_is_refused(k, v):
    s, mgc = _store(filter={"tenant": "a"})
    with pytest.raises(ValueError, match="contradicts"):
        s[k] = v
    with pytest.raises(ValueError, match="contradicts"):
        s.append(dict(k, **v))
    assert mgc.calls == []


def test_consistent_writes_unchanged():
    s, mgc = _store(filter={"tenant": "a"})
    s[{"_id": 1, "tenant": "a"}] = {"x": 1}
    s[{"_id": 2}] = {"x": 2}
    (name, _, kwargs), (_, _, kwargs2) = mgc.calls
    assert name == "replace_one"
    assert kwargs["replacement"] == {"tenant": "a", "_id": 1, "x": 1}
    assert kwargs["filter"] == {"$and": [{"tenant": "a"}, {"_id": 1, "tenant": "a"}]}
    assert kwargs2["replacement"] == {"tenant": "a", "_id": 2, "x": 2}


def test_on_write_filter_is_the_write_scope():
    s, mgc = _store(filter={"tenant": {"$in": ["a", "b"]}}, on_write_filter={"tenant": "a"})
    s.append({"x": 1})
    assert mgc.calls[0][1][0] == {"tenant": "a", "x": 1}
    with pytest.raises(ValueError, match="contradicts"):
        s.append({"x": 1, "tenant": "b"})


@pytest.mark.parametrize(
    "scope, v",
    [
        ({"active": True}, {"active": 1}),  # equal in Python, not in MongoDB
        ({"m": {"a": 1, "b": 2}}, {"m": {"b": 2, "a": 1}}),  # field order matters
    ],
)
def test_contradiction_uses_mongodb_equality(scope, v):
    s, mgc = _store(filter=scope)
    with pytest.raises(ValueError, match="contradicts"):
        s.append(v)
    assert mgc.calls == []


def test_operator_scope_writes():
    s, mgc = _store(filter={"tenant": {"$in": ["a", "b"]}})
    s[{"_id": 1, "tenant": "a"}] = {"x": 1}  # inside the scope: allowed, as before
    assert mgc.calls[0][2]["replacement"]["tenant"] == "a"
    with pytest.raises(ValueError, match="contradicts"):
        s[{"_id": 2, "tenant": "c"}] = {"x": 1}


def test_dotted_scope_fields_refuse_writes():
    s, mgc = _store(filter={"org.id": "a"})
    with pytest.raises(ValueError, match="dotted"):
        s.append({"x": 1})
    assert mgc.calls == []


def test_on_write_filter_confines_replace_and_delete():
    s, mgc = _store(on_write_filter={"tenant": "a"})
    del s[{"_id": 1}]
    s[{"_id": 2}] = {"x": 1}
    (_, delete_args, _), (_, _, replace_kwargs) = mgc.calls
    assert delete_args[0] == {"$and": [{}, {"tenant": "a"}, {"_id": 1}]}
    assert replace_kwargs["filter"] == {"$and": [{}, {"tenant": "a"}, {"_id": 2}]}
