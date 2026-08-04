"""Tests for the ``Mapping``-contract invariants of mongo store views.

The invariants pinned here are the ones every ``Mapping`` owes its user::

    list(store.values()) == [store[k] for k in store]
    list(store.items())  == [(k, store[k]) for k in store]

They are easy to break in mongodol because its views take a *bulk-read* fast path
(one ``find`` for the whole collection instead of one per key) and, before
:mod:`mongodol.views`, that fast path punched through any ``dol`` wrapper -- see
`i2mint/mongodol#7 <https://github.com/i2mint/mongodol/issues/7>`_.
"""

from operator import itemgetter

import pytest
from dol import filt_iter, wrap_kvs

from mongodol.base import MongoBaseStore, MongoCollectionReader
from mongodol.stores import (
    MongoCollectionFirstDocPersister,
    MongoCollectionMultipleDocsPersister,
)
from mongodol.tests.util import get_test_collection_object
from mongodol.views import (
    NoBulkReadPath,
    bulk_values,
    is_crossable,
    provides_bulk_read,
    resolve_bulk_source,
)

#: Collection used by this module. Its own, so parallel modules can't disturb it.
TEST_COLLECTION_NAME = "views_test"

#: Docs written to the test collection before each test.
TEST_DOCS = (
    {"_id": "123", "name": "Matthew", "age": 42},
    {"_id": "456", "name": "Mark", "age": 43},
)


def assert_mapping_view_invariants(store):
    """Assert that ``store``'s views agree with its ``__iter__``/``__getitem__``."""
    keys = list(store)
    assert list(store.keys()) == keys
    assert list(store.values()) == [store[k] for k in keys]
    assert list(store.items()) == list(zip(keys, (store[k] for k in keys)))


@pytest.fixture
def store():
    """A populated ``MongoCollectionFirstDocPersister`` over a dedicated collection."""
    mgc = get_test_collection_object(collection_name=TEST_COLLECTION_NAME)
    s = MongoCollectionFirstDocPersister(mgc)
    for k in list(s):
        del s[k]
    for doc in TEST_DOCS:
        s[{"_id": doc["_id"]}] = {k: v for k, v in doc.items() if k != "_id"}
    return s


# --------------------------------------------------------------------------------------
# The issue #7 reproduction


def test_wrap_kvs_value_trans_reaches_values_and_items(store):
    """``dol.wrap_kvs`` value transforms must show up in ``values()``/``items()``.

    This is the i2mint/mongodol#7 reproduction: before the fix, ``list(ss.values())``
    returned the raw, untransformed mongo documents.
    """
    ss = wrap_kvs(store, obj_of_data=itemgetter("name", "age"))

    assert [ss[k] for k in ss] == [("Matthew", 42), ("Mark", 43)]
    assert list(ss.values()) == [("Matthew", 42), ("Mark", 43)]
    assert [v for _, v in ss.items()] == [("Matthew", 42), ("Mark", 43)]
    assert_mapping_view_invariants(ss)


def test_wrap_kvs_key_trans_reaches_items(store):
    """``dol.wrap_kvs`` key transforms must show up in ``keys()``/``items()``.

    Only the *keys* are checked against ``__iter__`` here: with no
    ``getitem_projection``, ``items()`` values still differ from ``store[k]`` by the
    key fields -- see ``test_items_values_equal_getitem_values_when_no_getitem_projection``.
    """
    ss = wrap_kvs(store, key_of_id=itemgetter("_id"), id_of_key=lambda k: {"_id": k})

    assert list(ss) == list(ss.keys()) == ["123", "456"]
    assert [k for k, _ in ss.items()] == ["123", "456"]
    assert list(ss.values()) == [ss[k] for k in ss]


def test_stacked_wrappers(store):
    """Every layer of a wrapper stack must contribute to the bulk stream."""
    ss = wrap_kvs(store, obj_of_data=itemgetter("name", "age"))
    sss = wrap_kvs(ss, obj_of_data=lambda name_and_age: name_and_age[0].upper())

    assert list(sss.values()) == ["MATTHEW", "MARK"]
    assert_mapping_view_invariants(sss)


def test_mongodol_wrap_kvs_still_honours_transforms(store):
    """The historical ``MongoBaseStore``-based wrapper keeps working."""
    ss = wrap_kvs(store, wrapper=MongoBaseStore, obj_of_data=itemgetter("name", "age"))

    assert list(ss.values()) == [("Matthew", 42), ("Mark", 43)]
    assert_mapping_view_invariants(ss)


# --------------------------------------------------------------------------------------
# Fallback: layers whose transforms can't be pushed onto a bulk stream


def test_filtered_store_falls_back_to_the_per_key_path(store):
    """``filt_iter`` changes the key set, so the bulk stream must not be used."""
    ss = filt_iter(store, filt=lambda k: k["_id"] == "123")

    with pytest.raises(NoBulkReadPath):
        bulk_values(ss)
    assert list(ss.values()) == [{"_id": "123", "name": "Matthew", "age": 42}]
    assert_mapping_view_invariants(ss)


def test_postget_wrapper_falls_back_to_the_per_key_path(store):
    """A user-supplied ``postget`` isn't expressible on the bulk stream."""
    ss = wrap_kvs(store, postget=lambda k, v: (k["_id"], v["name"]))

    with pytest.raises(NoBulkReadPath):
        bulk_values(ss)
    assert list(ss.values()) == [("123", "Matthew"), ("456", "Mark")]
    assert_mapping_view_invariants(ss)


def test_multiple_docs_store_uses_the_per_key_path():
    """``MongoCollectionMultipleDocsPersister`` values are *lists* of docs.

    Its inherited bulk stream yields single docs, so it declares itself
    bulk-unfaithful (``disable_bulk_read``) and the views take the per-key path.
    """
    mgc = get_test_collection_object(collection_name=TEST_COLLECTION_NAME)
    s = MongoCollectionMultipleDocsPersister(mgc)
    for k in list(s):
        del s[k]
    s[{"_id": "123"}] = {"name": "Matthew", "age": 42}
    s[{"_id": "456"}] = {"name": "Mark", "age": 43}

    assert s[{"_id": "123"}] == [{"_id": "123", "name": "Matthew", "age": 42}]
    with pytest.raises(NoBulkReadPath):
        bulk_values(s)
    assert_mapping_view_invariants(s)


# --------------------------------------------------------------------------------------
# Containment (``v in store.values()``, ``item in store.items()``)


def test_containment_through_a_non_invertible_value_trans(store):
    """A value transform with no declared inverse must not be pushed down to mongo.

    Before the fix this raised a pymongo ``OperationFailure`` (a tuple was handed
    to ``find`` as a filter).
    """
    ss = wrap_kvs(store, obj_of_data=itemgetter("name", "age"))

    assert ("Matthew", 42) in ss.values()
    assert ("Nobody", 0) not in ss.values()
    assert ({"_id": "123"}, ("Matthew", 42)) in ss.items()
    assert ({"_id": "123"}, ("Nobody", 0)) not in ss.items()


def test_containment_uses_the_bulk_path_when_the_trans_is_invertible(store):
    """With both directions declared, containment stays a single mongo query."""
    ss = wrap_kvs(
        store,
        obj_of_data=lambda d: dict(d, name=d["name"].upper()),
        data_of_obj=lambda d: dict(d, name=d["name"].capitalize()),
    )

    assert {"_id": "123", "name": "MATTHEW", "age": 42} in ss.values()
    assert {"_id": "123", "name": "NOBODY", "age": 42} not in ss.values()


# --------------------------------------------------------------------------------------
# The resolver's own contract


def test_unwrapped_reader_is_its_own_bulk_source():
    """No wrapper: the reader itself provides the fast path, nothing to cross."""
    mgc = get_test_collection_object(collection_name=TEST_COLLECTION_NAME)
    s = MongoCollectionReader(mgc)

    source, layers = resolve_bulk_source(s, "iter_values")
    assert source is s
    assert layers == []
    assert provides_bulk_read(s, "iter_values")
    assert not is_crossable(s)


def test_transform_only_wrapper_is_crossed_not_used_as_a_source(store):
    """A plain ``wrap_kvs`` layer is crossed on the way to the backend fast path."""
    ss = wrap_kvs(store, obj_of_data=itemgetter("name"))

    assert is_crossable(ss)
    assert not provides_bulk_read(ss, "iter_values")
    source, layers = resolve_bulk_source(ss, "iter_values")
    assert source is store
    assert layers[0] is ss  # dol may insert further pass-through layers behind it
    assert all(map(is_crossable, layers))


def test_bulk_source_lookup_ignores_store_attribute_delegation(store):
    """``hasattr`` lies on a ``Store``; ``provides_bulk_read`` must not.

    ``Store.__getattr__`` forwards to the wrapped store, so a wrapper *looks* like
    it implements the bulk protocol. This is precisely what made issue #7 silent.
    """
    ss = wrap_kvs(store, obj_of_data=itemgetter("name"))

    assert hasattr(ss, "iter_values")  # ...only because of delegation
    assert not provides_bulk_read(ss, "iter_values")


@pytest.mark.xfail(
    reason=(
        "Separate, pre-existing bug: MongoCollectionReader.iter_items pops the key "
        "fields out of the value even when getitem_projection is None, so items() "
        "values lack '_id' while store[k] has it. Fixing it changes behaviour that "
        "tests/int_tests/base_int_test.py explicitly encodes."
    ),
    strict=True,
)
def test_items_values_equal_getitem_values_when_no_getitem_projection(store):
    """``items()`` values must equal ``store[k]``, key fields included."""
    assert list(store.items()) == [(k, store[k]) for k in store]
