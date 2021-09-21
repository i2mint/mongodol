"""
Remember to test FEATURES, not OBJECTS!!

"""
from mongodol.constants import DFLT_TEST_DB
from mongodol.base import ID, MongoCollectionCollection, MongoCollectionReader
from mongodol.tests.util import (
    clear_all_and_populate,
    get_test_collection_persister,
)
from mongodol.tests.data import feature_cube


def test_mongo_collection_collection(
    mongo_collection_collection_cls=MongoCollectionCollection,
):
    persister = get_test_collection_persister()
    clear_all_and_populate(feature_cube, persister)

    mgc = persister.mgc
    s = mongo_collection_collection_cls(mgc=mgc)
    assert len(s) == 7
    assert list(s) == [
        {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}},
        {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID: 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
        {ID: 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
        {ID: 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}},
        {ID: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}},
    ]
    assert s.head() == {
        ID: 1,
        'number': 6,
        'color': 'red',
        'dims': {'x': 2, 'y': 3},
    }

    # Test filter

    s = mongo_collection_collection_cls(mgc=mgc, filter={'color': 'blue'})
    assert len(s) == 3
    assert list(s) == [
        {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}},
    ]
    assert s.head() == {
        ID: 2,
        'number': 6,
        'color': 'blue',
        'dims': {'x': 3, 'y': 2},
    }

    assert {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}} in s
    assert {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3},} not in s
    assert {'this': 'is', 'complete': 'nonsense'} not in s

    # Test the __repr__/__str__
    assert (
        str(s)
        == "MongoCollectionCollection(mgc=<mongodol/mongodol_test>, filter={'color': 'blue'}, iter_projection=None)"
    )

    # Test skip and limit

    s = mongo_collection_collection_cls(mgc=mgc, skip=2, limit=3)
    assert len(s) == 3
    assert list(s) == [
        {'_id': 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
        {'_id': 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
        {'_id': 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}},
    ]
    assert s.head() == next(iter(list(s)))


def test_mongo_collection_reader_without_test_data_dependencies(
    collection_kvreader_cls=None,
):
    """Tests reader functionality that doesn't need specific data to be injected in the data base."""
    if collection_kvreader_cls is None:
        from mongodol import MongoCollectionReader as collection_kvreader_cls

    from pymongo import MongoClient

    s = collection_kvreader_cls(MongoClient()[DFLT_TEST_DB]['test'])
    list_of_keys = list(s)
    fake_key = {'_id': 'this key does not exist'}
    assert fake_key not in s

    # ``s.keys()``, ``s.values()``, and ``s.items()`` are ``collections.abc.MappingViews`` instances
    # (specialized for mongo).
    from collections.abc import KeysView
    from mongodol.base import MongoValuesView, MongoItemsView

    assert isinstance(s.keys(), KeysView)
    assert isinstance(s.values(), MongoValuesView)
    assert isinstance(s.items(), MongoItemsView)

    # Recall that ``collections.abc.MappingViews`` have many set-like functionalities:

    assert fake_key not in s.keys()

    a_list_of_fake_keys = [{'_id': 'fake_key'}, {'_id': 'yet_another'}]
    assert s.keys().isdisjoint(a_list_of_fake_keys)

    assert s.keys() & a_list_of_fake_keys == set()

    fake_value = {'data': 'this does not exist'}
    assert not (
        fake_value in s.values()
    )  # Note: not (... in ...) instead of (... not in ...) to test "in" directly

    fake_item = (fake_key, fake_value)
    assert fake_item not in s.items()

    assert isinstance(len(s), int)

    # Note though that since keys and values are both dictionaries in mongo, some of these set-like functionalities
    # might not work (complaints such as ``TypeError: unhashable type: 'dict'``),
    # such as:

    # s.keys() | a_list_of_fake_keys
    # Traceback (most recent call last):
    #     ...
    # TypeError: unhashable type: 'dict'

    # But you can take care of that in higher level wrappers that have hashable keys and/or values.


def test_basic_mongo_kvreader_functionality(mongo_reader_cls=MongoCollectionReader,):
    test_persister = get_test_collection_persister()
    clear_all_and_populate(feature_cube[:4], test_persister)
    test_mgc = test_persister.mgc

    # By default, we get ``{"_id":...}`` as keys, and the full contents of the mongo docs as values

    s = mongo_reader_cls(test_mgc,)
    assert list(s) == list(s.keys()) == [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}]
    assert list(s.values()) == [
        {'_id': 1, 'color': 'red', 'dims': {'x': 2, 'y': 3}, 'number': 6},
        {'_id': 2, 'color': 'blue', 'dims': {'x': 3, 'y': 2}, 'number': 6},
        {'_id': 3, 'color': 'red', 'dims': {'x': 2, 'y': 5}, 'number': 10},
        {'_id': 4, 'color': 'red', 'dims': {'x': 5, 'y': 2}, 'number': 10},
    ]

    assert s.distinct('color') == ['blue', 'red']
    assert s.distinct('dims.x') == [2, 3, 5]

    # This default behavior is equivalent to the following settings:

    s = mongo_reader_cls(
        test_mgc,
        filter={},
        iter_projection=(ID,),
        getitem_projection=None,  # meaning "keep everything (including ID)"
    )
    assert list(s) == list(s.keys()) == [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}]
    assert list(s.values()) == [
        {'_id': 1, 'color': 'red', 'dims': {'x': 2, 'y': 3}, 'number': 6},
        {'_id': 2, 'color': 'blue', 'dims': {'x': 3, 'y': 2}, 'number': 6},
        {'_id': 3, 'color': 'red', 'dims': {'x': 2, 'y': 5}, 'number': 10},
        {'_id': 4, 'color': 'red', 'dims': {'x': 5, 'y': 2}, 'number': 10},
    ]

    # Let's ask to NOT have the ``"_id"`` field in the values.
    # The ``getitem_projection`` argument is what controls what we get as values.
    # It follows the ``pymongo`` ``projection`` argument language as is, so see ``pymongo`` documentation for details.

    s = mongo_reader_cls(test_mgc, getitem_projection={ID: False})
    assert list(s) == list(s.keys()) == [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}]
    assert list(s.values()) == [
        {'color': 'red', 'dims': {'x': 2, 'y': 3}, 'number': 6},
        {'color': 'blue', 'dims': {'x': 3, 'y': 2}, 'number': 6},
        {'color': 'red', 'dims': {'x': 2, 'y': 5}, 'number': 10},
        {'color': 'red', 'dims': {'x': 5, 'y': 2}, 'number': 10},
    ]

    # Let's ask to only have the ``"color"`` field in the values.
    # Note that for "_id" (==ID) we need to explicitly ask to NOT have it, or we'll get it by default

    s = mongo_reader_cls(test_mgc, getitem_projection={ID: False, 'color': True})
    assert list(s) == list(s.keys()) == [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}]
    assert list(s.values()) == [
        {'color': 'red'},
        {'color': 'blue'},
        {'color': 'red'},
        {'color': 'red'},
    ]

    # See that if we ask for ``color=False`` what we actually get is... everything BUT color

    s = mongo_reader_cls(test_mgc, getitem_projection={ID: False, 'color': False})
    assert list(s) == list(s.keys()) == [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}]
    assert list(s.values()) == [
        {'dims': {'x': 2, 'y': 3}, 'number': 6},
        {'dims': {'x': 3, 'y': 2}, 'number': 6},
        {'dims': {'x': 2, 'y': 5}, 'number': 10},
        {'dims': {'x': 5, 'y': 2}, 'number': 10},
    ]

    # Let's specify a different key now: Namely, let's use {color:, number:} pairs as keys, and just {dims:} as values
    # You control what you get as keys with the iter_projection (that, again, follows the pymongo specs of projection).

    s = mongo_reader_cls(
        test_mgc,
        iter_projection={ID: False, 'color': True, 'number': True},
        getitem_projection={ID: False, 'dims': True},
    )

    assert (
        list(s)
        == list(s.keys())
        == [
            {'color': 'red', 'number': 6},
            {'color': 'blue', 'number': 6},
            {'color': 'red', 'number': 10},
            {'color': 'red', 'number': 10},
        ]
    )

    assert list(s.values()) == [
        {'dims': {'x': 2, 'y': 3}},
        {'dims': {'x': 3, 'y': 2}},
        {'dims': {'x': 2, 'y': 5}},
        {'dims': {'x': 5, 'y': 2}},
    ]

    # See that you can retrieve values that match that key, as such:

    assert list(s[{'color': 'red', 'number': 6}]) == [{'dims': {'x': 2, 'y': 3}}]

    # The list(...) is needed because in fact, ``s[key]`` gives you a mongo CURSOR, that needs to be "consumed".

    from pymongo.cursor import Cursor

    assert isinstance(s[{}], Cursor)

    # Note that if several values match, you'll get all of them when you consume the cursor

    assert list(s[{'color': 'red', 'number': 10}]) == [
        {'dims': {'x': 2, 'y': 5}},
        {'dims': {'x': 5, 'y': 2}},
    ]

    # Also, see that if you specify a key that doesn't exist, you'll just get empty data
    #  (no complaints that the key doesn't exist)

    assert list(s[{'color': 'PINK', 'number': -42}]) == []

    # In fact, your key doesn't even have to match the "schema" expected for keys.
    # Any dict (but it does have to be a dict!) will do. It just might not match anything

    assert list(s[{'any': 'key', 'template': 'you', 'want': True}]) == []

    # This is often not a desirable behavior. Instead, one often wants to get a KeyError if a key is not valid.
    # But that concern is a separate one, and as such, the ability to validate keys has been separated.
    # You can easily slap such a key validation layer on though -- no worries!
