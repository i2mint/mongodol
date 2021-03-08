from mongodol.base import ID, MongoCollectionCollection
from mongodol.tests.util import clear_all_and_populate, get_test_collection_persister
from mongodol.tests.data import feature_cube


def test_mongo_collection_collection(mongo_collection_collection_cls=MongoCollectionCollection):
    persister = get_test_collection_persister()
    clear_all_and_populate(persister, feature_cube)

    mgc = persister._mgc
    s = mongo_collection_collection_cls(mgc=mgc)
    assert len(s) == 7
    assert list(s) == [
        {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}},
        {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID: 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
        {ID: 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
        {ID: 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}},
        {ID: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}}
    ]
    assert s.head() == {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}}

    # Test filter

    s = mongo_collection_collection_cls(mgc=mgc, filter={'color': 'blue'})
    assert len(s) == 3
    assert list(s) == [
        {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}}
    ]
    assert s.head() == {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}}

    assert {ID: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}} in s
    assert {ID: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}} not in s
    assert {'this': 'is', 'complete': 'nonsense'} not in s

    # Test the __repr__/__str__
    assert (str(s) ==
            "MongoCollectionCollection(mgc=<py2store/mongodol_test>, filter={'color': 'blue'}, projection=None)")

    # Test skip and limit

    s = mongo_collection_collection_cls(mgc=mgc, skip=2, limit=3)
    assert len(s) == 3
    assert list(s) == [
        {'_id': 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
        {'_id': 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
        {'_id': 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}}]
    assert s.head() == next(iter(list(s)))


def test_mongo_collection_reader_without_test_data_dependencies(collection_kvreader_cls=None):
    """Tests reader functionality that doesn't need specific data to be injected in the data base."""
    if collection_kvreader_cls is None:
        from mongodol import MongoCollectionReader as collection_kvreader_cls

    from pymongo import MongoClient
    s = collection_kvreader_cls(MongoClient()['py2store']['test'])
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

    fake_value = {'data': "this does not exist"}
    assert not (fake_value in s.values())  # Note: not (... in ...) instead of (... not in ...) to test "in" directly

    fake_item = (fake_key, fake_value)
    assert fake_item not in s.items()

    # Note though that since keys and values are both dictionaries in mongo, some of these set-like functionalities
    # might not work (complaints such as ``TypeError: unhashable type: 'dict'``),
    # such as:

    # s.keys() | a_list_of_fake_keys
    # Traceback (most recent call last):
    #     ...
    # TypeError: unhashable type: 'dict'

    # But you can take care of that in higher level wrappers that have hashable keys and/or values.


