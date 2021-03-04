from mongodol.base import ID_KEY, MongoCollectionCollection
from mongodol.tests.util import clear_all_and_populate, get_test_collection_persister
from mongodol.tests.data import feature_cube


def test_mongo_collection_collection():
    persister = get_test_collection_persister()
    clear_all_and_populate(persister, feature_cube)

    mgc = persister._mgc
    s = MongoCollectionCollection(mgc=mgc)
    assert len(s) == 7
    assert list(s) == [
        {ID_KEY: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}},
        {ID_KEY: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID_KEY: 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
        {ID_KEY: 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
        {ID_KEY: 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}},
        {ID_KEY: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID_KEY: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}}
    ]

    s = MongoCollectionCollection(mgc=mgc, filter={'color': 'blue'})
    assert len(s) == 3
    assert list(s) == [
        {ID_KEY: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}},
        {ID_KEY: 6, 'number': 15, 'color': 'blue', 'dims': {'x': 3, 'y': 5}},
        {ID_KEY: 7, 'number': 15, 'color': 'blue', 'dims': {'x': 5, 'y': 3}}
    ]

    assert {ID_KEY: 2, 'number': 6, 'color': 'blue', 'dims': {'x': 3, 'y': 2}} in s
    assert {ID_KEY: 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}} not in s
    assert {'this': 'is', 'complete': 'nonsense'} not in s

    # Test the __repr__/__str__
    assert (str(s) ==
            "MongoCollectionCollection(mgc=<py2store/mongodol_test>, filter={'color': 'blue'}, projection=None)")
