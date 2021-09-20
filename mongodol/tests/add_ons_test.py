from dol.util import has_enabled_clear_method

from mongodol import MongoCollectionPersister
from mongodol.stores import MongoStore
from mongodol.tests.util import populated_pymongo_collection
from mongodol.tests import data

from mongodol.add_ons import add_clear_method

# FIXME: calling "add_clear_method" on an instance is not working because the Store class already has a "clear" method.
# def test_clear_on_feature_cube(store_cls=None):
#     store_cls = store_cls or MongoCollectionPersister

#     whole_store = store_cls(populated_pymongo_collection(data.feature_cube))
#     whole_length_before_clear = len(whole_store)
#     assert whole_length_before_clear == 7
#     reds = MongoCollectionPersister(whole_store.mgc, filter={'color': 'red'})
#     n_reds = len(reds)
#     assert n_reds == 4
#     assert list(reds.values()) == [
#         {'_id': 1, 'number': 6, 'color': 'red', 'dims': {'x': 2, 'y': 3}},
#         {'_id': 3, 'number': 10, 'color': 'red', 'dims': {'x': 2, 'y': 5}},
#         {'_id': 4, 'number': 10, 'color': 'red', 'dims': {'x': 5, 'y': 2}},
#         {'_id': 5, 'number': 15, 'color': 'red', 'dims': {'x': 3, 'y': 5}}
#     ]

#     # ``reds`` doesn't have a clear method

#     assert not has_enabled_clear_method(reds)

#     # So let's give it one

#     reds_with_clear = add_clear_method(reds)
#     assert has_enabled_clear_method(reds_with_clear)

#     # And it's one that works too!

#     r = reds_with_clear.clear()
#     assert len(reds_with_clear) == 0

#     # It's the data that was deleted, not just the view. See what reds and whole_store say:

#     assert len(reds) == 0
#     assert len(whole_store) == whole_length_before_clear - n_reds == 3
