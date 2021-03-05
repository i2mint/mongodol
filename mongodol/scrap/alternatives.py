from collections.abc import KeysView, ValuesView, ItemsView
from typing import Mapping, Union, Iterable, Optional
from functools import cached_property

from py2store import KvReader
from mongodol.base import (
    MongoCollectionCollection,
    end_of_cursor,
    PyMongoCollectionSpec,
    get_mongo_collection_pymongo_obj
)


def get_key_value_specs(key_fields, data_fields):
    if isinstance(key_fields, str):
        key_fields = (key_fields,)
    if data_fields is None:
        pass
    key_projection = {k: True for k in key_fields}
    if "_id" not in key_fields:
        key_projection.update(
            _id=False
        )  # need to explicitly specify this since mongo includes _id by dflt
    if data_fields is None:
        data_fields = {k: False for k in key_fields}
        items_projection = None
    elif not isinstance(data_fields, dict):
        data_fields = {k: True for k in data_fields}
        if "_id" not in data_fields:
            data_fields["_id"] = False
        items_projection = (
                {k for k, v in data_fields.items() if v}
                | {k for k, v in key_projection.items() if v}
        )
    return key_fields, data_fields, key_projection, items_projection


class MongoCollectionReader(MongoCollectionCollection, KvReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

    >>> from mongodol import MongoCollectionReader
    >>> from pymongo import MongoClient
    >>> s = MongoCollectionReader(MongoClient()['py2store']['test'])
    >>> list_of_keys = list(s)
    >>> fake_key = {'_id': 'this key does not exist'}
    >>> fake_key in s
    False

    ``s.keys()``, ``s.values()``, and ``s.items()`` are ``collections.abc.MappingViews`` instances
    (specialized for mongo).

    >>> type(s.keys())
    <class 'collections.abc.KeysView'>
    >>> type(s.values())
    <class 'mongodol.base.MongoValuesView'>
    >>> type(s.items())
    <class 'mongodol.base.MongoItemsView'>

    Recall that ``collections.abc.MappingViews`` have many set-like functionalities:

    >>> fake_key in s.keys()
    False
    >>> a_list_of_fake_keys = [{'_id': 'fake_key'}, {'_id': 'yet_another'}]
    >>> s.keys().isdisjoint(a_list_of_fake_keys)
    True
    >>> s.keys() & a_list_of_fake_keys
    set()
    >>> fake_value = {'data': "this does not exist"}
    >>> fake_value in s.values()
    False
    >>> fake_item = (fake_key, fake_value)
    >>> fake_item in s.items()
    False

    Note though that since keys and values are both dictionaries in mongo, some of these set-like functionalities
    might not work (complaints such as ``TypeError: unhashable type: 'dict'``),
    such as:

    >>> s.keys() | a_list_of_fake_keys
    Traceback (most recent call last):
        ...
    TypeError: unhashable type: 'dict'

    But you can take care of that in higher level wrappers that have hashable keys and/or values.

    """

    def __init__(self,
                 mgc: Union[PyMongoCollectionSpec, KvReader] = None,
                 filter: Optional[dict] = None,
                 key_fields=("_id",),
                 data_fields: Optional[Iterable] = None):
        self._mgc = get_mongo_collection_pymongo_obj(mgc)
        key_fields, data_fields, key_projection, items_projection = get_key_value_specs(key_fields, data_fields)
        self._data_fields = data_fields
        self._key_fields = key_fields
        self._key_projection = key_projection
        self._items_projection = items_projection

        self.filter = filter or {}

    # @cached_property
    # def _key_projection(self):
    #     return {}
    #
    # @cached_property
    # def _items_projection(self):
    #     return {}

    def __getitem__(self, k):
        assert isinstance(k, Mapping), \
            f"k (key) must be a mapping (typically a dictionary). Was:\n\tk={k}"
        return self._mgc.find(filter=self._merge_with_filt(k), projection=self._data_fields)

    def __iter__(self):
        yield from self._mgc.find(filter=self.filter, projection=self._key_projection)

    def keys(self):
        return KeysView(self)

    def items(self):
        return MongoItemsView(self)

    def values(self):
        return MongoValuesView(self)


class MongoValuesView(ValuesView):

    def __contains__(self, v):
        m = self._mapping
        cursor = m._mgc.find(filter=m._merge_with_filt(v), projection=())
        return next(cursor, end_of_cursor) is not end_of_cursor

    def __iter__(self):
        m = self._mapping
        yield from m._mgc.find(filter=m.filter, projection=m._data_fields)


class MongoItemsView(ItemsView):

    def __contains__(self, item):
        m = self._mapping
        k, v = item
        # TODO: How do we have cursor return no data (here still has _id)
        cursor = m._mgc.find(filter=dict(v, **m._merge_with_filt(k)), projection=())
        # return cursor
        return next(cursor, end_of_cursor) is not end_of_cursor

    def __iter__(self):
        m = self._mapping
        for doc in m._mgc.find(filter=m.filter, projection=m._items_projection):
            key = {k: doc.pop(k) for k in m._key_fields}
            yield key, doc
