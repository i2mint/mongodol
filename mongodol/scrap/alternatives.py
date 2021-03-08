from collections.abc import KeysView, ValuesView, ItemsView
from typing import Mapping, Union, Iterable, Optional
from functools import cached_property, partial
from operator import or_

from py2store import KvReader
from linkup import key_aligned_val_op_with_forced_defaults

from mongodol.base import (
    ID_KEY,
    MongoCollectionCollection,
    end_of_cursor,
    PyMongoCollectionSpec,
    get_mongo_collection_pymongo_obj
)

ProjectionDict = dict  # TODO: Specify that keys are strings and values are boolean


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


def flatten_dict_items(d, prefix=''):
    """
    Computes a "flat" dict from a nested one. A flat dict's keys are the dot-paths of the input dict.

    :param d: a nested dict
    :param prefix: A string to prepend on all the paths
    :return: A flat dict

    >>> d = {'a': {
    ...         'a': '2a',
    ...         'c': {'a': 'aca', 'u': 4}
    ...         },
    ...      'c': 3
    ...     }
    >>> dict(flatten_dict_items(d))
    {'a.a': '2a', 'a.c.a': 'aca', 'a.c.u': 4, 'c': 3}
    """
    for k, v in d.items():
        if not isinstance(v, dict):
            yield prefix + k, v
        else:
            yield from flatten_dict_items(v, prefix + k + '.')


merge_projection_dicts = partial(key_aligned_val_op_with_forced_defaults,
                                 op=or_, dflt_val_for_x=True, dflt_val_for_y=True)


def normalize_projection(projection):
    """Normalize projection specification to be an explicit list of flattened dict of {path.to.key: True/False,...
    (or None if projection is None to start with)"""
    if projection is None:
        return None
    elif not isinstance(projection, dict):
        if isinstance(projection, str):
            projection = (projection,)
        elif isinstance(projection, Iterable) and len(projection) == 0:
            return projection  # it's probably the empty projection
        elif projection is None:
            projection = None
        projection = dict({field: True for field in projection})  # TODO: , **{ID_KEY: True}) ?

    return dict(flatten_dict_items(projection))


def projection_union(projection_1: ProjectionDict,
                     projection_2: ProjectionDict,
                     already_flattened=False):
    """

    >>> d = {'a': {
    ...         'a': True,
    ...         'c': {'a': True, 'u': True}
    ...         },
    ...      'b': True,
    ...      'c': False
    ...     }
    >>> dd = {'b': True, 'c': True, 'x': True, 'y': False}
    >>> projection_union(d, dd)
    {'a.a': True, 'a.c.a': True, 'a.c.u': True, 'b': True, 'c': True, 'x': True, 'y': True}

    """
    if not already_flattened:
        projection_1 = dict(flatten_dict_items(projection_1))
        projection_2 = dict(flatten_dict_items(projection_2))
    return merge_projection_dicts(projection_1, projection_2)


class MongoCollectionReaderBase(MongoCollectionCollection, KvReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

    """

    def __init__(self,
                 mgc: Union[PyMongoCollectionSpec, KvReader] = None,
                 filter: Optional[dict] = None,
                 key_fields=("_id",),
                 val_fields: Optional[Iterable] = None):
        super().__init__(mgc=mgc, filter=filter)
        # self._mgc = get_mongo_collection_pymongo_obj(mgc)
        # key_fields, data_fields, key_projection, items_projection = get_key_value_specs(key_fields, data_fields)
        self._key_fields = normalize_projection(key_fields)
        self._val_fields = normalize_projection(val_fields)

        # self.filter = filter or {}

    # @cached_property
    # def _key_projection(self):
    #     return {}
    #
    @cached_property
    def _items_projection(self):
        return projection_union(self._key_fields, self._val_fields, already_flattened=True)

    def __getitem__(self, k):
        assert isinstance(k, Mapping), \
            f"k (key) must be a mapping (typically a dictionary). Was:\n\tk={k}"
        return self._mgc.find(filter=self._merge_with_filt(k), projection=self._val_fields)

    def __iter__(self):
        yield from self._mgc.find(filter=self.filter, projection=self._key_fields)

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
