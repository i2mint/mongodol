from functools import partial
from operator import or_
from typing import Union, Iterable, Mapping

from pymongo.collection import Collection as PyMongoCollection
from pymongo import MongoClient

from linkup import key_aligned_val_op_with_forced_defaults

from mongodol.constants import (
    ID,
    DFLT_MONGO_CLIENT_ARGS,
    DFLT_TEST_DB,
    DFLT_TEST_COLLECTION,
)


def mk_dflt_mgc():
    return MongoClient(*DFLT_MONGO_CLIENT_ARGS)[DFLT_TEST_DB][DFLT_TEST_COLLECTION]


class KeyNotUniqueError(RuntimeError):
    """Raised when a key was expected to be unique, but wasn't (i.e. cursor has more than one match)"""

    @staticmethod
    def raise_error(k):
        raise KeyNotUniqueError(
            f'Key was not unique (i.e. cursor has more than one match): {k}'
        )


ProjectionDict = dict  # TODO: Specify that keys are strings and values are boolean
ProjectionSpec = Union[ProjectionDict, Iterable[str], None]


def get_key_value_specs(key_fields, data_fields):
    if isinstance(key_fields, str):
        key_fields = (key_fields,)
    if data_fields is None:
        pass
    key_projection = {k: True for k in key_fields}
    if '_id' not in key_fields:
        key_projection.update(
            _id=False
        )  # need to explicitly specify this since mongo includes _id by dflt
    if data_fields is None:
        data_fields = {k: False for k in key_fields}
        items_projection = None
    elif not isinstance(data_fields, dict):
        data_fields = {k: True for k in data_fields}
        if '_id' not in data_fields:
            data_fields['_id'] = False
        items_projection = {k for k, v in data_fields.items() if v} | {
            k for k, v in key_projection.items() if v
        }
    return key_fields, data_fields, key_projection, items_projection


def flatten_dict_items(d: Mapping, prefix=''):
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


merge_projection_dicts = partial(
    key_aligned_val_op_with_forced_defaults,
    op=or_,
    dflt_val_for_x=True,
    dflt_val_for_y=True,
)


def normalize_projection(projection: Union[Iterable, None]):
    """Normalize projection specification to be an explicit list of flattened dict of {path.to.key: True/False,...
    (or None if projection is None to start with).

    This is used to be able to have a consistent specification of mongo projections.

    If projection is None, the output will None as well:

    >>> assert normalize_projection(None) is None

    If projection is a dict, the dict will be "flattened" to use "dot-paths" instead of nested dicts:

    >>> normalize_projection({'name': {'first': True, 'last': False}, 'age': True})
    {'name.first': True, 'name.last': False, 'age': True}


    If projection is not a dict, it will make the "equivalent" dict version of the projection.
    One difference with mongodb's projection language: Here, if you don't specify that you want "_id",
    it will explicitly specify that you DO NOT want that field (because mongodb will otherwise assume that you do!)

    >>> normalize_projection(['name.first', 'age'])
    {'name.first': True, 'age': True, '_id': False}

    But if you actually want that "_id", just say so:

    >>> normalize_projection(['name.first', 'age', '_id'])
    {'name.first': True, 'age': True, '_id': True}

    Also, if you specify a string, it will think of this as a tuple containing just that string:

    >>> normalize_projection('name.last')
    {'name.last': True, '_id': False}

    """
    if projection is None:
        return None
    elif not isinstance(projection, dict):
        if isinstance(projection, str):
            projection = (projection,)
        elif isinstance(projection, Iterable) and len(projection) == 0:
            return projection  # it's probably the empty projection
        elif projection is None:
            projection = None
        projection = dict({field: True for field in projection})
        if (
            ID not in projection
        ):  # if the projection doesn't contain the ID, we need to explicitly say this...
            projection.update(
                **{ID: False}
            )  # ... or mongo will resolve this to {ID: True}

    return dict(flatten_dict_items(projection))


def projection_union(
    projection_1: ProjectionDict, projection_2: ProjectionDict, already_flattened=False,
):
    """

    >>> d = {'a': {
    ...         'a': True,
    ...         'c': {'a': True, 'u': True}
    ...         },
    ...      'b': True,
    ...      'c': False
    ...     }
    >>> dd = {'b': True, 'c': True, 'x': True, 'y': False}
    >>> assert projection_union(d, dd) == (
    ...     {'a.a': True, 'a.c.a': True, 'a.c.u': True, 'b': True, 'c': True, 'x': True, 'y': True}
    ... )

    """
    if not already_flattened:
        projection_1 = dict(flatten_dict_items(projection_1))
        projection_2 = dict(flatten_dict_items(projection_2))
    return merge_projection_dicts(projection_1, projection_2)


def get_mongo_collection_pymongo_obj(obj=None):
    """Get a pymongo.collection.Collection object for a mongo collection, flexibly.

    ```
    get_mongo_collection_pymongo_obj()  # gives you a default mongo collection ({DFLT_TEST_DB}/test)
    get_mongo_collection_pymongo_obj('database_name/collection_name')  # does the obvious (with default host)
    get_mongo_collection_pymongo_obj(... an object that has an _mgc attribute...)  # return the _mgc attribute
    get_mongo_collection_pymongo_obj(obj)  # else, asserts pymongo.collection.Collection and returns it
    ```
    """
    if obj is None:
        obj = mk_dflt_mgc()
    elif isinstance(obj, str):
        if obj.startswith('mongodb://'):
            raise ValueError(
                'No support (yet) for URI access. '
                'If you want to implement, see: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html'
            )
        database_name, collection_name = obj.split('/')
        return PyMongoCollection()[database_name][collection_name]
    elif hasattr(obj, '_mgc') and isinstance(obj._mgc, PyMongoCollection):
        obj = obj._mgc
    if not isinstance(obj, PyMongoCollection):
        raise TypeError(f'Unknown pymongo collection specification: {obj}')
    return obj
