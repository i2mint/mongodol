"""Transformative functionality"""

from copy import deepcopy
from abc import ABC
from typing import Mapping
from functools import partial, wraps
from typing import Iterable, Optional, TypedDict
from dol import wrap_kvs as dol_wrap_kvs

from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

from dol.trans import (
    condition_function_call,
    double_up_as_factory,
    store_decorator,
)
from mongodol.base import MongoBaseStore
from mongodol.util import KeyNotUniqueError


#
# def _key_does_not_exist(store, k, v):
#     """Condition function for use in condition_function_call. Returns true iff the k is not in store"""
#     return k in not store
#
# @store_decorator
# def only_allow_keys


class PersistentObjectBase(ABC):
    """
    Base class to propagate a modification event through a parent-child chain structure.
    """

    def __init__(self, container):
        self._container = container

    def persist_data(self, *args):
        return self._container.persist_data(self)


class PersistentDict(dict, PersistentObjectBase):
    '''
    Extension of a dict wich triggers an event to notify the object that contains the dict that a modification
    has been made.

    Requirement: The container object needs to implement the method "persist_data(self, data: Mapping)".

    >>> d = {
    ...     'a': 1,
    ...     'b': {'ba': 2, 'bb': 3},
    ...     'c': [
    ...         {'c1a': 4, 'c1b': 5},
    ...         {'c2a': 6, 'c2b': '7'}
    ...     ]
    ... }
    >>> class Container:
    ...     def persist_data(self, data):
    ...         """Here, you'd normally put code to ACTUALLY persist the data"""
    ...         print(f"persisting {data}")
    >>> pd = PersistentDict(Container(), d)
    >>> pd['a'] = 8
    persisting {'a': 8, 'b': {'ba': 2, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['a'] == 8  # and indeed pd['a'] is 8 now!
    >>> pd['b']['ba'] = 8
    persisting {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['b']['ba'] == 8  # and indeed pd['b']['ba'] is 8 now!
    >>> pd['c'][0]['c1a'] = 8
    persisting {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['c'][0]['c1a'] == 8  # and indeed pd['c'][0]['c1a'] is 8 now!
    >>> pd.update({'a': 9})
    persisting {'a': 9, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['a'] == 9
    >>> pd['b'].update({'ba': 9})
    persisting {'a': 9, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['b']['ba'] == 9
    >>> pd['c'][0].update({'c1a': 9})
    persisting {'a': 9, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['c'][0]['c1a'] == 9
    >>> pd.update([('a', 10)])
    persisting {'a': 10, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['a'] == 10
    >>> pd['b'].update([('ba', 10)])
    persisting {'a': 10, 'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['b']['ba'] == 10
    >>> pd['c'][0].update([('c1a', 10)])
    persisting {'a': 10, 'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert pd['c'][0]['c1a'] == 10
    >>> del pd['a']
    persisting {'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert 'a' not in pd  # indeed, 'a' no longer in pd
    >>> del pd['b']['ba']
    persisting {'b': {'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert 'ba' not in pd['b']
    >>> del pd['c'][0]['c1a']
    persisting {'b': {'bb': 3}, 'c': [{'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
    >>> assert 'c1a' not in pd['c'][0]
    '''

    def __init__(self, container, wrapped_dict: Mapping):
        PersistentObjectBase.__init__(self, container)
        persistent_kwargs = {
            k: get_persistent_obj(self, v) for k, v in wrapped_dict.items()
        }
        dict.__init__(self, persistent_kwargs)

    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        return self.persist_data()

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        return self.persist_data()

    def __delitem__(self, v):
        super().__delitem__(v)
        return self.persist_data()


class PersistentList(list, PersistentObjectBase):
    '''
    Extension of a list wich triggers an event to notify the object that contains the list that a modification
    has been made.

    Requirement: The container object needs to implement the method "persist_data(self, data: Mapping)".

    >>> l = [1, 2, 3]
    >>> class Container:
    ...     def persist_data(self, data):
    ...         """Here, you'd normally put code to ACTUALLY persist the data"""
    ...         print(f"persisting {data}")
    >>> pl = PersistentList(Container(), l)
    >>> assert pl == [1, 2, 3]  # pl is equal to [1, 2, 3]
    >>> pl.append(4)
    persisting [1, 2, 3, 4]
    >>> assert pl == [1, 2, 3, 4]  # indeed pl is now [1, 2, 3, 4]
    >>> pl.extend([5, 6])
    persisting [1, 2, 3, 4, 5, 6]
    >>> assert pl == [1, 2, 3, 4, 5, 6]
    >>> pl += [7, 8]
    persisting [1, 2, 3, 4, 5, 6, 7, 8]
    >>> assert pl == [1, 2, 3, 4, 5, 6, 7, 8]
    >>> pl[0] = 9
    persisting [9, 2, 3, 4, 5, 6, 7, 8]
    >>> assert pl == [9, 2, 3, 4, 5, 6, 7, 8]
    >>> n = pl.pop(0)
    persisting [2, 3, 4, 5, 6, 7, 8]
    >>> assert pl == [2, 3, 4, 5, 6, 7, 8]
    >>> pl.remove(2)
    persisting [3, 4, 5, 6, 7, 8]
    >>> assert pl == [3, 4, 5, 6, 7, 8]
    >>> del pl[0]
    persisting [4, 5, 6, 7, 8]
    >>> assert pl == [4, 5, 6, 7, 8]
    '''

    def __init__(self, container, iterable: Iterable):
        self._initial_iterable = iterable
        PersistentObjectBase.__init__(self, container)
        persistent_iterable = [get_persistent_obj(self, x) for x in iterable]
        list.__init__(self, persistent_iterable)

    def append(self, __object):
        super().append(__object)
        return self.persist_data()

    def extend(self, __iterable):
        super().extend(__iterable)
        return self.persist_data()

    def __iadd__(self, x):
        r = super().__iadd__(x)
        self.persist_data()
        return r

    def __setitem__(self, i, o):
        super().__setitem__(i, o)
        return self.persist_data()

    def pop(self, __index):
        r = super().pop(__index)
        self.persist_data()
        return r

    def remove(self, __value):
        super().remove(__value)
        return self.persist_data()

    def __delitem__(self, i):
        super().__delitem__(i)
        return self.persist_data()

    def deepcopy(self):
        return deepcopy(self._initial_iterable)


def get_persistent_obj(container, v):
    if isinstance(v, Mapping):
        return PersistentDict(container, v)
    elif isinstance(v, Iterable) and not isinstance(v, str):
        return PersistentList(container, v)
    else:
        return v


# TODO: Make trans funcs/method carry their role and find their place in wrap_kvs automatically
class PostGet:
    @staticmethod
    def single_value_fetch_with_unicity_validation(store, k, cursor):
        doc = next(cursor, None)
        if doc is not None:
            if (
                next(cursor, None) is not None
            ):  # TODO: Fetches! Faster way to check if there's more than one hit?
                raise KeyNotUniqueError.raise_error(k)
            # return PersistentDict(store, doc)
            return doc
        else:
            raise KeyError(f'No document found for query: {k}')

    @staticmethod
    def single_value_fetch_without_unicity_validation(store, k, cursor):
        doc = next(cursor, None)
        if doc is not None:
            # return PersistentDict(store, doc)
            return doc
        else:
            raise KeyError(f'No document found for query: {k}')


class ObjOfData:
    @staticmethod
    def all_docs_fetch(cursor, doc_collector=list):
        # return doc_collector(map(lambda x: PersistentDict(x), cursor))
        return doc_collector(cursor)


WriteOpResult = TypedDict('WriteOpResult', ok=bool, n=int, ids=Optional[Iterable[str]])

DFLT_METHOD_NAMES_TO_NORMALIZE = (
    '__setitem__',
    '__delitem__',
    'append',
    'extend',
    'flush',
    'commit',
)


def normalize_result(obj, *, method_names_to_normalize=DFLT_METHOD_NAMES_TO_NORMALIZE):
    """Decorator to transform a pymongo result object to a WriteOpResult object.

    :param func: [description]
    :type func: [type]
    """

    if not isinstance(obj, type):
        assert callable(obj), f'Should be callable: {obj}'
        func = obj

        @wraps(func)
        def result_mapper(*args, **kwargs):
            raw_result = func(*args, **kwargs)
            result: WriteOpResult = {'n': 0}
            if raw_result is None:
                return None
            if isinstance(raw_result, InsertOneResult) and raw_result.inserted_id:
                result['n'] = 1
                result['ids'] = [str(raw_result.inserted_id)]
            elif isinstance(raw_result, InsertManyResult) and raw_result.inserted_ids:
                result['n'] = len(raw_result.inserted_ids)
                result['ids'] = raw_result.inserted_ids
            elif isinstance(raw_result, (DeleteResult, UpdateResult)):
                result['n'] = raw_result.raw_result['n']
            elif isinstance(raw_result, BulkWriteResult):
                result['n'] = (
                    raw_result.inserted_count
                    + raw_result.upserted_count
                    + raw_result.modified_count
                    + raw_result.deleted_count
                )
            else:
                raise NotImplementedError(
                    f'Interpretation of result type {type(raw_result)} is not implemented.'
                )
            result['ok'] = result['n'] > 0
            return result

        return result_mapper
    else:  # obj is a type
        cls = obj
        for method_name in method_names_to_normalize:
            if hasattr(cls, method_name):
                setattr(
                    cls,
                    method_name,
                    normalize_result(
                        getattr(cls, method_name),
                        method_names_to_normalize=method_names_to_normalize,
                    ),
                )
        return cls


wrap_kvs = partial(dol_wrap_kvs, wrapper=MongoBaseStore)
