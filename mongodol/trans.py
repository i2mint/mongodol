from typing import Mapping
from functools import wraps
from typing import Iterable, Optional, TypedDict

from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

from py2store.trans import (
    condition_function_call,
    double_up_as_factory,
    store_decorator,
)
from mongodol.util import KeyNotUniqueError

#
# def _key_does_not_exist(store, k, v):
#     """Condition function for use in condition_function_call. Returns true iff the k is not in store"""
#     return k in not store
#
# @store_decorator
# def only_allow_keys


class PersistentDict(dict):
    '''
    Extension of a dict wich trigger an event to notify the object that contains the dict that a modification
    as been made.
    Requirement: The container object need to implement the method "persist_data(self, data: Mapping)".
    >>> d = {
    ...     'a': 1,
    ...     'b': {'ba': 2, 'bb': 3},
    ...     'c': [
    ...         {'c1a': 4, 'c1b': 5},
    ...         {'c2a': 6, 'c2b': 7}
    ...     ]
    ... }
    >>> class Container:
    ...     def persist_data(self, data):
    ...         print(data)
    >>> pd = PersistentDict(Container(), **d)
    >>> pd['a'] = 8
    {'a': 8, 'b': {'ba': 2, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': 7}]}
    >>> pd['b']['ba'] = 8
    {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': 7}]}
    >>> pd['c'][0]['c1a'] = 8
    {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': 7}]}
    '''
    def __init__(self, container, **kwargs):
        def get_value(v):
            if isinstance(v, Mapping):
                return PersistentDict(self, **v)
            if isinstance(v, Iterable):
                return [get_value(x) for x in v]
            return v

        self._container = container
        persistent_kwargs = {
            k: get_value(v)
            for k, v in kwargs.items()
        }
        super().__init__(**persistent_kwargs)

    def update(self, __m, **kwargs):
        super().update(__m, **kwargs)
        return self.persist_data()

    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        return self.persist_data()

    def persist_data(self, *args):
        return self._container.persist_data(self)


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
            return PersistentDict(store, **doc)
        else:
            raise KeyError(f"No document found for query: {k}")

    @staticmethod
    def single_value_fetch_without_unicity_validation(store, k, cursor):
        doc = next(cursor, None)
        if doc is not None:
            return PersistentDict(store, **doc)
        else:
            raise KeyError(f"No document found for query: {k}")


class ObjOfData:
    @staticmethod
    def all_docs_fetch(cursor, doc_collector=list):
        return doc_collector(map(lambda x: PersistentDict(x), cursor))


WriteOpResult = TypedDict(
    "WriteOpResult", ok=bool, n=int, ids=Optional[Iterable[str]]
)

DFLT_METHOD_NAMES_TO_NORMALIZE = (
    "__setitem__",
    "__delitem__",
    "append",
    "extend",
)


def normalize_result(
    obj, *, method_names_to_normalize=DFLT_METHOD_NAMES_TO_NORMALIZE
):
    """Decorator to transform a pymongo result object to a WriteOpResult object.

    :param func: [description]
    :type func: [type]
    """

    if not isinstance(obj, type):
        assert callable(obj), f"Should be callable: {obj}"
        func = obj

        @wraps(func)
        def result_mapper(*args, **kwargs):
            raw_result = func(*args, **kwargs)
            result: WriteOpResult = {"n": 0}
            if (
                isinstance(raw_result, InsertOneResult)
                and raw_result.inserted_id
            ):
                result["n"] = 1
                result["ids"] = [str(raw_result.inserted_id)]
            elif (
                isinstance(raw_result, InsertManyResult)
                and raw_result.inserted_ids
            ):
                result["n"] = len(raw_result.inserted_ids)
                result["ids"] = raw_result.inserted_ids
            elif isinstance(raw_result, (DeleteResult, UpdateResult)):
                result["n"] = raw_result.raw_result["n"]
            elif isinstance(raw_result, BulkWriteResult):
                result["n"] = (
                    raw_result.inserted_count
                    + raw_result.upserted_count
                    + raw_result.modified_count
                    + raw_result.deleted_count
                )
            else:
                raise NotImplementedError(
                    f"Interpretation of result type {type(raw_result)} is not implemented."
                )
            result["ok"] = result["n"] > 0
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
