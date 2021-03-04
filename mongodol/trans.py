from typing import Mapping
from mongodol.util import KeyNotUniqueError


class PersistentDict(dict):
    def __init__(self, container, **kwargs):
        self._container = container
        persistent_kwargs = {k: PersistentDict(self, **v) if isinstance(v, Mapping) else v for k, v in kwargs.items()}
        super().__init__(**persistent_kwargs)

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
            if next(cursor, None) is not None:  # TODO: Fetches! Faster way to check if there's more than one hit?
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
