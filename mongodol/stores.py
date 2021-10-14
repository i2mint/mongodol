"""Some useful stores for mongoDB"""

from functools import wraps, partial
from typing import Collection, Mapping

from dol import Store, wrap_kvs
from dol.util import lazyprop

from mongodol.constants import DFLT_TEST_DB
from mongodol.base import MongoCollectionReader, MongoCollectionPersister
from mongodol.trans import PostGet, ObjOfData, normalize_result

single_value_fetch_with_unicity_validation = partial(
    wrap_kvs, postget=PostGet.single_value_fetch_with_unicity_validation
)
single_value_fetch_without_unicity_validation = partial(
    wrap_kvs, postget=PostGet.single_value_fetch_without_unicity_validation
)


@normalize_result
class MongoCollectionPersisterWithResultMapping(MongoCollectionPersister):
    """MongoCollectionPersister with result mapping"""


@single_value_fetch_with_unicity_validation
class MongoCollectionUniqueDocReader(MongoCollectionReader):
    """A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).
    :raises KeyNotUniqueError if the k matches more than a single unique doc.

    >>> from mongodol.stores import MongoCollectionUniqueDocReader
    >>> from mongodol.tests import data, util
    >>> test_mgc = util.populated_pymongo_collection(data.three_simple_docs)
    >>> s = MongoCollectionUniqueDocReader(test_mgc,
    ...     iter_projection={'s': True, '_id': False}, getitem_projection=['n'])
    >>> assert list(s) == [{'s': 'a'}, {'s': 'b'}, {'s': 'b'}]

    And you see where the problem will be: There's two {'s': 'b'} in that listing,
    so though getting the value for {'s': 'a'} won't be a problem:

    >>> assert s[{'s': 'a'}] == {'_id': 0, 'n': 1}  # there's only one doc matching {'s': 'a'}

    ... but there's more than one doc matching {'s': 'b'}

    >>> s[{'s': 'b'}]
    Traceback (most recent call last):
      ...
    mongodol.util.KeyNotUniqueError: Key was not unique (i.e. cursor has more than one match): {'s': 'b'}

    """


@single_value_fetch_without_unicity_validation
class MongoCollectionFirstDocReader(MongoCollectionReader):
    """A mongo collection (kv-)reader where s[key] is the first key-matching value found.
    Unlike MongoCollectionUniqueDocReader, MongoCollectionFirstDocReader doesn't check for uniqueness.

    Typically, this should be used when you don't want the overhead of checking for uniqueness,
    because it doesn't matter, you like risk, or you told the mongo collection indexing system itself to
    ensure uniqueness for you.
    """


@wrap_kvs(
    postget=partial(ObjOfData.all_docs_fetch, doc_collector=list)
)  # list is default but explicit here to show that other choices possible
class MongoCollectionMultipleDocsReader(MongoCollectionReader):
    """A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.
    If no docs match, will return an empty list.
    """


# TODO: Use adapter pattern to generate below and above


@single_value_fetch_with_unicity_validation
class MongoCollectionUniqueDocPersister(MongoCollectionPersisterWithResultMapping):
    """A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).
    :raises KeyNotUniqueError if the k matches more than a single unique doc.
    """


@single_value_fetch_without_unicity_validation
class MongoCollectionFirstDocPersister(MongoCollectionPersisterWithResultMapping):
    """A mongo collection (kv-)reader where s[key] is the first key-matching value found.
    Unlike MongoCollectionUniqueDocReader, MongoCollectionFirstDocReader doesn't check for uniqueness.

    Typically, this should be used when you don't want the overhead of checking for uniqueness,
    because it doesn't matter, you like risk, or you told the mongo collection indexing system itself to
    ensure uniqueness for you.
    """


@wrap_kvs(
    postget=partial(ObjOfData.all_docs_fetch, doc_collector=list)
)  # list is default but explicit here to show that other choices possible
class MongoCollectionMultipleDocsPersister(MongoCollectionPersisterWithResultMapping):
    """A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.
    If no docs match, will return an empty list.
    """

    def __setitem__(self, k, v):
        assert isinstance(
            k, Mapping
        ), f'k (key) must be a mapping (typically a dictionary). Was:\n\tk={k}'
        assert isinstance(v, Mapping) or (
            isinstance(v, Collection) and all([isinstance(i, Mapping) for i in v])
        ), f'v (value) must be mappings (often dictionaries) or a collection of mappings. Were:\n\tk={k}\n\tv={v}'
        self._mgc.delete_many(self._merge_with_filt(k))
        _v = v if isinstance(v, Collection) else [v]
        return self._mgc.insert_many([self._build_doc(k, vi) for vi in _v])


from mongodol.scrap.old01 import OldMongoPersister


class MongoStore(Store):
    @wraps(OldMongoPersister.__init__)
    def __init__(self, *args, **kwargs):
        persister = OldMongoPersister(*args, **kwargs)
        super().__init__(persister)


# class MongoStore(Store):
#     @wraps(MongoCollectionPersister.__init__)
#     def __init__(self, *args, **kwargs):
#         persister = MongoCollectionPersister(*args, **kwargs)
#         super().__init__(persister)
#
#     from_params = MongoCollectionPersister.from_params


class MongoTupleKeyStore(MongoStore):
    """
    MongoStore using tuple keys.

    >>> s = MongoTupleKeyStore(db_name=DFLT_TEST_DB, collection_name='tmp', key_fields=('_id', 'user'))
    >>> _ = s._mgc.remove({})
    >>> k = (1234, 'user')
    >>> v = {'name': 'bob', 'age': 42}
    >>> if k in s:  # deleting all docs in tmp
    ...     if k in s:
    ...         del s[k]
    >>> assert (k in s) == False  # see that key is not in store (and testing __contains__)
    >>> orig_length = len(s)
    >>> s[k] = v
    >>> assert len(s) == orig_length + 1
    >>> assert k in list(s)
    >>> assert s[k] == v
    >>> assert s.get(k) == v
    >>> assert v in list(s.values())
    >>> assert (k in s) == True # testing __contains__ again
    >>> del s[k]
    >>> assert len(s) == orig_length
    """

    @lazyprop
    def _key_fields(self):
        return self.store._key_fields

    def _id_of_key(self, k):
        return {field: field_val for field, field_val in zip(self._key_fields, k)}

    def _key_of_id(self, _id):
        return tuple(_id[x] for x in self._key_fields)


# TODO: Finish
class MongoAnyKeyStore(MongoStore):
    """
    MongoStore using tuple keys.

    >>> s = MongoAnyKeyStore(db_name=DFLT_TEST_DB, collection_name='tmp', )
    >>> for k in s: del s[k]
    >>> s['foo'] = {'must': 'be', 'a': 'dict'}
    >>> s['foo']
    {'must': 'be', 'a': 'dict'}
    """

    @wraps(MongoStore.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(
            self._key_fields, tuple
        ), 'key_fields should be a tuple or a string'
        assert (
            len(self._key_fields) == 1
        ), 'key_fields must have one and only one element (a string)'
        self._key_field = self._key_fields[0]

    @lazyprop
    def _key_fields(self):
        return self.store._key_fields

    def _id_of_key(self, k):
        return {self._key_field: k}

    def _key_of_id(self, _id):
        return _id[self._key_field]

    def __setitem__(self, k, v):
        if k in self:
            del self[k]
        super().__setitem__(k, v)
