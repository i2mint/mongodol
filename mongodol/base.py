from functools import wraps, cached_property
from typing import Mapping, Optional, Union, Iterable
from collections.abc import KeysView, ValuesView, ItemsView
from collections import ChainMap

from pymongo import MongoClient

from dol import KvReader
from dol import Collection as DolCollection

from mongodol.constants import ID, PyMongoCollectionSpec, end_of_cursor, DFLT_TEST_DB
from mongodol.util import (
    ProjectionSpec,
    normalize_projection,
    projection_union,
    get_mongo_collection_pymongo_obj,
)


# TODO: mgc type annotation
#  See https://stackoverflow.com/questions/66464191/referencing-a-python-class-within-its-definition-but-outside-a-method
class MongoCollectionCollection(DolCollection):
    def __init__(
        self,
        mgc: Union[PyMongoCollectionSpec, DolCollection] = None,
        filter: Optional[dict] = None,
        iter_projection: Optional[dict] = None,
        **mgc_find_kwargs,
    ):
        self.mgc = get_mongo_collection_pymongo_obj(mgc)
        self.filter = filter or {}
        self._iter_projection = iter_projection
        self._mgc_find_kwargs = mgc_find_kwargs

    def _merge_with_filt(self, m: Mapping) -> dict:
        """

        :param args: dictionaries that are valid mongo queries
        :return:

        >>> class Mock(MongoCollectionCollection):
        ...     def __init__(self, filter):
        ...         self.filter = filter
        >>> s = Mock(filter={'a': 3, 'b': {'$in': [1, 2, 3]}})
        >>> s._merge_with_filt({'c': 'me'})
        {'$and': [{'a': 3, 'b': {'$in': [1, 2, 3]}}, {'c': 'me'}]}
        >>> s._merge_with_filt({'b': 4})
        {'$and': [{'a': 3, 'b': {'$in': [1, 2, 3]}}, {'b': 4}]}
        """
        # return {"$and": [self.filter, *args]}  # in case we want to move to handling several elements to merge
        return {'$and': [self.filter, m]}

    def __iter__(self):
        return self.mgc.find(
            filter=self.filter,
            projection=self._iter_projection,
            **self._mgc_find_kwargs,
        )

    def __len__(self):
        return self.mgc.count_documents(**self._count_kwargs)

    def __contains__(self, k: dict):
        cursor = self.mgc.find(self._merge_with_filt(k), projection=())
        return next(cursor, end_of_cursor) is not end_of_cursor

    @cached_property
    def _count_kwargs(self):
        search_map = ChainMap(self._mgc_find_kwargs, dict(filter=self.filter))
        return {
            x: search_map[x]
            for x in ['filter', 'skip', 'limit', 'hint']
            if x in search_map
        }

    @cached_property
    def mgc_repr(self):
        return f'<{self.mgc.database.name}/{self.mgc.name}>'

    def __repr__(self):
        return (
            f'{type(self).__name__}(mgc={self.mgc_repr}, filter={self.filter}, iter_projection={self._iter_projection}'
            f"{', '.join(f'{k}={v}' for k, v in self._mgc_find_kwargs.items())})"
        )


class MongoValuesView(ValuesView):
    def __contains__(self, v):
        m = self._mapping
        cursor = m.mgc.find(filter=m._merge_with_filt(v), projection=())
        return next(cursor, end_of_cursor) is not end_of_cursor

    def __iter__(self):
        m = self._mapping
        return m.mgc.find(filter=m.filter, projection=m._getitem_projection)

    # def distinct(self, key, filter=None, **kwargs):
    #     m = self._mapping
    #     # TODO: Check if this is correct (what about $ cases?): filter=m._merge_with_filt(filter)
    #     return m.mgc.distinct(key, filter=m._merge_with_filt(filter), **kwargs)
    #
    # unique = distinct


class MongoItemsView(ItemsView):
    def __contains__(self, item):
        m = self._mapping
        k, v = item
        # TODO: How do we have cursor return no data (here still has _id)
        cursor = m.mgc.find(filter=dict(v, **m._merge_with_filt(k)), projection=())
        # return cursor
        return next(cursor, end_of_cursor) is not end_of_cursor

    def __iter__(self):
        m = self._mapping
        for doc in m.mgc.find(filter=m.filter, projection=m._items_projection):
            key = {k: doc.pop(k) for k in m.key_fields}
            yield key, doc


class MongoCollectionReader(MongoCollectionCollection, KvReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

    Some examples below. For examples using actual data (with setup and tear down) see the tests/ folder.

    >>> from pymongo import MongoClient
    >>> s = MongoCollectionReader(MongoClient()['mongodol']['mongodol_test'])
    >>> list_of_keys = list(s)
    >>> fake_key = {'_id': 'this key does not exist'}
    >>> fake_key in s
    False

    It's important to note that ``s[k]`` (for any base MongoCollectionReader instance ``s``) returns a Cursor,
    and will always return a Cursor, no matter what key ``k`` you ask for
    -- as long as the key is a valid mapping (dict usually).
    This cursor is a (pymongo) object that is used to iterate over the results of the ``k`` lookup.
    It may yield no results what-so-ever, or one, or many.

    >>> v = s[fake_key]
    >>> type(v).__name__
    'Cursor'
    >>> len(list(v))  # but the cursor yields no results
    0

    Indeed, ``MongoCollectionReader`` is really meant to provide a low level key-value interface to a mongo collection
    that is really meant to be wrapped in order to produce the actual key-value interfaces one needs.
    You shouldn't think of it's instances as a normal dict where any request for the value under a key,
    for a key that doesn't exist, will result in a ``KeyError``.
    Note that this means that `s.get(k, default)` will never result in the default being returned,
    since there are no missing keys here; only empty results (cursors that don't yield anything).

    >>> v = s.get(fake_key, {'the': 'default'})
    >>> assert v != {'the': 'default'}

    ``s.keys()``, ``s.values()``, and ``s.items()`` are ``collections.abc.MappingViews`` instances
    (specialized for mongo).

    >>> type(s.keys()).__name__
    'KeysView'
    >>> type(s.values()).__name__
    'MongoValuesView'
    >>> type(s.items()).__name__
    'MongoItemsView'

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

    _projections_are_flattened = False
    _wrap_for_method = {
        'values': MongoValuesView,
        'items': MongoItemsView,
    }

    def __init__(
        self,
        mgc: Union[PyMongoCollectionSpec, KvReader] = None,
        filter: Optional[dict] = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        **mgc_find_kwargs,
    ):
        if not isinstance(iter_projection, dict):
            iter_projection = {k: True for k in iter_projection}
        assert iter_projection is not None, 'iter_projection cannot be None'
        super().__init__(
            mgc=mgc, filter=filter, iter_projection=iter_projection, **mgc_find_kwargs,
        )
        self._getitem_projection = getitem_projection

    def __getitem__(self, k):
        assert isinstance(
            k, Mapping
        ), f'k (key) must be a mapping (typically a dictionary). Was:\n\tk={k}'
        return self.mgc.find(
            filter=self._merge_with_filt(k), projection=self._getitem_projection,
        )

    def keys(self):
        return KeysView(self)

    def values(self) -> MongoValuesView:
        return MongoValuesView(self)

    @cached_property
    def _items_projection(self):
        iter_projection = self._iter_projection
        getitem_projection = self._getitem_projection
        if iter_projection is None or getitem_projection is None:
            return None
        if not isinstance(self._iter_projection, Mapping):
            iter_projection = {k: True for k in iter_projection}
        return projection_union(
            iter_projection,
            getitem_projection,
            already_flattened=self._projections_are_flattened,
        )

    @cached_property
    def key_fields(self):
        _iter_projection = normalize_projection(self._iter_projection)
        return tuple(
            field for field in _iter_projection if _iter_projection[field] is True
        )

    @cached_property
    def val_fields(self):
        if self._getitem_projection is None:
            return None
        else:
            _getitem_projection = normalize_projection(self._getitem_projection)
            return tuple(
                field
                for field in _getitem_projection
                if _getitem_projection[field] is True
            )

    def items(self) -> MongoItemsView:
        return MongoItemsView(self)

    @classmethod
    def from_params(
        cls,
        db_name: str = DFLT_TEST_DB,
        collection_name: str = 'test',
        mongo_client: Optional[dict] = None,
        filter: Optional[dict] = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        **mgc_find_kwargs,
    ):
        if mongo_client is None:
            mongo_client = MongoClient()
        elif isinstance(mongo_client, dict):
            mongo_client = MongoClient(**mongo_client)

        return cls(
            mgc=mongo_client[db_name][collection_name],
            filter=filter,
            iter_projection=iter_projection,
            getitem_projection=getitem_projection,
            **mgc_find_kwargs,
        )

    def distinct(self, key, filter=None, **kwargs):
        # TODO: Check if this is correct (what about $ cases?): filter=m._merge_with_filt(filter)
        return self.mgc.distinct(
            key, filter=self._merge_with_filt(filter or {}), **kwargs
        )

    unique = distinct

    def aggregate(self, pipeline, **kwargs):
        _pipeline = pipeline.copy()
        _pipeline.insert(0, {'$match': self.filter})
        return self.mgc.aggregate(_pipeline, **kwargs)


class MongoCollectionFieldsReader(MongoCollectionReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

    An "easier" interface for the common case where we just want to specify fixed fields for keys and vals.

    """

    _projections_are_flattened = True

    def __init__(
        self,
        mgc: Union[PyMongoCollectionSpec, KvReader] = None,
        filter: Optional[dict] = None,
        key_fields: ProjectionSpec = (ID,),
        val_fields: ProjectionSpec = None,
    ):
        iter_projection = normalize_projection(key_fields)
        super().__init__(
            mgc=mgc,
            filter=filter,
            iter_projection=normalize_projection(key_fields),
            getitem_projection=normalize_projection(val_fields),
        )
        self.key_fields = key_fields
        self.val_fields = val_fields


class MongoCollectionPersister(MongoCollectionReader):
    """base class to read from and write to a mongo collection, or subset thereof, with the MutableMapping interface.

    >>> from mongodol.util import mk_dflt_mgc
    >>> mongo_collection_obj = mk_dflt_mgc()
    >>> s = MongoCollectionPersister(mongo_collection_obj, getitem_projection={'_id': False})
    >>> for k in s:  # deleting all docs in default collection
    ...     del s[k]
    >>> k = {'_id': 'foo'}
    >>> v = {'val': 'bar'}
    >>> k in s  # see that key is not in store (and testing __contains__)
    False
    >>> len(s)
    0
    >>> s[k] = v
    >>> len(s)
    1
    >>> list(s)
    [{'_id': 'foo'}]

    Since this is a base mongo store, the values are cursors, so to get an actual value, you need to fetch the first doc

    >>> next(s[k])
    {'val': 'bar'}
    >>> next(s.get(k))
    {'val': 'bar'}

    Remember (see ``MongoCollectionReader`` docs) that ``s.get`` will never reach its default since
    the reader will always return a cursor (possibly empty).
    So in the following case, we should get an empty cursor (not a default value)

    >>> list(s.get({'not': 'a key'}, {'default': 'val'}))  # testing s.get with default
    []


    >>> list(s.values())
    [{'val': 'bar'}]
    >>> k in s  # testing __contains__ again
    True
    >>> k in s.keys()  # test the contains capability of s.keys() (a MongoKeysView instance)
    True
    >>> del s[k]
    >>> len(s)
    0

    >>> # Making a persister whose keys are 2-dimensional and values are 3-dimensional
    >>> from mongodol.util import normalize_projection
    >>> s = MongoCollectionPersister(mongo_collection_obj,
    ...                     iter_projection={'first': True, 'last': True, '_id': False},
    ...                     getitem_projection=normalize_projection(('yob', 'proj', 'bdfl')))
    >>> for _id in s:  # deleting all docs in tmp
    ...     del s[_id]
    >>> # writing two items
    >>> s[{'first': 'Guido', 'last': 'van Rossum'}] = {'yob': 1956, 'proj': 'python', 'bdfl': False}
    >>> s[{'first': 'Vitalik', 'last': 'Buterin'}] = {'yob': 1994, 'proj': 'ethereum', 'bdfl': True}
    >>> # Seeing that those two items are there
    >>> for key, val in s.items():
    ...     print(f"{key} --> {val}")
    {'first': 'Guido', 'last': 'van Rossum'} --> {'yob': 1956, 'proj': 'python', 'bdfl': False}
    {'first': 'Vitalik', 'last': 'Buterin'} --> {'yob': 1994, 'proj': 'ethereum', 'bdfl': True}

    """

    def __init__(
        self,
        mgc: Union[PyMongoCollectionSpec, KvReader] = None,
        filter: Optional[dict] = None,
        on_write_filter: Optional[dict] = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        **mgc_find_kwargs,
    ):
        super().__init__(
            mgc=mgc,
            filter=filter,
            iter_projection=iter_projection,
            getitem_projection=getitem_projection,
            **mgc_find_kwargs,
        )
        self._on_write_filter = on_write_filter

    def __setitem__(self, k, v):
        assert isinstance(k, Mapping) and isinstance(
            v, Mapping
        ), f'k (key) and v (value) must both be mappings (often dictionaries). Were:\n\tk={k}\n\tv={v}'
        return self.mgc.replace_one(
            filter=self._merge_with_filt(k),
            replacement=self._build_doc(k, v),
            upsert=True,
        )

    def __delitem__(self, k):
        assert isinstance(
            k, Mapping
        ), f'k (key) must be a mapping (most often a dictionary). Were:\n\tk={k}'
        if len(k) > 0:
            return self.mgc.delete_one(self._merge_with_filt(k))
        else:
            raise KeyError(f"You can't remove that key: {k}")

    def append(self, v):
        assert isinstance(
            v, Mapping
        ), f' v (value) must be a mapping (often a dictionary). Were:\n\tv={v}'
        return self.mgc.insert_one(self._build_doc(v))

    def extend(self, values):
        assert all(
            [isinstance(v, Mapping) for v in values]
        ), f' values must be mappings (often dictionaries)'
        if values:
            return self.mgc.insert_many([self._build_doc(v) for v in values])

    def _build_doc(self, *args):
        def merge_doc_elements_with_filter():
            d = self._on_write_filter or self.filter
            for v in args:
                if v is None:
                    v = {}
                assert isinstance(
                    v, Mapping
                ), f' v (value) must be a mapping (often a dictionary). Were:\n\tv={v}'
                d = dict(d, **v)
            return d

        doc = merge_doc_elements_with_filter()
        is_invalid = (
            len(
                [
                    x
                    for x in doc.values()
                    if isinstance(x, Mapping)
                    and len([k for k in x.keys() if '$' in k]) > 0
                ]
            )
            > 0
        )
        if is_invalid:
            raise ValueError('The doc contains some query-specific values.')
        return doc

    def persist_data(self, data):
        return self.__setitem__({ID: data[ID]}, data)


# class MongoAppendablePersister(MongoCollectionPersister):
#     """MongoCollectionPersister endowed with an append and an extend that will write any dict (doc) to the collection
#     (as is, with no key-value validation)"""
#
#     def append(self, v):
#         return self._mgc.insert_one(v)
#
#     def extend(self, items):
#         return self._mgc.insert_many(items)


class MongoClientReader(KvReader):
    @wraps(MongoClient.__init__)
    def __init__(self, **mongo_client_kwargs):
        self._mongo_client = MongoClient(**mongo_client_kwargs)

    def __iter__(self):
        yield from self._mongo_client.list_database_names()

    def __getitem__(self, k):
        return MongoDbReader(
            db_name=k, mongo_client=self._mongo_client
        )  # or just wrap self._mongo_client[k]?


class MongoDbReader(KvReader):
    def __init__(
        self,
        db_name=DFLT_TEST_DB,
        mk_collection_store=MongoCollectionReader,
        mongo_client=None,
        **mongo_client_kwargs,
    ):
        """Base Mongo Db Reader. Keys are collection names and values are collection store instances.

        :param db_name: Name of db
        :param mk_collection_store: Function that is called on a key (collection name) to make the
            collection store instance.
            Use mk_collection_store to define what kind of collection stores you want to make.
            Will be called with only one unnamed argument; the collection name.
            Use custom classes here, and/or partials (curried functions) thereof, to fix any parameters you want to fix.
        :param mongo_client: MongoClient instance, kwargs to make it (MongoClient(**kwargs)), or callable to make it
        :param mongo_client_kwargs: **kwargs to make a MongoClient, that is used if mongo_client is callable
        """
        if mongo_client is None:
            self._mongo_client = MongoClient(**mongo_client_kwargs)
        elif isinstance(mongo_client, dict):
            self._mongo_client = MongoClient(**mongo_client)
        else:
            self._mongo_client = mongo_client
        self._db_name = db_name
        self.db = self._mongo_client[db_name]
        self.collection_store_cls = mk_collection_store

    def __iter__(self):
        yield from self.db.list_collection_names()

    def __getitem__(self, k):
        return self.collection_store_cls(self.db[k])
