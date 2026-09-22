"""Base mongoDB data object layers"""

from functools import wraps, cached_property
from typing import Optional, Union
from collections.abc import Mapping
from collections import ChainMap
from dol.base import Store

from pymongo import MongoClient

from dol import KvReader, Collection as DolCollection

from mongodol.constants import ID, PyMongoCollectionSpec, end_of_cursor, DFLT_TEST_DB
from mongodol.util import (
    ProjectionSpec,
    normalize_projection,
    projection_union,
    get_mongo_collection_pymongo_obj,
)
from mongodol.views import (
    MongoItemsView,
    MongoValuesView,
    bulk_items,
    bulk_values,
)


def operator_field_names(obj) -> list:
    """The ``$``-prefixed field names found anywhere in ``obj`` (mappings and lists).

    >>> operator_field_names({'a': 1, 'b': {'c': [{'$gt': 2}]}})
    ['$gt']
    >>> operator_field_names({'a': 1})
    []
    """
    found = []
    if isinstance(obj, Mapping):
        for field, value in obj.items():
            if isinstance(field, str) and field.startswith("$"):
                found.append(field)
            found.extend(operator_field_names(value))
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            found.extend(operator_field_names(value))
    return found


# TODO: mgc type annotation
#  See https://stackoverflow.com/questions/66464191/referencing-a-python-class-within-its-definition-but-outside-a-method
class MongoCollectionCollection(DolCollection):
    """Base class wrapping a mongo collection with a fixed ``filter`` and ``iter_projection``."""

    def __init__(
        self,
        mgc: PyMongoCollectionSpec | DolCollection = None,
        filter: dict | None = None,
        iter_projection: dict | None = None,
        **mgc_find_kwargs,
    ):
        self.mgc = get_mongo_collection_pymongo_obj(mgc)
        self.filter = filter or {}
        self._iter_projection = iter_projection
        self._mgc_find_kwargs = mgc_find_kwargs

    def _merge_with_filt(self, m: Mapping) -> dict:
        """:param args: dictionaries that are valid mongo queries
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
        return {"$and": [self.filter, m]}

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
            for x in ["filter", "skip", "limit", "hint"]
            if x in search_map
        }

    @cached_property
    def mgc_repr(self):
        """A short ``<database/collection>`` string identifying the wrapped mongo collection."""
        return f"<{self.mgc.database.name}/{self.mgc.name}>"

    def __repr__(self):
        return (
            f"{type(self).__name__}(mgc={self.mgc_repr}, filter={self.filter}, iter_projection={self._iter_projection}"
            f"{', '.join(f'{k}={v}' for k, v in self._mgc_find_kwargs.items())})"
        )


class MongoCollectionReader(MongoCollectionCollection, KvReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping
    (i.e. dict-like) interface.

    Some examples below. For examples using actual data (with setup and tear down)
    see the tests/ folder.

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
    Note that this means that ``s.get(k, default)`` will never result in the default being returned,
    since there are no missing keys here; only empty results (cursors that don't yield anything).

    >>> v = s.get(fake_key, {'the': 'default'})
    >>> assert v != {'the': 'default'}

    ``s.keys()``, ``s.values()``, and ``s.items()`` are ``collections.abc.MappingViews`` instances
    (specialized for mongo -- see :mod:`mongodol.views`: they fetch the whole collection in
    a single query, and keep doing so, correctly, when the store is wrapped by ``dol``).

    >>> assert type(s.keys()) == s.KeysView
    >>> assert type(s.values()) == s.ValuesView
    >>> assert type(s.items()) == s.ItemsView

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

    #: Views that resolve the bulk-read fast path through any ``dol`` wrapper chain,
    #: rather than through blind attribute delegation. See :mod:`mongodol.views`.
    ValuesView = MongoValuesView
    ItemsView = MongoItemsView

    def __init__(
        self,
        mgc: PyMongoCollectionSpec | KvReader = None,
        filter: dict | None = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        **mgc_find_kwargs,
    ):
        if iter_projection is not None and not isinstance(iter_projection, dict):
            iter_projection = {k: True for k in iter_projection}
        super().__init__(
            mgc=mgc,
            filter=filter,
            iter_projection=iter_projection,
            **mgc_find_kwargs,
        )
        self._getitem_projection = getitem_projection

    def __getitem__(self, k):
        assert isinstance(k, Mapping), (
            f"k (key) must be a mapping (typically a dictionary). Was:\n\tk={k}"
        )
        return self.mgc.find(
            filter=self._merge_with_filt(k),
            projection=self._getitem_projection,
        )

    def contains_value(self, v):
        """Bulk-read counterpart of ``__contains__`` for values: is there a doc matching ``v``?"""
        cursor = self.mgc.find(
            filter=self._merge_with_filt(v), projection=(), **self._mgc_find_kwargs
        )
        return next(cursor, end_of_cursor) is not end_of_cursor

    def iter_values(self):
        """Bulk-read all values in a single ``find`` query (see the module's bulk-read protocol)."""
        return self.mgc.find(
            filter=self.filter,
            projection=self._getitem_projection,
            **self._mgc_find_kwargs,
        )

    def contains_item(self, item):
        """Bulk-read counterpart of ``__contains__`` for ``(key, value)`` pairs."""
        k, v = item
        # TODO: How do we have cursor return no data (here still has _id)
        cursor = self.mgc.find(
            filter=dict(v, **self._merge_with_filt(k)),
            projection=(),
            **self._mgc_find_kwargs,
        )
        return next(cursor, end_of_cursor) is not end_of_cursor

    def iter_items(self):
        """Bulk-read all ``(key, value)`` pairs in a single ``find`` query, splitting each doc into
        its key fields and the rest.
        """
        cursor = self.mgc.find(
            filter=self.filter,
            projection=self._items_projection,
            **self._mgc_find_kwargs,
        )
        for doc in cursor:
            key = {k: doc.pop(k) for k in self.key_fields}
            yield (key, doc)

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
        """The field names (from ``iter_projection``) that make up a key."""
        _iter_projection = normalize_projection(self._iter_projection)
        return tuple(
            field for field in _iter_projection if _iter_projection[field] is True
        )

    @cached_property
    def val_fields(self):
        """The field names (from ``getitem_projection``) that make up a value, or None if unset."""
        if self._getitem_projection is None:
            return None
        else:
            _getitem_projection = normalize_projection(self._getitem_projection)
            return tuple(
                field
                for field in _getitem_projection
                if _getitem_projection[field] is True
            )

    @classmethod
    def from_params(
        cls,
        db_name: str = DFLT_TEST_DB,
        collection_name: str = "test",
        mongo_client: dict | None = None,
        filter: dict | None = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        **mgc_find_kwargs,
    ):
        """Make an instance from db/collection names and connection params, instead of a live mongo collection object."""
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
        """The distinct values of ``key`` across docs matching ``filter`` (merged with this store's own filter)."""
        # TODO: Check if this is correct (what about $ cases?): filter=m._merge_with_filt(filter)
        return self.mgc.distinct(
            key, filter=self._merge_with_filt(filter or {}), **kwargs
        )

    unique = distinct

    def aggregate(self, pipeline, **kwargs):
        """Run a mongo aggregation ``pipeline``, prefixed with a ``$match`` on this store's filter."""
        _pipeline = pipeline.copy()
        _pipeline.insert(0, {"$match": self.filter})
        return self.mgc.aggregate(_pipeline, **kwargs)


class MongoCollectionFieldsReader(MongoCollectionReader):
    """A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

    An "easier" interface for the common case where we just want to specify fixed fields for keys and vals.

    """

    _projections_are_flattened = True

    def __init__(
        self,
        mgc: PyMongoCollectionSpec | KvReader = None,
        filter: dict | None = None,
        key_fields: ProjectionSpec = (ID,),
        val_fields: ProjectionSpec = None,
    ):
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

    Writes stay inside the store's scope: a key or value that contradicts a field
    of the write filter (``on_write_filter``, else ``filter``) raises
    ``ValueError``, and keys used to replace or delete docs may not contain
    ``$``-operators (pass ``allow_operators_in_write_keys=True`` to allow them).
    Reads (``s[k]``, ``k in s``) still accept query keys, always within the scope.

    """

    #: Whether keys given to write/delete operations may contain ``$``-operators.
    allow_operators_in_write_keys = False

    def __init__(
        self,
        mgc: PyMongoCollectionSpec | KvReader = None,
        filter: dict | None = None,
        on_write_filter: dict | None = None,
        iter_projection: ProjectionSpec = (ID,),
        getitem_projection: ProjectionSpec = None,
        *,
        allow_operators_in_write_keys: bool = False,
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
        self.allow_operators_in_write_keys = allow_operators_in_write_keys

    def __setitem__(self, k, v):
        assert isinstance(k, Mapping) and isinstance(v, Mapping), (
            f"k (key) and v (value) must both be mappings (often dictionaries). Were:\n\tk={k}\n\tv={v}"
        )
        return self.mgc.replace_one(
            filter=self._write_filter_for_key(k),
            replacement=self._build_doc(k, v),
            upsert=True,
        )

    def __delitem__(self, k):
        assert isinstance(k, Mapping), (
            f"k (key) must be a mapping (most often a dictionary). Were:\n\tk={k}"
        )
        if len(k) > 0:
            return self.mgc.delete_one(self._write_filter_for_key(k))
        else:
            raise KeyError(f"You can't remove that key: {k}")

    def _write_filter_for_key(self, k: Mapping) -> dict:
        """The query selecting the doc(s) that a write/delete of key ``k`` targets.

        Refuses ``$``-operators in ``k`` (unless ``allow_operators_in_write_keys``),
        since a query-shaped key would select arbitrary docs of the scope.
        """
        if not self.allow_operators_in_write_keys:
            operators = operator_field_names(k)
            if operators:
                raise ValueError(
                    f"Keys used to write or delete may not contain query operators "
                    f"({', '.join(sorted(set(operators)))}). Key was: {k}"
                )
        return self._merge_with_filt(k)

    def append(self, v):
        """Insert a single doc ``v``, merged with ``on_write_filter`` if set, else this store's filter."""
        assert isinstance(v, Mapping), (
            f" v (value) must be a mapping (often a dictionary). Were:\n\tv={v}"
        )
        return self.mgc.insert_one(self._build_doc(v))

    def extend(self, values):
        """Insert several docs ``values``, each merged with ``on_write_filter`` if set, else this store's filter."""
        assert all([isinstance(v, Mapping) for v in values]), (
            f" values must be mappings (often dictionaries)"
        )
        if values:
            return self.mgc.insert_many([self._build_doc(v) for v in values])

    def _build_doc(self, *args):
        def merge_doc_elements_with_filter():
            scope = self._on_write_filter or self.filter
            d = dict(scope)
            for v in args:
                if v is None:
                    v = {}
                assert isinstance(v, Mapping), (
                    f" v (value) must be a mapping (often a dictionary). Were:\n\tv={v}"
                )
                for field, value in v.items():
                    if field in scope and value != scope[field]:
                        raise ValueError(
                            f"Field {field!r} is {value!r}, which contradicts this "
                            f"store's write scope ({field!r}: {scope[field]!r})."
                        )
                d = dict(d, **v)
            return d

        doc = merge_doc_elements_with_filter()
        is_invalid = (
            len(
                [
                    x
                    for x in doc.values()
                    if isinstance(x, Mapping)
                    and len([k for k in x.keys() if "$" in k]) > 0
                ]
            )
            > 0
        )
        if is_invalid:
            raise ValueError("The doc contains some query-specific values.")
        return doc

    def persist_data(self, data):
        """Write ``data`` (a doc with an ``_id``) under the key ``{ID: data[ID]}``."""
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
    """A ``Mapping`` view of a mongo client. Keys are database names, values are
    ``MongoDbReader`` instances for the corresponding database.

    Takes the same arguments as ``pymongo.MongoClient``.

    >>> from mongodol.base import MongoClientReader, MongoDbReader
    >>> from mongodol.util import mk_dflt_mgc
    >>> _ = mk_dflt_mgc().insert_one({'x': 1})  # ensure the default db/collection exist
    >>> client_reader = MongoClientReader()
    >>> 'mongodol' in client_reader
    True
    >>> db_reader = client_reader['mongodol']
    >>> isinstance(db_reader, MongoDbReader)
    True

    """

    @wraps(MongoClient.__init__)
    def __init__(self, *mongo_client_args, **mongo_client_kwargs):
        self._mongo_client = MongoClient(*mongo_client_args, **mongo_client_kwargs)

    def __iter__(self):
        yield from self._mongo_client.list_database_names()

    def __getitem__(self, k):
        return MongoDbReader(
            db_name=k, mongo_client=self._mongo_client
        )  # or just wrap self._mongo_client[k]?


class MongoDbReader(KvReader):
    """Base Mongo Db Reader. Keys are collection names and values are collection store instances.

    :param db_name: Name of db
    :param mk_collection_store: Function that is called on a key (collection name) to make the
        collection store instance.
        Use mk_collection_store to define what kind of collection stores you want to make.
        Will be called with only one unnamed argument; the collection name.
        Use custom classes here, and/or partials (curried functions) thereof, to fix any parameters you want to fix.
    :param mongo_client: MongoClient instance, kwargs to make it (``MongoClient(**kwargs)``), or callable to make it
    :param mongo_client_kwargs: ``**kwargs`` to make a MongoClient, that is used if mongo_client is callable

    >>> from mongodol.base import MongoDbReader
    >>> from mongodol.util import mk_dflt_mgc
    >>> _ = mk_dflt_mgc().insert_one({'x': 1})  # ensure the default db/collection exist
    >>> db_reader = MongoDbReader()
    >>> 'mongodol_test' in db_reader
    True

    """

    def __init__(
        self,
        db_name=DFLT_TEST_DB,
        mk_collection_store=MongoCollectionReader,
        mongo_client=None,
        **mongo_client_kwargs,
    ):
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


class MongoBaseStore(Store):
    """A ``Store`` that forwards the mongo bulk-read protocol through its transforms.

    Historically this was the *only* way to get ``values()``/``items()`` to honour a
    wrapper's transforms -- hence ``mongodol.trans.wrap_kvs``, which uses it as the
    wrapper class. It is no longer needed for that: :mod:`mongodol.views` resolves the
    bulk path through any wrapper chain, so plain ``dol.wrap_kvs`` now works too. It is
    kept because it also forwards the write-side bulk methods (``append``/``extend``),
    and because code may call ``iter_values()``/``contains_value()`` directly.
    """

    def contains_value(self, v):
        """Forward ``contains_value`` to the wrapped store, transforming ``v`` first."""
        return self.store.contains_value(self._data_of_obj(v))

    def iter_values(self):
        """Bulk-read all values, transforming each with ``_obj_of_data``."""
        return map(self._obj_of_data, bulk_values(self.store))

    def contains_item(self, item):
        """Forward ``contains_item`` to the wrapped store, transforming key and value first."""
        k, v = item
        return self.store.contains_item((self._id_of_key(k), self._data_of_obj(v)))

    def iter_items(self):
        """Bulk-read all ``(key, value)`` pairs, transforming each with ``_key_of_id``/``_obj_of_data``."""
        return (
            (self._key_of_id(key), self._obj_of_data(doc))
            for key, doc in bulk_items(self.store)
        )

    def append(self, v):
        """Forward ``append`` to the wrapped store, transforming ``v`` first."""
        return self.store.append(self._data_of_obj(v))

    def extend(self, values):
        """Forward ``extend`` to the wrapped store, transforming each value first."""
        return self.store.extend(list(map(self._data_of_obj, values)))

    def persist_data(self, data, key=None):
        """Write ``data`` under ``key``, through this wrapper's own ``__setitem__``.

        Unlike the leaf's ``persist_data`` (a thin ``{ID: data[ID]} -> data`` shortcut),
        this routes through ``self[key] = data``, so it applies ``_id_of_key``/
        ``_data_of_obj`` instead of bypassing them (see i2mint/mongodol#11).

        ``key`` defaults to being inferred from ``data[ID]``, for backward compatibility
        with the previous leaf-bound behaviour -- but that inference itself bypasses the
        key codec, so pass ``key`` explicitly wherever the caller already knows it.
        """
        if key is None:
            key = self._key_of_id({ID: self._data_of_obj(data)[ID]})
        return self.__setitem__(key, data)
