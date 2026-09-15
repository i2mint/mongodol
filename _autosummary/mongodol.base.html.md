# mongodol.base

Base mongoDB data object layers

### Classes

| [`MongoBaseStore`](#mongodol.base.MongoBaseStore)([store])                         | A `Store` that forwards the mongo bulk-read protocol through its transforms.                                   |
|--------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| [`MongoClientReader`](#mongodol.base.MongoClientReader)([host, port, ...])            | A `Mapping` view of a mongo client.                                                                            |
| [`MongoCollectionCollection`](#mongodol.base.MongoCollectionCollection)([mgc, filter, ...])   | Base class wrapping a mongo collection with a fixed `filter` and `iter_projection`.                            |
| [`MongoCollectionFieldsReader`](#mongodol.base.MongoCollectionFieldsReader)([mgc, filter, ...]) | A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.  |
| [`MongoCollectionPersister`](#mongodol.base.MongoCollectionPersister)([mgc, filter, ...])    | base class to read from and write to a mongo collection, or subset thereof, with the MutableMapping interface. |
| [`MongoCollectionReader`](#mongodol.base.MongoCollectionReader)([mgc, filter, ...])       | A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.  |
| [`MongoDbReader`](#mongodol.base.MongoDbReader)([db_name, ...])                   | Base Mongo Db Reader.                                                                                          |

### *class* mongodol.base.MongoBaseStore(store=<class 'dict'>)

Bases: `Store`

A `Store` that forwards the mongo bulk-read protocol through its transforms.

Historically this was the *only* way to get `values()`/`items()` to honour a
wrapper’s transforms – hence `mongodol.trans.wrap_kvs`, which uses it as the
wrapper class. It is no longer needed for that: [`mongodol.views`](mongodol.views.html.md#module-mongodol.views) resolves the
bulk path through any wrapper chain, so plain `dol.wrap_kvs` now works too. It is
kept because it also forwards the write-side bulk methods (`append`/`extend`),
and because code may call `iter_values()`/`contains_value()` directly.

#### append(v)

Forward `append` to the wrapped store, transforming `v` first.

#### contains_item(item)

Forward `contains_item` to the wrapped store, transforming key and value first.

#### contains_value(v)

Forward `contains_value` to the wrapped store, transforming `v` first.

#### extend(values)

Forward `extend` to the wrapped store, transforming each value first.

#### iter_items()

Bulk-read all `(key, value)` pairs, transforming each with `_key_of_id`/`_obj_of_data`.

#### iter_values()

Bulk-read all values, transforming each with `_obj_of_data`.

### *class* mongodol.base.MongoClientReader(host=None, port=None, document_class=<class 'dict'>, tz_aware=None, connect=None, type_registry=None, \*\*kwargs)

Bases: `KvReader`

A `Mapping` view of a mongo client. Keys are database names, values are
`MongoDbReader` instances for the corresponding database.

Takes the same arguments as `pymongo.MongoClient`.

```pycon
>>> from mongodol.base import MongoClientReader, MongoDbReader
>>> from mongodol.util import mk_dflt_mgc
>>> _ = mk_dflt_mgc().insert_one({'x': 1})  # ensure the default db/collection exist
>>> client_reader = MongoClientReader()
>>> 'mongodol' in client_reader
True
>>> db_reader = client_reader['mongodol']
>>> isinstance(db_reader, MongoDbReader)
True
```

### *class* mongodol.base.MongoCollectionCollection(mgc=None, filter=None, iter_projection=None, \*\*mgc_find_kwargs)

Bases: `Collection`

Base class wrapping a mongo collection with a fixed `filter` and `iter_projection`.

#### *property* mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

### *class* mongodol.base.MongoCollectionFieldsReader(mgc=None, filter=None, key_fields=('_id',), val_fields=None)

Bases: [`MongoCollectionReader`](#mongodol.base.MongoCollectionReader)

A base class to read from a mongo collection, or subset thereof, with the Mapping (i.e. dict-like) interface.

An “easier” interface for the common case where we just want to specify fixed fields for keys and vals.

### *class* mongodol.base.MongoCollectionPersister(mgc=None, filter=None, on_write_filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Bases: [`MongoCollectionReader`](#mongodol.base.MongoCollectionReader)

base class to read from and write to a mongo collection, or subset thereof, with the MutableMapping interface.

```pycon
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
```

Since this is a base mongo store, the values are cursors, so to get an actual value, you need to fetch the first doc

```pycon
>>> next(s[k])
{'val': 'bar'}
>>> next(s.get(k))
{'val': 'bar'}
```

Remember (see `MongoCollectionReader` docs) that `s.get` will never reach its default since
the reader will always return a cursor (possibly empty).
So in the following case, we should get an empty cursor (not a default value)

```pycon
>>> list(s.get({'not': 'a key'}, {'default': 'val'}))  # testing s.get with default
[]
```

```pycon
>>> list(s.values())
[{'val': 'bar'}]
>>> k in s  # testing __contains__ again
True
>>> k in s.keys()  # test the contains capability of s.keys() (a MongoKeysView instance)
True
>>> del s[k]
>>> len(s)
0
```

```pycon
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
```

#### append(v)

Insert a single doc `v`, merged with `on_write_filter` if set, else this store’s filter.

#### extend(values)

Insert several docs `values`, each merged with `on_write_filter` if set, else this store’s filter.

#### persist_data(data)

Write `data` (a doc with an `_id`) under the key `{ID: data[ID]}`.

### *class* mongodol.base.MongoCollectionReader(mgc=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Bases: [`MongoCollectionCollection`](#mongodol.base.MongoCollectionCollection), `KvReader`

A base class to read from a mongo collection, or subset thereof, with the Mapping
(i.e. dict-like) interface.

Some examples below. For examples using actual data (with setup and tear down)
see the tests/ folder.

```pycon
>>> from pymongo import MongoClient
>>> s = MongoCollectionReader(MongoClient()['mongodol']['mongodol_test'])
>>> list_of_keys = list(s)
>>> fake_key = {'_id': 'this key does not exist'}
>>> fake_key in s
False
```

It’s important to note that `s[k]` (for any base MongoCollectionReader instance `s`) returns a Cursor,
and will always return a Cursor, no matter what key `k` you ask for
– as long as the key is a valid mapping (dict usually).
This cursor is a (pymongo) object that is used to iterate over the results of the `k` lookup.
It may yield no results what-so-ever, or one, or many.

```pycon
>>> v = s[fake_key]
>>> type(v).__name__
'Cursor'
>>> len(list(v))  # but the cursor yields no results
0
```

Indeed, `MongoCollectionReader` is really meant to provide a low level key-value interface to a mongo collection
that is really meant to be wrapped in order to produce the actual key-value interfaces one needs.
You shouldn’t think of it’s instances as a normal dict where any request for the value under a key,
for a key that doesn’t exist, will result in a `KeyError`.
Note that this means that `s.get(k, default)` will never result in the default being returned,
since there are no missing keys here; only empty results (cursors that don’t yield anything).

```pycon
>>> v = s.get(fake_key, {'the': 'default'})
>>> assert v != {'the': 'default'}
```

`s.keys()`, `s.values()`, and `s.items()` are `collections.abc.MappingViews` instances
(specialized for mongo – see [`mongodol.views`](mongodol.views.html.md#module-mongodol.views): they fetch the whole collection in
a single query, and keep doing so, correctly, when the store is wrapped by `dol`).

```pycon
>>> assert type(s.keys()) == s.KeysView
>>> assert type(s.values()) == s.ValuesView
>>> assert type(s.items()) == s.ItemsView
```

Recall that `collections.abc.MappingViews` have many set-like functionalities:

```pycon
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
```

Note though that since keys and values are both dictionaries in mongo, some of these set-like functionalities
might not work (complaints such as `TypeError: unhashable type: 'dict'`),
such as:

```pycon
>>> s.keys() | a_list_of_fake_keys
Traceback (most recent call last):
    ...
TypeError: unhashable type: 'dict'
```

But you can take care of that in higher level wrappers that have hashable keys and/or values.

#### ItemsView

alias of [`MongoItemsView`](mongodol.views.html.md#mongodol.views.MongoItemsView)

#### ValuesView

Views that resolve the bulk-read fast path through any `dol` wrapper chain,
rather than through blind attribute delegation. See [`mongodol.views`](mongodol.views.html.md#module-mongodol.views).

alias of [`MongoValuesView`](mongodol.views.html.md#mongodol.views.MongoValuesView)

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### *classmethod* from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### *property* key_fields

The field names (from `iter_projection`) that make up a key.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### *property* val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.base.MongoDbReader(db_name='mongodol', mk_collection_store=<class 'mongodol.base.MongoCollectionReader'>, mongo_client=None, \*\*mongo_client_kwargs)

Bases: `KvReader`

Base Mongo Db Reader. Keys are collection names and values are collection store instances.

* **Parameters:**
  * **db_name** – Name of db
  * **mk_collection_store** – Function that is called on a key (collection name) to make the
    collection store instance.
    Use mk_collection_store to define what kind of collection stores you want to make.
    Will be called with only one unnamed argument; the collection name.
    Use custom classes here, and/or partials (curried functions) thereof, to fix any parameters you want to fix.
  * **mongo_client** – MongoClient instance, kwargs to make it (`MongoClient(**kwargs)`), or callable to make it
  * **mongo_client_kwargs** – `**kwargs` to make a MongoClient, that is used if mongo_client is callable

```pycon
>>> from mongodol.base import MongoDbReader
>>> from mongodol.util import mk_dflt_mgc
>>> _ = mk_dflt_mgc().insert_one({'x': 1})  # ensure the default db/collection exist
>>> db_reader = MongoDbReader()
>>> 'mongodol_test' in db_reader
True
```
