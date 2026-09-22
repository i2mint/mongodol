# mongodol.stores

Some useful stores for mongoDB

### Classes

| [`MongoCollectionFirstDocPersister`](#mongodol.stores.MongoCollectionFirstDocPersister)([mgc, ...])      | A mongo collection (kv-)reader where s[key] is the first key-matching value found.             |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| [`MongoCollectionFirstDocReader`](#mongodol.stores.MongoCollectionFirstDocReader)([mgc, filter, ...]) | A mongo collection (kv-)reader where s[key] is the first key-matching value found.             |
| [`MongoCollectionMultipleDocsPersister`](#mongodol.stores.MongoCollectionMultipleDocsPersister)([mgc, ...])  | A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.     |
| [`MongoCollectionMultipleDocsReader`](#mongodol.stores.MongoCollectionMultipleDocsReader)([mgc, ...])     | A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.     |
| [`MongoCollectionPersisterWithResultMapping`](#mongodol.stores.MongoCollectionPersisterWithResultMapping)([...])  | MongoCollectionPersister with result mapping                                                   |
| [`MongoCollectionUniqueDocPersister`](#mongodol.stores.MongoCollectionUniqueDocPersister)([mgc, ...])     | A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).        |
| [`MongoCollectionUniqueDocReader`](#mongodol.stores.MongoCollectionUniqueDocReader)([mgc, ...])        | A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).        |
| [`MongoStore`](#mongodol.stores.MongoStore)([store])                               | A `Store` wrapping a `MongoCollectionUniqueDocPersister`, built from host/db/collection names. |

### *class* mongodol.stores.MongoCollectionFirstDocPersister(mgc=None, filter=None, on_write_filter=None, iter_projection=('_id',), getitem_projection=None, , allow_operators_in_write_keys=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] is the first key-matching value found.
Unlike MongoCollectionUniqueDocReader, MongoCollectionFirstDocReader doesn’t check for uniqueness.

Typically, this should be used when you don’t want the overhead of checking for uniqueness,
because it doesn’t matter, you like risk, or you told the mongo collection indexing system itself to
ensure uniqueness for you.

```pycon
>>> from mongodol.stores import MongoCollectionFirstDocPersister
>>> from mongodol.tests import util
>>> test_mgc = util.populated_pymongo_collection([])
>>> s = MongoCollectionFirstDocPersister(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection={'n': True, '_id': False})
>>> s[{'s': 'a'}] = {'n': 1}
>>> s[{'s': 'a'}]
{'n': 1}
```

A second doc matching the same key does not raise; `s[key]` keeps returning
the first match found:

```pycon
>>> _ = s.mgc.insert_one({'s': 'a', 'n': 999})
>>> s[{'s': 'a'}]
{'n': 1}
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### allow_operators_in_write_keys

bool(x) -> bool

Returns True when the argument x is true, False otherwise.
The builtins True and False are the only two instances of the class bool.
The class bool is a subclass of the class int, and cannot be subclassed.

#### append(v)

Insert a single doc `v`, merged with `on_write_filter` if set, else this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### extend(values)

Insert several docs `values`, each merged with `on_write_filter` if set, else this store’s filter.

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### persist_data(data)

Write `data` (a doc with an `_id`) under the key `{ID: data[ID]}`.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoCollectionFirstDocReader(mgc=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] is the first key-matching value found.
Unlike MongoCollectionUniqueDocReader, MongoCollectionFirstDocReader doesn’t check for uniqueness.

Typically, this should be used when you don’t want the overhead of checking for uniqueness,
because it doesn’t matter, you like risk, or you told the mongo collection indexing system itself to
ensure uniqueness for you.

```pycon
>>> from mongodol.stores import MongoCollectionFirstDocReader
>>> from mongodol.tests import data, util
>>> test_mgc = util.populated_pymongo_collection(data.three_simple_docs)
>>> s = MongoCollectionFirstDocReader(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection=['n'])
>>> assert list(s) == [{'s': 'a'}, {'s': 'b'}, {'s': 'b'}]
```

Unlike `MongoCollectionUniqueDocReader`, a key matching more than one doc
doesn’t raise; it just returns the first match found:

```pycon
>>> s[{'s': 'a'}]
{'_id': 0, 'n': 1}
>>> s[{'s': 'b'}]
{'_id': 1, 'n': 2}
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoCollectionMultipleDocsPersister(mgc=None, filter=None, on_write_filter=None, iter_projection=('_id',), getitem_projection=None, , allow_operators_in_write_keys=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.
If no docs match, will return an empty list.

`s[key] = v` first deletes every doc matching `key`, then inserts `v`
(a doc, or a collection of docs) merged with `key`:

```pycon
>>> from mongodol.stores import MongoCollectionMultipleDocsPersister
>>> from mongodol.tests import util
>>> test_mgc = util.populated_pymongo_collection([])
>>> s = MongoCollectionMultipleDocsPersister(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection={'n': True, '_id': False})
>>> s[{'s': 'a'}] = [{'n': 1}, {'n': 2}]
>>> s[{'s': 'a'}]
[{'n': 1}, {'n': 2}]
```

```pycon
>>> s[{'s': 'a'}] = {'n': 3}  # replaces the two docs above with just this one
>>> s[{'s': 'a'}]
[{'n': 3}]
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### allow_operators_in_write_keys

bool(x) -> bool

Returns True when the argument x is true, False otherwise.
The builtins True and False are the only two instances of the class bool.
The class bool is a subclass of the class int, and cannot be subclassed.

#### append(v)

Insert a single doc `v`, merged with `on_write_filter` if set, else this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### extend(values)

Insert several docs `values`, each merged with `on_write_filter` if set, else this store’s filter.

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### persist_data(data)

Write `data` (a doc with an `_id`) under the key `{ID: data[ID]}`.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoCollectionMultipleDocsReader(mgc=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] will return the list of all key-matching docs.
If no docs match, will return an empty list.

```pycon
>>> from mongodol.stores import MongoCollectionMultipleDocsReader
>>> from mongodol.tests import data, util
>>> test_mgc = util.populated_pymongo_collection(data.three_simple_docs)
>>> s = MongoCollectionMultipleDocsReader(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection=['n'])
>>> s[{'s': 'a'}]
[{'_id': 0, 'n': 1}]
>>> s[{'s': 'b'}]
[{'_id': 1, 'n': 2}, {'_id': 2, 'n': 3}]
>>> s[{'s': 'nonexistent'}]
[]
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoCollectionPersisterWithResultMapping(mgc=None, filter=None, on_write_filter=None, iter_projection=('_id',), getitem_projection=None, , allow_operators_in_write_keys=None, \*\*mgc_find_kwargs)

Bases: [`MongoCollectionPersister`](mongodol.base.html.md#mongodol.base.MongoCollectionPersister)

MongoCollectionPersister with result mapping

#### append(v)

Insert a single doc `v`, merged with `on_write_filter` if set, else this store’s filter.

#### extend(values)

Insert several docs `values`, each merged with `on_write_filter` if set, else this store’s filter.

### *class* mongodol.stores.MongoCollectionUniqueDocPersister(mgc=None, filter=None, on_write_filter=None, iter_projection=('_id',), getitem_projection=None, , allow_operators_in_write_keys=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).

* **Raises:**
  [**KeyNotUniqueError**](mongodol.util.html.md#mongodol.util.KeyNotUniqueError) – if the k matches more than a single unique doc.

```pycon
>>> from mongodol.stores import MongoCollectionUniqueDocPersister
>>> from mongodol.tests import util
>>> test_mgc = util.populated_pymongo_collection([])
>>> s = MongoCollectionUniqueDocPersister(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection={'n': True, '_id': False})
>>> s[{'s': 'a'}] = {'n': 1}
>>> list(s)
[{'s': 'a'}]
>>> s[{'s': 'a'}]
{'n': 1}
```

```pycon
>>> s[{'s': 'b'}] = {'n': 2}
>>> _ = s.mgc.insert_one({'s': 'b', 'n': 99})
>>> s[{'s': 'b'}]
Traceback (most recent call last):
  ...
mongodol.util.KeyNotUniqueError: Key was not unique (i.e. cursor has more than one match): {'s': 'b'}
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### allow_operators_in_write_keys

bool(x) -> bool

Returns True when the argument x is true, False otherwise.
The builtins True and False are the only two instances of the class bool.
The class bool is a subclass of the class int, and cannot be subclassed.

#### append(v)

Insert a single doc `v`, merged with `on_write_filter` if set, else this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### extend(values)

Insert several docs `values`, each merged with `on_write_filter` if set, else this store’s filter.

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### persist_data(data)

Write `data` (a doc with an `_id`) under the key `{ID: data[ID]}`.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoCollectionUniqueDocReader(mgc=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Bases: `Store`

A mongo collection (kv-)reader where s[key] is the dict (a mongo doc matching the key).

* **Raises:**
  [**KeyNotUniqueError**](mongodol.util.html.md#mongodol.util.KeyNotUniqueError) – if the k matches more than a single unique doc.

```pycon
>>> from mongodol.stores import MongoCollectionUniqueDocReader
>>> from mongodol.tests import data, util
>>> test_mgc = util.populated_pymongo_collection(data.three_simple_docs)
>>> s = MongoCollectionUniqueDocReader(test_mgc,
...     iter_projection={'s': True, '_id': False}, getitem_projection=['n'])
>>> assert list(s) == [{'s': 'a'}, {'s': 'b'}, {'s': 'b'}]
```

And you see where the problem will be: There’s two {‘s’: ‘b’} in that listing,
so though getting the value for {‘s’: ‘a’} won’t be a problem:

```pycon
>>> assert s[{'s': 'a'}] == {'_id': 0, 'n': 1}  # there's only one doc matching {'s': 'a'}
```

… but there’s more than one doc matching {‘s’: ‘b’}

```pycon
>>> s[{'s': 'b'}]
Traceback (most recent call last):
  ...
mongodol.util.KeyNotUniqueError: Key was not unique (i.e. cursor has more than one match): {'s': 'b'}
```

#### aggregate(pipeline, \*\*kwargs)

Run a mongo aggregation `pipeline`, prefixed with a `$match` on this store’s filter.

#### contains_item(item)

Bulk-read counterpart of `__contains__` for `(key, value)` pairs.

#### contains_value(v)

Bulk-read counterpart of `__contains__` for values: is there a doc matching `v`?

#### distinct(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### from_params(db_name='mongodol', collection_name='test', mongo_client=None, filter=None, iter_projection=('_id',), getitem_projection=None, \*\*mgc_find_kwargs)

Make an instance from db/collection names and connection params, instead of a live mongo collection object.

#### iter_items()

Bulk-read all `(key, value)` pairs in a single `find` query, splitting each doc into
its key fields and the rest.

#### iter_values()

Bulk-read all values in a single `find` query (see the module’s bulk-read protocol).

#### key_fields

The field names (from `iter_projection`) that make up a key.

#### mgc_repr

A short `<database/collection>` string identifying the wrapped mongo collection.

#### unique(key, filter=None, \*\*kwargs)

The distinct values of `key` across docs matching `filter` (merged with this store’s own filter).

#### val_fields

The field names (from `getitem_projection`) that make up a value, or None if unset.

### *class* mongodol.stores.MongoStore(store=<class 'dict'>)

Bases: `Store`

A `Store` wrapping a `MongoCollectionUniqueDocPersister`, built from host/db/collection names.
