# mongodol.util

Util functions

### Functions

| [`flatten_dict_items`](#mongodol.util.flatten_dict_items)(d[, prefix])              | Computes a "flat" dict from a nested one.                                                                                                                                                                                                       |
|-----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`get_key_value_specs`](#mongodol.util.get_key_value_specs)(key_fields, data_fields) | Derive `key_projection` (and, when `data_fields` is None or a non-dict iterable, `items_projection`) from `key_fields`/`data_fields`.                                                                                                           |
| [`get_mongo_collection_pymongo_obj`](#mongodol.util.get_mongo_collection_pymongo_obj)([obj, ...]) | Get a pymongo.collection.Collection object for a mongo collection, flexibly.                                                                                                                                                                    |
| [`mk_dflt_client`](#mongodol.util.mk_dflt_client)()                             | Make a `pymongo.MongoClient` with the default client args.                                                                                                                                                                                      |
| [`mk_dflt_mgc`](#mongodol.util.mk_dflt_mgc)()                                | Make a default `pymongo.collection.Collection`, connecting with default client args to the default test database and collection.                                                                                                                |
| [`normalize_projection`](#mongodol.util.normalize_projection)(projection)             | Normalize projection specification to be an explicit list of flattened dict of {path.to.key: True/False,.                                                                                                                                       |
| [`projection_union`](#mongodol.util.projection_union)(projection_1, projection_2) | Flatten and merge two mongo projection dicts, OR-ing every field against a forced default of `True` -- so a field appearing in only one of the two dicts (or with a `False` value) still comes out `True` unless both dicts agree it's `False`. |

### Exceptions

| [`KeyNotUniqueError`](#mongodol.util.KeyNotUniqueError)   | Raised when a key was expected to be unique, but wasn't (i.e. cursor has more than one match).   |
|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|

### *exception* mongodol.util.KeyNotUniqueError

Bases: [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError)

Raised when a key was expected to be unique, but wasn’t (i.e. cursor has more than one match)

#### *static* raise_error(k)

Raise `KeyNotUniqueError` for the non-unique key `k`.

### mongodol.util.flatten_dict_items(d, prefix='')

Computes a “flat” dict from a nested one. A flat dict’s keys are the dot-paths of the input dict.

* **Parameters:**
  * **d** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)) – a nested dict
  * **prefix** – A string to prepend on all the paths
* **Returns:**
  A flat dict

```pycon
>>> d = {'a': {
...         'a': '2a',
...         'c': {'a': 'aca', 'u': 4}
...         },
...      'c': 3
...     }
>>> dict(flatten_dict_items(d))
{'a.a': '2a', 'a.c.a': 'aca', 'a.c.u': 4, 'c': 3}
```

### mongodol.util.get_key_value_specs(key_fields, data_fields)

Derive `key_projection` (and, when `data_fields` is None or a non-dict iterable,
`items_projection`) from `key_fields`/`data_fields`.

#### NOTE
when `data_fields` is already a dict, `items_projection` is never assigned,
so this branch raises `UnboundLocalError` on the `return` below.

### mongodol.util.get_mongo_collection_pymongo_obj(obj=None, client_factory=<function mk_dflt_client>)

Get a pymongo.collection.Collection object for a mongo collection, flexibly.

```text
get_mongo_collection_pymongo_obj()  # gives you a default mongo collection (mongodol/mongodol_test)
get_mongo_collection_pymongo_obj('database_name/collection_name')  # does the obvious (with default host)
get_mongo_collection_pymongo_obj(... an object that has an _mgc attribute...)  # return the _mgc attribute
get_mongo_collection_pymongo_obj(obj)  # else, asserts pymongo.collection.Collection and returns it
```

```pycon
>>> from mongodol.util import get_mongo_collection_pymongo_obj
>>> c = get_mongo_collection_pymongo_obj()
>>> c.name, c.database.name
('mongodol_test', 'mongodol')
```

An object with an `_mgc` attribute (such as a mongodol store) has that
attribute returned directly:

```pycon
>>> from mongodol.tests import util
>>> mgc = util.populated_pymongo_collection([])
>>> store = type('Store', (), {'_mgc': mgc})()
>>> get_mongo_collection_pymongo_obj(store) is mgc
True
```

### mongodol.util.mk_dflt_client()

Make a `pymongo.MongoClient` with the default client args.

### mongodol.util.mk_dflt_mgc()

Make a default `pymongo.collection.Collection`, connecting with default
client args to the default test database and collection.

```pycon
>>> from mongodol.util import mk_dflt_mgc
>>> c = mk_dflt_mgc()
>>> c.name, c.database.name
('mongodol_test', 'mongodol')
```

### mongodol.util.normalize_projection(projection)

Normalize projection specification to be an explicit list of flattened dict of {path.to.key: True/False,…
(or None if projection is None to start with).

This is used to be able to have a consistent specification of mongo projections.

If projection is None, the output will None as well:

```pycon
>>> assert normalize_projection(None) is None
```

If projection is a dict, the dict will be “flattened” to use “dot-paths” instead of nested dicts:

```pycon
>>> normalize_projection({'name': {'first': True, 'last': False}, 'age': True})
{'name.first': True, 'name.last': False, 'age': True}
```

If projection is not a dict, it will make the “equivalent” dict version of the projection.
One difference with mongodb’s projection language: Here, if you don’t specify that you want “_id”,
it will explicitly specify that you DO NOT want that field (because mongodb will otherwise assume that you do!)

```pycon
>>> normalize_projection(['name.first', 'age'])
{'name.first': True, 'age': True, '_id': False}
```

But if you actually want that “_id”, just say so:

```pycon
>>> normalize_projection(['name.first', 'age', '_id'])
{'name.first': True, 'age': True, '_id': True}
```

Also, if you specify a string, it will think of this as a tuple containing just that string:

```pycon
>>> normalize_projection('name.last')
{'name.last': True, '_id': False}
```

### mongodol.util.projection_union(projection_1, projection_2, already_flattened=False)

Flatten and merge two mongo projection dicts, OR-ing every field against a forced
default of `True` – so a field appearing in only one of the two dicts (or with a
`False` value) still comes out `True` unless both dicts agree it’s `False`.

```pycon
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
```
