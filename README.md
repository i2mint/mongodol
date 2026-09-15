# mongodol

Access MongoDB through a `Mapping` (dict-like) interface.

`mongodol` wraps `pymongo` collections as `Mapping`/`MutableMapping` objects (readers and
persisters), so you can read and write mongo data with normal `dict`-like syntax, and
compose your own key/value transforms with [`dol`](https://github.com/i2mint/dol) wrappers
instead of writing backend-specific boilerplate.

To install:

```
pip install mongodol
```

And of course, you need a running MongoDB -- see the
[installation instructions](https://www.mongodb.com/docs/manual/installation/).

<!-- epythet:agentic-readme:start -->
## For AI agents

`mongodol` publishes its documentation in forms made for coding agents. If you are one, start here.

**The documentation, machine-readable**: [`llms.txt`](https://i2mint.github.io/mongodol/llms.txt) indexes every page; [`mongodol.md`](https://i2mint.github.io/mongodol/mongodol.md) is the whole documentation in one file; every page has a `.md` twin; [`objects.inv`](https://i2mint.github.io/mongodol/objects.inv) maps symbols to URLs.

If you are a control freak, the rest of this README is written for you, starting at [Quick start](#quick-start).
<!-- epythet:agentic-readme:end -->

## Quick start

```python
from mongodol import MongoCollectionPersister, mk_dflt_mgc

# mk_dflt_mgc() gives you a pymongo collection to play with (mongodol/mongodol_test by default)
mgc = mk_dflt_mgc()
mgc.delete_many(
    {}
)  # start from an empty collection (skip this to keep what's already there)
s = MongoCollectionPersister(mgc, getitem_projection={"_id": False})

len(s)
# 0

k = {"_id": "my_id"}
s[k] = {"mongo": "uses", "json": "data"}
list(s)
# [{'_id': 'my_id'}]
```

Since the base reader is a thin, low-level wrapper, `s[k]` returns a `pymongo.cursor.Cursor`
(a key may match zero, one, or many docs), so you fetch the value(s) explicitly:

```python
next(s[k])
# {'mongo': 'uses', 'json': 'data'}

del s[k]
len(s)
# 0
```

## Beyond the base classes

The base `MongoCollectionReader`/`MongoCollectionPersister` classes always return cursors
and never validate uniqueness. For the common case of "one key maps to one doc", use one
of the `*UniqueDoc*`/`*FirstDoc*` reader and persister classes instead:

```python
from mongodol import MongoCollectionUniqueDocReader
```

`MongoCollectionUniqueDocReader` gives you `s[k]` as a plain `dict` (not a cursor), and
raises `KeyNotUniqueError` if more than one doc matches `k`. See its docstring for a
runnable example.

For custom key/value shapes, business logic, or connecting `mongodol` stores to the rest
of the [`dol`](https://github.com/i2mint/dol) ecosystem (caching, serialization,
key transforms, etc.), wrap a `mongodol` store with `dol.wrap_kvs` like you would any
other `dol` store.

## More

See the [package documentation](https://i2mint.github.io/mongodol/) and the flat
[`mongodol.md`](https://i2mint.github.io/mongodol/mongodol.md) aggregate for the full API.
