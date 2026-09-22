# mongodol.views

Mapping views that keep working when a mongo store is wrapped by `dol`.

A mongo collection can serve a store’s whole `(key, value)` stream in a single
`find` round trip, so [`MongoCollectionReader`](mongodol.base.md#mongodol.base.MongoCollectionReader) implements a
**bulk-read protocol** – `iter_values`, `iter_items`, `contains_value` and
`contains_item` – and exposes it through the `values()`/`items()` views
defined here. One query instead of N is the whole point of these views.

The catch is *composition*. A `dol` `Store` wrapper (what
`wrap_kvs` builds) forwards every attribute it doesn’t define to the store it
wraps. A view that simply calls `self._mapping.iter_values()` therefore punches
straight through the wrappers and yields raw backend documents, silently skipping
the value transforms the user asked for – breaking the `Mapping` contract:

```default
list(store.values()) == [store[k] for k in store]
```

(see [i2mint/mongodol#7](https://github.com/i2mint/mongodol/issues/7)).

This module resolves the bulk stream **explicitly** instead of relying on
attribute delegation. Given the store a view was built on, [`bulk_values()`](#mongodol.views.bulk_values)
and [`bulk_items()`](#mongodol.views.bulk_items) walk the wrapper chain inward, remembering each layer they
cross, until they reach a store that actually implements the bulk-read protocol.
The bulk stream is then re-transformed by the crossed layers, innermost first, so
that it lands in exactly the same space as `store[k]`.

A layer may only be crossed if its read path is *plain transform composition* –
“read from the inner store, then apply `_key_of_id`/`_obj_of_data`”, which is
what `Store` does. A layer that redefines `__getitem__` or
`__iter__` (`wrap_kvs(postget=...)`, `filt_iter`, `cached_keys`, …)
changes values or key sets in ways that cannot be pushed onto a bulk stream, so
the resolver refuses to guess: it raises [`NoBulkReadPath`](#mongodol.views.NoBulkReadPath) and the views
fall back to the generic per-key behaviour. That fallback is correct, just one
round trip per key – correctness first, efficiency when it is provable.

Simple use is invisible: build a mongo store, wrap it however you like, and
`values()`/`items()` agree with `__getitem__`. The knobs, for store authors:

- Implement the bulk-read methods to *provide* the fast path.
- Set the [`BULK_READ_IS_FAITHFUL_ATTR`](#mongodol.views.BULK_READ_IS_FAITHFUL_ATTR) class attribute to `False`
  (see [`disable_bulk_read()`](#mongodol.views.disable_bulk_read)) when a class inherits bulk-read methods
  that no longer agree with its own `__getitem__`.

Known limitation. [`MongoCollectionReader`](mongodol.base.md#mongodol.base.MongoCollectionReader) is deliberately a
*cursor*-level store: `s[k]` is a pymongo `Cursor`, while its bulk stream
already yields *documents* – one per key. The two only line up once a single-doc
layer (`MongoCollectionFirstDocReader` and friends) has turned cursors into
docs, which is why those are the stores you are meant to wrap. Hanging an
`obj_of_data` that expects a cursor directly off the raw reader is outside the
protocol: such a transform cannot be pushed onto a doc-level bulk stream, and is
not detectable from here.

Nothing here is mongo-specific; it is a general answer to “how does a store with
a bulk-read fast path compose with `dol` wrappers?”, and would be a reasonable
thing for `dol` itself to own one day.

### Module Attributes

| [`ITER_VALUES_METHOD`](#mongodol.views.ITER_VALUES_METHOD)         | Bulk-read method yielding a store's values in one backend round trip.                                                       |
|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| [`ITER_ITEMS_METHOD`](#mongodol.views.ITER_ITEMS_METHOD)          | Bulk-read method yielding a store's `(key, value)` pairs in one backend round trip.                                         |
| [`CONTAINS_VALUE_METHOD`](#mongodol.views.CONTAINS_VALUE_METHOD)      | Bulk-read method answering "is this value in the store?" in one backend round trip.                                         |
| [`CONTAINS_ITEM_METHOD`](#mongodol.views.CONTAINS_ITEM_METHOD)       | Bulk-read method answering "is this item in the store?" in one backend round trip.                                          |
| [`BULK_READ_IS_FAITHFUL_ATTR`](#mongodol.views.BULK_READ_IS_FAITHFUL_ATTR) | Class attribute through which a store declares whether its bulk-read methods are value-equivalent to its own `__getitem__`. |
| [`INNER_STORE_ATTR`](#mongodol.views.INNER_STORE_ATTR)           | The `dol` `Store` attribute holding the store a wrapper wraps.                                                              |

### Functions

| [`bulk_contains_item`](#mongodol.views.bulk_contains_item)(store, item)         | Ask the backend whether `item` is one of `store`'s items, in one round trip.          |
|------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`bulk_contains_value`](#mongodol.views.bulk_contains_value)(store, v)           | Ask the backend whether `v` is one of `store`'s values, in one round trip.            |
| [`bulk_items`](#mongodol.views.bulk_items)(store)                       | Iterate `store`'s `(key, value)` pairs via the backend's bulk-read path.              |
| [`bulk_values`](#mongodol.views.bulk_values)(store)                      | Iterate `store`'s values via the backend's bulk-read path, transforms honoured.       |
| [`disable_bulk_read`](#mongodol.views.disable_bulk_read)(store_cls)            | Class decorator declaring that inherited bulk-read methods are not to be trusted.     |
| [`is_crossable`](#mongodol.views.is_crossable)(store)                     | Whether `store` is a `Store` layer whose read path is plain transform composition.    |
| [`provides_bulk_read`](#mongodol.views.provides_bulk_read)(store, method_name)  | Whether `store`'s own class implements bulk-read `method_name`, faithfully.           |
| [`resolve_bulk_source`](#mongodol.views.resolve_bulk_source)(store, method_name) | Find the store providing bulk-read `method_name`, and the layers crossed to reach it. |
| [`store_layers`](#mongodol.views.store_layers)(store)                     | Yield `store` then each store it wraps, outermost first, innermost last.              |

### Classes

| [`MongoItemsView`](#mongodol.views.MongoItemsView)(mapping)   | An `items()` view that uses the backend's bulk read when -- and only when -- that stream provably equals `((k, store[k]) for k in store)`.   |
|----------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| [`MongoValuesView`](#mongodol.views.MongoValuesView)(mapping)  | A `values()` view that uses the backend's bulk read when -- and only when -- that stream provably equals `(store[k] for k in store)`.        |

### Exceptions

| [`NoBulkReadPath`](#mongodol.views.NoBulkReadPath)   | No bulk-read stream can be *proven* equivalent to the store's per-key reads.   |
|-------------------------------------------------------------------|--------------------------------------------------------------------------------|

### mongodol.views.BULK_READ_IS_FAITHFUL_ATTR *= '_bulk_read_is_faithful'*

Class attribute through which a store declares whether its bulk-read methods are
value-equivalent to its own `__getitem__`. It defaults to `True` (a class that
implements the protocol is trusted to implement it faithfully). It exists because
`dol`’s class-decorator wrapping *copies* the wrapped class’s extra methods onto
the wrapper, so a wrapper that redefines value semantics – `wrap_kvs(postget=...)`
– silently inherits bulk-read methods that no longer match it. Such a class sets
this to `False`; see [`disable_bulk_read()`](#mongodol.views.disable_bulk_read).

### mongodol.views.CONTAINS_ITEM_METHOD *= 'contains_item'*

Bulk-read method answering “is this item in the store?” in one backend round trip.

### mongodol.views.CONTAINS_VALUE_METHOD *= 'contains_value'*

Bulk-read method answering “is this value in the store?” in one backend round trip.

### mongodol.views.INNER_STORE_ATTR *= 'store'*

The `dol` `Store` attribute holding the store a wrapper wraps.

### mongodol.views.ITER_ITEMS_METHOD *= 'iter_items'*

Bulk-read method yielding a store’s `(key, value)` pairs in one backend round trip.

### mongodol.views.ITER_VALUES_METHOD *= 'iter_values'*

Bulk-read method yielding a store’s values in one backend round trip.

### *class* mongodol.views.MongoItemsView(mapping)

Bases: [`ItemsView`](https://docs.python.org/3/library/collections.abc.html#collections.abc.ItemsView)

An `items()` view that uses the backend’s bulk read when – and only when –
that stream provably equals `((k, store[k]) for k in store)`.

### *class* mongodol.views.MongoValuesView(mapping)

Bases: [`ValuesView`](https://docs.python.org/3/library/collections.abc.html#collections.abc.ValuesView)

A `values()` view that uses the backend’s bulk read when – and only when –
that stream provably equals `(store[k] for k in store)`.

### *exception* mongodol.views.NoBulkReadPath

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

No bulk-read stream can be *proven* equivalent to the store’s per-key reads.

Raised by the resolvers of this module, and caught by the views, which then
fall back to the generic (correct, one-round-trip-per-key) `Mapping`
behaviour. It is a control-flow signal, not a user-facing error.

### mongodol.views.bulk_contains_item(store, item)

Ask the backend whether `item` is one of `store`’s items, in one round trip.

* **Raises:**
  [**NoBulkReadPath**](#mongodol.views.NoBulkReadPath) – when `item` cannot be pushed down to backend space.
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### mongodol.views.bulk_contains_value(store, v)

Ask the backend whether `v` is one of `store`’s values, in one round trip.

* **Raises:**
  [**NoBulkReadPath**](#mongodol.views.NoBulkReadPath) – when `v` cannot be pushed down to backend space.
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### mongodol.views.bulk_items(store)

Iterate `store`’s `(key, value)` pairs via the backend’s bulk-read path.

* **Raises:**
  [**NoBulkReadPath**](#mongodol.views.NoBulkReadPath) – when the bulk stream cannot be proven equivalent to
  `((k, store[k]) for k in store)`.
* **Return type:**
  [`Iterator`](https://docs.python.org/3/library/typing.html#typing.Iterator)[[`Tuple`](https://docs.python.org/3/library/typing.html#typing.Tuple)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]]

### mongodol.views.bulk_values(store)

Iterate `store`’s values via the backend’s bulk-read path, transforms honoured.

* **Raises:**
  [**NoBulkReadPath**](#mongodol.views.NoBulkReadPath) – when the bulk stream cannot be proven equivalent to
  `(store[k] for k in store)`.
* **Return type:**
  [`Iterator`](https://docs.python.org/3/library/typing.html#typing.Iterator)

### mongodol.views.disable_bulk_read(store_cls)

Class decorator declaring that inherited bulk-read methods are not to be trusted.

Use it on a class that changes what `__getitem__` returns (typically via
`wrap_kvs(postget=...)`) while inheriting – or being handed, by `dol`’s
class-decorator wrapping – bulk-read methods written for the *un*-changed
semantics. Views then take the correct per-key path instead.

* **Return type:**
  [`type`](https://docs.python.org/3/builtins/functions.html#type)

### mongodol.views.is_crossable(store)

Whether `store` is a `Store` layer whose read path is plain transform composition.

Such a layer reads from the store it wraps and applies `_key_of_id` to keys
and `_obj_of_data` to values – and nothing else. Those two transforms can be
mapped over a bulk stream, so the layer can be “crossed” on the way to the
backend’s fast path. A layer that redefines `__getitem__` (`postget`) or
`__iter__` (key filtering/caching) cannot.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### mongodol.views.provides_bulk_read(store, method_name)

Whether `store`’s own class implements bulk-read `method_name`, faithfully.

“Faithfully” means the store has not declared, via
[`BULK_READ_IS_FAITHFUL_ATTR`](#mongodol.views.BULK_READ_IS_FAITHFUL_ATTR), that its bulk-read methods disagree with
its `__getitem__`.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### mongodol.views.resolve_bulk_source(store, method_name)

Find the store providing bulk-read `method_name`, and the layers crossed to reach it.

* **Returns:**
  `(source, layers)` where `layers` are the crossed
  `Store` wrappers, outermost first.
* **Raises:**
  [**NoBulkReadPath**](#mongodol.views.NoBulkReadPath) – if a layer that cannot be crossed is met before a
  provider is found.

### mongodol.views.store_layers(store)

Yield `store` then each store it wraps, outermost first, innermost last.

The chain ends at the first non-`Store` – the actual backend. Note that
`dol` is free to insert pass-through `Store` layers of its own, so never
assume one `wrap_kvs` call means exactly one layer.

* **Return type:**
  [`Iterator`](https://docs.python.org/3/library/typing.html#typing.Iterator)

```pycon
>>> from dol import wrap_kvs
>>> layers = list(store_layers(wrap_kvs({'a': 1}, obj_of_data=str)))
>>> type(layers[0]).__name__, type(layers[-1]).__name__
('Store', 'dict')
>>> all(isinstance(x, Store) for x in layers[:-1])
True
```
