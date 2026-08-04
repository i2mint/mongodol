"""Mapping views that keep working when a mongo store is wrapped by ``dol``.

A mongo collection can serve a store's whole ``(key, value)`` stream in a single
``find`` round trip, so :class:`~mongodol.base.MongoCollectionReader` implements a
**bulk-read protocol** -- ``iter_values``, ``iter_items``, ``contains_value`` and
``contains_item`` -- and exposes it through the ``values()``/``items()`` views
defined here. One query instead of N is the whole point of these views.

The catch is *composition*. A ``dol`` :class:`~dol.base.Store` wrapper (what
``wrap_kvs`` builds) forwards every attribute it doesn't define to the store it
wraps. A view that simply calls ``self._mapping.iter_values()`` therefore punches
straight through the wrappers and yields raw backend documents, silently skipping
the value transforms the user asked for -- breaking the ``Mapping`` contract::

    list(store.values()) == [store[k] for k in store]

(see `i2mint/mongodol#7 <https://github.com/i2mint/mongodol/issues/7>`_).

This module resolves the bulk stream **explicitly** instead of relying on
attribute delegation. Given the store a view was built on, :func:`bulk_values`
and :func:`bulk_items` walk the wrapper chain inward, remembering each layer they
cross, until they reach a store that actually implements the bulk-read protocol.
The bulk stream is then re-transformed by the crossed layers, innermost first, so
that it lands in exactly the same space as ``store[k]``.

A layer may only be crossed if its read path is *plain transform composition* --
"read from the inner store, then apply ``_key_of_id``/``_obj_of_data``", which is
what :class:`~dol.base.Store` does. A layer that redefines ``__getitem__`` or
``__iter__`` (``wrap_kvs(postget=...)``, ``filt_iter``, ``cached_keys``, ...)
changes values or key sets in ways that cannot be pushed onto a bulk stream, so
the resolver refuses to guess: it raises :class:`NoBulkReadPath` and the views
fall back to the generic per-key behaviour. That fallback is correct, just one
round trip per key -- correctness first, efficiency when it is provable.

Simple use is invisible: build a mongo store, wrap it however you like, and
``values()``/``items()`` agree with ``__getitem__``. The knobs, for store authors:

- Implement the bulk-read methods to *provide* the fast path.
- Set the :data:`BULK_READ_IS_FAITHFUL_ATTR` class attribute to ``False`` (see
  :func:`disable_bulk_read`) when a class inherits bulk-read methods that no
  longer agree with its own ``__getitem__``.

Known limitation. :class:`~mongodol.base.MongoCollectionReader` is deliberately a
*cursor*-level store: ``s[k]`` is a pymongo ``Cursor``, while its bulk stream
already yields *documents* -- one per key. The two only line up once a single-doc
layer (``MongoCollectionFirstDocReader`` and friends) has turned cursors into
docs, which is why those are the stores you are meant to wrap. Hanging an
``obj_of_data`` that expects a cursor directly off the raw reader is outside the
protocol: such a transform cannot be pushed onto a doc-level bulk stream, and is
not detectable from here.

Nothing here is mongo-specific; it is a general answer to "how does a store with
a bulk-read fast path compose with ``dol`` wrappers?", and would be a reasonable
thing for ``dol`` itself to own one day.
"""

from typing import Any, Callable, Iterable, Iterator, Tuple

from dol import BaseItemsView, BaseValuesView
from dol.base import Store

#: Bulk-read method yielding a store's values in one backend round trip.
ITER_VALUES_METHOD = "iter_values"
#: Bulk-read method yielding a store's ``(key, value)`` pairs in one backend round trip.
ITER_ITEMS_METHOD = "iter_items"
#: Bulk-read method answering "is this value in the store?" in one backend round trip.
CONTAINS_VALUE_METHOD = "contains_value"
#: Bulk-read method answering "is this item in the store?" in one backend round trip.
CONTAINS_ITEM_METHOD = "contains_item"

#: Class attribute through which a store declares whether its bulk-read methods are
#: value-equivalent to its own ``__getitem__``. It defaults to ``True`` (a class that
#: implements the protocol is trusted to implement it faithfully). It exists because
#: ``dol``'s class-decorator wrapping *copies* the wrapped class's extra methods onto
#: the wrapper, so a wrapper that redefines value semantics -- ``wrap_kvs(postget=...)``
#: -- silently inherits bulk-read methods that no longer match it. Such a class sets
#: this to ``False``; see :func:`disable_bulk_read`.
BULK_READ_IS_FAITHFUL_ATTR = "_bulk_read_is_faithful"

#: The ``dol`` :class:`~dol.base.Store` attribute holding the store a wrapper wraps.
INNER_STORE_ATTR = "store"

KeyValStream = Iterator[Tuple[Any, Any]]


class NoBulkReadPath(Exception):
    """No bulk-read stream can be *proven* equivalent to the store's per-key reads.

    Raised by the resolvers of this module, and caught by the views, which then
    fall back to the generic (correct, one-round-trip-per-key) ``Mapping``
    behaviour. It is a control-flow signal, not a user-facing error.
    """


# -------------------------------------------------------------------------------------
# Inspecting a store layer
#
# Everything here looks attributes up on ``type(store)``, never on the instance:
# ``dol``'s ``Store.__getattr__`` forwards *instance* lookups to the wrapped store, so
# ``hasattr(store, 'iter_values')`` is True even for a wrapper that has no idea what a
# bulk read is. Class lookup does not delegate, so it tells the truth.


def _class_attr(store, attr: str, default=None):
    """Look ``attr`` up on ``type(store)``, bypassing ``Store.__getattr__`` delegation."""
    return getattr(type(store), attr, default)


def provides_bulk_read(store, method_name: str) -> bool:
    """Whether ``store``'s own class implements bulk-read ``method_name``, faithfully.

    "Faithfully" means the store has not declared, via
    :data:`BULK_READ_IS_FAITHFUL_ATTR`, that its bulk-read methods disagree with
    its ``__getitem__``.
    """
    if not _class_attr(store, BULK_READ_IS_FAITHFUL_ATTR, True):
        return False
    return _class_attr(store, method_name) is not None


def is_crossable(store) -> bool:
    """Whether ``store`` is a ``Store`` layer whose read path is plain transform composition.

    Such a layer reads from the store it wraps and applies ``_key_of_id`` to keys
    and ``_obj_of_data`` to values -- and nothing else. Those two transforms can be
    mapped over a bulk stream, so the layer can be "crossed" on the way to the
    backend's fast path. A layer that redefines ``__getitem__`` (``postget``) or
    ``__iter__`` (key filtering/caching) cannot.
    """
    if not isinstance(store, Store):
        return False
    cls = type(store)
    return cls.__getitem__ is Store.__getitem__ and cls.__iter__ is Store.__iter__


def store_layers(store) -> Iterator:
    """Yield ``store`` then each store it wraps, outermost first, innermost last.

    The chain ends at the first non-``Store`` -- the actual backend. Note that
    ``dol`` is free to insert pass-through ``Store`` layers of its own, so never
    assume one ``wrap_kvs`` call means exactly one layer.

    >>> from dol import wrap_kvs
    >>> layers = list(store_layers(wrap_kvs({'a': 1}, obj_of_data=str)))
    >>> type(layers[0]).__name__, type(layers[-1]).__name__
    ('Store', 'dict')
    >>> all(isinstance(x, Store) for x in layers[:-1])
    True
    """
    yield store
    while isinstance(store, Store):
        inner = getattr(store, INNER_STORE_ATTR, None)
        if inner is None:  # a Store that never got one: nothing further to walk
            return
        store = inner
        yield store


def resolve_bulk_source(store, method_name: str):
    """Find the store providing bulk-read ``method_name``, and the layers crossed to reach it.

    :return: ``(source, layers)`` where ``layers`` are the crossed
        :class:`~dol.base.Store` wrappers, outermost first.
    :raises NoBulkReadPath: if a layer that cannot be crossed is met before a
        provider is found.
    """
    layers = []
    for layer in store_layers(store):
        if provides_bulk_read(layer, method_name):
            return layer, layers
        if not is_crossable(layer):
            raise NoBulkReadPath(
                f"No bulk {method_name!r} path: {type(layer).__name__} neither provides "
                "it nor is a plain transform-composition layer that can be crossed."
            )
        layers.append(layer)
    raise NoBulkReadPath(f"No store in the chain provides {method_name!r}")


# -------------------------------------------------------------------------------------
# Pushing a layer's transforms onto a bulk stream


def _outgoing_trans(layer, trans_attr: str) -> Callable[[Any], Any]:
    """The layer's outgoing (backend -> user) transform named ``trans_attr``."""
    return getattr(layer, trans_attr)


def _ingoing_trans(
    layer, *, outgoing_attr: str, ingoing_attr: str
) -> Callable[[Any], Any]:
    """The layer's ingoing (user -> backend) transform, if it is usable as an inverse.

    A layer that transforms outgoing values/keys but declares no ingoing transform
    has no inverse, so a user-space value cannot be pushed down to the backend.
    """
    cls = type(layer)
    transforms_outgoing = getattr(cls, outgoing_attr) is not getattr(
        Store, outgoing_attr
    )
    has_inverse = getattr(cls, ingoing_attr) is not getattr(Store, ingoing_attr)
    if transforms_outgoing and not has_inverse:
        raise NoBulkReadPath(
            f"{type(layer).__name__} transforms outgoing values via {outgoing_attr!r} "
            f"but declares no {ingoing_attr!r} inverse, so nothing can be pushed down "
            "to the backend."
        )
    return getattr(layer, ingoing_attr)


def _map_outward(stream: Iterable, layers, trans_attr: str) -> Iterator:
    """Apply each crossed layer's ``trans_attr`` to ``stream``, innermost layer first."""
    for layer in reversed(layers):
        stream = map(_outgoing_trans(layer, trans_attr), stream)
    return iter(stream)


def _trans_items(items: KeyValStream, key_of_id, obj_of_data) -> KeyValStream:
    """Apply one layer's key and value transforms to an item stream.

    A function -- not an inlined generator expression in :func:`_map_items_outward` --
    so that each layer's transforms are captured in their own scope. A genexpr would
    look them up lazily, in a scope the next loop iteration has already overwritten.
    """
    return ((key_of_id(k), obj_of_data(v)) for k, v in items)


def _map_items_outward(items: KeyValStream, layers) -> KeyValStream:
    """Apply each crossed layer's key *and* value transforms to an item stream."""
    for layer in reversed(layers):
        items = _trans_items(items, layer._key_of_id, layer._obj_of_data)
    return items


def _push_inward(x, layers, *, outgoing_attr: str, ingoing_attr: str):
    """Push a user-space ``x`` down to backend space through ``layers``, outermost first."""
    for layer in layers:
        x = _ingoing_trans(
            layer, outgoing_attr=outgoing_attr, ingoing_attr=ingoing_attr
        )(x)
    return x


def _push_value_inward(v, layers):
    return _push_inward(
        v, layers, outgoing_attr="_obj_of_data", ingoing_attr="_data_of_obj"
    )


def _push_key_inward(k, layers):
    return _push_inward(
        k, layers, outgoing_attr="_key_of_id", ingoing_attr="_id_of_key"
    )


# -------------------------------------------------------------------------------------
# The bulk-read facade: what the views (and store authors) call


def bulk_values(store) -> Iterator:
    """Iterate ``store``'s values via the backend's bulk-read path, transforms honoured.

    :raises NoBulkReadPath: when the bulk stream cannot be proven equivalent to
        ``(store[k] for k in store)``.
    """
    source, layers = resolve_bulk_source(store, ITER_VALUES_METHOD)
    return _map_outward(source.iter_values(), layers, "_obj_of_data")


def bulk_items(store) -> KeyValStream:
    """Iterate ``store``'s ``(key, value)`` pairs via the backend's bulk-read path.

    :raises NoBulkReadPath: when the bulk stream cannot be proven equivalent to
        ``((k, store[k]) for k in store)``.
    """
    source, layers = resolve_bulk_source(store, ITER_ITEMS_METHOD)
    return _map_items_outward(source.iter_items(), layers)


def bulk_contains_value(store, v) -> bool:
    """Ask the backend whether ``v`` is one of ``store``'s values, in one round trip.

    :raises NoBulkReadPath: when ``v`` cannot be pushed down to backend space.
    """
    source, layers = resolve_bulk_source(store, CONTAINS_VALUE_METHOD)
    return source.contains_value(_push_value_inward(v, layers))


def bulk_contains_item(store, item) -> bool:
    """Ask the backend whether ``item`` is one of ``store``'s items, in one round trip.

    :raises NoBulkReadPath: when ``item`` cannot be pushed down to backend space.
    """
    source, layers = resolve_bulk_source(store, CONTAINS_ITEM_METHOD)
    k, v = item
    return source.contains_item(
        (_push_key_inward(k, layers), _push_value_inward(v, layers))
    )


def disable_bulk_read(store_cls: type) -> type:
    """Class decorator declaring that inherited bulk-read methods are not to be trusted.

    Use it on a class that changes what ``__getitem__`` returns (typically via
    ``wrap_kvs(postget=...)``) while inheriting -- or being handed, by ``dol``'s
    class-decorator wrapping -- bulk-read methods written for the *un*-changed
    semantics. Views then take the correct per-key path instead.
    """
    setattr(store_cls, BULK_READ_IS_FAITHFUL_ATTR, False)
    return store_cls


# -------------------------------------------------------------------------------------
# The views themselves


class MongoValuesView(BaseValuesView):
    """A ``values()`` view that uses the backend's bulk read when -- and only when --
    that stream provably equals ``(store[k] for k in store)``."""

    def __iter__(self):
        try:
            return bulk_values(self._mapping)
        except NoBulkReadPath:
            return (self._mapping[k] for k in self._mapping)

    def __contains__(self, v):
        try:
            return bulk_contains_value(self._mapping, v)
        except NoBulkReadPath:
            return any(v == value for value in self)


class MongoItemsView(BaseItemsView):
    """An ``items()`` view that uses the backend's bulk read when -- and only when --
    that stream provably equals ``((k, store[k]) for k in store)``."""

    def __iter__(self):
        try:
            return bulk_items(self._mapping)
        except NoBulkReadPath:
            return ((k, self._mapping[k]) for k in self._mapping)

    def __contains__(self, item):
        try:
            return bulk_contains_item(self._mapping, item)
        except NoBulkReadPath:
            k, v = item
            try:
                return self._mapping[k] == v
            except KeyError:
                return False
