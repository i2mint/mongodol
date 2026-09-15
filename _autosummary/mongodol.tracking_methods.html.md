# mongodol.tracking_methods

Tracking functionality

### Functions

| [`add_tracked_methods`](#mongodol.tracking_methods.add_tracked_methods)([tracked_methods, ...])     | Factory of decorators to add method call tracking to a class                                                                                  |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| [`consume`](#mongodol.tracking_methods.consume)(gen)                                    | Exhaust an iterable/generator `gen` for its side effects, discarding all values.                                                              |
| [`forward_method_calls`](#mongodol.tracking_methods.forward_method_calls)(method)                    | Wrap `method` so calls on `self` are forwarded to `self._instance` instead.                                                                   |
| [`track_calls_of_method`](#mongodol.tracking_methods.track_calls_of_method)(method[, ...])            | Wrap `method` so every call is appended to `self._tracks`, and (if `execute_call`) also run.                                                  |
| [`track_calls_without_executing`](#mongodol.tracking_methods.track_calls_without_executing)(method)           | Wrap `method` so every call is appended to `self._tracks`, but never actually run.                                                            |
| [`track_method_calls`](#mongodol.tracking_methods.track_method_calls)([obj, tracked_methods, ...]) | Wrapping objects (classes or instances) so that specific method calls are tracked (i.e. a list of (method_func, args, kwargs) is maintained). |

### Classes

| [`MongoBulkWritesMixin`](#mongodol.tracking_methods.MongoBulkWritesMixin)()   | Used to accumulate write operations and execute them in bulk, efficiently                                                           |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| [`TrackableMixin`](#mongodol.tracking_methods.TrackableMixin)()         | Mixin that provides a container for method call tracking, execution, and a context manager that will execute tracks and empty them. |

### *class* mongodol.tracking_methods.MongoBulkWritesMixin

Bases: [`TrackableMixin`](#mongodol.tracking_methods.TrackableMixin)

Used to accumulate write operations and execute them in bulk, efficiently

#### commit()

Execute all pending tracked calls, clear the tracks, and return the call results.

### *class* mongodol.tracking_methods.TrackableMixin

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Mixin that provides a container for method call tracking, execution,
and a context manager that will execute tracks and empty them.

TrackableMixin is used as the default tracking_mixin in track_method_calls.

It uses list as the collection for tracks, and implements a basic execute_tracks
(which loops through tracks, executes them, and accumulates results in a list which it returns).

TrackableMixin is meant to be subclassed and execute_tracks overwritten by a custom handler.

#### clear_tracks()

Discard all pending tracked calls without executing them.

#### flush()

Execute all pending tracked calls, clear the tracks, and return the call results.

#### tracks_factory

alias of [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)

### mongodol.tracking_methods.add_tracked_methods(tracked_methods=frozenset({}), calls_tracker=<function track_calls_of_method>)

Factory of decorators to add method call tracking to a class

* **Parameters:**
  * **tracked_methods** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – Method name or iterable of method names to track
  * **tracking_mixin** – The mixin class to use to inject the \_tracks attribute, and other tracking utils (flush…)
  * **calls_tracker** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)) – The method decorator that implements the actual tracking

### mongodol.tracking_methods.consume(gen)

Exhaust an iterable/generator `gen` for its side effects, discarding all values.

### mongodol.tracking_methods.forward_method_calls(method)

Wrap `method` so calls on `self` are forwarded to `self._instance` instead.

### mongodol.tracking_methods.track_calls_of_method(method, execute_call=True, tracks_factory=<class 'list'>)

Wrap `method` so every call is appended to `self._tracks`, and (if `execute_call`) also run.

### mongodol.tracking_methods.track_calls_without_executing(method)

Wrap `method` so every call is appended to `self._tracks`, but never actually run.

### mongodol.tracking_methods.track_method_calls(obj=None, \*, tracked_methods=frozenset({}), tracking_mixin=<class 'mongodol.tracking_methods.TrackableMixin'>, calls_tracker=<function track_calls_of_method>)

Wrapping objects (classes or instances) so that specific method calls are tracked
(i.e. a list of (method_func, args, kwargs) is maintained)

* **Parameters:**
  * **obj**
  * **tracked_methods** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – Method name or iterable of method names to track
  * **tracking_mixin** ([`type`](https://docs.python.org/3/builtins/functions.html#type)) – The mixin class to use to inject the \_tracks attribute, and other tracking utils (flush…)
  * **calls_tracker** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)) – The method decorator that implements the actual tracking
* **Returns:**
  A decorated class (of obj is a type) or instance (if obj is an instance) that implements method tracking

```pycon
>>> @track_method_calls(tracked_methods='__setitem__')
... class D(dict):
...     pass
>>> d = D(a=1, b=[1, 2], c={'hello': 'world'})
>>> assert repr(d) == "{'a': 1, 'b': [1, 2], 'c': {'hello': 'world'}}"
>>> assert d._tracks == []
>>> d['a']
1
>>> d._tracks  # accessing 'a' didn't make any tracks
[]
>>> d['a'] = 42
>>> d['a']  # verifying that dd['a'] is now 42
42
>>> len(d._tracks)  # see that dd._tracks is now non-empty
1
>>> d._tracks
[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]
```

A common use of `track_method_calls` is to accumulate method calls without executing them,
so as to be able to change the way they’re called. For example, making the calls differently
(e.g. in a parallel process) or aggregating several operations and running them in bulk
(e.g. data base writes).

If you want to reuse your tracker decorator, it’s a good idea of use partial to make a
decorator with the settings you want, like this:

```pycon
>>> from functools import partial
>>> my_write_tracker = partial(
...     track_method_calls,
...     tracked_methods='__setitem__',
...     calls_tracker=track_calls_without_executing
...     )
```

Now let’s decorate a dict type with it.

```pycon
>>> @my_write_tracker
... class D(dict):
...     pass
>>> d = D(a=1, b=[1, 2], c={'hello': 'world'})
```

The suggested use is to do write operations in a with block. This will have the effect of
automatically executing the calls accumulated in tracks and clearing the tracks when you exit the with
block.

```pycon
>>> with d:
...     d['a'] = 21
...     assert d['a'] == 1  # still in the with block, so the operation hasn't executed yet
>>> d['a']  # but now that we exited the block, we have d['a'] == 21
21
```

But if you really need/want to, you can perform these operations manually.

```pycon
>>> assert d._tracks == []  # see that we have no _tracks (these are deleted when we exit the with block
>>> d['a'] = 42
>>> assert d['a'] == 21  # verifying that dd['a'] is STILL 21
>>> assert len(d._tracks) > 0  # but dd._tracks is now non-empty
>>> assert str(d._tracks) == "[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]"
```

To execute the command in \_tracks, you can use the `.flush()` method

```pycon
>>> _ = d.flush()
>>> # See that the setitem call was indeed made
>>> assert d['a'] == 42
>>> assert len(d._tracks) == 0
```

Here’s what’s happening behind the scenes:

```pycon
>>> d['b'] = [3, 4]  # write to 'b'
>>> assert d['b'] != [3, 4]  # but it's not actually written
>>> func, args, kwargs = d._tracks[0]  # the tracks now has a (func, args, kwargs) triple
>>> func(d, *args, **kwargs)  # if we cann that function on the instance (and *args, **kwargs)
>>> assert d['b'] == [3, 4]  # Not the write is actually performed and d['b'] becomes [3, 4]
```

Above, we were wrapping a class, but you can also wrap an instance!

```pycon
>>> d = dict(a=1, b=[1,2], c={'hello': 'world'})
>>> dd = track_method_calls(d, tracked_methods='__getitem__')
>>> v = dd['a']  # TypeError: __getitem__() takes exactly one argument (2 given)
>>> assert v == 1  # you got the value alright!
>>> dd._tracks
[(proxy __getitem__, ('a',), {})]
>>> # It's a weird name for the function, but the function still works:
>>> func, args, kwargs = dd._tracks[0]
>>> func(dd, *args, **kwargs)
1
```

### mongodol.tracking_methods.with_bulk_writes(obj=None, \*, tracked_methods=frozenset({}), tracking_mixin=<class 'mongodol.tracking_methods.MongoBulkWritesMixin'>, calls_tracker=<function track_calls_without_executing>)

Wrapping objects (classes or instances) so that specific method calls are tracked
(i.e. a list of (method_func, args, kwargs) is maintained)

* **Parameters:**
  * **obj**
  * **tracked_methods** – Method name or iterable of method names to track
  * **tracking_mixin** – The mixin class to use to inject the \_tracks attribute, and other tracking utils (flush…)
  * **calls_tracker** – The method decorator that implements the actual tracking
* **Returns:**
  A decorated class (of obj is a type) or instance (if obj is an instance) that implements method tracking

```pycon
>>> @track_method_calls(tracked_methods='__setitem__')
... class D(dict):
...     pass
>>> d = D(a=1, b=[1, 2], c={'hello': 'world'})
>>> assert repr(d) == "{'a': 1, 'b': [1, 2], 'c': {'hello': 'world'}}"
>>> assert d._tracks == []
>>> d['a']
1
>>> d._tracks  # accessing 'a' didn't make any tracks
[]
>>> d['a'] = 42
>>> d['a']  # verifying that dd['a'] is now 42
42
>>> len(d._tracks)  # see that dd._tracks is now non-empty
1
>>> d._tracks
[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]
```

A common use of `track_method_calls` is to accumulate method calls without executing them,
so as to be able to change the way they’re called. For example, making the calls differently
(e.g. in a parallel process) or aggregating several operations and running them in bulk
(e.g. data base writes).

If you want to reuse your tracker decorator, it’s a good idea of use partial to make a
decorator with the settings you want, like this:

```pycon
>>> from functools import partial
>>> my_write_tracker = partial(
...     track_method_calls,
...     tracked_methods='__setitem__',
...     calls_tracker=track_calls_without_executing
...     )
```

Now let’s decorate a dict type with it.

```pycon
>>> @my_write_tracker
... class D(dict):
...     pass
>>> d = D(a=1, b=[1, 2], c={'hello': 'world'})
```

The suggested use is to do write operations in a with block. This will have the effect of
automatically executing the calls accumulated in tracks and clearing the tracks when you exit the with
block.

```pycon
>>> with d:
...     d['a'] = 21
...     assert d['a'] == 1  # still in the with block, so the operation hasn't executed yet
>>> d['a']  # but now that we exited the block, we have d['a'] == 21
21
```

But if you really need/want to, you can perform these operations manually.

```pycon
>>> assert d._tracks == []  # see that we have no _tracks (these are deleted when we exit the with block
>>> d['a'] = 42
>>> assert d['a'] == 21  # verifying that dd['a'] is STILL 21
>>> assert len(d._tracks) > 0  # but dd._tracks is now non-empty
>>> assert str(d._tracks) == "[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]"
```

To execute the command in \_tracks, you can use the `.flush()` method

```pycon
>>> _ = d.flush()
>>> # See that the setitem call was indeed made
>>> assert d['a'] == 42
>>> assert len(d._tracks) == 0
```

Here’s what’s happening behind the scenes:

```pycon
>>> d['b'] = [3, 4]  # write to 'b'
>>> assert d['b'] != [3, 4]  # but it's not actually written
>>> func, args, kwargs = d._tracks[0]  # the tracks now has a (func, args, kwargs) triple
>>> func(d, *args, **kwargs)  # if we cann that function on the instance (and *args, **kwargs)
>>> assert d['b'] == [3, 4]  # Not the write is actually performed and d['b'] becomes [3, 4]
```

Above, we were wrapping a class, but you can also wrap an instance!

```pycon
>>> d = dict(a=1, b=[1,2], c={'hello': 'world'})
>>> dd = track_method_calls(d, tracked_methods='__getitem__')
>>> v = dd['a']  # TypeError: __getitem__() takes exactly one argument (2 given)
>>> assert v == 1  # you got the value alright!
>>> dd._tracks
[(proxy __getitem__, ('a',), {})]
>>> # It's a weird name for the function, but the function still works:
>>> func, args, kwargs = dd._tracks[0]
>>> func(dd, *args, **kwargs)
1
```
