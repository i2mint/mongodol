from functools import wraps, partial, cached_property
from inspect import signature
from typing import Iterable, Callable
from i2.signatures import Sig

import pymongo

# from dol.base import cls_wrap
from dol.trans import double_up_as_factory
from mongodol.utils.werk_local import LocalProxy


def track_calls_of_method(method: Callable, execute_call=True, tracks_factory=list):
    @wraps(method)
    def tracked_method(self, *args, **kwargs):
        try:
            tracks = self._tracks
        except AttributeError:
            tracks = self._tracks = tracks_factory()

        tracks.append((method, args, kwargs))

        if execute_call:
            return method(self, *args, **kwargs)

    return tracked_method


def track_calls_without_executing(method: Callable):
    @wraps(method)
    def tracked_method(self, *args, **kwargs):
        self._tracks.append((method, args, kwargs))

    return tracked_method


def forward_method_calls(method):
    @wraps(method)
    def forwarded_method(self, *args, **kwargs):
        return method(self._instance, *args, **kwargs)

    return forwarded_method


class TrackableMixin:
    """Mixin that provides a container for method call tracking, execution,
    and a context manager that will execute tracks and empty them.

    TrackableMixin is used as the default tracking_mixin in track_method_calls.

    It uses list as the collection for tracks, and implements a basic execute_tracks
    (which loops through tracks, executes them, and accumulates results in a list which it returns).

    TrackableMixin is meant to be subclassed and execute_tracks overwritten by a custom handler.
    """

    tracks_factory = list

    @cached_property
    def _tracks(self):
        return self.tracks_factory()

    def _execute_tracks(self):
        def gen():
            for func, args, kwargs in self._tracks:
                yield func(self, *args, **kwargs)

        return list(gen())

    def __enter__(self):  # TODO: Should we clear tracks on entry?
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.flush()

    # commit_execution
    def flush(self):
        call_results = self._execute_tracks()
        self.clear_tracks()
        return call_results

    def clear_tracks(self):
        self._tracks.clear()


@double_up_as_factory
def track_method_calls(
    obj=None,
    *,
    tracked_methods: Iterable[str] = frozenset(),
    tracking_mixin: type = TrackableMixin,
    calls_tracker: Callable = track_calls_of_method,
):
    """Wrapping objects (classes or instances) so that specific method calls are tracked
    (i.e. a list of (method_func, args, kwargs) is maintained)

    :param obj:
    :param tracked_methods: Method name or iterable of method names to track
    :param tracking_mixin: The mixin class to use to inject the _tracks attribute, and other tracking utils (flush...)
    :param calls_tracker: The method decorator that implements the actual tracking
    :return: A decorated class (of obj is a type) or instance (if obj is an instance) that implements method tracking

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

    A common use of ``track_method_calls`` is to accumulate method calls without executing them,
    so as to be able to change the way they're called. For example, making the calls differently
    (e.g. in a parallel process) or aggregating several operations and running them in bulk
    (e.g. data base writes).

    If you want to reuse your tracker decorator, it's a good idea of use partial to make a
    decorator with the settings you want, like this:

    >>> from functools import partial
    >>> my_write_tracker = partial(
    ...     track_method_calls,
    ...     tracked_methods='__setitem__',
    ...     calls_tracker=track_calls_without_executing
    ...     )

    Now let's decorate a dict type with it.

    >>> @my_write_tracker
    ... class D(dict):
    ...     pass
    >>> d = D(a=1, b=[1, 2], c={'hello': 'world'})

    The suggested use is to do write operations in a with block. This will have the effect of
    automatically executing the calls accumulated in tracks and clearing the tracks when you exit the with
    block.

    >>> with d:
    ...     d['a'] = 21
    ...     assert d['a'] == 1  # still in the with block, so the operation hasn't executed yet
    >>> d['a']  # but now that we exited the block, we have d['a'] == 21
    21

    But if you really need/want to, you can perform these operations manually.

    >>> assert d._tracks == []  # see that we have no _tracks (these are deleted when we exit the with block
    >>> d['a'] = 42
    >>> assert d['a'] == 21  # verifying that dd['a'] is STILL 21
    >>> assert len(d._tracks) > 0  # but dd._tracks is now non-empty
    >>> assert str(d._tracks) == "[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]"

    To execute the command in _tracks, you can use the ``.flush()`` method
    >>> _ = d.flush()
    >>> # See that the setitem call was indeed made
    >>> assert d['a'] == 42
    >>> assert len(d._tracks) == 0

    Here's what's happening behind the scenes:

    >>> d['b'] = [3, 4]  # write to 'b'
    >>> assert d['b'] != [3, 4]  # but it's not actually written
    >>> func, args, kwargs = d._tracks[0]  # the tracks now has a (func, args, kwargs) triple
    >>> func(d, *args, **kwargs)  # if we cann that function on the instance (and *args, **kwargs)
    >>> assert d['b'] == [3, 4]  # Not the write is actually performed and d['b'] becomes [3, 4]

    Above, we were wrapping a class, but you can also wrap an instance!

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

    """
    if isinstance(tracked_methods, str):
        tracked_methods = {tracked_methods}

    # TODO: Include this logic in a wrap_first_arg_if_not_a_type decorator instead.
    if isinstance(obj, type):

        @add_tracked_methods(tracked_methods, calls_tracker)
        class TrackedObj(tracking_mixin, obj):
            pass

        return TrackedObj

    else:  # obj is NOT a type, so wrap it in one

        @add_tracked_methods(tracked_methods, calls_tracker)
        class TrackedObj(tracking_mixin, LocalProxy):
            def __init__(self, local,) -> None:
                object.__setattr__(self, '_TrackedObj__local', local)
                object.__setattr__(self, '__wrapped__', local)

            def _get_current_object(self):
                return self.__local

            def __dir__(self):
                return list(set(dir(self.__local)).union(dir(tracking_mixin)))

        return TrackedObj(obj)


def add_tracked_methods(
    tracked_methods: Iterable[str] = frozenset(),
    calls_tracker: Callable = track_calls_of_method,
):
    """Factory of decorators to add method call tracking to a class

    :param tracked_methods: Method name or iterable of method names to track
    :param tracking_mixin: The mixin class to use to inject the _tracks attribute, and other tracking utils (flush...)
    :param calls_tracker: The method decorator that implements the actual tracking
    """

    def _add_tracked_methods(cls):
        assert hasattr(cls, '_tracks')
        for method_name in tracked_methods:
            method_to_track = getattr(cls, method_name)
            setattr(cls, method_name, calls_tracker(method_to_track))
        return cls

    return _add_tracked_methods


def consume(gen):
    for _ in gen:
        pass


class MongoBulkWritesMixin(TrackableMixin):
    """Used to accumulate write operations and execute them in bulk, efficiently"""

    def _execute_tracks(self):
        def get_op_request(func, *args, **kwargs):
            _kwargs = Sig(func).extract_kwargs(
                None, *args, **kwargs
            )  # First None value to ignore the 'self' parameter
            func_name = func.__name__
            k = _kwargs.get('k', {})
            v = _kwargs.get('v')
            if func_name == '__setitem__':
                k = _kwargs.get('k', {})
                return pymongo.ReplaceOne(
                    filter=self._merge_with_filt(k),
                    replacement=self._build_doc(k, v),
                    upsert=True,
                )
            elif func_name == '__delitem__':
                return pymongo.DeleteOne(filter=self._merge_with_filt(k))
            elif func_name == 'append':
                return pymongo.InsertOne(document=self._build_doc(v))
            elif func_name == 'extend':
                values = _kwargs.get('values')
                return [
                    pymongo.InsertOne(document=self._build_doc(value))
                    for value in values
                ]

        op_requests = []
        for func, args, kwargs in self._tracks:
            request = get_op_request(func, *args, **kwargs)
            if isinstance(request, Iterable):
                op_requests.extend(request)
            else:
                op_requests.append(request)
        return self.mgc.bulk_write(requests=op_requests)

    differ_writes = (
        TrackableMixin.__enter__
    )  # alias for those who think "with obj:" is not explicit enough
    commit = TrackableMixin.flush


with_bulk_writes = partial(
    track_method_calls,
    tracking_mixin=MongoBulkWritesMixin,
    calls_tracker=track_calls_without_executing,
)

# MyBulkWriteStore = with_bulk_writes(OriginalStore)
#
# @with_bulk_writes
# class MyBulkWriteStore(OriginalStore):
#     """Specific docs"""
