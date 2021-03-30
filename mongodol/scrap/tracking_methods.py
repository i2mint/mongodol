from functools import wraps, partial, cached_property
from inspect import signature
from typing import Iterable, Callable
# from py2store.base import cls_wrap
from py2store.trans import double_up_as_factory


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

    def execute_tracks(self):
        def gen():
            for func, args, kwargs in self._tracks:
                yield func(self, *args, **kwargs)

        return list(gen())

    def __enter__(self):  # TODO: Should we clear tracks on entry?
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.execute_and_clear_tracks()

    def execute_and_clear_tracks(self):
        call_results = self.execute_tracks()
        self.clear_tracks()
        return call_results

    def clear_tracks(self):
        self._tracks.clear()


@double_up_as_factory
def track_method_calls(obj=None,
                       *,
                       tracked_methods: Iterable[str] = frozenset(),
                       tracking_mixin: type = TrackableMixin,
                       calls_tracker: Callable = track_calls_of_method,
                       # forwarded_methods: Iterable[str] = frozenset()
                       ):
    """Wrapping objects (classes or instances) so that specific method calls are tracked 
    (i.e. a list of (method_func, args, kwargs) is maintained) 
    
    :param obj: 
    :param tracked_methods: 
    :param execute_call: 
    :param tracks_factory: 
    :param forwarded_methods: 
    :return:
    
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
    >>> _ = d.execute_and_clear_tracks()
    >>> # See that the setitem call was indeed made
    >>> assert d['a'] == 42
    >>> assert len(d._tracks) == 0
    
    """
    if isinstance(tracked_methods, str):
        tracked_methods = {tracked_methods}

    # TODO: Include this logic in a wrap_first_arg_if_not_a_type decorator instead.
    if isinstance(obj, type):
        class TrackedObj(tracking_mixin, obj):
            pass

        for method_name in tracked_methods:
            method_to_track = getattr(TrackedObj, method_name)
            setattr(TrackedObj, method_name, calls_tracker(method_to_track))

        return TrackedObj

    else:  # obj is NOT a type, so wrap it in one
        # TODO: Make it work
        """
        >>> d = dict(a=1, b=[1,2], c={'hello': 'world'})
        >>> dd = track_method_calls(d, tracked_methods='__getitem__')
        >>> t = dd['a']  # TypeError: __getitem__() takes exactly one argument (2 given)
        >>> repr(dd)
        """

        raise NotImplementedError("Wrapping instances hasn't been implemented yet.")
        # tracked_or_forwarded = set(forwarded_methods).union(tracked_methods)
        #
        # #         print(f"instance: {obj=}: {tracked_or_forwarded=}")
        #
        # class WrappedInstance:
        #     _instance = None
        #
        #     def __getattr__(self, a):
        #         return getattr(self._instance, a)
        #
        # WrappedInstance._instance = obj
        #
        # for method_name in tracked_or_forwarded:
        #     forwarded_method = getattr(obj, method_name)
        #     setattr(WrappedInstance, method_name, forward_method_calls(forwarded_method))
        #
        # wrapped_cls = track_method_calls(
        #     WrappedInstance,
        #     tracked_methods=tracked_methods,
        #     tracking_mixin=tracking_mixin,
        #     execute_call=execute_call,
        #     forwarded_methods=forwarded_methods)
        # #         print(wrapped_cls)
        #
        # return wrapped_cls()


def consume(gen):
    for _ in gen:
        pass


class BulkWritesMixin(TrackableMixin):

    def execute_and_clear_tracks(self):
        return_values = list(self.execute_tracks_and_yield_results())
        self.clear_tracks()
        return return_values

    def execute_tracks_and_yield_results(self):
        for func, args, kwargs in self._tracks:
            yield func(self, *args, **kwargs)


with_bulk_writes = partial(
    track_method_calls,
    tracking_mixin=BulkWritesMixin,
    calls_tracker=track_calls_without_executing
)
