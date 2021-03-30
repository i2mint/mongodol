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


def forward_method_calls(method):
    @wraps(method)
    def forwarded_method(self, *args, **kwargs):
        return method(self._instance, *args, **kwargs)

    return forwarded_method


class TrackableMixin:
    tracks_factory = list

    @cached_property
    def _tracks(self):
        return self.tracks_factory()

    def clear_tracks(self):
        self._tracks.clear()

    # def _execute_and_clear_tracks(self):
    #     for func, args, kwargs in self._tracks:
    #         func(self, *args, **kwargs)
    #     self._clear_tracks()


@double_up_as_factory
def track_method_calls(obj=None,
                       *,
                       tracked_methods: Iterable[str] = frozenset(),
                       tracking_mixin: type = TrackableMixin,
                       execute_call: bool = True,
                       forwarded_methods: Iterable[str] = frozenset()
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
    >>> dd = D(a=1, b=[1, 2], c={'hello': 'world'})
    >>> assert repr(dd) == "{'a': 1, 'b': [1, 2], 'c': {'hello': 'world'}}"
    >>> assert dd._tracks == []
    >>> assert dd['a'] == 1
    >>> assert dd._tracks == []  # accessing 'a' didn't make any tracks
    >>> dd['a'] = 42
    >>> assert dd['a'] == 42  # verifying that dd['a'] is now 42
    >>> assert len(dd._tracks) > 0  # see that dd._tracks is now non-empty
    >>> assert str(dd._tracks) == "[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]"

    A common use of ``track_method_calls`` is to accumulate method calls without executing them,
    so as to be able to change the way they're called. For example, making the calls differently
    (e.g. in a parallel process) or aggregating several operations and running them in bulk
    (e.g. data base writes).

    >>> @track_method_calls(tracked_methods='__setitem__', execute_call=False)
    ... class D(dict):
    ...     pass
    >>> dd = D(a=1, b=[1, 2], c={'hello': 'world'})
    >>> assert dd._tracks == []  # accessing 'a' didn't make any tracks
    >>> dd['a'] = 42
    >>> assert dd['a'] == 1  # verifying that dd['a'] is STILL 1
    >>> assert len(dd._tracks) > 0  # but dd._tracks is now non-empty
    >>> assert str(dd._tracks) == "[(<slot wrapper '__setitem__' of 'dict' objects>, ('a', 42), {})]"
    >>> # loop through tracks, execute, and clear all tracks
    >>> for func, args, kwargs in dd._tracks:
    ...     func(dd, *args, **kwargs)
    >>> dd.clear_tracks()
    >>> # See that the setitem call was indeed made
    >>> assert dd['a'] == 42
    >>> assert len(dd._tracks) == 0 
    
    """
    if isinstance(tracked_methods, str):
        tracked_methods = {tracked_methods}
    if isinstance(forwarded_methods, str):
        forwarded_methods = {forwarded_methods}

    # TODO: Include this logic in a wrap_first_arg_if_not_a_type decorator instead.
    if isinstance(obj, type):
        call_tracker = partial(
            track_calls_of_method,
            execute_call=execute_call,
            tracks_factory=tracking_mixin.tracks_factory
        )

        tracked_and_forwarded = set(forwarded_methods).intersection(tracked_methods)
        assert not tracked_and_forwarded, f"tracked_methods and forwarded_methods intersected on some method names: {', '.join(tracked_and_forwarded)}"

        class TrackedObj(tracking_mixin, obj):
            pass

        for method_name in tracked_methods:
            method_to_track = getattr(TrackedObj, method_name)
            setattr(TrackedObj, method_name, call_tracker(method_to_track))

        for method_name in forwarded_methods:
            forwarded_method = getattr(TrackedObj, method_name)
            setattr(TrackedObj, method_name, forward_method_calls(forwarded_method))

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
        tracked_or_forwarded = set(forwarded_methods).union(tracked_methods)

        #         print(f"instance: {obj=}: {tracked_or_forwarded=}")

        class WrappedInstance:
            _instance = None

            def __getattr__(self, a):
                return getattr(self._instance, a)

        WrappedInstance._instance = obj

        for method_name in tracked_or_forwarded:
            forwarded_method = getattr(obj, method_name)
            setattr(WrappedInstance, method_name, forward_method_calls(forwarded_method))

        wrapped_cls = track_method_calls(
            WrappedInstance,
            tracked_methods=tracked_methods,
            tracking_mixin=tracking_mixin,
            execute_call=execute_call,
            forwarded_methods=forwarded_methods)
        #         print(wrapped_cls)

        return wrapped_cls()


def consume(gen):
    for _ in gen:
        pass


class DifferedExecutionTrackableMixin(TrackableMixin):
    def __enter__(self):
        self.tracked_self = track_method_calls(self, ...)
        # return self.tracked_self
        self.execute_call = False
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.execute_call = True
        return self.execute_and_clear_tracks()

    def execute_and_clear_tracks(self):
        return_values = list(self.execute_tracks_and_yield_results())
        self.clear_tracks()
        return return_values

    def execute_tracks_and_yield_results(self):
        for func, args, kwargs in self._tracks:
            yield func(self, *args, **kwargs)


differed_track_method_calls = partial(track_method_calls, tracking_mixin=DifferedExecutionTrackableMixin)