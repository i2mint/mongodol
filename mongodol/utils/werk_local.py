"""
Vendored from werkzeug's local.py module, edited to our needs.
That single need is have a LocalProxy to subclass in making TrackedObj (see tracking_methods.py).
"""
import copy
import math
import operator
import typing as t
from functools import partial


class _ProxyLookup:
    """Descriptor that handles proxied attribute lookup for
    :class:`LocalProxy`.

    :param f: The built-in function this attribute is accessed through.
        Instead of looking up the special method, the function call
        is redone on the object.
    :param fallback: Call this method if the proxy is unbound instead of
        raising a :exc:`RuntimeError`.
    :param class_value: Value to return when accessed from the class.
        Used for ``__doc__`` so building docs still works.
    """

    __slots__ = ('bind_f', 'fallback', 'class_value', 'name')

    def __init__(self, f=None, fallback=None, class_value=None):
        if hasattr(f, '__get__'):
            # A Python function, can be turned into a bound method.

            def bind_f(instance, obj):
                return f.__get__(obj, type(obj))

        elif f is not None:
            # A C function, use partial to bind the first argument.

            def bind_f(instance, obj):
                return partial(f, obj)

        else:
            # Use getattr, which will produce a bound method.
            bind_f = None

        self.bind_f = bind_f
        self.fallback = fallback
        self.class_value = class_value

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner=None):
        if instance is None:
            if self.class_value is not None:
                return self.class_value

            return self

        try:
            obj = instance._get_current_object()
        except RuntimeError:
            if self.fallback is None:
                raise

            return self.fallback.__get__(instance, owner)

        if self.bind_f is not None:
            return self.bind_f(instance, obj)

        return getattr(obj, self.name)

    def __repr__(self):
        return f'proxy {self.name}'

    def __call__(self, instance, *args, **kwargs):
        """Support calling unbound methods from the class. For example,
        this happens with ``copy.copy``, which does
        ``type(x).__copy__(x)``. ``type(x)`` can't be proxied, so it
        returns the proxy type and descriptor.
        """
        return self.__get__(instance, type(instance))(*args, **kwargs)


class _ProxyIOp(_ProxyLookup):
    """Look up an augmented assignment method on a proxied object. The
    method is wrapped to return the proxy instead of the object.
    """

    __slots__ = ()

    def __init__(self, f=None, fallback=None):
        super().__init__(f, fallback)

        def bind_f(instance, obj):
            def i_op(self, other):
                f(self, other)
                return instance

            return i_op.__get__(obj, type(obj))

        self.bind_f = bind_f


def _l_to_r_op(op):
    """Swap the argument order to turn an l-op into an r-op."""

    def r_op(obj, other):
        return op(other, obj)

    return r_op


class LocalProxy:
    """A proxy to the object bound to a :class:`Local`. All operations
    on the proxy are forwarded to the bound object. If no object is
    bound, a :exc:`RuntimeError` is raised.

    .. code-block:: python

        from werkzeug.local import Local
        l = Local()

        # a proxy to whatever l.user is set to
        user = l("user")

        from werkzeug.local import LocalStack
        _request_stack = LocalStack()

        # a proxy to _request_stack.top
        request = _request_stack()

        # a proxy to the session attribute of the request proxy
        session = LocalProxy(lambda: request.session)

    ``__repr__`` and ``__class__`` are forwarded, so ``repr(x)`` and
    ``isinstance(x, cls)`` will look like the proxied object. Use
    ``issubclass(type(x), LocalProxy)`` to check if an object is a
    proxy.

    .. code-block:: python

        repr(user)  # <User admin>
        isinstance(user, User)  # True
        issubclass(type(user), LocalProxy)  # True

    :param local: The :class:`Local` or callable that provides the
        proxied object.
    :param name: The attribute name to look up on a :class:`Local`. Not
        used if a callable is given.

    .. versionchanged:: 2.0
        Updated proxied attributes and methods to reflect the current
        data model.

    .. versionchanged:: 0.6.1
        The class can be instantiated with a callable.
    """

    __slots__ = ('__local', '__name', '__wrapped__')

    def __init__(
        self,
        local: t.Union['Local', t.Callable[[], t.Any]],
        name: t.Optional[str] = None,
    ) -> None:
        object.__setattr__(self, '_LocalProxy__local', local)
        object.__setattr__(self, '_LocalProxy__name', name)

        if callable(local) and not hasattr(local, '__release_local__'):
            # "local" is a callable that is not an instance of Local or
            # LocalManager: mark it as a wrapped function.
            object.__setattr__(self, '__wrapped__', local)

    def _get_current_object(self) -> t.Any:
        """Return the current object.  This is useful if you want the real
        object behind the proxy at a time for performance reasons or because
        you want to pass the object into a different context.
        """
        if not hasattr(self.__local, '__release_local__'):  # type: ignore
            return self.__local()  # type: ignore

        try:
            return getattr(self.__local, self.__name)  # type: ignore
        except AttributeError:
            raise RuntimeError(f'no object bound to {self.__name}')  # type: ignore

    __doc__ = _ProxyLookup(  # type: ignore
        class_value=__doc__, fallback=lambda self: type(self).__doc__
    )
    # __del__ should only delete the proxy
    __repr__ = _ProxyLookup(
        repr, fallback=lambda self: f'<{type(self).__name__} unbound>'
    )
    __str__ = _ProxyLookup(str)
    __bytes__ = _ProxyLookup(bytes)
    __format__ = _ProxyLookup()  # type: ignore
    __lt__ = _ProxyLookup(operator.lt)
    __le__ = _ProxyLookup(operator.le)
    __eq__ = _ProxyLookup(operator.eq)
    __ne__ = _ProxyLookup(operator.ne)
    __gt__ = _ProxyLookup(operator.gt)
    __ge__ = _ProxyLookup(operator.ge)
    __hash__ = _ProxyLookup(hash)  # type: ignore
    __bool__ = _ProxyLookup(bool, fallback=lambda self: False)
    __getattr__ = _ProxyLookup(getattr)
    # __getattribute__ triggered through __getattr__
    __setattr__ = _ProxyLookup(setattr)
    __delattr__ = _ProxyLookup(delattr)
    __dir__ = _ProxyLookup(dir, fallback=lambda self: [])  # type: ignore
    # __get__ (proxying descriptor not supported)
    # __set__ (descriptor)
    # __delete__ (descriptor)
    # __set_name__ (descriptor)
    # __objclass__ (descriptor)
    # __slots__ used by proxy itself
    # __dict__ (__getattr__)
    # __weakref__ (__getattr__)
    # __init_subclass__ (proxying metaclass not supported)
    # __prepare__ (metaclass)
    __class__ = _ProxyLookup(fallback=lambda self: type(self))  # type: ignore
    __instancecheck__ = _ProxyLookup(lambda self, other: isinstance(other, self))
    __subclasscheck__ = _ProxyLookup(lambda self, other: issubclass(other, self))
    # __class_getitem__ triggered through __getitem__
    __call__ = _ProxyLookup(lambda self, *args, **kwargs: self(*args, **kwargs))
    __len__ = _ProxyLookup(len)
    __length_hint__ = _ProxyLookup(operator.length_hint)
    __getitem__ = _ProxyLookup(operator.getitem)
    __setitem__ = _ProxyLookup(operator.setitem)
    __delitem__ = _ProxyLookup(operator.delitem)
    # __missing__ triggered through __getitem__
    __iter__ = _ProxyLookup(iter)
    __next__ = _ProxyLookup(next)
    __reversed__ = _ProxyLookup(reversed)
    __contains__ = _ProxyLookup(operator.contains)
    __add__ = _ProxyLookup(operator.add)
    __sub__ = _ProxyLookup(operator.sub)
    __mul__ = _ProxyLookup(operator.mul)
    __matmul__ = _ProxyLookup(operator.matmul)
    __truediv__ = _ProxyLookup(operator.truediv)
    __floordiv__ = _ProxyLookup(operator.floordiv)
    __mod__ = _ProxyLookup(operator.mod)
    __divmod__ = _ProxyLookup(divmod)
    __pow__ = _ProxyLookup(pow)
    __lshift__ = _ProxyLookup(operator.lshift)
    __rshift__ = _ProxyLookup(operator.rshift)
    __and__ = _ProxyLookup(operator.and_)
    __xor__ = _ProxyLookup(operator.xor)
    __or__ = _ProxyLookup(operator.or_)
    __radd__ = _ProxyLookup(_l_to_r_op(operator.add))
    __rsub__ = _ProxyLookup(_l_to_r_op(operator.sub))
    __rmul__ = _ProxyLookup(_l_to_r_op(operator.mul))
    __rmatmul__ = _ProxyLookup(_l_to_r_op(operator.matmul))
    __rtruediv__ = _ProxyLookup(_l_to_r_op(operator.truediv))
    __rfloordiv__ = _ProxyLookup(_l_to_r_op(operator.floordiv))
    __rmod__ = _ProxyLookup(_l_to_r_op(operator.mod))
    __rdivmod__ = _ProxyLookup(_l_to_r_op(divmod))
    __rpow__ = _ProxyLookup(_l_to_r_op(pow))
    __rlshift__ = _ProxyLookup(_l_to_r_op(operator.lshift))
    __rrshift__ = _ProxyLookup(_l_to_r_op(operator.rshift))
    __rand__ = _ProxyLookup(_l_to_r_op(operator.and_))
    __rxor__ = _ProxyLookup(_l_to_r_op(operator.xor))
    __ror__ = _ProxyLookup(_l_to_r_op(operator.or_))
    __iadd__ = _ProxyIOp(operator.iadd)
    __isub__ = _ProxyIOp(operator.isub)
    __imul__ = _ProxyIOp(operator.imul)
    __imatmul__ = _ProxyIOp(operator.imatmul)
    __itruediv__ = _ProxyIOp(operator.itruediv)
    __ifloordiv__ = _ProxyIOp(operator.ifloordiv)
    __imod__ = _ProxyIOp(operator.imod)
    __ipow__ = _ProxyIOp(operator.ipow)
    __ilshift__ = _ProxyIOp(operator.ilshift)
    __irshift__ = _ProxyIOp(operator.irshift)
    __iand__ = _ProxyIOp(operator.iand)
    __ixor__ = _ProxyIOp(operator.ixor)
    __ior__ = _ProxyIOp(operator.ior)
    __neg__ = _ProxyLookup(operator.neg)
    __pos__ = _ProxyLookup(operator.pos)
    __abs__ = _ProxyLookup(abs)
    __invert__ = _ProxyLookup(operator.invert)
    __complex__ = _ProxyLookup(complex)
    __int__ = _ProxyLookup(int)
    __float__ = _ProxyLookup(float)
    __index__ = _ProxyLookup(operator.index)
    __round__ = _ProxyLookup(round)
    __trunc__ = _ProxyLookup(math.trunc)
    __floor__ = _ProxyLookup(math.floor)
    __ceil__ = _ProxyLookup(math.ceil)
    __enter__ = _ProxyLookup()
    __exit__ = _ProxyLookup()
    __await__ = _ProxyLookup()
    __aiter__ = _ProxyLookup()
    __anext__ = _ProxyLookup()
    __aenter__ = _ProxyLookup()
    __aexit__ = _ProxyLookup()
    __copy__ = _ProxyLookup(copy.copy)
    __deepcopy__ = _ProxyLookup(copy.deepcopy)
    # __getnewargs_ex__ (pickle through proxy not supported)
    # __getnewargs__ (pickle)
    # __getstate__ (pickle)
    # __setstate__ (pickle)
    # __reduce__ (pickle)
    # __reduce_ex__ (pickle)
