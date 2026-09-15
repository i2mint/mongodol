# mongodol.utils.werk_local

Vendored from werkzeug’s local.py module, edited to our needs.
That single need is have a LocalProxy to subclass in making TrackedObj (see tracking_methods.py).

### Classes

| [`LocalProxy`](#mongodol.utils.werk_local.LocalProxy)(local[, name])   | A proxy to the object bound to a `Local`.   |
|------------------------------------------------------------------------------|---------------------------------------------|

### *class* mongodol.utils.werk_local.LocalProxy(local, name=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A proxy to the object bound to a `Local`. All operations
on the proxy are forwarded to the bound object. If no object is
bound, a [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError) is raised.

```python
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
```

`__repr__` and `__class__` are forwarded, so `repr(x)` and
`isinstance(x, cls)` will look like the proxied object. Use
`issubclass(type(x), LocalProxy)` to check if an object is a
proxy.

```python
repr(user)  # <User admin>
isinstance(user, User)  # True
issubclass(type(user), LocalProxy)  # True
```

* **Parameters:**
  * **local** (`Union`[Local, [`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)[[], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]]) – The `Local` or callable that provides the
    proxied object.
  * **name** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – The attribute name to look up on a `Local`. Not
    used if a callable is given.

#### Versionchanged
Changed in version 2.0: Updated proxied attributes and methods to reflect the current
data model.

#### Versionchanged
Changed in version 0.6.1: The class can be instantiated with a callable.
