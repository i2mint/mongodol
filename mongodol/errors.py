"""Where mongodol error objects are"""


class AlreadyExists(ValueError):
    """To use if an object already exists (and shouldn't; for example, to protect overwrites)"""


class NotAllowed(Exception):
    """To use to indicate that something is not allowed"""


class NotValid(ValueError, TypeError):
    """To use to indicate when an object doesn't fit expected properties"""


class MethodNameAlreadyExists(AlreadyExists):
    """To use when a method name already exists (and shouldn't)"""


class MethodFuncNotValid(NotValid):
    """Use when method function is not valid"""


class SetattrNotAllowed(NotAllowed):
    """An attribute was requested to be set, but some conditions didn't apply"""
