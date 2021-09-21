"""Module to centralize constants used throughout project.

This includes enums, aliases, defaults, types, etc.
"""
from pymongo.collection import Collection as PyMongoCollection
from typing import Union

ID = '_id'

PyMongoCollectionSpec = Union[None, PyMongoCollection, str]

end_of_cursor = type('end_of_cursor', (object,), {})()
end_of_cursor.__doc__ = 'Sentinel used to signal that the cursor has no more data'

DFLT_MONGO_CLIENT_ARGS = ()
DFLT_TEST_DB = 'mongodol'
DFLT_TEST_COLLECTION = 'mongodol_test'
