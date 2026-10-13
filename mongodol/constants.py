"""Module to centralize constants used throughout project.

This includes enums, aliases, defaults, types, etc.
"""

from pymongo.collection import Collection as PyMongoCollection
from typing import Union

ID = "_id"

PyMongoCollectionSpec = Union[None, PyMongoCollection, str]

end_of_cursor = type("end_of_cursor", (object,), {})()
end_of_cursor.__doc__ = "Sentinel used to signal that the cursor has no more data"

DFLT_MONGO_CLIENT_ARGS = ()
#: The UUID codec of the clients mongodol makes. pymongo 3 defaulted to the legacy
#: Python layout (BSON subtype 3); pymongo 4 defaults to "unspecified", which cannot
#: encode a ``uuid.UUID`` and reads old ones back as ``Binary``. Keeping the 3.x
#: layout means data written before the upgrade still reads back as ``uuid.UUID``.
DFLT_UUID_REPRESENTATION = "pythonLegacy"
DFLT_TEST_HOST = "mongodb://localhost:27017"
DFLT_TEST_DB = "mongodol"
DFLT_TEST_COLLECTION = "mongodol_test"
