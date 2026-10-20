"""Access mongo through a Mapping interface"""

from mongodol.base import (
    MongoClientReader,
    MongoDbReader,
    MongoCollectionReader,
    MongoCollectionPersister,
)
from mongodol.stores import (
    MongoCollectionUniqueDocReader,
    MongoCollectionFirstDocReader,
    MongoCollectionMultipleDocsReader,
    MongoCollectionUniqueDocPersister,
    MongoCollectionFirstDocPersister,
    MongoCollectionMultipleDocsPersister,
)
from mongodol.util import (
    mk_dflt_mgc,
    normalize_projection,
    get_mongo_collection_pymongo_obj,
    ensure_unique_index,
    has_unique_index,
)
from mongodol.tests.util import get_test_collection_persister
