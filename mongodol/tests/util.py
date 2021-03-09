from functools import lru_cache
from pymongo import MongoClient
from mongodol.base import MongoCollectionPersister, MongoCollectionReader
from mongodol.stores import MongoStore

DFLT_MONGO_CLIENT_ARGS = ()
DFLT_TEST_DB = 'py2store'
DFLT_TEST_COLLECTION = 'mongodol_test'


@lru_cache(maxsize=1)
def get_test_collection_object(mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
                               db_name=DFLT_TEST_DB,
                               collection_name=DFLT_TEST_COLLECTION):
    return MongoClient(*mongo_client_args)[db_name][collection_name]


def get_test_collection_persister(mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
                                  db_name=DFLT_TEST_DB,
                                  collection_name=DFLT_TEST_COLLECTION):
    mgc = get_test_collection_object(mongo_client_args, db_name, collection_name)
    return MongoCollectionPersister(mgc)


# TODO: Add some protection to clearing all store?
def clear_all_and_populate(test_store, docs=()):
    for k in test_store:
        del test_store[k]
    test_store.extend(docs)

