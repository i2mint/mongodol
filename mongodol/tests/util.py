from functools import lru_cache
from pymongo import MongoClient

from mongodol.base import MongoCollectionPersister, MongoCollectionReader
from mongodol.tests.data import number_docs, feature_cube, bdfl_docs

DFLT_MONGO_CLIENT_ARGS = ()
DFLT_TEST_DB = 'mongodol'
DFLT_TEST_COLLECTION = 'mongodol_test'


@lru_cache(maxsize=1)
def get_test_database(mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
                      db_name=DFLT_TEST_DB):
    return MongoClient(*mongo_client_args)[db_name]


@lru_cache()
def get_test_collection_object(mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
                               db_name=DFLT_TEST_DB,
                               collection_name=DFLT_TEST_COLLECTION):
    db = get_test_database(mongo_client_args, db_name)
    return db[collection_name]


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


def init_db():
    client = MongoClient(*DFLT_MONGO_CLIENT_ARGS)
    db = client[DFLT_TEST_DB]
    for collection in db.list_collection_names():
        db[collection].delete_many({})

    db.number.insert_many(number_docs)
    db.feature_cube.insert_many(feature_cube)
    db.bdfl.insert_many(bdfl_docs)

