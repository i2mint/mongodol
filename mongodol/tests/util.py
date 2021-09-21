from functools import lru_cache

from mongodol.constants import (
    DFLT_MONGO_CLIENT_ARGS,
    DFLT_TEST_DB,
    DFLT_TEST_COLLECTION,
)
from pymongo import MongoClient

from mongodol.base import MongoCollectionPersister

# from mongodol.util import mk_dflt_mgc
from mongodol.tests import data as test_data


@lru_cache(maxsize=1)
def get_test_database(mongo_client_args=DFLT_MONGO_CLIENT_ARGS, db_name=DFLT_TEST_DB):
    return MongoClient(*mongo_client_args)[db_name]


@lru_cache()
def get_test_collection_object(
    mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
    db_name=DFLT_TEST_DB,
    collection_name=DFLT_TEST_COLLECTION,
):
    db = get_test_database(mongo_client_args, db_name)
    return db[collection_name]


def get_test_collection_persister(
    mongo_client_args=DFLT_MONGO_CLIENT_ARGS,
    db_name=DFLT_TEST_DB,
    collection_name=DFLT_TEST_COLLECTION,
):
    mgc = get_test_collection_object(mongo_client_args, db_name, collection_name)
    return MongoCollectionPersister(mgc)


# TODO: Add some protection to clearing all store?
def clear_all_and_populate(docs=(), test_store=None):
    test_store = test_store or get_test_collection_persister()
    for k in test_store:
        del test_store[k]
    test_store.extend(docs)


def init_db():
    client = MongoClient(*DFLT_MONGO_CLIENT_ARGS)
    db = client[DFLT_TEST_DB]
    for collection in db.list_collection_names():
        db[collection].delete_many({})

    db.number.insert_many(test_data.nums_and_lans)
    db.feature_cube.insert_many(test_data.feature_cube)
    db.bdfl.insert_many(test_data.bdfl)


def populated_pymongo_collection(docs=test_data.nums_and_lans):
    """Get a pymongo collection that has been populated with docs."""
    test_store = get_test_collection_persister()
    clear_all_and_populate(docs, test_store)
    return test_store.mgc
