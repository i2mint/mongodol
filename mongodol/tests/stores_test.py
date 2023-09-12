from mongodol.constants import DFLT_TEST_COLLECTION, DFLT_TEST_DB, DFLT_TEST_HOST
from mongodol.stores import MongoStore
from mongodol.util import normalize_projection


def test_mongo_store(
    s=MongoStore(
        host=DFLT_TEST_HOST,
        db_name=DFLT_TEST_DB,
        collection_name=DFLT_TEST_COLLECTION,
        getitem_projection=normalize_projection(['val']),
    ),
    k=None,
    v=None,
):

    # Empty collection
    for k in s:
        del s[k]

    if k is None:
        k = {'_id': 'foo'}
    if v is None:
        v = {'val': 'bar'}
    if k in s:  # deleting all docs in tmp
        del s[k]
    assert (k in s) is False  # see that key is not in store (and testing __contains__)
    orig_length = len(s)
    s[k] = v
    assert len(s) == orig_length + 1
    assert k in list(s)
    assert s[k] == v
    assert s.get(k) == v
    assert v in list(s.values())
    assert (k in s) == True  # testing __contains__ again
    del s[k]
    assert len(s) == 0
