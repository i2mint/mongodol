import pytest
from mongodol.tests.util import populated_pymongo_collection
from mongodol.base import MongoCollectionReader


@pytest.mark.xfail(reason='TDD')
def test_mongo_values_view_when_wrapping():
    s = MongoCollectionReader(mgc=populated_pymongo_collection())

    assert type(s.values()).__name__ == 'MongoValuesView'

    from dol import wrap_kvs

    ss = wrap_kvs(s)
    assert type(ss.values()).__name__ == 'MongoValuesView'

    from dol import wrap_kvs

    ss = wrap_kvs(s)
    ss.values()

    # TODO: But we want this to NOT raise an error. Need to use factories
    with pytest.raises(AttributeError) as excinfo:
        list(ss.values())  # BOOM

    assert "'ValuesView' object has no attribute 'mgc'" in str(excinfo.value)
