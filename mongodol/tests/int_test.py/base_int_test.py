import pytest
from itertools import islice

from mongodol.base import MongoCollectionCollection
from mongodol.tests.data import number_docs, feature_cube, bdfl_docs
from mongodol.tests.util import get_test_collection_object


@pytest.mark.parametrize(
    'mgc_name, filter, iter_projection, mgc_find_kwargs, expected_result',
    [
        # NUMBER COLLECTION
        (
            'number',       # mgc_name
            {},             # filter
            None,           # iter_projection
            {},             # mgc_find_kwargs
            number_docs     # expected_result
        ),
        (
            'number',
            {'sp': {'$exists': True}},
            None,
            {},
            [x for x in number_docs if 'sp' in x]
        ),
        (
            'number',
            {},
            {'so_far': False},
            {},
            [{k: v for k, v in x.items() if k != 'so_far'} for x in number_docs]
        ),
        (
            'number',
            {},
            None,
            {'skip': 1, 'limit': 2},
            list(islice(islice(number_docs, 1, None), 2))
        ),
        (
            'feature_cube',
            {},
            None,
            {},
            feature_cube
        ),
        (
            'feature_cube',
            {'number': {'$gte': 10}},
            None,
            {},
            [x for x in feature_cube if x['number'] >= 10]
        ),
        (
            'feature_cube',
            {},
            {'number': True, 'dims': True},
            {},
            [{k: x[k] for k in ('_id', 'number', 'dims')} for x in feature_cube]
        ),
        (
            'feature_cube',
            {},
            None,
            {'skip': 3, 'limit': 2},
            list(islice(islice(feature_cube, 3, None), 2))
        ),
        (
            'bdfl',
            {},
            None,
            {},
            bdfl_docs
        ),
        (
            'bdfl',
            {'Type': 'Web framework'},
            None,
            {},
            [x for x in bdfl_docs if x['Type'] == 'Web framework']
        ),
        (
            'bdfl',
            {},
            {'_id': False, 'Name': True},
            {},
            [{'Name': x['Name']} for x in bdfl_docs]
        ),
        (
            'bdfl',
            {},
            None,
            {'skip': 2, 'limit': 2},
            list(islice(islice(bdfl_docs, 2, None), 2))
        ),
    ],
)
def test_iterate_collection(mgc_name, filter, iter_projection, mgc_find_kwargs, expected_result):
    mgc = get_test_collection_object(collection_name=mgc_name)
    collection = MongoCollectionCollection(mgc, filter, iter_projection, **mgc_find_kwargs)
    assert list(iter(collection)) == expected_result
