import pytest
from itertools import islice

from mongodol.base import MongoCollectionCollection
from mongodol.tests.data import nums_and_lans, feature_cube, bdfl
from mongodol.tests.util import get_test_collection_object
from mongodol.tests import (
    NUMBER_MGC_NAME,
    FEATURE_CUBE_MGC_NAME,
    BDFL_MGC_NAME,
)


@pytest.mark.parametrize(
    'mgc_name, filter, iter_projection, mgc_find_kwargs, expected_result',
    [
        # NUMBER COLLECTION
        (
            NUMBER_MGC_NAME,  # mgc_name
            None,  # filter
            None,  # iter_projection
            None,  # mgc_find_kwargs
            nums_and_lans,  # expected_result
        ),
        (
            NUMBER_MGC_NAME,
            {'sp': {'$exists': True}},
            None,
            None,
            [x for x in nums_and_lans if 'sp' in x],
        ),
        (
            NUMBER_MGC_NAME,
            None,
            {'so_far': False},
            None,
            [{k: v for k, v in x.items() if k != 'so_far'} for x in nums_and_lans],
        ),
        (
            NUMBER_MGC_NAME,
            None,
            None,
            {'skip': 1, 'limit': 2},
            list(islice(islice(nums_and_lans, 1, None), 2)),
        ),
        # FEATURE CUBE COLLECTION
        (FEATURE_CUBE_MGC_NAME, None, None, None, feature_cube),
        (
            FEATURE_CUBE_MGC_NAME,
            {'number': {'$gte': 10}},
            None,
            None,
            [x for x in feature_cube if x['number'] >= 10],
        ),
        (
            FEATURE_CUBE_MGC_NAME,
            None,
            {'number': True, 'dims': True},
            None,
            [{k: x[k] for k in ('_id', 'number', 'dims')} for x in feature_cube],
        ),
        (
            FEATURE_CUBE_MGC_NAME,
            None,
            None,
            {'skip': 3, 'limit': 2},
            list(islice(islice(feature_cube, 3, None), 2)),
        ),
        # BDFL COLLECTION
        (BDFL_MGC_NAME, None, None, None, bdfl),
        (
            BDFL_MGC_NAME,
            {'Type': 'Web framework'},
            None,
            None,
            [x for x in bdfl if x['Type'] == 'Web framework'],
        ),
        (
            BDFL_MGC_NAME,
            None,
            {'_id': False, 'Name': True},
            None,
            [{'Name': x['Name']} for x in bdfl],
        ),
        (
            BDFL_MGC_NAME,
            None,
            None,
            {'skip': 2, 'limit': 2},
            list(islice(islice(bdfl, 2, None), 2)),
        ),
    ],
)
def test_iterate_collection(
    mgc_name, filter, iter_projection, mgc_find_kwargs, expected_result
):
    mgc = get_test_collection_object(collection_name=mgc_name)
    mgc_find_kwargs = mgc_find_kwargs or {}
    collection = MongoCollectionCollection(
        mgc, filter, iter_projection, **mgc_find_kwargs
    )
    assert list(iter(collection)) == expected_result
