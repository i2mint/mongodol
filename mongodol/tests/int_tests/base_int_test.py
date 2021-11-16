import pytest
from itertools import islice
from dol import wrap_kvs

from mongodol import MongoCollectionUniqueDocPersister, MongoCollectionReader
from mongodol.base import MongoBaseStore
from mongodol.constants import ID
from mongodol.tests.data import nums_and_lans, feature_cube, bdfl
from mongodol.tests.util import get_test_collection_object
from mongodol.tests import (
    NUMBER_MGC_NAME,
    FEATURE_CUBE_MGC_NAME,
    BDFL_MGC_NAME,
)


@pytest.mark.parametrize(
    'mgc_name, filt, getitem_projection, mgc_find_kwargs, expected_result',
    [
        # NUMBER COLLECTION
        (
            NUMBER_MGC_NAME,  # mgc_name
            None,  # filter
            None,  # getitem_projection
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
def test_iterate_values(
    mgc_name, filt, getitem_projection, mgc_find_kwargs, expected_result
):
    mgc = get_test_collection_object(collection_name=mgc_name)
    mgc_find_kwargs = mgc_find_kwargs or {}
    store = MongoCollectionReader(
        mgc=mgc, filter=filt, getitem_projection=getitem_projection, **mgc_find_kwargs
    )
    assert list(store.values()) == expected_result


@pytest.mark.parametrize(
    'key_input_mapper, key_output_mapper, value_input_mapper, value_output_mapper, new_doc',
    [
        (
            None,
            None,
            None,
            None,
            {
                ID: 6,
                'en': 'six',
                'fr': 'six',
                'sp': 'seis',
                'so_far': [1, 2, 3, 4, 5, 6],
            },  # new_doc
        ),
        (
            lambda key: {ID: key // 10},  # key_input_mapper
            lambda key: key[ID] * 10,  # key_output_mapper
            lambda value: {
                k: v.lower() if isinstance(v, str) else v for k, v in value.items()
            },  # value_input_mapper
            lambda value: {
                k: v.upper() if isinstance(v, str) else v for k, v in value.items()
            },  # value_output_mapper
            {
                ID: 6,
                'en': 'SIX',
                'fr': 'SIX',
                'sp': 'SEIS',
                'so_far': [1, 2, 3, 4, 5, 6],
            },  # new_doc
        ),
    ],
)
def test_store_with_mappers(
    key_input_mapper,
    key_output_mapper,
    value_input_mapper,
    value_output_mapper,
    new_doc,
):
    def verify_new_doc_is_in_store():
        assert store[new_doc_id] == new_doc
        assert new_doc_id in store
        assert new_doc_id in store.keys()
        assert new_doc in store.values()
        assert (new_doc_id, new_doc_with_no_id) in store.items()
        assert len(store) == len(nums_and_lans) + 1
        del store[new_doc_id]
        assert store.get(new_doc_id) is None
        assert new_doc_id not in store
        assert new_doc not in store.values()
        assert (new_doc_id, new_doc_with_no_id) not in store.items()
        assert len(store) == len(nums_and_lans)

    def identity(x):
        return x

    key_input_mapper = key_input_mapper or identity
    key_output_mapper = key_output_mapper or identity
    value_input_mapper = value_input_mapper or identity
    value_output_mapper = value_output_mapper or identity
    store = wrap_kvs(
        MongoCollectionUniqueDocPersister(
            mgc=get_test_collection_object(collection_name=NUMBER_MGC_NAME)
        ),
        wrapper=MongoBaseStore,
        id_of_key=key_input_mapper,
        key_of_id=key_output_mapper,
        data_of_obj=value_input_mapper,
        obj_of_data=value_output_mapper,
    )
    new_doc_id = key_output_mapper(new_doc)
    new_doc_with_no_id = {k: v for k, v in new_doc.items() if k != ID}
    store.append(new_doc)
    verify_new_doc_is_in_store()
    store.extend([new_doc])
    verify_new_doc_is_in_store()
    store[new_doc_id] = new_doc_with_no_id
    verify_new_doc_is_in_store()
    assert list(store.keys()) == [key_output_mapper({ID: n[ID]}) for n in nums_and_lans]
    assert list(store.values()) == [value_output_mapper(n) for n in nums_and_lans]
    assert list(store.items()) == [
        (
            key_output_mapper({ID: n[ID]}),
            value_output_mapper({k: v for k, v in n.items() if k != ID}),
        )
        for n in nums_and_lans
    ]
