"""Testing trans.py objects"""


def test_set_key_and_data_fields():
    from mongodol.base import MongoCollectionCollection
    from mongodol.stores import MongoCollectionFirstDocPersister
    from mongodol.trans import set_key_and_data_fields

    def delete_all_keys(store):
        for k in store:
            del store[k]

    mgc_path = 'test/scrap'
    raw = MongoCollectionCollection(mgc_path)
    s = MongoCollectionFirstDocPersister(mgc_path)
    delete_all_keys(s)

    assert list(raw) == []  # is empty!

    s[{'_id': '123'}] = {'name': 'John', 'age': 42}
    s[{'_id': '456'}] = {'name': 'Jane', 'age': 43}

    # What was actually persisted:
    assert list(raw) == [
        {'_id': '123', 'name': 'John', 'age': 42},
        {'_id': '456', 'name': 'Jane', 'age': 43},
    ]

    delete_all_keys(s)

    ss = set_key_and_data_fields(s, key_fields='_id', data_fields=('name', 'age'))

    ss['123'] = ('John', 42)
    ss['456'] = ('Jane', 43)
    assert list(ss) == ['123', '456']
    assert (ss['123'], ss['456']) == (('John', 42), ('Jane', 43))
    assert list(ss.values()) == [('John', 42), ('Jane', 43)]

    # What was actually persisted:
    assert list(raw) == [
        {'_id': '123', 'name': 'John', 'age': 42},
        {'_id': '456', 'name': 'Jane', 'age': 43},
    ]
