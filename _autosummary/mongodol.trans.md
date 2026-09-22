# mongodol.trans

Transformative functionality

### Functions

| [`get_persistent_obj`](#mongodol.trans.get_persistent_obj)(container, v)                  | Wrap `v` in a `PersistentDict`/`PersistentList` if it's a mapping/iterable, else return it as is.   |
|----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| [`normalize_result`](#mongodol.trans.normalize_result)(obj, \*[, ...])                  | Decorator to transform a pymongo result object to a WriteOpResult object.                           |
| [`set_key_and_data_fields`](#mongodol.trans.set_key_and_data_fields)([store, key_fields, ...]) | Decorator to set key_fields and data_fields on a store.                                             |

### Classes

| [`ObjOfData`](#mongodol.trans.ObjOfData)()                             | `obj_of_data` (value-only) transform functions for `wrap_kvs`.                                                            |
|------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| [`PersistentDict`](#mongodol.trans.PersistentDict)(container, wrapped_dict) | Extension of a dict wich triggers an event to notify the object that contains the dict that a modification has been made. |
| [`PersistentList`](#mongodol.trans.PersistentList)(container, iterable)     | Extension of a list wich triggers an event to notify the object that contains the list that a modification has been made. |
| [`PersistentObjectBase`](#mongodol.trans.PersistentObjectBase)(container)         | Base class to propagate a modification event through a parent-child chain structure.                                      |
| [`PostGet`](#mongodol.trans.PostGet)()                               | `postget` (key-aware) transform functions for `wrap_kvs`, turning a cursor into a value.                                  |
| [`WriteOpResult`](#mongodol.trans.WriteOpResult)                           | The shape of a normalized mongo write-operation result (see `normalize_result`).                                          |

### *class* mongodol.trans.ObjOfData

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

`obj_of_data` (value-only) transform functions for `wrap_kvs`.

#### *static* all_docs_fetch(cursor, doc_collector=<class 'list'>)

Collect every doc in `cursor` into `doc_collector` (default: a list).

The value-only (`obj_of_data`) counterpart of [`PostGet.all_docs_fetch()`](#mongodol.trans.PostGet.all_docs_fetch).

### *class* mongodol.trans.PersistentDict(container, wrapped_dict)

Bases: [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict), [`PersistentObjectBase`](#mongodol.trans.PersistentObjectBase)

Extension of a dict wich triggers an event to notify the object that contains the dict that a modification
has been made.

Requirement: The container object needs to implement the method “persist_data(self, data: Mapping)”.

```pycon
>>> d = {
...     'a': 1,
...     'b': {'ba': 2, 'bb': 3},
...     'c': [
...         {'c1a': 4, 'c1b': 5},
...         {'c2a': 6, 'c2b': '7'}
...     ]
... }
>>> class Container:
...     def persist_data(self, data):
...         """Here, you'd normally put code to ACTUALLY persist the data"""
...         print(f"persisting {data}")
>>> pd = PersistentDict(Container(), d)
>>> pd['a'] = 8
persisting {'a': 8, 'b': {'ba': 2, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['a'] == 8  # and indeed pd['a'] is 8 now!
>>> pd['b']['ba'] = 8
persisting {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 4, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['b']['ba'] == 8  # and indeed pd['b']['ba'] is 8 now!
>>> pd['c'][0]['c1a'] = 8
persisting {'a': 8, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['c'][0]['c1a'] == 8  # and indeed pd['c'][0]['c1a'] is 8 now!
>>> pd.update({'a': 9})
persisting {'a': 9, 'b': {'ba': 8, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['a'] == 9
>>> pd['b'].update({'ba': 9})
persisting {'a': 9, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 8, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['b']['ba'] == 9
>>> pd['c'][0].update({'c1a': 9})
persisting {'a': 9, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['c'][0]['c1a'] == 9
>>> pd.update([('a', 10)])
persisting {'a': 10, 'b': {'ba': 9, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['a'] == 10
>>> pd['b'].update([('ba', 10)])
persisting {'a': 10, 'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 9, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['b']['ba'] == 10
>>> pd['c'][0].update([('c1a', 10)])
persisting {'a': 10, 'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert pd['c'][0]['c1a'] == 10
>>> del pd['a']
persisting {'b': {'ba': 10, 'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert 'a' not in pd  # indeed, 'a' no longer in pd
>>> del pd['b']['ba']
persisting {'b': {'bb': 3}, 'c': [{'c1a': 10, 'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert 'ba' not in pd['b']
>>> del pd['c'][0]['c1a']
persisting {'b': {'bb': 3}, 'c': [{'c1b': 5}, {'c2a': 6, 'c2b': '7'}]}
>>> assert 'c1a' not in pd['c'][0]
```

#### update(\*args, \*\*kwargs)

Update like a normal dict, then persist the updated dict.

### *class* mongodol.trans.PersistentList(container, iterable)

Bases: [`list`](https://docs.python.org/3/builtins/stdtypes.html#list), [`PersistentObjectBase`](#mongodol.trans.PersistentObjectBase)

Extension of a list wich triggers an event to notify the object that contains the list that a modification
has been made.

Requirement: The container object needs to implement the method “persist_data(self, data: Mapping)”.

```pycon
>>> l = [1, 2, 3]
>>> class Container:
...     def persist_data(self, data):
...         """Here, you'd normally put code to ACTUALLY persist the data"""
...         print(f"persisting {data}")
>>> pl = PersistentList(Container(), l)
>>> assert pl == [1, 2, 3]  # pl is equal to [1, 2, 3]
>>> pl.append(4)
persisting [1, 2, 3, 4]
>>> assert pl == [1, 2, 3, 4]  # indeed pl is now [1, 2, 3, 4]
>>> pl.extend([5, 6])
persisting [1, 2, 3, 4, 5, 6]
>>> assert pl == [1, 2, 3, 4, 5, 6]
>>> pl += [7, 8]
persisting [1, 2, 3, 4, 5, 6, 7, 8]
>>> assert pl == [1, 2, 3, 4, 5, 6, 7, 8]
>>> pl[0] = 9
persisting [9, 2, 3, 4, 5, 6, 7, 8]
>>> assert pl == [9, 2, 3, 4, 5, 6, 7, 8]
>>> n = pl.pop(0)
persisting [2, 3, 4, 5, 6, 7, 8]
>>> assert pl == [2, 3, 4, 5, 6, 7, 8]
>>> pl.remove(2)
persisting [3, 4, 5, 6, 7, 8]
>>> assert pl == [3, 4, 5, 6, 7, 8]
>>> del pl[0]
persisting [4, 5, 6, 7, 8]
>>> assert pl == [4, 5, 6, 7, 8]
```

#### append(\_PersistentList_\_object)

Append `__object`, then persist the updated list.

#### deepcopy()

A deep copy of the original iterable this list was built from (not the persistent list itself).

#### extend(\_PersistentList_\_iterable)

Extend with `__iterable`, then persist the updated list.

#### pop(\_PersistentList_\_index)

Pop the item at `__index`, then persist the updated list.

#### remove(\_PersistentList_\_value)

Remove the first occurrence of `__value`, then persist the updated list.

### *class* mongodol.trans.PersistentObjectBase(container)

Bases: [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Base class to propagate a modification event through a parent-child chain structure.

#### persist_data(\*args)

Notify the container that this object’s data has changed, by forwarding to its `persist_data`.

### *class* mongodol.trans.PostGet

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

`postget` (key-aware) transform functions for `wrap_kvs`, turning a cursor into a value.

#### *static* all_docs_fetch(k, cursor, doc_collector=<class 'list'>)

Collect every doc matching `k`, so `s[k]` is a collection of docs.

The key-aware (`postget`) counterpart of [`ObjOfData.all_docs_fetch()`](#mongodol.trans.ObjOfData.all_docs_fetch).
`wrap_kvs` calls `obj_of_data` with the value alone and `postget` with
`(key, value)`, so a store wired through `postget` needs this signature.

#### *static* single_value_fetch_with_unicity_validation(store, k, cursor)

Return the single doc in `cursor`; raise if there’s none or more than one.

#### *static* single_value_fetch_without_unicity_validation(store, k, cursor)

Return the first doc in `cursor`; raise only if there’s none (no uniqueness check).

### *class* mongodol.trans.WriteOpResult

Bases: [`TypedDict`](https://docs.python.org/3/library/typing.html#typing.TypedDict)

The shape of a normalized mongo write-operation result (see `normalize_result`).

### mongodol.trans.get_persistent_obj(container, v)

Wrap `v` in a `PersistentDict`/`PersistentList` if it’s a mapping/iterable, else return it as is.

### mongodol.trans.normalize_result(obj, , method_names_to_normalize=('_\_setitem_\_', '_\_delitem_\_', 'append', 'extend', 'flush', 'commit'))

Decorator to transform a pymongo result object to a WriteOpResult object.

* **Parameters:**
  **func** ( *[*[*type*](https://docs.python.org/3/builtins/functions.html#type) *]*) – [description]

### mongodol.trans.set_key_and_data_fields(store=None, , key_fields=None, data_fields=None, \_\_module_\_=None, \_\_name_\_=None, \_\_qualname_\_=None, \_\_doc_\_=None, \_\_annotations_\_=None, \_\_defaults_\_=None, \_\_kwdefaults_\_=None)

Decorator to set key_fields and data_fields on a store.

This is to make it easier to get from an interface like this

`store[{'folder': 'path', 'file': 'name'}] = {'field1': 'value1', 'field2': 'value2'}`

to an interface like this:

`store['path', 'name'] = ('value1', 'value2')`
