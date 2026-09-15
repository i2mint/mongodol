# mongodol.add_ons

Add-ons
[https://github.com/i2mint/mongodol/issues/3](https://github.com/i2mint/mongodol/issues/3)

### Functions

| [`add_clear_method`](#mongodol.add_ons.add_clear_method)(store, \*[, clear_method, ...])   | Add a clear method to a store that doesn't have one                                      |
|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| [`disallow_if_name_exists_already`](#mongodol.add_ons.disallow_if_name_exists_already)(store, ...)        | Raise `MethodNameAlreadyExists` if `store` already has an attribute named `method_name`. |
| [`has_exactly_one_non_defaulted_input`](#mongodol.add_ons.has_exactly_one_non_defaulted_input)(func)          | Return True iff function has exactly one argument without defaults                       |
| [`number_of_non_defaulted_arguments`](#mongodol.add_ons.number_of_non_defaulted_arguments)(func)            | Return the number of arguments that don't have defaults in it's signature                |

### Classes

| [`Addons`](#mongodol.add_ons.Addons)()   | A collection of add-on methods.   |
|-------------------------------------------------------------|-----------------------------------|

### *class* mongodol.add_ons.Addons

Bases: [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

A collection of add-on methods. Addons can’t (and is not meant to) be instantiated.
It’s just to group add-on functions (meant to be injected in stores) in one place

#### clear()

Delete every doc matching this store’s filter, without confirmation.

#### clear_after_checking_with_user()

Delete every doc matching this store’s filter, after the user confirms the count on stdin.

#### dflt_clear_method()

Delete every doc matching this store’s filter, without confirmation.

### mongodol.add_ons.add_clear_method(store, \*, clear_method=<function Addons.clear>, validator=<function \_clear_method_injection_validator>)

Add a clear method to a store that doesn’t have one

* **Parameters:**
  * **store**
  * **clear_method**
* **Returns:**

```pycon
>>> from dol.util import has_enabled_clear_method
>>> from mongodol.base import MongoCollectionPersister
>>> from mongodol.tests import data, populated_pymongo_collection
>>>
>>> whole_store = MongoCollectionPersister(populated_pymongo_collection(data.feature_cube))
>>> whole_length_before_clear = len(whole_store)
>>> assert whole_length_before_clear == 7
>>> reds = MongoCollectionPersister(whole_store.mgc, filter={'color': 'red'})
>>> n_reds = len(reds)
>>> assert n_reds == 4
```

`reds` doesn’t have a clear method

```pycon
>>> assert not has_enabled_clear_method(reds)
```

So let’s give it one

```pycon
>>> reds_with_clear = add_clear_method(reds)
>>> assert has_enabled_clear_method(reds_with_clear)
```

And it’s one that works too!

```pycon
>>> r = reds_with_clear.clear()
>>> assert len(reds_with_clear) == 0
```

It’s the data that was deleted, not just the view. See what reds and whole_store say:

```pycon
>>> assert len(reds) == 0
>>> assert len(whole_store) == whole_length_before_clear - n_reds == 3
```

### mongodol.add_ons.disallow_if_name_exists_already(store, method_name)

Raise `MethodNameAlreadyExists` if `store` already has an attribute named `method_name`.

### mongodol.add_ons.has_exactly_one_non_defaulted_input(func)

Return True iff function has exactly one argument without defaults

### mongodol.add_ons.number_of_non_defaulted_arguments(func)

Return the number of arguments that don’t have defaults in it’s signature
