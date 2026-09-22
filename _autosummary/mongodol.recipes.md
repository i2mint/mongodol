# mongodol.recipes

Mongodol Recipes

### Functions

| [`disallow_sourced_interval_overlaps`](#mongodol.recipes.disallow_sourced_interval_overlaps)(store)   | Disallow writing to a key that shares the same "source" field value and overlapping ("bt", "tt") interval.   |
|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|

### Exceptions

| [`WriteNotAllowedToThatKey`](#mongodol.recipes.WriteNotAllowedToThatKey)   | To indicate that once cannot write to some specific key one is trying to write to   |
|-----------------------------------------------------------------------------|-------------------------------------------------------------------------------------|

### *exception* mongodol.recipes.WriteNotAllowedToThatKey

Bases: [`KeyError`](https://docs.python.org/3/builtins/exceptions.html#KeyError)

To indicate that once cannot write to some specific key one is trying to write to

### mongodol.recipes.disallow_sourced_interval_overlaps(store)

Disallow writing to a key that shares the same “source” field value and overlapping (“bt”, “tt”) interval.

* **Parameters:**
  **store** – `KvPersister` (instance or class) `s`
* **Returns:**
  The same store, but where `s[dict(source=source, bt=bt, tt=tt)] = v` writes are not permitted if
  there is another doc, with the same source, and an overlapping (bt, tt) interval.

```pycon
>>> from mongodol.tests import get_test_collection_persister, clear_all_and_populate
>>>
>>> # We're going to take (make really) a store s with the two follwing documents:
>>>
>>> data = [
...     {'source': 'audio', 'bt': 6, 'tt': 9, 'annot': 'dog'},
...     {'source': 'visual', 'bt': 6, 'tt': 15, 'annot': 'dog'}
... ]
>>>
>>> # Then try to do s[k] = v with the following (k, v) pairs.
>>> k1 = {'source': 'audio', 'bt': 12, 'tt': 16}
>>> v1 = {'annot': 'cat'}
>>> # Note here that (7, 10) overlaps with (6, 9) (in source=audio)
>>> k2 = {'source': 'audio', 'bt': 7, 'tt': 10}
>>> v2 = {'annot': 'cat'}
>>>
>>>
>>>
>>> s = get_test_collection_persister()  # Make a persister
>>> clear_all_and_populate(data,s)  # empty it and populate it with the two data docs
>>> assert len(s) == 2  # yep, two docs
>>> assert s.distinct('annot') == ['dog']  # and only has a dog
>>>
>>>
>>> s[k1] = v1
>>> assert len(s) == 3  # Now has three docs
>>> assert 'cat' in s.distinct('annot')  # has a cat now
>>>
>>> s[k2] = v2
>>> assert len(s) == 4  # v2 was written, indeed
>>>
>>>
>>> # Let's start over, usingour disallow_sourced_interval_overlaps decorator this time
>>>
>>> protected_s = disallow_sourced_interval_overlaps(get_test_collection_persister())
>>> clear_all_and_populate(data,protected_s)  # empty it and populate it with the two data docs
>>>
>>> s[k1] = v1
>>> # No problem! And see that you have a cat annot now!
>>> assert 'cat' in s.distinct('annot')
>>> # But this next one will not work since the (7, 10) overlaps with (6, 9) (in source=audio)
>>> try:
...     protected_s[k2] = v2
... except WriteNotAllowedToThatKey:
...     print("WriteNotAllowedToThatKey expected!")
WriteNotAllowedToThatKey expected!
```
