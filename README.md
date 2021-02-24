
# mongodol
MongoDB Data Object Layer.

Tools to create data abstractions over mongoDB data.

To install:	```pip install mongodol```


# The base objects


```python
from mongodol import MongoClientReader, MongoDbReader, MongoCollectionReaderBase, MongoCollectionReader, MongoCollectionPersister
```

`MongoClientReader` gives you access to the databases for a mongoDB host (default is localhost). 
The keys are database names...


```python
client = MongoClientReader()
list(client)
```




    ['admin', 'config', 'local', 'py2store', 'py2store_tests', 'yf']



... and the values are db objects. 
The keys of db objects are collection names...


```python
db = client['py2store']
list(db)
```




    ['tmp', 'test', 'annots_example']



... and the values are collection objects. 


```python
mgc = db['test']
len(mgc)
```




    0



The collection is empty. Let's get a collection object that we can actually write with.

Here, we show how you can write by appending data:


```python
writable_mgc = MongoCollectionPersister(mgc)
writable_mgc.append({'mongo': 'uses', 'json': 'data'})
```




    <pymongo.results.InsertOneResult at 0x1203f9940>



See that we have data in the collection now:


```python
keys = list(mgc)
keys
```




    [{'_id': ObjectId('60359a2993b7670664918663')}]



But that's just showing the key, let's see the value under that key:


```python
k = keys[0]
mgc[k]
```




    <pymongo.cursor.Cursor at 0x120fc1be0>



Oh... you get a cursor back. It's okay, a cursor is the object that will provide you with the data you requested if and when you want it. 

Let's say you want it now. Just "consume" the cursor. If you're expecting just one item under that key, do this:


```python
v = next(mgc[k], None)  # the None is there as a sentinel -- it will be used to indicate if mgc[k] has no data for you.
v
```




    {'mongo': 'uses', 'json': 'data'}



So indeed it worked. 

You can also use extend to write in bulk.


```python
writable_mgc.extend([{'kind': 'example', 'data': 2}, 
                     {'kind': 'example', 'data': [1, 2, 3]},
                     {'kind': 'example', 'data': {'nested': 'dict'}}
                    ])
```




    <pymongo.results.InsertManyResult at 0x11e520200>




```python
list(mgc)
```




    [{'_id': ObjectId('60359a2993b7670664918663')},
     {'_id': ObjectId('60359ac193b7670664918664')},
     {'_id': ObjectId('60359ac193b7670664918665')},
     {'_id': ObjectId('60359ac193b7670664918666')}]




```python

```

So far, MongoDB gave us an id. MongoDB will make it's own id if we don't ask for a particular one.

But you can also write data to a key of your choice. With the base persister which we're demoing now, with it's base defaults, you need to specify your key as a `{'_id': YOUR_CHOICE_OF_ID}`.


```python
writable_mgc[{'_id': 'my_id'}] = {'my': 'data'}
list(mgc)
```




    [{'_id': ObjectId('60359a2993b7670664918663')},
     {'_id': ObjectId('60359ac193b7670664918664')},
     {'_id': ObjectId('60359ac193b7670664918665')},
     {'_id': ObjectId('60359ac193b7670664918666')},
     {'_id': 'my_id'}]




```python
mgc[{'_id': 'my_id'}]
```




    {'my': 'data'}



You can delete data given a key:


```python
del writable_mgc[{'_id': 'my_id'}]
```


```python
list(mgc)
```




    [{'_id': ObjectId('60359a2993b7670664918663')},
     {'_id': ObjectId('60359ac193b7670664918664')},
     {'_id': ObjectId('60359ac193b7670664918665')},
     {'_id': ObjectId('60359ac193b7670664918666')}]



So far, we've seen the base classes. 

So far, you have no reason what-so-ever to use `mongodol`. Might as well use `pymongo` (which it wraps) directly. 

The real reason for using `mongodol` is that it is a gateway to enabling all the `py2store` goodies to create the key-value perspectives that make sense to **you**, without all the backend-dependent boilerplate over the business logic. 

So let's show one example of how to do that.


# The real reason you want to use mongodol (an example)

Let's say we have the collection we just made above, but
- We want to access data by doing `s['60359a2993b7670664918663']` instead of the (annoying) `s[{'_id': ObjectId('60359a2993b7670664918663')}]`
- We'd like our values to to come in the form of actual ready to use data. Namely, we want to automatically ask the cursor for it's first element (assuming it's unique for that key), and we'd like to extract the 'data' field from that result. 
- We'd like to peruse only part of the mongo collection; only if there's a 'kind' field and it's equal to 'example'.

Here's how it can be done:


```python
from bson import ObjectId
from py2store import wrap_kvs
from mongodol import MongoCollectionReaderBase

@wrap_kvs(id_of_key=lambda x: {'_id': ObjectId(x)}, 
          key_of_id=lambda x: str(x['_id']), 
          obj_of_data=lambda doc: next(doc, None)['data'])
class MyStore(MongoCollectionReaderBase):
    """my special store"""
```


```python
s = MyStore(mgc=mgc, 
            key_fields=('_id',), 
            data_fields=('data',), 
            filt={'kind': 'example'})
```


```python
list(s)
```




    ['60359ac193b7670664918664',
     '60359ac193b7670664918665',
     '60359ac193b7670664918666']




```python
s['60359ac193b7670664918664']
```




    2




```python
list(s.values())
```




    [2, [1, 2, 3], {'nested': 'dict'}]

