"""Add-ons
https://github.com/i2mint/mongodol/issues/3
"""
from inspect import signature, Parameter
from functools import partial
from typing import Callable, Optional

from py2store.trans import store_decorator
from py2store.util import has_enabled_clear_method
from mongodol.base import MongoCollectionCollection
from mongodol.errors import MethodNameAlreadyExists, SetattrNotAllowed


# Util functions for making method validations ########################################################################

def disallow_if_name_exists_already(store, method_name):
    if hasattr(store, method_name):
        raise MethodNameAlreadyExists(f"Method name already exists: {method_name}")


def number_of_non_defaulted_arguments(func):
    """Return the number of arguments that don't have defaults in it's signature"""
    return sum([1 for p in signature(func).parameters.values() if p.default is Parameter.empty])


def has_exactly_one_non_defaulted_input(func):
    """Return True iff function has exactly one argument without defaults"""
    return number_of_non_defaulted_arguments(func) == 1


#######################################################################################################################


class Addons:

    def clear(self: MongoCollectionCollection):
        return self.mgc.delete_many(self.filter)

    def clear_after_checking_with_user(self: MongoCollectionCollection):
        n = len(self)
        answer = input(
            f"Are you sure you want to delete all {n} docs matching the filter: {self.filter}?\n"
            "To confirm, type the number of docs you're deleting: "
        )
        try:
            number = int(answer)
            if number == n:
                return self.mgc.delete_many(self.filter)
            else:
                print(f"You typed {number}, but {n} is the correct number, so I won't delete anything")
        except:
            print(f"Okay, I will NOT delete anything")

    dflt_clear_method = clear


InjectionValidator = Callable[[type, Callable], bool]


@store_decorator
def add_method(
        store: type,
        *,
        method_func,
        method_name=None,
        validator: Optional[InjectionValidator] = None
):
    """Add methods to store classes or instances

    :param store: A store type or instance
    :param method_func: The function of the method to be added
    :param method_name: The name of the store attribute this function should be written to
    :param validator: An optional validator. If not None, ``validator(store, method_func)`` will be called.
        If it doesn't return True, a ``SetattrNotAllowed`` will be raised.
        Note that ``validator`` can also raise its own exception.
    :return: A store with the added (or modified) method
    """
    method_name = method_name or method_func.__name__
    if validator is not None:
        if not validator(store, method_func):
            raise SetattrNotAllowed(f"Method is not allowed to be set (according to {validator}): {method_func}")
    setattr(store, method_name, method_func)
    return store


def _clear_method_injection_validator(store, method_func):
    if has_enabled_clear_method(store):
        raise SetattrNotAllowed(f"An ENABLED clear method exist's already, so won't set it as {method_func}")
    else:
        return has_exactly_one_non_defaulted_input(method_func)


def add_clear_method(
        store,
        *,
        clear_method=Addons.dflt_clear_method,
        validator=_clear_method_injection_validator  # because should be .clear(self)
):
    """Add a clear method to a store that doesn't have one

    :param store:
    :param clear_method:
    :return:

    >>> from py2store.util import has_enabled_clear_method
    >>> from mongodol.base import MongoCollectionPersister
    >>> from mongodol.tests import data, populated_pymongo_collection
    >>>
    >>> whole_store = MongoCollectionPersister(populated_pymongo_collection(data.feature_cube))
    >>> whole_length_before_clear = len(whole_store)
    >>> assert whole_length_before_clear == 7
    >>> reds = MongoCollectionPersister(whole_store.mgc, filter={'color': 'red'})
    >>> n_reds = len(reds)
    >>> assert n_reds == 4

    ``reds`` doesn't have a clear method

    >>> assert not has_enabled_clear_method(reds)

    So let's give it one

    >>> reds_with_clear = add_clear_method(reds)
    >>> assert has_enabled_clear_method(reds_with_clear)

    And it's one that works too!

    >>> r = reds_with_clear.clear()
    >>> assert len(reds_with_clear) == 0

    It's the data that was deleted, not just the view. See what reds and whole_store say:

    >>> assert len(reds) == 0
    >>> assert len(whole_store) == whole_length_before_clear - n_reds == 3

    """
    """Add a clear method to the store"""
    return add_method(store, method_func=clear_method, method_name='clear', validator=validator)
