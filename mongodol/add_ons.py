"""Add-ons
https://github.com/i2mint/mongodol/issues/3
"""
from abc import ABC
from inspect import signature, Parameter
from typing import Callable

from dol.util import has_enabled_clear_method
from dol.trans import add_store_method
from mongodol.base import MongoCollectionCollection
from mongodol.errors import MethodNameAlreadyExists, SetattrNotAllowed


# Util functions for making method validations ########################################################################


def disallow_if_name_exists_already(store, method_name):
    if hasattr(store, method_name):
        raise MethodNameAlreadyExists(f'Method name already exists: {method_name}')


def number_of_non_defaulted_arguments(func):
    """Return the number of arguments that don't have defaults in it's signature"""
    return sum(
        [1 for p in signature(func).parameters.values() if p.default is Parameter.empty]
    )


def has_exactly_one_non_defaulted_input(func):
    """Return True iff function has exactly one argument without defaults"""
    return number_of_non_defaulted_arguments(func) == 1


#######################################################################################################################


class Addons(ABC):
    """A collection of add-on methods. Addons can't (and is not meant to) be instantiated.
    It's just to group add-on functions (meant to be injected in stores) in one place"""

    def clear(self: MongoCollectionCollection):
        return self.mgc.delete_many(self.filter)

    def clear_after_checking_with_user(self: MongoCollectionCollection):
        n = len(self)
        answer = input(
            f'Are you sure you want to delete all {n} docs matching the filter: {self.filter}?\n'
            "To confirm, type the number of docs you're deleting: "
        )
        try:
            number = int(answer)
            if number == n:
                return self.mgc.delete_many(self.filter)
            else:
                print(
                    f"You typed {number}, but {n} is the correct number, so I won't delete anything"
                )
        except:
            print(f'Okay, I will NOT delete anything')

    dflt_clear_method = clear


# InjectionValidator
def _clear_method_injection_validator(store: type, method_func: Callable) -> bool:
    if has_enabled_clear_method(store):
        raise SetattrNotAllowed(
            f"An ENABLED clear method exist's already, so won't set it as {method_func}"
        )
    else:
        return has_exactly_one_non_defaulted_input(method_func)


def add_clear_method(
    store,
    *,
    clear_method=Addons.dflt_clear_method,
    validator=_clear_method_injection_validator,  # because should be .clear(self)
):
    """Add a clear method to a store that doesn't have one

    :param store:
    :param clear_method:
    :return:

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
    return add_store_method(
        store, method_func=clear_method, method_name='clear', validator=validator
    )
