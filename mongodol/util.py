



class KeyNotUniqueError(RuntimeError):
    """Raised when a key was expected to be unique, but wasn't (i.e. cursor has more than one match)"""

    @staticmethod
    def raise_error(self, k):
        raise KeyNotUniqueError(f"Key was not unique (i.e. cursor has more than one match): {k}")
