from mongodol.util import KeyNotUniqueError

# TODO: Make trans funcs/method carry their role and find their place in wrap_kvs automatically
class PostGet:
    @staticmethod
    def single_value_fetch_with_unicity_validation(k, cursor):
        doc = next(cursor, None)
        if doc is not None:
            if next(cursor, None) is not None:  # TODO: Fetches! Faster way to check if there's more than one hit?
                raise KeyNotUniqueError.raise_error(k)
            return doc
        else:
            raise KeyError(f"No document found for query: {k}")

    @staticmethod
    def single_value_fetch_without_unicity_validation(k, cursor):
        doc = next(cursor, None)
        if doc is not None:
            return doc
        else:
            raise KeyError(f"No document found for query: {k}")


class ObjOfData:
    @staticmethod
    def all_docs_fetch(cursor, doc_collector=list):
        return doc_collector(cursor)


