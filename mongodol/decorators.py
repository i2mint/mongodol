from functools import wraps
from typing import Iterable, Optional, TypedDict
from pymongo.results import BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

WriteOpResult = TypedDict(
    'WriteOpResult', ok=bool, n=int, ids=Optional[Iterable[str]]
)


def normalize_result(func):
    """Decorator to transform a pymongo result object to a WriteOpResult object.

    :param func: [description]
    :type func: [type]
    """

    @wraps(func)
    def result_mapper(*args, **kwargs):
        raw_result = func(*args, **kwargs)
        result: WriteOpResult = {'n': 0}
        if isinstance(raw_result, InsertOneResult) and raw_result.inserted_id:
            result['n'] = 1
            result['ids'] = [str(raw_result.inserted_id)]
        elif (
                isinstance(raw_result, InsertManyResult)
                and raw_result.inserted_ids
        ):
            result['n'] = len(raw_result.inserted_ids)
            result['ids'] = raw_result.inserted_ids
        elif isinstance(raw_result, (DeleteResult, UpdateResult)):
            result['n'] = raw_result.raw_result['n']
        elif isinstance(raw_result, BulkWriteResult):
            result['n'] = (
                    raw_result.inserted_count
                    + raw_result.upserted_count
                    + raw_result.modified_count
                    + raw_result.deleted_count
            )
        else:
            raise NotImplementedError(
                f'Interpretation of result type {type(raw_result)} is not implemented.'
            )
        result['ok'] = result['n'] > 0
        return result

    return result_mapper
