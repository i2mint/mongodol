"""pymongo 3 -> 4 differences that mongodol's own code has to absorb (i2mint/mongodol#16).

None of these needs a MongoDB server: they use a never-connected client and a
recording stand-in for the collection, so they run in CI and without Mongo alike.
"""

import uuid

from bson.binary import UuidRepresentation
from pymongo import MongoClient

from mongodol.base import MongoCollectionReader, _same_bson_value
from mongodol.constants import ID
from mongodol.util import mk_client, normalize_projection


class _RecordingCollection:
    """Answers ``find`` with an empty cursor and remembers the projection it got."""

    def __init__(self):
        self.projections = []

    def find(self, *args, **kwargs):
        self.projections.append(kwargs.get("projection"))
        return iter(())


def _reader(**kwargs):
    unconnected = MongoClient("mongodb://localhost:1", connect=False)["db"]["c"]
    reader = MongoCollectionReader(unconnected, **kwargs)
    reader.mgc = _RecordingCollection()
    return reader


def test_an_existence_check_asks_for_the_id_only_not_for_whole_documents():
    # pymongo 3 sent the empty projection as "_id only"; pymongo 4 sends none at all.
    reader = _reader()
    assert ({ID: "x"} in reader) is False
    assert reader.mgc.projections == [{ID: True}]


def test_an_empty_projection_means_the_id_only_as_it_did_on_pymongo_3():
    assert normalize_projection(()) == {ID: True}
    assert normalize_projection([]) == {ID: True}
    reader = _reader(iter_projection=())
    assert reader._iter_projection == {ID: True}


def test_a_uuid_in_a_scope_can_be_compared():
    # Under pymongo 4's default codec, bson.encode refuses a native uuid.UUID.
    u = uuid.uuid4()
    assert _same_bson_value(u, u)
    assert not _same_bson_value(u, uuid.uuid4())


def test_clients_mongodol_makes_keep_pymongo_3s_uuid_layout():
    legacy = UuidRepresentation.PYTHON_LEGACY
    assert mk_client(connect=False).codec_options.uuid_representation == legacy
    standard = mk_client(connect=False, uuidRepresentation="standard")
    assert standard.codec_options.uuid_representation == UuidRepresentation.STANDARD
    uri = mk_client("mongodb://localhost:1/?uuidRepresentation=standard", connect=False)
    assert uri.codec_options.uuid_representation == UuidRepresentation.STANDARD
