import pytest

from mongodol.tests.util import init_db


@pytest.fixture(autouse=True)
def configure_test(pytestconfig):
    init_db()
