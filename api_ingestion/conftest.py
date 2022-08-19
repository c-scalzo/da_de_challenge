import pytest
import gcsfs
from src.data_api import ApiClient


@pytest.fixture
def get_filesystem() -> gcsfs.GCSFileSystem:
    return gcsfs.GCSFileSystem()

@pytest.fixture
def test_data() -> dict:
    return [{"one": 1, "two": "A"}, {"one": 2, "two": "B"}]
