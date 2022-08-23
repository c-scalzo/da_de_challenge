
import pytest
from src.data_api import ApiClient

URL = "https://dog.ceo/api/breeds/image/random"


class MockResponse:
    def __init__(self, content='{"no": 1}'.encode()):
        self.content = content

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, data):
        self._content = data


class TestApiClient:

    client = ApiClient(url=URL)

    def test_transform_response_raise_error(self):
        bad_response = MockResponse()
        with pytest.raises(KeyError):
            self.client.transform_response(bad_response)
