""" Example solution for data API ingestion to BigQuery

Creator: stephen.wallace@slalom.com
"""

import requests
import json


class ApiClient:
    """Contains methods to help get and clean data from API """

    def __init__(self, url: str) -> None:
        self.url = url

    @staticmethod
    def _request_get(url: str) -> requests.models.Response:
        """Make an API call and return the response
        Args:
            url - the url of the endpoint
            [future] - add auth options

        Returns:
            the response object
        """
        response = requests.get(url)

        return response

    @staticmethod
    def transform_response(response: requests.models.Response) -> dict:
        """Transforms response data into json
        
        Args:
            response - a response object from a request

        Returns:
            A dictionary with the data from the response
        """
        response_dict = json.loads(response.content.decode())
        try:
            data = response_dict["data"]
            return data

        except KeyError:
            raise KeyError(
                'Response does not contain "data" key. Is this a successful response?'
            )


    def get_data(self, url: str = None) -> dict:
        """ Runs all steps """
        if url is None:
            url = self.url

        response = self._request_get(url)

        data = self.transform_response(response=response)

        return data
