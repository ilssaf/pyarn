import requests


class BaseYarnApi:
    """
    Base class for Yarn API.
    """

    def __init__(self, cluster_url: str):
        """
        Initialize the base Yarn API.

        :param cluster_url: The URL of the Yarn cluster.
        """
        self._cluster_url = cluster_url
        self.__headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._endpoint = None

    def send_request(self, endpoint: str, method: str = "GET", data: dict = None):
        """
        Send a request to the Yarn API.

        :param endpoint: The API endpoint.
        :param method: The HTTP method (GET, POST, etc.).
        :param data: The data to send with the request.
        :return: The response from the API.
        """

        url = f"{self._endpoint}{endpoint}"
        response = requests.request(
            method,
            url,
            headers=self.__headers,
            json=data,
        )
        response.raise_for_status()
        try:
            response = response.json()
        except requests.exceptions.JSONDecodeError:
            response = response.text
        return response
