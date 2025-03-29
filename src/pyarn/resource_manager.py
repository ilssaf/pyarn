from pyarn import BaseYarnApi
from typing import List, Dict, Any


class ResourceManagerApi(BaseYarnApi):
    """
    ResourceManager API for Yarn.
    """

    def __init__(self, cluster_url: str):
        """
        Initialize the ResourceManager API.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
        :param cluster_url: The URL of the Yarn cluster.
        """
        super().__init__(cluster_url)
        self._endpoint = f"{self._cluster_url}/ws/v1/cluster"

    def get_apps(self, **query_params) -> List[Dict[str, Any]]:
        """
        Get the list of applications from the ResourceManager.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API

        Available query parameters:
        - states - applications matching the given application states, specified as a comma-separated list.
        - finalStatus - the final status of the application - reported by the application itself
        - user - user name
        - queue - unfinished applications that are currently in this queue
        - limit - total number of app objects to be returned
        - startedTimeBegin - applications with start time beginning with this time, specified in ms since epoch
        - startedTimeEnd - applications with start time ending with this time, specified in ms since epoch
        - finishedTimeBegin - applications with finish time beginning with this time, specified in ms since epoch
        - finishedTimeEnd - applications with finish time ending with this time, specified in ms since epoch
        - applicationTypes - applications matching the given application types, specified as a comma-separated list.
        - applicationTags - applications matching any of the given application tags, specified as a comma-separated list.
        - name - name of the application
        - deSelects - a generic fields which will be skipped in the result.

        :param query_params: Optional query parameters for filtering applications.
        :return: A list of applications.
        """

        if query_params:
            query_string = "&".join(
                f"{key}={value}" for key, value in query_params.items()
            )
            endpoint = f"/apps?{query_string}"
        else:
            endpoint = "/apps"
        response = self.send_request(endpoint)
        return response.get("apps", {}).get("app", [])

    def get_app(self, applcation_id: str) -> Dict[str, Any]:
        """
        Get details of a specific application.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_API

        :param applcation_id: The ID of the application.
        :return: Job details.
        """
        response = self.send_request(f"/apps/{applcation_id}")
        return response.get("app", {})

    def create_app(self) -> Dict[str, Any]:
        """
        Create a new application.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
        #Cluster_Application_Creation_API
        :return: Application creation response.
        """
        response = self.send_request("/apps/new-application", method="POST")
        return response

    def sumbit_app(self, app_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit a new application.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
        #Cluster_Application_Submission_API
        :param app_request: The application request data.
        :return: Application submission response.
        """
        response = self.send_request("/apps", method="POST", data=app_request)
        return response

    def kill_app(self, application_id: str) -> Dict[str, Any]:
        """
        Kill a running application.
        Documentation: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_State_API
        :param application_id: The ID of the application to kill.
        :return: Application kill response.
        """
        response = self.send_request(
            f"/apps/{application_id}/state", method="PUT", data={"state": "KILLED"}
        )
        return response
