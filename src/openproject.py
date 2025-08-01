import requests
from typing import Optional, List, Dict, Any
from config import Config


class WorkPackageParser:
    """
    This class is used to parse the workpackage data from the API.
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        """
        Initialize the WorkpackageParser class.

        :param config:
        """
        if config is None:
            config = Config().get('workpackages')
        self.config = config
        self.apikey = config['apikey']
        self.url = config['url']
        self.users = []

    def get_members(self) -> List[Dict[str, Any]]:
        """
        Get the workpackages from the API.
        :return:
        """
        url = f"{self.url}/api/v3/projects/18/work_packages"
        response = requests.get(url, auth=('apikey', self.apikey))
        if response.status_code == 200:
            return self.build_result_dict(response, dataset_name='members')
        else:
            return None

    def check_user(self,
                   email: str,
                   username: str,
                   firstname: str,
                   lastname: str) -> Dict[str, Any]:
        """
        Check if a user exists in the Openproject App using the API.
        :param email:
        :param username:
        :param firstname:
        :param lastname:
        :return: Dictionary containing the user data or None if the user does not exist
        """
        if not self.users:
            res = self.get_users()
            self.users = res.get('users')
        for user in self.users:
            if user['login'] == username or user['email'] == email or user['firstName'] == firstname and user['lastName'] == lastname:
                return user
        return None

    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from the API using pagination.
        Continues fetching pages until all users are retrieved (total == count).
        Handles potential duplicate users by tracking user IDs.
        :return: Dictionary containing all users
        """
        url = f"{self.url}/api/v3/users" # Adjust this value based on API limits
        all_users = []
        params = {
            'offset': 1,
            'pageSize': 20
        }
        response = requests.get(url, params=params, auth=('apikey', self.apikey))

        if response.status_code != 200:
            return None
        response_data = response.json()
        total_users = response_data['total']
        if '_embedded' in response_data and 'elements' in response_data['_embedded']:
            all_users += response_data['_embedded']['elements']

        while response_data['_links'].get('nextByOffset'):
            url = f"{self.url}{response_data['_links']['nextByOffset']['href']}"
            response = requests.get(url, auth=('apikey', self.apikey))
            if response.status_code != 200:
                break
            response_data = response.json()
            if '_embedded' in response_data and 'elements' in response_data['_embedded']:
                all_users += response_data['_embedded']['elements']

            # Check if we've retrieved all users
            if len(all_users) >= total_users:
                break

        # Build the final result
        result = {
            'total': total_users,
            'count': len(all_users),
            'users': all_users
        }
        
        # for user in all_users:
        #     result['users'].append(self.user2dict(user))
            
        return result

    def build_user_dict(self, response: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build a Dictionary of users from API response.
        :param response:
        :return: Dict[str, Any]
        """
        result = {'total': response.json()['total'],
                 'count': response.json()['count'],
                  'users': []}
        for user in response.json()['_embedded']['elements']:
            result['users'].append(self.user2dict(user))
        return result

    def user2dict(self,user: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a user to a dictionary.
        :param user:
        :return:
        """
        return {'id': user['id'],
                'name': user['name'],
                'login': user['login'],
                'email': user['email'],
                'status': user['status']}

    def build_result_dict(self, workpackages: List[Dict[str, Any]], dataset_name='workpackages') -> Dict[str, Any]:
        """
        Build the result dictionary.
        :param workpackages:
        :return:
        """
        result = {'total': workpackages.json()['total'],
                  'count': workpackages.json()['count'],
                  dataset_name: []}
        for workpackage in workpackages.json()['_embedded']['elements']:
            result[dataset_name].append(self.workpackage2dict(workpackage))
        return result

    def get_workpackages(self) -> Dict[str, Any]:
        """
        Get open workpackages from the API.
        :return:
        """
        url = f"{self.url}/api/v3/projects/3/work_packages/"
        params = {
            "filters": '[{"status":{"operator": "o","values": []}}]'
        }
        response = requests.get(url, params=params, auth=('apikey', self.apikey))
        if response.status_code == 200:
            return self.build_result_dict(response)
        else:
            return None

    def workpackage2dict(self, workpackage: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts a given work package dictionary into a processed dictionary format. This method takes
        a work package represented as a dictionary containing relevant details and returns a transformed
        dictionary suited to specific requirements.

        :param workpackage: A dictionary containing key-value pairs representing a work package.
        :type workpackage: Dict[str, Any]
        :return: A processed dictionary derived from the input work package.
        :rtype: Dict[str, Any]
        """
        wpdict = {
            'id': workpackage['id'],
            'subject': workpackage['subject'],
            'description': workpackage['description']['raw'],
            'status': workpackage['_links']['status']['title'],
            'priority': workpackage['_links']['priority']['title'],
        }
        return wpdict

     