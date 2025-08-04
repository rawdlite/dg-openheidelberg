import requests
import json
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
        self.members = []
        
    def check_member_exists(self,
                subject: str,            
                email: str,
                username: str = '',
                firstname: str = '',
                lastname: str = '') -> Dict[str, Any]:
        """
        Check if a user exists in the Openproject App using the API.
        :param email:
        :param username:
        :param firstname:
        :param lastname:
        :return: Dictionary containing the user data or None if the user does not exist
        """
        if not self.members:
            url = f"{self.url}/api/v3/projects/18/work_packages"
            response = requests.get(url, auth=('apikey', self.apikey))
            if response.status_code == 200:
                self.members = response.json().get('_embedded', {}).get('elements', [])
        for user in self.members:
            if user['subject'] == subject \
                or user['customField20'] == username \
                or user['customField7'] == email \
                or (user['customField5'] == firstname and user['customField6'] == lastname):
                return user
        return None

    def get_project_18_form_info(self) -> Optional[Dict[str, Any]]:
        """
        Get form information for project 18 from OpenProject via REST API.

        :return: Dictionary containing form information or None if request fails
        """
        url = f"{self.url}/api/v3/projects/18/form"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.post(url, auth=('apikey', self.apikey), headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch form information for project 18. Status code: {response.status_code}")
            return None

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

    def get_member(self, member_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific member from OpenProject by ID.

        :param member_id: The ID of the member to retrieve
        :return: Dictionary containing member information or None if request fails
        """
        url = f"{self.url}/api/v3/work_packages/{member_id}"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.get(url, auth=('apikey', self.apikey), headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch member {member_id}. Status code: {response.status_code}")
            return None

    def get_lockVersion(self, workpackage_id):
        """
        get the lock version of a workpackage
        :param workpackage_id: The ID of the workpackage to get lock version
        :return: lock version or None if request fails
        """
        wp_info = self.get_member(workpackage_id)
        if wp_info is not None:
            return wp_info['lockVersion']
        else:
            return None

    def create_member(self, payload) -> Dict[str, Any]:
        """
        Create a new workpackage with the given member details
        :param payload:
        :return: The created member as a dictionary
        """
        url = f"{self.url}/api/v3/projects/18/work_packages"
        headers = {
            'content-type': 'application/json'
        }
        response = requests.post(
            url=url,
            auth=('apikey', self.apikey),
            data=json.dumps(payload),
            headers=headers
        )
        if response.status_code == 201: # Created
            return response.json()
        else:
            return None

    def update_member(self, member_id: str, payload) -> Dict[str, Any]:
        """
        update workpackage entry with given member details
        :param member_id:
        :param payload:
        """
        url = f"{self.url}/api/v3/work_packages/{member_id}"
        headers = {
            'content-type': 'application/json'
        }
        response = requests.patch(
            url=url,
            auth=('apikey', self.apikey),
            data=json.dumps(payload),
            headers=headers
        )
        print(f"Update response status code: {response.status_code}")
        print(f"Update response content: {response.text}")
        
        if response.status_code in [200, 204]:  # Success - could be 200 or 204
            if response.status_code == 200 and response.text:
                return response.json()
            else:
                return {'success': True, 'status_code': response.status_code}
        else:   # Error occurred
            return None

    def delete_member(self, member_id: str) -> bool:
        """
        delete workpackage entry with given member id
        :param member_id:
        """
        url = f"{self.url}/api/v3/work_packages/{member_id}"
        headers = {
            'content-type': 'application/json'
        }
        response = requests.delete(url=url,
                                   auth=('apikey', self.apikey),
                                   headers=headers)
        if response.status_code == 204:
            return True
        else:
            return False


    def build_result_dict(self, workpackages: List[Dict[str, Any]], dataset_name='workpackages') -> List[Dict[str, Any]]:
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

class UserParser:

    def __init__(self, config: Optional[dict] = None) -> None:
        """
        Initialize the UserParser class.

        :param config:
        """
        if config is None:
            config = Config().get('workpackages')
        self.config = config
        self.apikey = config['apikey']
        self.url = config['url']
        self.users = []

    def check_user(self,
                   email: str,
                   username: str = '',
                   firstname: str = '',
                   lastname: str = '') -> Dict[str, Any]:
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
            if user['login'] == username or user['email'] == email or user['firstName'] == firstname and user[
                'lastName'] == lastname:
                return user
        return None

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Get a user by their ID.
        :param user_id: ID of the user to retrieve.
        :return: Dictionary containing the user data or None if the user does not exist.
        """
        url = f"{self.url}/api/v3/users/{user_id}"
        response = requests.get(url,
                                auth=('apikey', self.apikey))
        if response.status_code == 200:
            return response.json()
        else:
            return None

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new user with the provided payload.
        :param payload: Dictionary containing user data to create the user.
        :return: Dictionary containing the created user or None if creation failed.
        """
        url = f"{self.url}/api/v3/users"
        headers = {
            'content-type': 'application/json'
        }
        response = requests.post(
            url=url,
            auth=('apikey', self.apikey),
            data=json.dumps(payload),
            headers=headers)
        if response.status_code == 201: # Created
            return response.json()
        else:
            return None


    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from the API using pagination.
        Continues fetching pages until all users are retrieved (total == count).
        Handles potential duplicate users by tracking user IDs.
        :return: Dictionary containing all users
        """
        url = f"{self.url}/api/v3/users"  # Adjust this value based on API limits
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

    def user2dict(self, user: Dict[str, Any]) -> Dict[str, Any]:
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


    def add_user_to_group(self, user_id: int, group_id: int) -> bool:
        """
        Add a user to a specific group in OpenProject.

        :param user_id: The ID of the user to add to the group
        :param group_id: The ID of the group to add the user to
        :return: True if successful, False otherwise
        """
        url = f"{self.url}/api/v3/groups/{group_id}/users"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        payload = {
            'userId': user_id
        }

        try:
            response = requests.post(url, json=payload, auth=('apikey', self.apikey), headers=headers)
            return response.status_code == 201
        except requests.exceptions.RequestException:
            return False

    def set_membership(self, payload):
        """
        Set membership for a user in OpenProject.
        :param payload: The payload containing the user and group information
        """
        url = f"{self.url}/api/v3/memberships"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        try:
            response = requests.post(url, json=payload, auth=('apikey', self.apikey), headers=headers)
            return response.status_code == 201
        except requests.exceptions.RequestException:
            return False