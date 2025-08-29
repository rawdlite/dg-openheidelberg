import requests
import json
from typing import Optional, List, Dict, Any
from config import Config

CUSTOMFIELD = {
    'email': 'customField7',
    'firstname': 'customField5',
    'lastname': 'customField6',
    'username': 'customField20',
    'agenda': 'customField2',
    'xwiki': 'customField16',
    'git': 'customField17',
    'nextcloud': 'customField10',
    'openproject': 'customField8',
    'public key': 'customField12',
    'telephone': 'customField13',
    'training': 'customField18',
    'altstadt': 'customField15',
    'neuenheim': 'customField14'
}
STATUS = {
    'New': 1,
    'In specification': 2,
    'Specified': 3,
    'Confirmed': 4,
    'To be scheduled': 5,
    'Scheduled': 6,
    'In progress': 7,
    'Developed': 8,
    'In testing': 9,
    'Tested': 10,
    'Test failed': 11,
    'Closed': 12,
    'On hold': 13,
    'Rejected': 14
}


class WorkPackageParser:
    """
    This class is used to parse the workpackage data from the API.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the WorkpackageParser class.

        :param config:
        """
        self.config = config or Config().get('workpackages')
        self.apikey = self.config['apikey']
        self.url = self.config['url']
        self.members = []
        
    def check_member_exists(self,
                subject: str,            
                email: str,
                username: str = '',
                firstname: str = '',
                lastname: str = '') -> Dict[str, Any]|None:
        """
        Check if a user exists in the Openproject App using the API.
        :param email:
        :param username:
        :param firstname:
        :param lastname:
        :return: Dictionary containing the user data or None if the user does not exist
        """
        if not self.members:
            self.members = self.get_members().get('members', [])
        # Check if the user exists in the members list
        for user in self.members:
            if user.get(CUSTOMFIELD['email']) and email:
                if user[CUSTOMFIELD['email']] == email:
                    return user
            if user['subject'].lower() == subject.lower():
                return user
            if user.get(CUSTOMFIELD['username']) and username:
                if user[CUSTOMFIELD['username']].lower() == username.lower():
                    return user
            if user.get(CUSTOMFIELD['firstname']) and user.get(CUSTOMFIELD['lastname']):
                if user[CUSTOMFIELD['firstname']].lower() == firstname.lower() and user[CUSTOMFIELD['lastname']].lower() == lastname.lower():
                    return user
        return None

    def get_workpackages(self, project_id: int|None = None, status_id: int|None = None) -> List[Dict[str, Any]]:
        """
        Get all workpackages from the API for a specific project.
        :param project_id: The ID of the project to fetch workpackages from
        :return: List of workpackage dicts or None if request fails
        """
        if project_id:
            url = f"{self.url}/api/v3/projects/{project_id}/work_packages"
        else:
            url = f"{self.url}/api/v3/work_packages"
        if status_id:
            params = {
                "filters": f'[{{"status":{{"operator":"=","values":["{status_id}"]}}}}]'
            }
            response = requests.get(url, params=params, auth=('apikey', self.apikey))
        else:
            response = requests.get(url, auth=('apikey', self.apikey))
        if response.status_code == 200:
            data = response.json()
            return data.get('_embedded', {}).get('elements', [])
        else:
            print(f"Failed to fetch workpackages. Status code: {response.status_code}")
            return []
        
    def get_members(self) -> Dict[str, Any]:
        """
        Get the workpackages from the API.
        :return:
        """
        url = f"{self.url}/api/v3/projects/18/work_packages"
        all_members = []
        params = {
            'offset': 1,
            'pageSize': 20
        }
        response = requests.get(url, params=params, auth=('apikey', self.apikey))

        if response.status_code != 200:
            return {"members": [], "total": 0, "count": 0}
        response_data = response.json()
        total_members = response_data['total']
        if '_embedded' in response_data and 'elements' in response_data['_embedded']:
            all_members += response_data['_embedded']['elements']

        while response_data['_links'].get('nextByOffset'):
            url = f"{self.url}{response_data['_links']['nextByOffset']['href']}"
            response = requests.get(url, auth=('apikey', self.apikey))
            if response.status_code != 200:
                break
            response_data = response.json()
            if '_embedded' in response_data and 'elements' in response_data['_embedded']:
                all_members += response_data['_embedded']['elements']

            # Check if we've retrieved all users
            if len(all_members) >= total_members:
                break

        # Build the final result
        result = {
            'total': total_members,
            'count': len(all_members),
            'members': all_members
        }
        return result

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
            return {"error": "Failed to create member"}

    def initialize_member_from_doc(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Initialize a member in OpenProject from a document.
        :param doc: The document containing user data
        :return: The created member as a dictionary or None if creation fails
        """
        payload = {
            'subject': doc['_id'],
            CUSTOMFIELD['email']: doc.get('email', ''),
            CUSTOMFIELD['firstname']: doc.get('firstname', '').capitalize(),
            CUSTOMFIELD['lastname']: doc.get('lastname', '').capitalize(),
            CUSTOMFIELD['username']: doc.get('username', ''),
            '_links': {
                'status': {'href': f"/api/v3/statuses/{STATUS['New']}"}
            }
        }
        res = self.create_member(payload)
        if res is not None:
            return res
        return None

    def create_member_from_doc(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a member in OpenProject from a document.
        :param doc: The document containing user data
        :return: The created member as a dictionary or None if creation fails
        """
        payload = {
            'subject': doc['_id'],
            'description': doc.get('description', ''),
            CUSTOMFIELD['email']: doc.get('email', ''),
            CUSTOMFIELD['firstname']: doc.get('firstname', '').capitalize(),
            CUSTOMFIELD['lastname']: doc.get('lastname', '').capitalize(),
            CUSTOMFIELD['username']: doc.get('username', ''),
            CUSTOMFIELD['nextcloud']: doc.get('nextcloud', "") != "",
            CUSTOMFIELD['openproject']: doc.get('openproject', "") != "",
            'lockVersion': 0,
            '_links': {
                'status': {'href': f"/api/v3/statuses/{STATUS['In specification']}"}
            }
        }
        res = self.create_member(payload)
        if res is not None:
            return res
        return None

    def update_member_task(self, doc: Dict[str, Any], member: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Update the member task in OpenProject.
        :param doc: The document containing user data
        :return: The updated member task or None if update fails
        """
        member_task = member or self.get_member(doc['member_id'])
        if member_task is None:
            print(f"Member task with ID {doc['member_id']} not found.")
            return None
        payload = {
            'lockVersion': member_task['lockVersion'],
            "_links": {
  	            "status": { "href": f"/api/v3/statuses/{STATUS['In progress']}" }
            },
            CUSTOMFIELD['nextcloud']: doc.get('nextcloud', "") != "",
            CUSTOMFIELD['openproject']: doc.get('openproject', "") != ""
        }
        if member_task[CUSTOMFIELD['firstname']].capitalize() != member_task[CUSTOMFIELD['firstname']]:
            payload[CUSTOMFIELD['firstname']] = member_task[CUSTOMFIELD['firstname']].capitalize()
        if member_task[CUSTOMFIELD['lastname']].capitalize() != member_task[CUSTOMFIELD['lastname']]:
            payload[CUSTOMFIELD['lastname']] = member_task[CUSTOMFIELD['lastname']].capitalize()
        if member_task['subject'] != doc['_id']:
            payload['subject'] = doc['_id']
        # TODO: set telephone etc if empy in member
        res = self.update_member(member_id=member_task['id'], payload=payload)
        if res is not None:
            return res
        return None

    def update_status(self, task, status: str) -> Dict[str, Any]:
        """
        Update the status of a workpackage.
        :param task: The workpackage to update
        :param status: The new status to set
        :return: The updated workpackage as a dictionary
        """
        payload = {
            'lockVersion': task['lockVersion'],
            "_links": {
  	            "status": { "href": f"/api/v3/statuses/{STATUS[status]}" }
            }    
        }
        result =self.update_member(member_id=task['id'],payload=payload)
        return result

    def add_comment(self, member_id: str, comment: str) -> Dict[str, Any]:
        """
        Add a comment to a workpackage.
        :param member_id: The ID of the workpackage to add the comment to
        :param comment: The comment to add
        :return: The updated workpackage as a dictionary
        """
        url = f"{self.url}/api/v3/work_packages/{member_id}/activities"
        headers = {
            'content-type': 'application/json'
        }
        payload = {
            'comment': {
                'raw': comment
            }
        }
        response = requests.post(
            url=url,
            auth=('apikey', self.apikey),
            data=json.dumps(payload),
            headers=headers
        )
        if response.status_code == 201:
            return response.json()
        else:
            print(f"Failed to add comment. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return {"error": "Failed to add comment"}

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
            return {"error": "Failed to update member"}

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


    # def build_result_dict(self, workpackages: List[Dict[str, Any]], dataset_name='workpackages') -> List[Dict[str, Any]]:
    #     """
    #     Build the result dictionary.
    #     :param workpackages:
    #     :return:
    #     """
    #     result = {'total': workpackages.json()['total'],
    #               'count': workpackages.json()['count'],
    #               dataset_name: []}
    #     for workpackage in workpackages.json()['_embedded']['elements']:
    #         result[dataset_name].append(self.workpackage2dict(workpackage))
    #     return result

    # def get_open_workpackages(self) -> Dict[str, Any]:
    #     """
    #     Get open workpackages from the API.
    #     :return:
    #     """
    #     url = f"{self.url}/api/v3/projects/3/work_packages/"
    #     params = {
    #         "filters": '[{"status":{"operator": "o","values": []}}]'
    #     }
    #     response = requests.get(url, params=params, auth=('apikey', self.apikey))
    #     if response.status_code == 200:
    #         return self.build_result_dict(response)
    #     else:
    #         return {"error": "Failed to retrieve open workpackages"}

    # def workpackage2dict(self, workpackage: Dict[str, Any]) -> Dict[str, Any]:
    #     """
    #     Converts a given work package dictionary into a processed dictionary format. This method takes
    #     a work package represented as a dictionary containing relevant details and returns a transformed
    #     dictionary suited to specific requirements.

    #     :param workpackage: A dictionary containing key-value pairs representing a work package.
    #     :type workpackage: Dict[str, Any]
    #     :return: A processed dictionary derived from the input work package.
    #     :rtype: Dict[str, Any]
    #     """
    #     wpdict = {
    #         'id': workpackage['id'],
    #         'subject': workpackage['subject'],
    #         'description': workpackage['description']['raw'],
    #         'status': workpackage['_links']['status']['title'],
    #         'priority': workpackage['_links']['priority']['title'],
    #     }
    #     return wpdict

class UserParser:

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the UserParser class.

        :param config:
        """
        self.config = config or Config().get('workpackages')
        self.apikey = self.config['apikey']
        self.url = self.config['url']
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
        for user in self.users or []:
            if user['login'] == username or user['email'] == email or user['firstName'] == firstname and user[
                'lastName'] == lastname:
                return user
        return {}

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
            return {}

    def create_new_user(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new user in OpenProject based on the provided task information.
        :param task: Dictionary containing task information.
        :return: Dictionary containing the created user data or None if creation failed.
        """
        user_data = {
            'firstName': task.get(CUSTOMFIELD['firstname'], ''),
            'lastName': task.get(CUSTOMFIELD['lastname'], ''),
            'login': task.get(CUSTOMFIELD['username'], ''),
            'email': task.get(CUSTOMFIELD['email'], ''),
            'status': 'invited'  # Assuming status is invited for new users
        }
        return self.create_user(user_data)

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
            return {}


    def get_users(self) -> Dict[str, Any]:
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
            return {'users': [], 'total': 0, 'count': 0}
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
        return result

    # def build_user_dict(self, response: Dict[str, Any]) -> Dict[str, Any]:
    #     """
    #     Build a Dictionary of users from API response.
    #     :param response:
    #     :return: Dict[str, Any]
    #     """
    #     result = {'total': response.json()['total'],
    #               'count': response.json()['count'],
    #               'users': []}
    #     for user in response.json()['_embedded']['elements']:
    #         result['users'].append(self.user2dict(user))
    #     return result

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

    def user_info(self, user: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get user data from the API response.
        :param user:
        :return:
        """
        return {
            'openproject_id': user.get('id', ''),
            'openproject_login': user.get('login', ''),
            'openproject_firstname': user.get('firstName', ''),
            'openproject_lastname': user.get('lastName', ''),
            'openproject_email': user.get('email', ''),
            'openproject_status': user.get('status', ''),
            'openproject_admin': user.get('admin', ''),
            'openproject_created_at': user.get('createdAt', ''),
            'openproject_updated_at': user.get('updatedAt', ''),
            'openproject_language': user.get('language', ''),
            'openproject_matrix': user.get('customField3', '')
            }

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