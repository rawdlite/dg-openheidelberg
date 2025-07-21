import json
from typing import Optional, List, Dict, Any
from nc_py_api import Nextcloud
from config import Config

class NextcloudClient:
    """
    Nextcloud client to interact with the Nextcloud API
    """

    def __init__(self) -> None:
        self.config = Config().get('nextcloud')
        self.nc = Nextcloud(nextcloud_url=self.config['url'], nc_auth_user=self.config['username'], nc_auth_pass=self.config['password'])
        self.users = []


    def show_capabilities(self):
        pretty_capabilities = json.dumps(self.nc.capabilities, indent=4, sort_keys=True)
        print(pretty_capabilities)

    def get_users(self):
        user_ids = self.nc.users.get_list()
        all_users = []
        for user_id in user_ids:
            user = self.nc.users.get_user(user_id)
            user_dict = {
                'id': user.user_id,
                'email': user.email,
                'displayname': user.display_name,
                'enabled': user.enabled,
                'last_login': user.last_login.strftime('%Y-%m-%d')
            }
            all_users.append(user_dict)
        return all_users

    def check_user(self,
                   email: str,
                   username: str,
                   firstname: str,
                   lastname: str) -> Dict[str, Any]:
        if not self.users:
            self.users = self.get_users()
        for user in self.users:
            if username == user['id'] or email == user['email'] or f"{firstname} {lastname}" == user['displayname']:
                return user
        return None


    def upload_file(self, remote_path, file_path):
        self.nc.files.upload_stream(path=remote_path, fp=file_path,)

    def download_file(self, remote_file, local_path):
        """Download a file from Nextcloud to local path.
        
        Args:
            remote_file: Path to file on Nextcloud
            local_path: Local path where file will be saved
        """
        with open(local_path, 'wb') as f:
            self.nc.files.download2stream(remote_file, f)
