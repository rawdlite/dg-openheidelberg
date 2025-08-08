import unittest
from couchdbclient import Client

class TestDagsterAssets(unittest.TestCase):
    def setUp(self):
        # Initialize client with real config
        self.client = Client()
        
    def test_initialize_user(self):
        # Act
        from src.dg_openheidelberg.defs.assets import user_initialisation
        payload = user_initialisation()  # This will call the function to initialize users
        self.assertIsNotNone(payload)
        
    def test_migation_couchdb(self):
        # Act
        from src.dg_openheidelberg.defs.assets import migration_couchdb
        res = migration_couchdb()
        self.assertIsNotNone(res)
        
    def test_user_openproject_data(self):
        # Act
        from src.dg_openheidelberg.defs.assets import user_openproject_data
        res = user_openproject_data()
        self.assertIsNotNone(res)
        
    def test_user_nextcloud_data(self):
        # Act
        from src.dg_openheidelberg.defs.assets import user_nextcloud_data
        res = user_nextcloud_data()
        self.assertIsNotNone(res)
        
    def test_create_openproject_member_tasks(self):
        # Act
        from src.dg_openheidelberg.defs.assets import create_openproject_member_tasks
        res = create_openproject_member_tasks()
        self.assertIsNotNone(res)

    def test_create_user_accounts(self):
        # Act
        from src.dg_openheidelberg.defs.assets import create_user_accounts
        res = create_user_accounts()
        self.assertIsNotNone(res)

if __name__ == "__main__":
    unittest.main()