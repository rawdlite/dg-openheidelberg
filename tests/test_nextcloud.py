import unittest
from nextcloud import NextcloudClient

class TestDagsterAssets(unittest.TestCase):
    def setUp(self):
        # Initialize client with real config
        self.client = NextcloudClient()
        
    def test_get_users(self):
        # Act
        users = self.client.get_users()
        self.assertIsNotNone(users)
        self.assertIsInstance(users, list)

    def test_create_user(self):
        # Act
        user = self.client.create_user(
            email="rawdlite@gmail.com",
            username="testuser",
            firstname="Test",
            lastname="User"
        )
        self.assertIsNotNone(user)
        self.assertIn('id', user)
        self.assertIn('email', user)
        self.assertIn('displayname', user)
        self.assertIn('enabled', user)
