import unittest
from couchdbclient import Client

class TestCouchDBClient(unittest.TestCase):
    def setUp(self):
        # Initialize client with real config
        self.client = Client()
        
    def test_get_docs_without_openproject_key(self):
        # Act
        result = self.client.get_docs_without_openproject_key()
        
        # Assert
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()