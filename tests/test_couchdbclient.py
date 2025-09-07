import unittest
from couchdbclient import Client

class TestCouchDBClient(unittest.TestCase):
    def setUp(self):
        # Initialize client with real config
        self.client = Client()
        
    def test_get_doc_by_member_id(self):
        # Arrange
        test_member_id = "182"
        # Act
        result = self.client.get_doc_by_member_id(test_member_id)
        # Assert
        self.assertIsInstance(result, list)
        print(result)

    def test_get_docs_without_member_id(self):
        # Act
        result = self.client.get_docs_without_member_id()
        # Assert
        self.assertIsInstance(result, list)
        print(result)
        
    def test_get_doc_by_email(self):
        # Arrange
        test_email = "koch@sirius-network.de"
        # Act
        result = self.client.get_doc_by_email(test_email)
        # Assert
        self.assertIsInstance(result, list)
        print(result)

    def test_get_docs_without_openproject_key(self):
        # Act
        result = self.client.get_docs_without_openproject_key()
        # Assert
        self.assertIsInstance(result, list)
        print(result)

    def test_get_docs_by_nextcloud_id(self):
        # Arrange
        test_nextcloud_id = "akock"
        # Act
        result = self.client.get_doc_by_nextcloud_id(test_nextcloud_id)
        # Assert
        self.assertIsInstance(result, list)
        
    def test_get_all_docs(self):
        # Act
        result = self.client.get_all_docs()
        # Assert
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()