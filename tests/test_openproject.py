from unittest import TestCase
from unittest.mock import patch, MagicMock
from src.openproject import WorkPackageParser


class TestWorkPackageParser(TestCase):

    def test_get_member_api(self):
        wp = WorkPackageParser()
        members = wp.get_members()
        print(members)
        self.assertIsNotNone(members)
        self.assertIsInstance(members, dict)
        self.assertIn('total', members)

    def test_get_users(self):
        wp = WorkPackageParser()
        users = wp.get_users()
        print(users)
        self.assertIsNotNone(users)
        self.assertIn('total', users)
        
    @patch("src.openproject.requests.get")
    def test_get_users_pagination(self, mock_get):
        # Create mock responses for pagination
        # First page response
        first_response = MagicMock()
        first_response.status_code = 200
        first_response.json.return_value = {
            'total': 3,  # Total of 3 users
            'count': 2,  # But only 2 per page
            '_embedded': {
                'elements': [
                    {
                        'id': 1,
                        'name': 'User 1',
                        'login': 'user1',
                        'email': 'user1@example.com',
                        'status': 'active'
                    },
                    {
                        'id': 2,
                        'name': 'User 2',
                        'login': 'user2',
                        'email': 'user2@example.com',
                        'status': 'active'
                    }
                ]
            }
        }
        
        # Second page response
        second_response = MagicMock()
        second_response.status_code = 200
        second_response.json.return_value = {
            'total': 3,
            'count': 1,  # Only 1 user on second page
            '_embedded': {
                'elements': [
                    {
                        'id': 3,
                        'name': 'User 3',
                        'login': 'user3',
                        'email': 'user3@example.com',
                        'status': 'active'
                    }
                ]
            }
        }
        
        # Configure mock to return different responses for different calls
        mock_get.side_effect = [first_response, second_response]
        
        # Create parser instance with test config
        config = {
            'apikey': 'test-api-key',
            'url': 'https://test.openproject.com'
        }
        parser = WorkPackageParser(config)
        
        # Call the method
        result = parser.get_users()
        
        # Verify the API calls were made correctly
        self.assertEqual(mock_get.call_count, 2)  # Should make 2 API calls
        
        # Check first call (page 1)
        first_call_args = mock_get.call_args_list[0]
        self.assertEqual(first_call_args[0][0], 'https://test.openproject.com/api/v3/users')
        self.assertEqual(first_call_args[1]['params'], {'page': 1, 'per_page': 50})
        self.assertEqual(first_call_args[1]['auth'], ('apikey', 'test-api-key'))
        
        # Check second call (page 2)
        second_call_args = mock_get.call_args_list[1]
        self.assertEqual(second_call_args[0][0], 'https://test.openproject.com/api/v3/users')
        self.assertEqual(second_call_args[1]['params'], {'page': 2, 'per_page': 50})
        self.assertEqual(second_call_args[1]['auth'], ('apikey', 'test-api-key'))
        
        # Verify the result structure
        expected_result = {
            'total': 3,
            'count': 3,
            'users': [
                {
                    'id': 1,
                    'name': 'User 1',
                    'login': 'user1',
                    'email': 'user1@example.com',
                    'status': 'active'
                },
                {
                    'id': 2,
                    'name': 'User 2',
                    'login': 'user2',
                    'email': 'user2@example.com',
                    'status': 'active'
                },
                {
                    'id': 3,
                    'name': 'User 3',
                    'login': 'user3',
                    'email': 'user3@example.com',
                    'status': 'active'
                }
            ]
        }
        
        self.assertEqual(result, expected_result)

    @patch("src.openproject.requests.get")
    def test_get_members(self, mock_get):
        # Test successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'total': 2,
            'count': 2,
            '_embedded': {
                'elements': [
                    {
                        'id': 1,
                        'subject': 'Test Work Package 1',
                        'description': {'raw': 'Description for WP 1'},
                        '_links': {
                            'status': {'title': 'New'},
                            'priority': {'title': 'High'},
                        }
                    },
                    {
                        'id': 2,
                        'subject': 'Test Work Package 2',
                        'description': {'raw': 'Description for WP 2'},
                        '_links': {
                            'status': {'title': 'In Progress'},
                            'priority': {'title': 'Normal'},
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Create parser instance with test config
        config = {
            'apikey': 'test-api-key',
            'url': 'https://test.openproject.com'
        }
        parser = WorkPackageParser(config)

        # Call the method
        result = parser.get_members()

        # Verify the API call was made correctly
        mock_get.assert_called_once_with(
            'https://test.openproject.com/api/v3/projects/18/work_packages',
            auth=('apikey', 'test-api-key')
        )

        # Verify the result structure
        expected_result = {
            'total': 2,
            'count': 2,
            'members': [
                {
                    'id': 1,
                    'subject': 'Test Work Package 1',
                    'description': 'Description for WP 1',
                    'status': 'New',
                    'priority': 'High'
                },
                {
                    'id': 2,
                    'subject': 'Test Work Package 2',
                    'description': 'Description for WP 2',
                    'status': 'In Progress',
                    'priority': 'Normal'
                }
            ]
        }

        self.assertEqual(result, expected_result)

    @patch("src.openproject.requests.get")
    def test_get_members_api_error(self, mock_get):
        # Test API error response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        config = {
            'apikey': 'test-api-key',
            'url': 'https://test.openproject.com'
        }
        parser = WorkPackageParser(config)

        # Call the method
        result = parser.get_members()

        # Verify None is returned on error
        self.assertIsNone(result)

