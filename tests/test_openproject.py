import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock
from openproject import WorkPackageParser, UserParser, CUSTOMFIELD, STATUS


class TestWorkPackageParser(TestCase):
    def setUp(self):
        # Initialize client with real config
        self.wp = WorkPackageParser()

    def test_get_member_api(self):
        members = self.wp.get_members()
        print(members)
        self.assertIsNotNone(members)
        self.assertIsInstance(members, dict)
        self.assertIn('total', members)
        
    def test_get_scheduled_workpackages(self):
        scheduled_workpackages = self.wp.get_workpackages(project_id=18, status_id=6)
        print(scheduled_workpackages)
        self.assertIsNotNone(scheduled_workpackages)
        self.assertIsInstance(scheduled_workpackages, list)
       
            
    def test_create_member(self):
        payload = {
            "projectId": "18",
            'customField5': "firstname",
            'customField6': "lastname",
            'customField7': "firstname.last@mail.com",
            "subject": "Test entry",
            "status_id": 1
        }
        member = self.wp.create_member(payload)
        assert member
        
    def test_member_exists(self):
        firstname = 'Stephan'
        lastname = 'Frenzel'
        username = 's.frenzel'
        email = 'firstname.last@mail.com'
        user = self.wp.check_member_exists(
            email=email,
            subject="Stephan.Frenzel",
            firstname=firstname, 
            lastname=lastname,
            username=username)
        assert user is not None
        assert user.get('id') is not None
        
        
    def test_get_member(self):
        member_info = self.wp.get_member(203)
        assert member_info is not None

    @patch("openproject.requests.get")
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

    def test_get_lockVersion(self):
        version = self.wp.get_lockVersion('203')
        assert version == 3

    def test_add_comment(self):
        workpackage_id = '235'
        comment = 'This is a test comment'
        result = self.wp.add_comment(workpackage_id, comment)
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, dict))
        self.assertIn('_type', result)

    def test_update_member(self):
        workpackage_id = '237'
        vers = self.wp.get_lockVersion(workpackage_id)
        payload = {
            "lockVersion": vers,
            "description": "Updated Description",
            'subject': 'Updated Subject'
        }
        result = self.wp.update_member(workpackage_id, payload)
        assert result
        
    def test_update_status(self):
        member = self.wp.get_member(237)
        status = 'In progress'
        result = self.wp.update_status(member, status)
        assert result
        assert result.get('_links', {}).get('status', {}).get('title') == status

    def test_delete_member(self):
        workpackage_id = '203'
        self.wp.delete_member(workpackage_id)
        # Assuming the delete operation is successful and no exception is raised


class TestUserParser(TestCase):
    def setUp(self):
        # Initialize client with real config
        self.up = UserParser()

    def test_get_users(self):
        users = self.up.get_users()
        print(users)
        self.assertIsNotNone(users)
        self.assertIn('total', users)

    def test_check_user(self):
        email = 'valantis.hatzimagkas@gmail.com'
        user = self.up.check_user(email)
        print(user)
        assert isinstance(user, dict)
        assert 'id' in user

    def test_get_user_by_id(self):
        user_id = '6'
        user = self.up.get_user(user_id)
        print(user)
        self.assertIsNotNone(user)
        self.assertIn('id', user)

    def test_create_user(self):
        new_user_data = {
            'firstName': 'John',
            'lastName': 'Doe',
            'login': 'johndoe',
            'email': 'rawdlite@gmail.com',
            'status': 'invited'
        }
        created_user = self.up.create_user(new_user_data)
        print(created_user)
        self.assertIsNotNone(created_user)

    def test_add_user_to_group(self):
        user_id = 26
        group_id = 16
        res = self.up.add_user_to_group(user_id, group_id)
        print(res)

        self.assertTrue(res)

    def test_set_membership(self):
        # ToDo: Implement this test
        payload = {
            'principal': {'href': '/api/v3/users/26'},
            'project': {'href': '/api/v3/projects/1'},
            'roles': {'href': '/api/v3/roles/1'}}

if __name__ == "__main__":
    unittest.main()