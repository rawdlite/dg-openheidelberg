import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from unittest import TestCase
from unittest.mock import patch, MagicMock
from openproject import WorkPackageParser, UserParser


class TestWorkPackageParser(TestCase):

    def test_get_member_api(self):
        wp = WorkPackageParser()
        members = wp.get_members()
        print(members)
        self.assertIsNotNone(members)
        self.assertIsInstance(members, dict)
        self.assertIn('total', members)

    def test_get_project_18_form_info_with_invalid_project(self):
        """Test that we get None when accessing non-existent project"""
        # This test assumes we have valid credentials but invalid project
        # You can modify this to test different scenarios

        # For now, just check that the method exists and doesn't crash
        wp = WorkPackageParser()
        result = wp.get_project_18_form_info()
        # If it returns None or a dict, that's acceptable
        assert result is None or isinstance(result, dict)
        
    def test_get_scheduled_workpackages(self):
        wp = WorkPackageParser()
        scheduled_workpackages = wp.get_workpackages(project_id=18, status_id=6)
        print(scheduled_workpackages)
        self.assertIsNotNone(scheduled_workpackages)
        self.assertIsInstance(scheduled_workpackages, list)
       
            
    def test_create_member(self):
        wp = WorkPackageParser()
        #
        payload = {
            "projectId": "18",
            'customField5': "firstname",
            'customField6': "lastname",
            'customField7': "firstname.last@mail.com",
            "subject": "Test entry",
            "status_id": 1
        }
        member = wp.create_member(payload)
        assert member
        
    def test_member_exists(self):
        wp = WorkPackageParser()
        firstname = 'Stephan'
        lastname = 'Frenzel'
        username = 's.frenzel'
        email = 'firstname.last@mail.com'
        user = wp.check_member_exists(
            email=email,
            subject="Stephan.Frenzel",
            firstname=firstname, 
            lastname=lastname,
            username=username)
        assert user is not None
        assert user.get('id') is not None
        
        
    def test_get_member(self):
        wp = WorkPackageParser()
        member_info = wp.get_member(203)
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
        wp = WorkPackageParser()
        version = wp.get_lockVersion('203')
        assert version == 3

    def test_update_member(self):
        wp = WorkPackageParser()
        workpackage_id = '237'
        vers = wp.get_lockVersion(workpackage_id)
        payload = {
            "lockVersion": vers,
            "description": "Updated Description",
            'subject': 'Updated Subject'
        }
        result = wp.update_member(workpackage_id, payload)
        assert result
        
    def test_update_status(self):
        wp = WorkPackageParser()
        workpackage_id = '237'
        member = wp.get_member(workpackage_id)
        status = 'In progress'
        result = wp.update_status(member, status)
        assert result
        assert result.get('_links', {}).get('status', {}).get('title') == status

    def test_delete_member(self):
        wp = WorkPackageParser()
        workpackage_id = '203'
        wp.delete_member(workpackage_id)
        # Assuming the delete operation is successful and no exception is raised


class TestUserParser(TestCase):

    def test_get_users(self):
        up = UserParser()
        users = up.get_users()
        print(users)
        self.assertIsNotNone(users)
        self.assertIn('total', users)

    def test_check_user(self):
        up = UserParser()
        email = 'valantis.hatzimagkas@gmail.com'
        user = up.check_user(email)
        print(user)
        assert isinstance(user, dict)
        assert 'id' in user

    def test_get_user_by_id(self):
        up = UserParser()
        user_id = '6'
        user = up.get_user(user_id)
        print(user)
        self.assertIsNotNone(user)
        self.assertIn('id', user)

    def test_create_user(self):
        up = UserParser()
        new_user_data = {
            'firstName': 'John',
            'lastName': 'Doe',
            'login': 'johndoe',
            'email': 'rawdlite@gmail.com',
            'status': 'invited'
        }
        created_user = up.create_user(new_user_data)
        print(created_user)
        self.assertIsNotNone(created_user)
        self.assertIn('id', created_user)

    def test_add_user_to_group(self):
        up = UserParser()
        user_id = 26
        group_id = 16
        res = up.add_user_to_group(user_id, group_id)
        print(res)

        self.assertTrue(res)

    def test_set_membership(self):
        up = UserParser()
        payload = {
            'principal': {'href': '/api/v3/users/26'},
            'project': {'href': '/api/v3/projects/1'},
            'roles': {'href': '/api/v3/roles/1'}}

