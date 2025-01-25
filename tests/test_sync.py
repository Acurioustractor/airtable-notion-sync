import unittest
from unittest.mock import patch, MagicMock
import os
from sync_airtable_to_notion import sync_records

class TestSync(unittest.TestCase):
    def setUp(self):
        # Setup test environment
        os.environ['AIRTABLE_API_KEY'] = 'test_key'
        os.environ['AIRTABLE_BASE_ID'] = 'test_base'
        os.environ['NOTION_API_KEY'] = 'test_key'
        os.environ['NOTION_DATABASE_ID'] = 'test_db'

    @patch('sync_airtable_to_notion.requests')
    def test_sync_records_returns_list(self, mock_requests):
        """Test that sync_records returns a list"""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"records": []}
        mock_requests.get.return_value = mock_response
        
        result = sync_records()
        self.assertIsInstance(result, list)

    @patch('sync_airtable_to_notion.requests')
    def test_sync_records_calls_airtable_api(self, mock_requests):
        """Test that sync_records calls the Airtable API"""
        # Mock the Airtable API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"records": []}
        mock_requests.get.return_value = mock_response
        
        sync_records()
        
        # Verify Airtable API was called
        mock_requests.get.assert_called_once()
        self.assertIn('airtable.com', mock_requests.get.call_args[0][0])

    @patch('sync_airtable_to_notion.requests')
    def test_sync_records_calls_notion_api(self, mock_requests):
        """Test that sync_records calls the Notion API"""
        # Mock the API responses
        mock_response = MagicMock()
        mock_response.json.side_effect = [
            {"records": [{"id": "rec1", "fields": {"Name": "Test"}}]},  # Airtable response
            {"results": []}  # Notion response
        ]
        mock_requests.get.return_value = mock_response
        mock_requests.post.return_value = mock_response
        
        sync_records()
        
        # Verify Notion API was called
        mock_requests.post.assert_called_once()
        self.assertIn('notion.co', mock_requests.post.call_args[0][0])

    def test_missing_environment_variables(self):
        """Test that sync_records raises an error when environment variables are missing"""
        # Temporarily remove environment variables
        stored_env = {}
        for key in ['AIRTABLE_API_KEY', 'AIRTABLE_BASE_ID', 'NOTION_API_KEY', 'NOTION_DATABASE_ID']:
            stored_env[key] = os.environ.pop(key, None)
        
        # Test that it raises an error
        with self.assertRaises(ValueError):
            sync_records()
            
        # Restore environment variables
        for key, value in stored_env.items():
            if value is not None:
                os.environ[key] = value

if __name__ == '__main__':
    unittest.main()
