import unittest
import os
import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

class TestSync(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        # Add mock environment variables for testing
        os.environ.update({
            'NOTION_API_KEY': 'test_key',
            'NOTION_DATABASE_ID': 'test_db',
            'AIRTABLE_API_KEY': 'test_key',
            'AIRTABLE_BASE_ID': 'test_base'
        })

    def test_import(self):
        """Test that the sync module can be imported"""
        try:
            import sync_airtable_to_notion
            self.assertTrue(True, "Successfully imported sync module")
        except ImportError as e:
            self.fail(f"Failed to import sync module: {e}")

if __name__ == '__main__':
    unittest.main()
