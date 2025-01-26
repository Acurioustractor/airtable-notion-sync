import unittest
import os
import sys
from pathlib import Path

# Add parent directory to Python path
parent_dir = str(Path(__file__).parent.parent.absolute())
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

class TestImport(unittest.TestCase):
    """Test cases for module import"""
    
    def setUp(self):
        """Set up test environment"""
        self.env_vars = {
            'NOTION_API_KEY': 'test_key',
            'NOTION_DATABASE_ID': 'test_db',
            'AIRTABLE_API_KEY': 'test_key',
            'AIRTABLE_BASE_ID': 'test_base'
        }
        os.environ.update(self.env_vars)

    def test_module_imports(self):
        """Test that the module can be imported"""
        try:
            import sync_airtable_to_notion
            self.assertTrue(True, "Module imported successfully")
        except Exception as e:
            self.fail(f"Failed to import module: {e}")

if __name__ == '__main__':
    unittest.main() 