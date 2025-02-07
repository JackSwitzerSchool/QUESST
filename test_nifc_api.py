import unittest
from nifc_api import NIFCApi
import logging
from pprint import pprint

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class TestNIFCApi(unittest.TestCase):
    def setUp(self):
        self.api = NIFCApi()
        self.test_year = 2023

    def test_connection(self):
        """Test basic API connectivity"""
        metadata = self.api.get_metadata()
        self.assertIsNotNone(metadata)
        print("\nAPI Metadata:")
        pprint(metadata)

    def test_record_count(self):
        """Test getting record count"""
        count = self.api.get_record_count(self.test_year)
        self.assertIsNotNone(count)
        print(f"\nTotal records for {self.test_year}: {count}")

    def test_get_fires(self):
        """Test retrieving fire records"""
        # Test with minimal fields
        fires = self.api.get_fires(
            year=self.test_year,
            fields=['FIRE_YEAR', 'STATE', 'FIRE_SIZE'],
            limit=5
        )
        
        self.assertIsNotNone(fires)
        self.assertTrue(len(fires) > 0)
        
        print(f"\nSample fire records ({len(fires)} records):")
        pprint(fires)

def run_interactive_test():
    """Run interactive tests with detailed output"""
    api = NIFCApi()
    
    print("=== NIFC API Interactive Test ===\n")
    
    # Test metadata
    print("1. Testing API metadata...")
    metadata = api.get_metadata()
    if metadata:
        print("✓ Successfully retrieved API metadata")
        if 'fields' in metadata:
            print("\nAvailable fields:")
            for field in metadata['fields']:
                print(f"- {field['name']}: {field['type']}")
    
    # Test record count
    print("\n2. Testing record count for 2023...")
    count = api.get_record_count(2023)
    if count is not None:
        print(f"✓ Found {count} records for 2023")
    
    # Test fire data retrieval
    print("\n3. Testing fire data retrieval...")
    fires = api.get_fires(2023, limit=5)
    if fires:
        print(f"✓ Successfully retrieved {len(fires)} fire records")
        print("\nSample record:")
        pprint(fires[0])

if __name__ == '__main__':
    # Run interactive tests first
    run_interactive_test()
    
    # Ask if user wants to run unit tests
    response = input("\nRun unit tests? (y/n): ")
    if response.lower() == 'y':
        print("\n=== Running Unit Tests ===")
        unittest.main(argv=[''], exit=False) 