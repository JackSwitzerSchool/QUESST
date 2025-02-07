import unittest
from nifc_api import NIFCApi
import logging
from datetime import datetime
import pandas as pd
from pprint import pprint

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class TestDataQuality(unittest.TestCase):
    def setUp(self):
        self.api = NIFCApi()
        self.test_year = 2023
        self.required_fields = [
            'FIRE_YEAR', 'DISCOVERY_DATE', 'CONT_DATE',
            'FIRE_SIZE', 'STATE', 'LATITUDE', 'LONGITUDE'
        ]

    def test_field_availability(self):
        """Test that all required fields are available in the API"""
        print("\nTesting field availability...")
        metadata = self.api.get_metadata()
        
        if not metadata or 'fields' not in metadata:
            self.fail("Could not retrieve metadata")
            
        available_fields = [f['name'] for f in metadata['fields']]
        print("\nAvailable fields:")
        pprint(available_fields)
        
        for field in self.required_fields:
            self.assertIn(field, available_fields, f"Required field {field} not available")

    def test_data_completeness(self):
        """Test data completeness for a small sample"""
        print("\nTesting data completeness...")
        fires = self.api.get_fires(
            year=self.test_year,
            fields=self.required_fields,
            limit=10
        )
        
        if not fires:
            self.fail("Could not retrieve fire data")
            
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame([f['attributes'] for f in fires])
        
        # Check for missing values
        missing_stats = df.isnull().sum()
        print("\nMissing value statistics:")
        print(missing_stats)
        
        # Check data types
        print("\nData types:")
        print(df.dtypes)
        
        # Basic value range checks
        self.assertTrue(all(df['FIRE_SIZE'] >= 0), "Found negative fire sizes")
        self.assertTrue(all(df['FIRE_YEAR'] == self.test_year), "Found incorrect years")

    def test_date_consistency(self):
        """Test date field consistency"""
        print("\nTesting date consistency...")
        fires = self.api.get_fires(
            year=self.test_year,
            fields=['FIRE_YEAR', 'DISCOVERY_DATE', 'CONT_DATE'],
            limit=10
        )
        
        if not fires:
            self.fail("Could not retrieve fire data")
            
        df = pd.DataFrame([f['attributes'] for f in fires])
        
        # Convert epoch timestamps to datetime
        for date_col in ['DISCOVERY_DATE', 'CONT_DATE']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], unit='ms')
        
        print("\nDate ranges:")
        for date_col in ['DISCOVERY_DATE', 'CONT_DATE']:
            if date_col in df.columns:
                print(f"{date_col}:")
                print(f"  Min: {df[date_col].min()}")
                print(f"  Max: {df[date_col].max()}")

def run_interactive_test():
    """Run interactive data quality tests"""
    print("=== NIFC Data Quality Test Suite ===\n")
    
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQuality)
    unittest.TextTestRunner(verbosity=2).run(test_suite)

if __name__ == '__main__':
    run_interactive_test() 