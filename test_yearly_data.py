import unittest
from nifc_api import NIFCApi
import logging
from datetime import datetime
import pandas as pd
from pprint import pprint
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_year(year: int = 2023):
    """Test data retrieval for a specific year"""
    api = NIFCApi()
    print(f"\n=== Testing Fire Data for {year} ===")
    
    # Get yearly summary
    print("\n1. Yearly Summary:")
    summary = api.get_yearly_summary(year)
    if summary and 'features' in summary:
        stats = summary['features'][0]['attributes']
        print(f"Total Fires: {stats.get('TOTAL_FIRES', 'N/A'):,}")
        print(f"Total Acres: {stats.get('TOTAL_ACRES', 'N/A'):,.2f}")
        print(f"Average Fire Size: {stats.get('AVG_FIRE_SIZE', 'N/A'):,.2f} acres")
    
    # Get sample of fires
    print("\n2. Sample Records:")
    fires = api.get_fires(year, limit=5)
    if fires:
        df = pd.DataFrame([f['attributes'] for f in fires])
        
        # Convert dates
        for date_col in ['DISCOVERY_DATE', 'CONT_DATE']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], unit='ms')
        
        print("\nSample Data:")
        print(df[['INCIDENT_NAME', 'STATE', 'FIRE_SIZE', 'DISCOVERY_DATE']].to_string())
        
        print("\nData Types:")
        print(df.dtypes)
    
    # Get metadata
    print("\n3. Available Fields:")
    metadata = api.get_metadata()
    if metadata and 'fields' in metadata:
        fields = metadata['fields']
        print("\nKey Fields:")
        for field in fields:
            if field['name'] in ['FIRE_YEAR', 'DISCOVERY_DATE', 'FIRE_SIZE', 'STATE']:
                print(f"- {field['name']}: {field['type']}")

def main():
    # Load API key from environment variable or config file
    api_key = os.getenv('ARCGIS_API_KEY')
    if not api_key:
        api_key = input("Please enter your ArcGIS API key: ")
    
    api = NIFCApi(api_key)
    
    # Test recent year
    test_year(2023)
    
    # Ask