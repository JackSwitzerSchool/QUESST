import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_api_connection():
    """Simple test to verify API accessibility"""
    url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Historic_Geomac_Perimeters_Archive/FeatureServer/0/query"
    
    # Test with minimal parameters for 2023
    params = {
        'where': 'FIRE_YEAR = 2023',
        'outFields': 'FIRE_YEAR,STATE',
        'returnGeometry': 'false',
        'f': 'json',
        'resultRecordCount': 1
    }
    
    try:
        print("Testing API connection...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response keys: {data.keys()}")
        
        if 'error' in data:
            print(f"API Error: {data['error']}")
            return False
            
        if 'features' in data:
            print(f"Successfully retrieved {len(data['features'])} features")
            return True
            
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
        return False

def test_count_query():
    """Test getting total count of records"""
    url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Historic_Geomac_Perimeters_Archive/FeatureServer/0/query"
    
    params = {
        'where': 'FIRE_YEAR = 2023',
        'returnCountOnly': 'true',
        'f': 'json'
    }
    
    try:
        print("\nTesting count query...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        if 'count' in data:
            print(f"Total records for 2023: {data['count']}")
            return True
        else:
            print("No count in response")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"Count query error: {e}")
        return False

def test_single_record():
    """Test retrieving a single record with all fields"""
    url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Historic_Geomac_Perimeters_Archive/FeatureServer/0/query"
    
    params = {
        'where': 'FIRE_YEAR = 2023',
        'outFields': '*',
        'returnGeometry': 'false',
        'f': 'json',
        'resultRecordCount': 1
    }
    
    try:
        print("\nTesting single record retrieval...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        if 'features' in data and len(data['features']) > 0:
            feature = data['features'][0]
            print("\nAvailable fields:")
            for field, value in feature['attributes'].items():
                print(f"{field}: {value}")
            return True
        else:
            print("No features found")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"Single record query error: {e}")
        return False

def main():
    print("=== NIFC API Test Suite ===\n")
    
    # Test basic connection
    if not test_api_connection():
        print("Basic API connection test failed!")
        return
        
    # Test count query
    if not test_count_query():
        print("Count query test failed!")
        return
        
    # Test single record retrieval
    if not test_single_record():
        print("Single record test failed!")
        return
        
    print("\nAll tests completed successfully!")

if __name__ == "__main__":
    main() 