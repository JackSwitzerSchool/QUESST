import requests
import pandas as pd
import folium
from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import logging
from tqdm import tqdm

class NIFCDataConnector:
    def __init__(self):
        # Base URLs for NIFC's ArcGIS REST services
        self.current_fires_url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Current_WildlandFire_Perimeters/FeatureServer/0/query"
        self.historic_fires_url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Historic_Geomac_Perimeters_Archive/FeatureServer/0/query"
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Create data directory if it doesn't exist
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
    def _make_request(self, url, params, max_retries=3, delay=0.1):
        """
        Make a rate-limited request with retries
        """
        for attempt in range(max_retries):
            try:
                # Add required parameters for ArcGIS REST API
                params.update({
                    'f': 'json',
                    'spatialRel': 'esriSpatialRelIntersects',
                    'geometryType': 'esriGeometryEnvelope',
                    'returnGeometry': 'false',
                    'returnCountOnly': 'false',
                    'resultRecordCount': 2000
                })
                
                self.logger.debug(f"Making request with params: {params}")
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                json_response = response.json()
                
                if 'error' in json_response:
                    self.logger.error(f"API Error: {json_response['error']}")
                    return None
                
                time.sleep(delay)
                return json_response
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(delay * (attempt + 1))
        return None

    def get_historic_fires(self, start_year=2000, end_year=2005):
        """
        Fetch historical wildfire data for multiple years at once
        """
        if end_year is None:
            end_year = datetime.now().year

        all_data = []
        
        # Only request essential fields
        fields = [
            'FIRE_YEAR', 'DISCOVERY_DATE', 'CONT_DATE', 'FIRE_SIZE',
            'STATE', 'LATITUDE', 'LONGITUDE'
        ]
        
        # Base query parameters
        params = {
            'outFields': ','.join(fields),
            'orderByFields': 'FIRE_YEAR ASC',
            'where': '',
            'resultOffset': 0
        }
        
        # Calculate total date range
        start_timestamp = int(datetime(start_year, 1, 1).timestamp() * 1000)
        end_timestamp = int(datetime(end_year, 12, 31, 23, 59, 59).timestamp() * 1000)
        
        # Query for all years at once
        params['where'] = f"FIRE_YEAR >= {start_year} AND FIRE_YEAR <= {end_year} AND DISCOVERY_DATE >= {start_timestamp} AND DISCOVERY_DATE <= {end_timestamp}"
        
        print(f"\nFetching fire data for years {start_year}-{end_year}")
        print("Initial request to get total count...")
        
        # Get total count first
        count_params = params.copy()
        count_params['returnCountOnly'] = 'true'
        count_response = self._make_request(self.historic_fires_url, count_params)
        
        if not count_response or 'count' not in count_response:
            print("Error: Could not get total record count")
            return None
        
        total_records = count_response['count']
        print(f"Total records to fetch: {total_records:,}")
        
        # Initialize progress bar
        progress = tqdm(total=total_records, desc="Downloading records", unit="records")
        
        params['resultOffset'] = 0
        batch_data = []
        
        while True:
            response_data = self._make_request(self.historic_fires_url, params)
            
            if not response_data or 'features' not in response_data or not response_data['features']:
                break
            
            features = response_data['features']
            batch_data.extend(features)
            
            # Update progress bar
            progress.update(len(features))
            
            # Save every 10000 records
            if len(batch_data) >= 10000:
                df_batch = pd.DataFrame([f['attributes'] for f in batch_data])
                self._save_checkpoint(df_batch, start_year, end_year)
                all_data.extend(batch_data)
                batch_data = []
                
                # Print summary of current progress
                years_found = df_batch['FIRE_YEAR'].unique()
                print(f"\nProcessed years: {sorted(years_found)}")
                print(f"Records by state:\n{df_batch['STATE'].value_counts().head()}\n")
            
            params['resultOffset'] += len(features)
            
            # Break if we got less than requested (means we're at the end)
            if len(features) < 2000:
                break
        
        progress.close()
        
        # Save remaining batch
        if batch_data:
            all_data.extend(batch_data)
        
        if not all_data:
            print("No data was collected")
            return None
        
        print("\nProcessing and saving final dataset...")
        return self._save_final_data(all_data)

    def _validate_feature(self, feature):
        """Validate individual feature data quality"""
        try:
            attrs = feature['attributes']
            
            # Basic validation checks
            required_fields = ['FIRE_YEAR', 'DISCOVERY_DATE', 'FIRE_SIZE']
            if not all(field in attrs for field in required_fields):
                return False
            
            # Validate fire size is positive
            if attrs['FIRE_SIZE'] <= 0:
                return False
            
            # Validate dates
            if attrs['DISCOVERY_DATE'] is None:
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False

    def _save_checkpoint(self, data, start_year, end_year):
        """Save checkpoint data"""
        if data:
            df = pd.DataFrame([feature['attributes'] for feature in data])
            checkpoint_file = self.data_dir / f'US_fires_checkpoint_{start_year}_to_{end_year}.csv'
            df.to_csv(checkpoint_file, index=False)
            self.logger.info(f"Saved checkpoint data to {checkpoint_file}")

    def _save_year_data(self, data, year):
        """Save individual year data"""
        if data:
            df = pd.DataFrame([feature['attributes'] for feature in data])
            year_file = self.data_dir / f'US_fires_{year}.csv'
            df.to_csv(year_file, index=False)
            self.logger.info(f"Saved year data to {year_file}")

    def _save_final_data(self, data):
        """Process and save the final dataset"""
        try:
            df = pd.DataFrame(data)
            
            # Clean and process the data with error handling
            for date_col in ['DISCOVERY_DATE', 'CONT_DATE']:
                try:
                    if date_col in df.columns:
                        df[date_col] = pd.to_datetime(df[date_col], unit='ms', errors='coerce')
                except Exception as e:
                    self.logger.warning(f"Error processing {date_col}: {e}")
                    df[date_col] = pd.NaT
            
            # Calculate duration in days where both dates are available
            if 'DISCOVERY_DATE' in df.columns and 'CONT_DATE' in df.columns:
                df['DURATION'] = (df['CONT_DATE'] - df['DISCOVERY_DATE']).dt.total_seconds() / (24 * 60 * 60)
            
            # Add season column if discovery date is available
            if 'DISCOVERY_DATE' in df.columns:
                df['SEASON'] = df['DISCOVERY_DATE'].dt.month.map({
                    12: 'Winter', 1: 'Winter', 2: 'Winter',
                    3: 'Spring', 4: 'Spring', 5: 'Spring',
                    6: 'Summer', 7: 'Summer', 8: 'Summer',
                    9: 'Fall', 10: 'Fall', 11: 'Fall'
                })
            
            # Save to CSV
            output_file = self.data_dir / 'US.csv'
            df.to_csv(output_file, index=False)
            self.logger.info(f"Saved final dataset to {output_file}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in final data processing: {e}")
            return None

def main():
    nifc = NIFCDataConnector()
    
    try:
        print("=== NIFC Fire Data Collector ===")
        print("\nStarting with test query for 2023...")
        
        # Test with just one recent year first
        df = nifc.get_historic_fires(start_year=2023, end_year=2023)
        
        if df is not None:
            print(f"\nTest successful! Downloaded {len(df):,} fire records")
            print("\nSample Summary:")
            print("-" * 50)
            print(f"Total records: {len(df):,}")
            if 'STATE' in df.columns:
                print("\nTop 5 states by number of fires:")
                print(df['STATE'].value_counts().head())
            
            proceed = input("\nContinue with larger date range 2020-2023? (y/n): ")
            if proceed.lower() == 'y':
                print("\nFetching 2020-2023 data...")
                df = nifc.get_historic_fires(start_year=2020, end_year=2023)
                
                if df is not None:
                    print("\nFinal Statistics:")
                    print("-" * 50)
                    print(f"Total records: {len(df):,}")
                    print(f"\nRecords by year:")
                    print(df['FIRE_YEAR'].value_counts().sort_index())
                    print(f"\nTotal fires by state:")
                    print(df['STATE'].value_counts().head())
                    print(f"\nAverage fire size by year:")
                    print(df.groupby('FIRE_YEAR')['FIRE_SIZE'].mean().round(2))
        else:
            print("Initial test failed - no data received")
            
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        logging.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
