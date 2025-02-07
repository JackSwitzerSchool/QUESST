import requests
import pandas as pd
import folium
from datetime import datetime, timedelta
import time
import os
from pathlib import Path
import logging

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
        
    def _make_request(self, url, params, max_retries=3, delay=1):
        """
        Make a rate-limited request with retries
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                time.sleep(delay)  # Rate limiting
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(delay * (attempt + 1))  # Exponential backoff
        return None

    def get_historic_fires(self, start_year=1920, end_year=None, chunk_size=1000):
        """
        Fetch historical wildfire data year by year
        """
        if end_year is None:
            end_year = datetime.now().year

        all_data = []
        
        for year in range(start_year, end_year + 1):
            self.logger.info(f"Fetching data for year {year}")
            
            params = {
                'where': f"FIRE_YEAR = {year}",
                'outFields': '*',  # Get all fields to handle different data structures
                'returnGeometry': 'false',
                'f': 'json',
                'resultOffset': 0,
                'resultRecordCount': chunk_size
            }
            
            while True:
                try:
                    response_data = self._make_request(self.historic_fires_url, params)
                    
                    if not response_data or 'features' not in response_data:
                        break
                    
                    features = response_data['features']
                    if not features:
                        break
                    
                    # Extract attributes from features
                    year_data = [feature['attributes'] for feature in features]
                    all_data.extend(year_data)
                    
                    # Update offset for next chunk
                    params['resultOffset'] += chunk_size
                    
                    self.logger.info(f"Fetched {len(features)} records for {year}")
                    
                    # Save intermediate results every 10,000 records
                    if len(all_data) % 10000 == 0:
                        self._save_intermediate_data(all_data, year)
                    
                except Exception as e:
                    self.logger.error(f"Error processing year {year}: {e}")
                    break

        if not all_data:
            self.logger.warning("No data was collected")
            return None
            
        return self._save_final_data(all_data)

    def _save_intermediate_data(self, data, current_year):
        """Save intermediate results to prevent data loss"""
        df = pd.DataFrame(data)
        intermediate_file = self.data_dir / f'US_fires_temp_{current_year}.csv'
        df.to_csv(intermediate_file, index=False)
        self.logger.info(f"Saved intermediate data to {intermediate_file}")

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
    # Initialize the connector
    nifc = NIFCDataConnector()
    
    try:
        # Fetch historical data
        df = nifc.get_historic_fires(start_year=1920)
        
        if df is not None:
            print(f"Successfully downloaded and processed {len(df)} fire records")
            print(f"Data saved to {nifc.data_dir / 'US.csv'}")
            
            # Display basic statistics
            print("\nBasic Statistics:")
            if 'DISCOVERY_DATE' in df.columns:
                print(f"Date Range: {df['DISCOVERY_DATE'].min()} to {df['DISCOVERY_DATE'].max()}")
            if 'STATE' in df.columns:
                print(f"Total number of fires by state:\n{df['STATE'].value_counts().head()}")
            if 'SEASON' in df.columns and 'FIRE_SIZE' in df.columns:
                print(f"Average fire size by season:\n{df.groupby('SEASON')['FIRE_SIZE'].mean()}")
        else:
            print("No data was processed")
            
    except Exception as e:
        print(f"An error occurred in main: {e}")
        logging.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
