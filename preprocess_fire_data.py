import pandas as pd
import geopandas as gpd
from pathlib import Path
import os
import logging
from datetime import datetime
from tqdm import tqdm
from shapely.geometry import Point
import numpy as np
import json
import pickle

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fire_preprocessing.log'),
        logging.StreamHandler()
    ]
)

class FireDataPreprocessor:
    def __init__(self, nasa_data_dir="data/NASA", output_dir="data/output"):
        self.nasa_data_dir = Path(nasa_data_dir)
        self.output_dir = Path(output_dir)
        self.cache_dir = self.output_dir / "cache"
        self.stats = {
            'total_files_processed': 0,
            'total_records': 0,
            'errors': [],
            'start_time': None,
            'end_time': None
        }
        
        # Create output directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Define seasonal boundaries
        self.seasons = {
            'Winter': [12, 1, 2],
            'Spring': [3, 4, 5],
            'Summer': [6, 7, 8],
            'Fall': [9, 10, 11]
        }
    
    def get_season(self, month):
        """Get season name from month number"""
        for season, months in self.seasons.items():
            if month in months:
                return season
        return 'Winter'  # Default case

    def standardize_columns(self, df):
        """Standardize column names and formats across different data sources"""
        # Dictionary to map various column names to standard names
        column_mapping = {
            'latitude': 'latitude',
            'longitude': 'longitude',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude',
            'brightness': 'brightness',
            'BRIGHTNESS': 'brightness',
            'scan': 'scan',
            'track': 'track',
            'acq_date': 'date',
            'ACQ_DATE': 'date',
            'acq_time': 'time',
            'ACQ_TIME': 'time',
            'satellite': 'satellite',
            'SATELLITE': 'satellite',
            'instrument': 'instrument',
            'INSTRUMENT': 'instrument',
            'confidence': 'confidence',
            'CONFIDENCE': 'confidence',
            'version': 'version',
            'VERSION': 'version',
            'bright_t31': 'brightness_t31',
            'frp': 'fire_radiative_power',
            'FRP': 'fire_radiative_power',
            'daynight': 'day_night',
            'DAYNIGHT': 'day_night'
        }
        
        # Rename columns if they exist
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, 
                 inplace=True)
        
        return df
    
    def validate_data(self, df):
        """Validate and clean the data"""
        original_len = len(df)
        
        # Convert coordinates to numeric
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # Remove rows with invalid coordinates
        df = df[
            df['latitude'].between(-90, 90) & 
            df['longitude'].between(-180, 180)
        ]
        
        # Convert date and time to datetime
        if 'date' in df.columns:
            if 'time' in df.columns:
                # Combine date and time
                df['datetime'] = pd.to_datetime(
                    df['date'].astype(str) + ' ' + df['time'].astype(str).str.zfill(4),
                    format='%Y-%m-%d %H%M',
                    errors='coerce'
                )
            else:
                df['datetime'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Remove rows with invalid dates or future dates
            df = df[
                df['datetime'].notna() & 
                (df['datetime'] <= pd.Timestamp.now())
            ]
        
        # Convert and clean numeric columns
        numeric_columns = ['brightness', 'fire_radiative_power', 'brightness_t31']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Remove extreme outliers (beyond 99th percentile)
                percentile_99 = np.percentile(df[col].dropna(), 99)
                df = df[df[col] <= percentile_99]
        
        # Log if we removed any rows
        rows_removed = original_len - len(df)
        if rows_removed > 0:
            logging.warning(f"Removed {rows_removed} invalid rows")
            
        return df
    
    def create_temporal_aggregations(self, df):
        """Create different temporal aggregations of the data"""
        logging.info("Creating temporal aggregations...")
        
        # Extract temporal components
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        df['week'] = df['datetime'].dt.isocalendar().week
        df['season'] = df['month'].map(self.get_season)
        
        # Create period identifiers
        df['yearly_period'] = df['year'].astype(str)
        df['monthly_period'] = df['datetime'].dt.strftime('%Y-%m')
        df['weekly_period'] = df['datetime'].dt.strftime('%Y-W%V')
        df['seasonal_period'] = df['year'].astype(str) + '-' + df['season']
        
        return df
    
    def create_aggregated_datasets(self, df):
        """Create and cache different aggregated versions of the dataset"""
        logging.info("Creating aggregated datasets...")
        
        # Define aggregation periods and their corresponding columns
        aggregations = {
            'seasonal': 'seasonal_period',
            'monthly': 'monthly_period',
            'weekly': 'weekly_period',
            'yearly': 'yearly_period'
        }
        
        # Metrics to aggregate
        agg_metrics = {
            'latitude': ['mean', 'min', 'max'],
            'longitude': ['mean', 'min', 'max'],
            'brightness': ['mean', 'min', 'max', 'count'],
            'fire_radiative_power': ['mean', 'min', 'max', 'sum']
        }
        
        # Create and cache each aggregation
        for agg_name, period_col in aggregations.items():
            logging.info(f"Processing {agg_name} aggregation...")
            
            # Group and aggregate data
            agg_df = df.groupby(period_col).agg(agg_metrics)
            agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
            
            # Reset index and add temporal components
            agg_df = agg_df.reset_index()
            
            # Cache the aggregated data
            cache_file = self.cache_dir / f"fire_data_{agg_name}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(agg_df, f, protocol=4)
            
            logging.info(f"Saved {agg_name} aggregation to {cache_file}")
            
            # Save a JSON metadata file for this aggregation
            metadata = {
                'period_type': agg_name,
                'num_records': len(agg_df),
                'columns': list(agg_df.columns),
                'date_range': [agg_df[period_col].min(), agg_df[period_col].max()],
                'created_at': datetime.now().isoformat()
            }
            
            metadata_file = self.cache_dir / f"fire_data_{agg_name}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
    
    def process_file(self, file_path):
        """Process a single CSV file"""
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Standardize columns
            df = self.standardize_columns(df)
            
            # Validate data
            df = self.validate_data(df)
            
            if len(df) > 0:
                # Create GeoDataFrame
                gdf = gpd.GeoDataFrame(
                    df,
                    geometry=gpd.points_from_xy(df.longitude, df.latitude),
                    crs="EPSG:4326"
                )
                
                self.stats['total_records'] += len(gdf)
                self.stats['total_files_processed'] += 1
                
                return gdf
            
            return None
            
        except Exception as e:
            error_msg = f"Error processing {file_path}: {str(e)}"
            logging.error(error_msg)
            self.stats['errors'].append(error_msg)
            return None
    
    def process_data(self):
        """Process all CSV files and create aggregated datasets"""
        logging.info("Starting data preprocessing...")
        self.stats['start_time'] = datetime.now()
        
        all_data = []
        
        # Get all CSV files
        csv_files = list(self.nasa_data_dir.glob("**/*.csv"))
        logging.info(f"Found {len(csv_files)} CSV files")
        
        # Process all files with progress bar
        for file_path in tqdm(csv_files, desc="Processing CSV files"):
            gdf = self.process_file(file_path)
            if gdf is not None and len(gdf) > 0:
                all_data.append(gdf)
        
        if not all_data:
            raise ValueError("No data was successfully processed")
        
        logging.info("Combining all processed data...")
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Sort by datetime
        if 'datetime' in combined_data.columns:
            combined_data = combined_data.sort_values('datetime')
        
        # Create temporal aggregations
        combined_data = self.create_temporal_aggregations(combined_data)
        
        # Save raw data
        output_file = self.output_dir / "fire_data.csv"
        combined_data.to_csv(output_file, index=False)
        
        # Create and cache aggregated datasets
        self.create_aggregated_datasets(combined_data)
        
        # Save metadata
        self.stats['end_time'] = datetime.now()
        processing_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        metadata = {
            'total_files_processed': self.stats['total_files_processed'],
            'total_records': self.stats['total_records'],
            'processing_time_seconds': processing_time,
            'earliest_date': combined_data['datetime'].min().strftime('%Y-%m-%d %H:%M'),
            'latest_date': combined_data['datetime'].max().strftime('%Y-%m-%d %H:%M'),
            'columns': list(combined_data.columns),
            'aggregations_available': ['seasonal', 'monthly', 'weekly', 'yearly'],
            'errors': self.stats['errors']
        }
        
        # Save metadata
        metadata_file = self.output_dir / "metadata.txt"
        with open(metadata_file, 'w') as f:
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")
        
        logging.info(f"Processing complete. Results saved to {output_file}")
        logging.info(f"Processed {self.stats['total_files_processed']} files")
        logging.info(f"Total records: {self.stats['total_records']}")
        logging.info(f"Processing time: {processing_time:.2f} seconds")
        
        if self.stats['errors']:
            logging.warning(f"Encountered {len(self.stats['errors'])} errors during processing")
        
        return combined_data

def main():
    try:
        preprocessor = FireDataPreprocessor()
        preprocessor.process_data()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 