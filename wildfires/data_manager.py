"""
Data Manager for North American Wildfire Analysis System
Handles data loading, processing, and storage operations
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
from datetime import datetime
import pickle
import logging
from typing import Dict, List, Optional, Tuple, Union
from shapely.geometry import Point

class DataManager:
    def __init__(self, data_dir: str = "data/NASA", output_dir: str = "data/processed"):
        """
        Initialize DataManager with data directories
        
        Args:
            data_dir: Directory containing NASA FIRMS data
            output_dir: Directory for processed data output
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.cache_dir = self.output_dir / "cache"
        self.raw_data = None
        self.processed_data = None
        self.states_gdf = None
        
        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'data_processing.log'),
                logging.StreamHandler()
            ]
        )
        
        # Define geographic bounds for North America
        self.bounds = {
            'north': 70,  # Include Alaska
            'south': 25,
            'west': -170, # Include Alaska
            'east': -50   # Include eastern Canada
        }
        
        # Define seasons
        self.seasons = {
            'Winter': [12, 1, 2],
            'Spring': [3, 4, 5],
            'Summer': [6, 7, 8],
            'Fall': [9, 10, 11]
        }
        
        # Load US states shapefile
        self._load_states_data()
    
    def _load_states_data(self):
        """Load US states boundary data"""
        try:
            self.states_gdf = gpd.read_file(
                'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json'
            )
            self.states_gdf = self.states_gdf.to_crs("EPSG:4326")  # Ensure correct projection
        except Exception as e:
            logging.error(f"Error loading states data: {e}")
            self.states_gdf = None
    
    def _assign_states(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign state information to each fire location"""
        if self.states_gdf is None:
            logging.warning("States data not available, skipping state assignment")
            df['state'] = 'Unknown'
            return df
        
        # Create GeoDataFrame from fire locations
        geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
        fires_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        # Spatial join with states
        joined = gpd.sjoin(fires_gdf, self.states_gdf, predicate='within')
        
        # Add state information back to original DataFrame
        df['state'] = joined['id'].fillna('Unknown')
        return df
    
    def load_raw_data(self, force_reload: bool = False) -> None:
        """
        Load raw data from NASA FIRMS shapefiles
        
        Args:
            force_reload: If True, bypass cache and reload from source files
        """
        cache_file = self.cache_dir / "raw_data.pkl"
        
        if not force_reload and cache_file.exists():
            logging.info("Loading data from cache...")
            try:
                with open(cache_file, 'rb') as f:
                    self.raw_data = pickle.load(f)
                logging.info(f"Loaded {len(self.raw_data):,} records from cache")
                return
            except Exception as e:
                logging.error(f"Error loading cache: {e}")
        
        logging.info("Loading data from source files...")
        shapefiles = list(self.data_dir.glob("**/*.shp"))
        if not shapefiles:
            raise FileNotFoundError("No shapefiles found in data directory")
        
        # Load and combine data from all shapefiles
        all_data = []
        for shp in shapefiles:
            try:
                gdf = gpd.read_file(shp)
                all_data.append(gdf)
            except Exception as e:
                logging.error(f"Error reading {shp}: {e}")
        
        if not all_data:
            raise ValueError("No data could be loaded from shapefiles")
        
        self.raw_data = pd.concat(all_data, ignore_index=True)
        
        # Cache the raw data
        with open(cache_file, 'wb') as f:
            pickle.dump(self.raw_data, f)
        
        logging.info(f"Loaded {len(self.raw_data):,} raw records")
    
    def clean_data(self) -> None:
        """Clean and validate the raw data"""
        if self.raw_data is None:
            raise ValueError("No raw data loaded. Call load_raw_data() first.")
        
        logging.info("Cleaning data...")
        df = self.raw_data.copy()
        
        # Convert dates
        df['datetime'] = pd.to_datetime(df['ACQ_DATE'])
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        
        # Filter to North America
        mask = (
            (df['LATITUDE'] >= self.bounds['south']) & 
            (df['LATITUDE'] <= self.bounds['north']) & 
            (df['LONGITUDE'] >= self.bounds['west']) & 
            (df['LONGITUDE'] <= self.bounds['east']) &
            (df['LATITUDE'].notna()) & 
            (df['LONGITUDE'].notna())
        )
        df = df[mask]
        
        # Clean numeric columns
        numeric_cols = ['BRIGHTNESS', 'SCAN', 'TRACK', 'CONFIDENCE']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove invalid measurements
        df = df.dropna(subset=['BRIGHTNESS', 'SCAN', 'TRACK'])
        
        # Remove outliers using IQR method
        for col in ['BRIGHTNESS', 'SCAN', 'TRACK']:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            df = df[
                (df[col] >= Q1 - 1.5 * IQR) & 
                (df[col] <= Q3 + 1.5 * IQR)
            ]
        
        # Calculate fire area and intensity metrics
        df['fire_area'] = df['SCAN'] * df['TRACK']
        df['intensity'] = (df['BRIGHTNESS'] - df['BRIGHTNESS'].mean()) / df['BRIGHTNESS'].std()
        
        # Add seasonal information
        df['season'] = df['month'].map(lambda m: next(
            season for season, months in self.seasons.items() 
            if m in months
        ))
        
        # Add state information
        df = self._assign_states(df)
        
        self.processed_data = df.sort_values('datetime')
        logging.info(f"Cleaned data: {len(self.processed_data):,} records remaining")
    
    def get_seasonal_aggregation(self) -> pd.DataFrame:
        """
        Aggregate data by season
        
        Returns:
            DataFrame with seasonal aggregations
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Run clean_data() first.")
        
        seasonal = self.processed_data.groupby(['year', 'season']).agg({
            'BRIGHTNESS': ['mean', 'max', 'count'],
            'fire_area': ['sum', 'mean'],
            'intensity': ['mean', 'max']
        }).round(2)
        
        seasonal.columns = ['_'.join(col).strip() for col in seasonal.columns.values]
        return seasonal.reset_index()
    
    def get_time_series(self) -> pd.DataFrame:
        """
        Get time series data for visualization
        
        Returns:
            DataFrame with time series data
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Run clean_data() first.")
        
        return self.processed_data[[
            'datetime', 'LATITUDE', 'LONGITUDE', 
            'BRIGHTNESS', 'fire_area', 'intensity',
            'state'
        ]].copy()
    
    def save_processed_data(self) -> None:
        """Save processed data to cache"""
        if self.processed_data is None:
            raise ValueError("No processed data available. Run clean_data() first.")
        
        cache_file = self.cache_dir / "processed_data.pkl"
        with open(cache_file, 'wb') as f:
            pickle.dump(self.processed_data, f)
        logging.info(f"Saved processed data to {cache_file}")
    
    def load_processed_data(self) -> Optional[pd.DataFrame]:
        """
        Load processed data from cache
        
        Returns:
            Processed DataFrame if available, None otherwise
        """
        cache_file = self.cache_dir / "processed_data.pkl"
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                self.processed_data = pickle.load(f)
            logging.info(f"Loaded processed data: {len(self.processed_data):,} records")
            return self.processed_data
        return None 