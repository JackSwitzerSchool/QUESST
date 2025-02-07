"""
Test script to verify the data pipeline functionality
"""

import logging
from pathlib import Path
from wildfires.data_manager import DataManager
import pandas as pd

def test_pipeline():
    """Test the complete data pipeline"""
    logging.info("Starting pipeline test...")
    
    # Initialize data manager
    dm = DataManager()
    
    # Test data loading
    logging.info("Testing data loading...")
    dm.load_raw_data(force_reload=True)
    assert dm.raw_data is not None, "Failed to load raw data"
    logging.info(f"Loaded {len(dm.raw_data):,} raw records")
    
    # Test data cleaning
    logging.info("Testing data cleaning...")
    dm.clean_data()
    assert dm.processed_data is not None, "Failed to clean data"
    logging.info(f"Processed {len(dm.processed_data):,} records")
    
    # Verify required columns
    required_cols = [
        'datetime', 'LATITUDE', 'LONGITUDE', 
        'BRIGHTNESS', 'fire_area', 'intensity'
    ]
    missing_cols = [col for col in required_cols if col not in dm.processed_data.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"
    
    # Test seasonal aggregation
    logging.info("Testing seasonal aggregation...")
    seasonal_data = dm.get_seasonal_aggregation()
    assert len(seasonal_data) > 0, "Failed to create seasonal aggregation"
    logging.info(f"Created {len(seasonal_data)} seasonal aggregations")
    
    # Test time series extraction
    logging.info("Testing time series extraction...")
    time_series = dm.get_time_series()
    assert len(time_series) > 0, "Failed to extract time series"
    logging.info(f"Extracted {len(time_series)} time series records")
    
    # Verify data quality
    logging.info("Verifying data quality...")
    assert not time_series['LATITUDE'].isna().any(), "Found null latitudes"
    assert not time_series['LONGITUDE'].isna().any(), "Found null longitudes"
    assert not time_series['datetime'].isna().any(), "Found null dates"
    assert not time_series['BRIGHTNESS'].isna().any(), "Found null brightness values"
    
    # Test data ranges
    assert time_series['LATITUDE'].between(25, 70).all(), "Invalid latitude range"
    assert time_series['LONGITUDE'].between(-170, -50).all(), "Invalid longitude range"
    assert (time_series['BRIGHTNESS'] > 0).all(), "Invalid brightness values"
    
    # Save processed data
    logging.info("Testing data saving...")
    dm.save_processed_data()
    
    # Test data loading
    logging.info("Testing processed data loading...")
    loaded_data = dm.load_processed_data()
    assert loaded_data is not None, "Failed to load processed data"
    assert len(loaded_data) == len(dm.processed_data), "Data size mismatch after loading"
    
    logging.info("Pipeline test completed successfully!")
    return dm

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run the test
    dm = test_pipeline()
    
    # Print summary statistics
    print("\nData Summary:")
    print("-" * 50)
    print(f"Total records: {len(dm.processed_data):,}")
    print(f"Date range: {dm.processed_data['datetime'].min():%Y-%m-%d} to {dm.processed_data['datetime'].max():%Y-%m-%d}")
    print(f"Number of unique fire locations: {len(dm.processed_data.groupby(['LATITUDE', 'LONGITUDE'])):,}")
    print("\nSeasonal Distribution:")
    print(dm.processed_data['season'].value_counts().sort_index())
    print("\nYearly Distribution:")
    print(dm.processed_data['year'].value_counts().sort_index()) 