"""
Data validation functions for the Wildfire Analysis System
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Union
from .config import BOUNDS, COLUMNS

def validate_coordinates(
    df: pd.DataFrame,
    lat_col: str = 'LATITUDE',
    lon_col: str = 'LONGITUDE'
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Validate geographic coordinates
    
    Args:
        df: Input DataFrame
        lat_col: Name of latitude column
        lon_col: Name of longitude column
    
    Returns:
        Tuple of (cleaned DataFrame, validation stats)
    """
    stats = {'original_rows': len(df)}
    
    # Check for missing coordinates
    valid_coords = df[lat_col].notna() & df[lon_col].notna()
    df = df[valid_coords]
    stats['missing_coordinates'] = stats['original_rows'] - len(df)
    
    # Check coordinate bounds
    valid_bounds = (
        (df[lat_col] >= BOUNDS['south']) & 
        (df[lat_col] <= BOUNDS['north']) & 
        (df[lon_col] >= BOUNDS['west']) & 
        (df[lon_col] <= BOUNDS['east'])
    )
    df = df[valid_bounds]
    stats['out_of_bounds'] = stats['original_rows'] - stats['missing_coordinates'] - len(df)
    
    return df, stats

def validate_numeric_columns(
    df: pd.DataFrame,
    columns: List[str]
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    """
    Validate numeric columns using IQR method
    
    Args:
        df: Input DataFrame
        columns: List of numeric columns to validate
    
    Returns:
        Tuple of (cleaned DataFrame, validation stats)
    """
    stats = {col: {'original_rows': len(df)} for col in columns}
    
    for col in columns:
        if col not in df.columns:
            continue
            
        # Convert to numeric
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Check for missing values
        valid_values = df[col].notna()
        df = df[valid_values]
        stats[col]['missing_values'] = stats[col]['original_rows'] - len(df)
        
        # Remove negative values for certain metrics
        if col in ['BRIGHTNESS', 'SCAN', 'TRACK']:
            valid_positive = df[col] > 0
            df = df[valid_positive]
            stats[col]['negative_values'] = stats[col]['original_rows'] - \
                                          stats[col]['missing_values'] - len(df)
        
        # Remove outliers using IQR method
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        valid_range = (
            (df[col] >= Q1 - 1.5 * IQR) & 
            (df[col] <= Q3 + 1.5 * IQR)
        )
        df = df[valid_range]
        stats[col]['outliers'] = stats[col]['original_rows'] - \
                                stats[col]['missing_values'] - \
                                stats[col].get('negative_values', 0) - \
                                len(df)
    
    return df, stats

def validate_dates(
    df: pd.DataFrame,
    date_col: str = 'ACQ_DATE'
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Validate date values
    
    Args:
        df: Input DataFrame
        date_col: Name of date column
    
    Returns:
        Tuple of (cleaned DataFrame, validation stats)
    """
    stats = {'original_rows': len(df)}
    
    # Convert to datetime
    df['datetime'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Remove missing dates
    valid_dates = df['datetime'].notna()
    df = df[valid_dates]
    stats['invalid_dates'] = stats['original_rows'] - len(df)
    
    # Remove future dates
    valid_timeframe = df['datetime'] <= pd.Timestamp.now()
    df = df[valid_timeframe]
    stats['future_dates'] = stats['original_rows'] - stats['invalid_dates'] - len(df)
    
    return df, stats

def validate_required_columns(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Check if all required columns are present
    
    Args:
        df: Input DataFrame
    
    Returns:
        Tuple of (validation result, list of missing columns)
    """
    missing_columns = [col for col in COLUMNS['required'] if col not in df.columns]
    return len(missing_columns) == 0, missing_columns

def compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived fields from validated data
    
    Args:
        df: Input DataFrame with validated data
    
    Returns:
        DataFrame with additional computed fields
    """
    # Calculate fire area
    if all(col in df.columns for col in ['SCAN', 'TRACK']):
        df['fire_area'] = df['SCAN'] * df['TRACK']
    
    # Calculate normalized intensity
    if 'BRIGHTNESS' in df.columns:
        df['intensity'] = (df['BRIGHTNESS'] - df['BRIGHTNESS'].mean()) / df['BRIGHTNESS'].std()
    
    # Add confidence level if available
    if 'CONFIDENCE' in df.columns:
        df['confidence_level'] = pd.qcut(
            df['CONFIDENCE'],
            q=3,
            labels=['low', 'medium', 'high']
        )
    
    return df 