"""
Configuration settings for the Wildfire Analysis System
"""

from pathlib import Path
from typing import Dict, List

# Data paths
DATA_DIR = Path("data/NASA")
PROCESSED_DIR = Path("data/processed")
CACHE_DIR = PROCESSED_DIR / "cache"
OUTPUT_DIR = Path("output")

# Geographic bounds for North America
BOUNDS = {
    'north': 70,  # Include Alaska
    'south': 25,
    'west': -170, # Include Alaska
    'east': -50   # Include eastern Canada
}

# Seasonal definitions
SEASONS = {
    'Winter': [12, 1, 2],
    'Spring': [3, 4, 5],
    'Summer': [6, 7, 8],
    'Fall': [9, 10, 11]
}

# Visualization settings
VIS_SETTINGS = {
    'map': {
        'default_zoom': 4,
        'default_center': [45, -100],  # Center on North America
        'min_zoom': 2,
        'max_zoom': 12
    },
    'fire_markers': {
        'min_radius': 2,     # Minimum radius in pixels
        'max_radius': 20,    # Maximum radius in pixels
        'base_opacity': 0.7,
        'highlight_opacity': 0.9
    },
    'colors': {
        'low_intensity': '#2196f3',    # Blue
        'medium_intensity': '#ff9800',  # Orange
        'high_intensity': '#f44336'     # Red
    },
    'animation': {
        'transition_time': 500,  # milliseconds
        'duration': 'P3M',      # 3 months per frame
        'period': 'P3M',        # 3 months between frames
        'speeds': [0.25, 0.5, 1, 2, 4]  # Available playback speeds
    }
}

# Performance settings
PERFORMANCE = {
    'max_points_per_frame': 1000,  # Maximum number of points to display per frame
    'cache_timeout': 3600,         # Cache timeout in seconds
    'chunk_size': 10000           # Number of records to process at once
}

# File patterns
FILE_PATTERNS = {
    'shapefiles': '**/*.shp',
    'csv_files': '**/*.csv'
}

# Column names and types
COLUMNS = {
    'required': [
        'LATITUDE',
        'LONGITUDE',
        'BRIGHTNESS',
        'SCAN',
        'TRACK',
        'ACQ_DATE'
    ],
    'optional': [
        'CONFIDENCE',
        'SATELLITE',
        'INSTRUMENT',
        'VERSION'
    ],
    'computed': [
        'fire_area',
        'intensity',
        'season'
    ]
}

# Logging configuration
LOGGING = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': PROCESSED_DIR / 'processing.log'
} 