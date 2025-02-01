import pandas as pd
import requests
from pathlib import Path
import logging
from datetime import datetime
import geopandas as gpd
from tqdm import tqdm
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FireDataCollector:
    """Collects wildfire data from multiple sources"""
    
    def __init__(self):
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different data sources
        self.dirs = {
            'nifc': self.data_dir / 'nifc',
            'mtbs': self.data_dir / 'mtbs',
            'firms': self.data_dir / 'firms',
            'combined': self.data_dir / 'combined'
        }
        for dir_path in self.dirs.values():
            dir_path.mkdir(exist_ok=True)

    def download_nifc_data(self, start_year=2000, end_year=None):
        """Download NIFC fire perimeter and incident data"""
        if end_year is None:
            end_year = datetime.now().year

        logger.info(f"Downloading NIFC data from {start_year} to {end_year}")
        
        # Perimeter data
        perimeter_url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-perimeters.csv"
        df_perimeters = pd.read_csv(perimeter_url)
        
        # Incident data
        incident_url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-locations.csv"
        df_incidents = pd.read_csv(incident_url)
        
        # Filter by year range
        df_perimeters = df_perimeters[
            (df_perimeters['FIRE_YEAR'] >= start_year) & 
            (df_perimeters['FIRE_YEAR'] <= end_year)
        ]
        df_incidents = df_incidents[
            (df_incidents['FIRE_YEAR'] >= start_year) & 
            (df_incidents['FIRE_YEAR'] <= end_year)
        ]
        
        # Save data
        df_perimeters.to_csv(self.dirs['nifc'] / 'fire_perimeters.csv', index=False)
        df_incidents.to_csv(self.dirs['nifc'] / 'fire_incidents.csv', index=False)
        
        return df_perimeters, df_incidents

    def download_mtbs_data(self):
        """Download MTBS (Monitoring Trends in Burn Severity) data"""
        logger.info("Downloading MTBS burn severity data")
        
        mtbs_url = "https://www.mtbs.gov/api/resources/csv/burns/CONUS"
        df_mtbs = pd.read_csv(mtbs_url)
        df_mtbs.to_csv(self.dirs['mtbs'] / 'burn_severity.csv', index=False)
        
        return df_mtbs

    def combine_datasets(self):
        """Combine and clean all downloaded datasets"""
        logger.info("Combining datasets...")
        
        # Load all datasets
        df_perimeters = pd.read_csv(self.dirs['nifc'] / 'fire_perimeters.csv')
        df_incidents = pd.read_csv(self.dirs['nifc'] / 'fire_incidents.csv')
        df_mtbs = pd.read_csv(self.dirs['mtbs'] / 'burn_severity.csv')
        
        # Combine datasets based on common fields
        # (This will need to be customized based on the actual data structure)
        combined = pd.merge(
            df_perimeters,
            df_incidents,
            on=['FIRE_YEAR', 'INCIDENT_NAME'],
            how='outer',
            suffixes=('_perimeter', '_incident')
        )
        
        # Save combined dataset
        combined.to_csv(self.dirs['combined'] / 'all_fire_data.csv', index=False)
        
        return combined

    def generate_summary(self, df):
        """Generate summary statistics from the combined dataset"""
        summary = {
            'total_fires': len(df),
            'fires_by_year': df['FIRE_YEAR'].value_counts().sort_index(),
            'fires_by_state': df['STATE'].value_counts(),
            'total_acres_burned': df['FIRE_SIZE'].sum(),
            'avg_fire_size': df['FIRE_SIZE'].mean()
        }
        
        # Save summary
        with open(self.dirs['combined'] / 'summary.txt', 'w') as f:
            for key, value in summary.items():
                f.write(f"\n{key}:\n{value}\n")
        
        return summary

def main():
    collector = FireDataCollector()
    
    # Download data from all sources
    perimeters, incidents = collector.download_nifc_data(start_year=2000)
    mtbs_data = collector.download_mtbs_data()
    
    # Combine datasets
    combined_data = collector.combine_datasets()
    
    # Generate summary
    summary = collector.generate_summary(combined_data)
    
    # Print summary
    print("\nData Collection Summary:")
    print(f"Total fires: {summary['total_fires']:,}")
    print("\nFires by year:")
    print(summary['fires_by_year'])
    print("\nTop 5 states by number of fires:")
    print(summary['fires_by_state'].head())
    print(f"\nTotal acres burned: {summary['total_acres_burned']:,.2f}")
    print(f"Average fire size: {summary['avg_fire_size']:,.2f} acres")

if __name__ == "__main__":
    main() 