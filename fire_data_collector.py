import pandas as pd
import requests
from pathlib import Path
import logging
from datetime import datetime
from tqdm import tqdm
import os
import json

logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose output
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

    def test_nifc_connection(self):
        """Test NIFC data connection and get available fields"""
        print("\nTesting NIFC data connection...")
        url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-perimeters.json"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            metadata = response.json()
            
            print("\nAvailable fields:")
            for field in metadata.get('fields', []):
                print(f"- {field['name']}: {field['type']}")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def download_single_year(self, year: int):
        """Download data for a single year as a test"""
        print(f"\nDownloading test data for year {year}...")
        
        url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-perimeters.csv"
        params = {
            'where': f'FIRE_YEAR = {year}',
            'outFields': '*'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            output_file = self.data_dir / f'fires_{year}.csv'
            with open(output_file, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_csv(output_file)
            print(f"\nDownloaded {len(df)} records for {year}")
            print("\nSample data:")
            print(df.head())
            return df
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return None

    def download_nifc_data(self, start_year=2000, end_year=None):
        """Download NIFC fire perimeter and incident data"""
        if end_year is None:
            end_year = datetime.now().year

        logger.info(f"Downloading NIFC data from {start_year} to {end_year}")
        
        # Check for existing data
        perimeter_file = self.dirs['nifc'] / 'fire_perimeters.csv'
        incident_file = self.dirs['nifc'] / 'fire_incidents.csv'
        
        try:
            # Perimeter data
            print("\nDownloading fire perimeter data...")
            perimeter_url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-perimeters.csv"
            
            with requests.get(perimeter_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                
                with open(perimeter_file, 'wb') as f, tqdm(
                    desc="Perimeters",
                    total=total_size,
                    unit='iB',
                    unit_scale=True
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        pbar.update(size)
            
            print("Loading and filtering perimeter data...")
            df_perimeters = pd.read_csv(perimeter_file)
            df_perimeters = df_perimeters[
                (df_perimeters['FIRE_YEAR'] >= start_year) & 
                (df_perimeters['FIRE_YEAR'] <= end_year)
            ]
            df_perimeters.to_csv(perimeter_file, index=False)
            print(f"Saved {len(df_perimeters):,} perimeter records")
            
            # Incident data
            print("\nDownloading fire incident data...")
            incident_url = "https://opendata.arcgis.com/datasets/nifc::public-wildland-fire-locations.csv"
            
            with requests.get(incident_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                
                with open(incident_file, 'wb') as f, tqdm(
                    desc="Incidents",
                    total=total_size,
                    unit='iB',
                    unit_scale=True
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        pbar.update(size)
            
            print("Loading and filtering incident data...")
            df_incidents = pd.read_csv(incident_file)
            df_incidents = df_incidents[
                (df_incidents['FIRE_YEAR'] >= start_year) & 
                (df_incidents['FIRE_YEAR'] <= end_year)
            ]
            df_incidents.to_csv(incident_file, index=False)
            print(f"Saved {len(df_incidents):,} incident records")
            
            return df_perimeters, df_incidents
            
        except Exception as e:
            logger.error(f"Error downloading NIFC data: {e}")
            # Try to load existing data if download fails
            if perimeter_file.exists() and incident_file.exists():
                logger.info("Loading existing NIFC data...")
                return pd.read_csv(perimeter_file), pd.read_csv(incident_file)
            raise

    def download_mtbs_data(self):
        """Download MTBS burn severity data"""
        logger.info("Downloading MTBS burn severity data")
        
        mtbs_file = self.dirs['mtbs'] / 'burn_severity.csv'
        
        try:
            print("\nDownloading MTBS burn severity data...")
            mtbs_url = "https://www.mtbs.gov/api/resources/csv/burns/CONUS"
            
            with requests.get(mtbs_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                
                with open(mtbs_file, 'wb') as f, tqdm(
                    desc="MTBS Data",
                    total=total_size,
                    unit='iB',
                    unit_scale=True
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        pbar.update(size)
            
            print("Loading MTBS data...")
            df_mtbs = pd.read_csv(mtbs_file)
            print(f"Saved {len(df_mtbs):,} MTBS records")
            return df_mtbs
            
        except Exception as e:
            logger.error(f"Error downloading MTBS data: {e}")
            if mtbs_file.exists():
                logger.info("Loading existing MTBS data...")
                return pd.read_csv(mtbs_file)
            raise

    def combine_datasets(self):
        """Combine and clean all downloaded datasets"""
        logger.info("Combining datasets...")
        
        combined_file = self.dirs['combined'] / 'all_fire_data.csv'
        
        # Check for existing combined data
        if combined_file.exists():
            print("\nFound existing combined dataset. Loading...")
            return pd.read_csv(combined_file)
        
        print("\nLoading individual datasets...")
        try:
            df_perimeters = pd.read_csv(self.dirs['nifc'] / 'fire_perimeters.csv')
            df_incidents = pd.read_csv(self.dirs['nifc'] / 'fire_incidents.csv')
            df_mtbs = pd.read_csv(self.dirs['mtbs'] / 'burn_severity.csv')
            
            print(f"Loaded {len(df_perimeters):,} perimeters, {len(df_incidents):,} incidents, {len(df_mtbs):,} MTBS records")
            
            print("Merging datasets...")
            combined = pd.merge(
                df_perimeters,
                df_incidents,
                on=['FIRE_YEAR', 'INCIDENT_NAME'],
                how='outer',
                suffixes=('_perimeter', '_incident')
            )
            
            print(f"Saving combined dataset with {len(combined):,} records...")
            combined.to_csv(combined_file, index=False)
            
            return combined
            
        except Exception as e:
            logger.error(f"Error combining datasets: {e}")
            if combined_file.exists():
                logger.info("Loading existing combined data...")
                return pd.read_csv(combined_file)
            raise

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
    
    # Test connection first
    if not collector.test_nifc_connection():
        print("Connection test failed. Exiting...")
        return
    
    # Test with a single recent year
    test_year = 2023
    print(f"\nTesting with year {test_year}...")
    df = collector.download_single_year(test_year)
    
    if df is not None:
        print("\nTest successful!")
        proceed = input("\nProceed with full download (1970-2023)? (y/n): ")
        if proceed.lower() == 'y':
            print("\nImplementing full download...")
            # We'll implement the full download after confirming the test works
    
    try:
        print("\n=== Starting Fire Data Collection ===")
        
        # Download data from all sources
        print("\nStep 1: Downloading NIFC Data")
        perimeters, incidents = collector.download_nifc_data(start_year=2000)
        
        print("\nStep 2: Downloading MTBS Data")
        mtbs_data = collector.download_mtbs_data()
        
        print("\nStep 3: Combining Datasets")
        combined_data = collector.combine_datasets()
        
        print("\nStep 4: Generating Summary")
        summary = collector.generate_summary(combined_data)
        
        # Print summary
        print("\n=== Data Collection Summary ===")
        print(f"Total fires: {summary['total_fires']:,}")
        print("\nFires by year:")
        print(summary['fires_by_year'])
        print("\nTop 5 states by number of fires:")
        print(summary['fires_by_state'].head())
        print(f"\nTotal acres burned: {summary['total_acres_burned']:,.2f}")
        print(f"Average fire size: {summary['avg_fire_size']:,.2f} acres")
        
        print("\nData collection complete! Files saved in ./data directory")
        
    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        raise

if __name__ == "__main__":
    main() 