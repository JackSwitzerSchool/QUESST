"""
Test script to create the wildfire visualization
"""

import logging
from wildfires.visualizer import FireVisualizer
from wildfires.data_manager import DataManager

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create data manager and load data
    dm = DataManager()
    dm.load_raw_data(force_reload=True)  # Force reload to include state information
    dm.clean_data()
    
    # Create visualizer
    visualizer = FireVisualizer(dm)
    
    # Create visualization
    visualizer.create_visualization("fire_visualization.html")

if __name__ == "__main__":
    main() 