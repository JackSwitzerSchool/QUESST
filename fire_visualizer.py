import folium
from folium.plugins import HeatMap, TimestampedGeoJson
import geopandas as gpd
import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
import pickle

class FireVisualizer:
    def __init__(self):
        self.data = None
        self.bounds = {
            'north': 70,  # Include Alaska
            'south': 25,
            'west': -170, # Include Alaska
            'east': -50   # Include eastern Canada
        }
        self.checkpoint_file = Path("data/output/fire_data_processed.pkl")
    
    def load_data(self, data_dir="data/NASA", force_reload=False):
        """Load fire data, using checkpoint if available"""
        if not force_reload and self.checkpoint_file.exists():
            print("Loading from checkpoint...")
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    self.data = pickle.load(f)
                print(f"Loaded {len(self.data):,} fire records from checkpoint")
                print(f"Date range: {self.data['datetime'].min():%Y-%m-%d} to {self.data['datetime'].max():%Y-%m-%d}")
                return
            except Exception as e:
                print(f"Error loading checkpoint: {e}. Processing from source files...")
        
        print("Loading fire data from shapefiles...")
        
        # Find all shapefiles
        shapefiles = list(Path(data_dir).glob("**/*.shp"))
        if not shapefiles:
            raise FileNotFoundError("No shapefiles found")
        
        # Use the most recent archive and NRT files
        archive_files = [f for f in shapefiles if 'archive' in f.name.lower()]
        nrt_files = [f for f in shapefiles if 'nrt' in f.name.lower()]
        
        selected_files = []
        if archive_files:
            selected_files.append(max(archive_files, key=lambda x: x.stat().st_mtime))
        if nrt_files:
            selected_files.append(max(nrt_files, key=lambda x: x.stat().st_mtime))
        
        all_data = []
        for shp in selected_files:
            print(f"Reading {shp.name}...")
            gdf = gpd.read_file(shp)
            
            # Basic data cleaning
            gdf['datetime'] = pd.to_datetime(gdf['ACQ_DATE'])
            gdf['year'] = gdf['datetime'].dt.year
            gdf['month'] = gdf['datetime'].dt.month
            
            # Filter to North America and remove invalid coordinates
            mask = (
                (gdf['LATITUDE'] >= self.bounds['south']) & 
                (gdf['LATITUDE'] <= self.bounds['north']) & 
                (gdf['LONGITUDE'] >= self.bounds['west']) & 
                (gdf['LONGITUDE'] <= self.bounds['east']) &
                (gdf['LATITUDE'].notna()) & 
                (gdf['LONGITUDE'].notna())
            )
            gdf = gdf[mask]
            
            # Clean numeric columns and remove invalid data
            numeric_cols = ['BRIGHTNESS', 'SCAN', 'TRACK', 'CONFIDENCE']
            for col in numeric_cols:
                if col in gdf.columns:
                    gdf[col] = pd.to_numeric(gdf[col], errors='coerce')
            
            # Remove rows with invalid measurements in critical columns
            critical_cols = ['BRIGHTNESS', 'SCAN', 'TRACK']
            gdf = gdf.dropna(subset=critical_cols)
            
            # Remove extreme outliers using IQR method
            for col in critical_cols:
                Q1 = gdf[col].quantile(0.25)
                Q3 = gdf[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                gdf = gdf[
                    (gdf[col] >= lower_bound) & 
                    (gdf[col] <= upper_bound)
                ]
            
            # Additional validation
            gdf = gdf[
                (gdf['BRIGHTNESS'] > 0) &  # Temperature must be positive
                (gdf['SCAN'] > 0) &        # Scan size must be positive
                (gdf['TRACK'] > 0)         # Track size must be positive
            ]
            
            all_data.append(gdf)
        
        # Combine all data
        self.data = pd.concat(all_data, ignore_index=True)
        self.data = self.data.sort_values('datetime')
        
        print(f"Loaded {len(self.data):,} fire records")
        print(f"Date range: {self.data['datetime'].min():%Y-%m-%d} to {self.data['datetime'].max():%Y-%m-%d}")
        
        # Save checkpoint
        print("Saving checkpoint...")
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(self.data, f, protocol=4)
        print("Checkpoint saved")
    
    def create_visualization(self, output_file="fire_visualization.html"):
        """Create an interactive visualization of fire data"""
        if self.data is None:
            self.load_data()
        
        # Create base map centered on North America
        m = folium.Map(
            location=[45, -100],  # Center on US
            zoom_start=4,
            tiles='cartodbpositron'
        )
        
        # Add state boundaries for context
        folium.GeoJson(
            'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json',
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#666',
                'weight': 1
            }
        ).add_to(m)
        
        # Create features for each month
        features = []
        print("Creating visualization features...")
        
        # Calculate global statistics for normalization
        brightness_mean = self.data['BRIGHTNESS'].mean()
        brightness_std = self.data['BRIGHTNESS'].std()
        
        # Calculate actual fire footprints in km²
        self.data['fire_area'] = self.data['SCAN'] * self.data['TRACK']
        area_mean = self.data['fire_area'].mean()
        area_std = self.data['fire_area'].std()
        
        for (year, month), month_data in self.data.groupby(['year', 'month']):
            # Grid-based sampling for better regional representation
            if len(month_data) > 200:
                # Create 20x20 grid
                month_data['lat_bin'] = pd.qcut(month_data['LATITUDE'], 20, labels=False)
                month_data['lon_bin'] = pd.qcut(month_data['LONGITUDE'], 20, labels=False)
                
                # Sample proportionally to fire intensity and size in each grid cell
                def smart_sample(group):
                    n = min(len(group), max(1, int(len(group) * 0.2)))  # Sample 20% or at least 1
                    weights = group['BRIGHTNESS'] * group['fire_area']
                    return group.sample(n=n, weights=weights)
                
                month_data = month_data.groupby(['lat_bin', 'lon_bin']).apply(smart_sample)
                month_data = month_data.reset_index(drop=True)
            
            for _, fire in month_data.iterrows():
                # Skip if any critical values are NaN
                if pd.isna(fire['BRIGHTNESS']) or pd.isna(fire['SCAN']) or pd.isna(fire['TRACK']):
                    continue
                
                # Calculate normalized intensity using z-score
                intensity = (fire['BRIGHTNESS'] - brightness_mean) / brightness_std
                intensity = np.clip(intensity, -2, 2)  # Clip to ±2 standard deviations
                intensity = (intensity + 2) / 4  # Normalize to [0,1]
                
                # Calculate actual fire radius in meters (from area)
                area_km2 = max(0.01, fire['SCAN'] * fire['TRACK'])  # Minimum 0.01 km² to avoid zero area
                radius_meters = np.sqrt(area_km2 * 1_000_000 / np.pi)  # Convert km² to m²
                
                # Scale radius for visibility - use smaller base size
                base_radius = np.clip(radius_meters / 1000, 5, 50)  # Much smaller base size, 5-50 pixels
                
                # Color based on intensity and confidence
                if intensity < 0.33:
                    color = '#2196f3'  # blue for cooler/smaller fires
                elif intensity < 0.66:
                    color = '#ff9800'  # orange for medium fires
                else:
                    color = '#f44336'  # red for intense fires
                
                feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [float(fire['LONGITUDE']), float(fire['LATITUDE'])]
                    },
                    'properties': {
                        'time': f"{year}-{month:02d}-01",
                        'style': {
                            'color': color,
                            'fillColor': color,
                            'fillOpacity': 0.6,
                            'weight': 1,
                            'radius': base_radius,
                            'bubblingMouseEvents': True
                        },
                        'icon': 'circle',
                        'popup': (
                            f"<div style='font-family: Arial; font-size: 12px;'>"
                            f"<b>Fire Detection</b><br>"
                            f"Date: {fire['ACQ_DATE']}<br>"
                            f"Temperature: {fire['BRIGHTNESS']:.1f}K<br>"
                            f"Area: {area_km2:.2f} km²<br>"
                            f"Satellite: {fire['SATELLITE']}"
                            f"</div>"
                        )
                    }
                }
                features.append(feature)
        
        print(f"Created {len(features)} visualization features")
        
        # Add the time slider with fire points
        TimestampedGeoJson(
            {
                'type': 'FeatureCollection',
                'features': features
            },
            period='P1M',
            duration='P15D',
            transition_time=300,  # Slightly longer transition for smoother animation
            auto_play=True,
            loop=True,
            max_speed=1.0,  # Increased max speed
            date_options='YYYY-MM'
        ).add_to(m)
        
        # Add enhanced playback controls and time display
        playback_html = '''
            <div style="
                position: fixed;
                bottom: 50px;
                left: 50%;
                transform: translateX(-50%);
                z-index: 9999;
                background-color: white;
                padding: 15px;
                border-radius: 8px;
                font-family: Arial;
                font-size: 14px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.2);
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 10px;
                min-width: 300px;">
                <div id="date-display" style="font-size: 18px; font-weight: bold;"></div>
                <div style="width: 100%; height: 4px; background: #eee; border-radius: 2px;">
                    <div id="progress-bar" style="width: 0%; height: 100%; background: #2196f3; border-radius: 2px; transition: width 0.3s;"></div>
                </div>
                <div style="display: flex; gap: 15px; align-items: center;">
                    <button id="play-pause" style="
                        padding: 8px 15px;
                        border: none;
                        border-radius: 4px;
                        background: #2196f3;
                        color: white;
                        cursor: pointer;
                        font-size: 14px;">
                        Pause
                    </button>
                    <select id="speed-control" style="
                        padding: 8px;
                        border: 1px solid #ddd;
                        border-radius: 4px;
                        font-size: 14px;">
                        <option value="0.25">0.25x</option>
                        <option value="0.5">0.5x</option>
                        <option value="1" selected>1x</option>
                        <option value="2">2x</option>
                        <option value="4">4x</option>
                    </select>
                </div>
            </div>
            <script>
                // Initialize variables
                var isPlaying = true;
                var speedControl = document.getElementById('speed-control');
                var playPauseButton = document.getElementById('play-pause');
                var dateDisplay = document.getElementById('date-display');
                var progressBar = document.getElementById('progress-bar');
                
                // Get time range
                var timeSlider;
                var startTime, endTime;
                
                setTimeout(function() {
                    timeSlider = document.querySelector(".timeSlider");
                    if (timeSlider) {
                        // Extract start and end dates from the time slider
                        var timeInfo = timeSlider.getAttribute('data-daterangelimits');
                        if (timeInfo) {
                            var dates = timeInfo.split('/');
                            startTime = new Date(dates[0]);
                            endTime = new Date(dates[1]);
                        }
                        
                        // Update progress and date display
                        var observer = new MutationObserver(function(mutations) {
                            mutations.forEach(function(mutation) {
                                if (mutation.type === "attributes") {
                                    var currentDate = new Date(timeSlider.getAttribute('time'));
                                    
                                    // Update date display
                                    dateDisplay.textContent = currentDate.toLocaleDateString('en-US', {
                                        year: 'numeric',
                                        month: 'long'
                                    });
                                    
                                    // Update progress bar
                                    if (startTime && endTime) {
                                        var progress = (currentDate - startTime) / (endTime - startTime) * 100;
                                        progressBar.style.width = progress + '%';
                                    }
                                }
                            });
                        });
                        
                        observer.observe(timeSlider, { attributes: true });
                        
                        // Play/Pause functionality
                        playPauseButton.addEventListener('click', function() {
                            var playButton = document.querySelector('.leaflet-control-timecontrol-play');
                            var pauseButton = document.querySelector('.leaflet-control-timecontrol-pause');
                            if (isPlaying) {
                                if (pauseButton) pauseButton.click();
                                playPauseButton.textContent = 'Play';
                            } else {
                                if (playButton) playButton.click();
                                playPauseButton.textContent = 'Pause';
                            }
                            isPlaying = !isPlaying;
                        });
                        
                        // Speed control
                        speedControl.addEventListener('change', function() {
                            var speed = parseFloat(this.value);
                            var speedButton = document.querySelector('.leaflet-control-timecontrol-speed');
                            if (speedButton) {
                                // Click the speed button until we reach desired speed
                                var currentSpeed = 1;
                                var clicksNeeded = 0;
                                
                                if (speed < 1) {
                                    while (currentSpeed > speed) {
                                        currentSpeed /= 2;
                                        clicksNeeded++;
                                    }
                                } else {
                                    while (currentSpeed < speed) {
                                        currentSpeed *= 2;
                                        clicksNeeded++;
                                    }
                                }
                                
                                for (var i = 0; i < clicksNeeded; i++) {
                                    speedButton.click();
                                }
                            }
                        });
                    }
                }, 1000);
            </script>
        '''
        
        m.get_root().html.add_child(folium.Element(playback_html))
        
        # Add zoom-based scaling for circles
        zoom_scale_js = '''
            <script>
            var map = document.querySelector('#map');  // Get the map element
            
            // Add zoom handler
            map.addEventListener('zoomend', function() {
                var zoom = document._leaflet_map.getZoom();
                var circles = document.querySelectorAll('.leaflet-marker-icon');
                
                circles.forEach(function(circle) {
                    var baseRadius = parseFloat(circle.style.width) / 2;  // Get original radius
                    var scale = Math.pow(1.5, zoom - 4);  // Scale factor based on zoom level
                    var newRadius = baseRadius / scale;
                    
                    // Limit minimum and maximum sizes
                    newRadius = Math.max(3, Math.min(newRadius, 50));
                    
                    circle.style.width = (newRadius * 2) + 'px';
                    circle.style.height = (newRadius * 2) + 'px';
                    circle.style.marginLeft = -newRadius + 'px';
                    circle.style.marginTop = -newRadius + 'px';
                });
            });
            </script>
        '''
        
        m.get_root().html.add_child(folium.Element(zoom_scale_js))
        
        print(f"Saving visualization to {output_file}...")
        m.save(output_file)
        print("Done!")

def main():
    visualizer = FireVisualizer()
    visualizer.create_visualization()

if __name__ == "__main__":
    main() 