"""
Visualization module for the Wildfire Analysis System
Creates interactive map-based visualizations of wildfire data
"""

import folium
from folium.plugins import HeatMap, TimestampedGeoJson
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Optional, Dict, List, Tuple
from .config import VIS_SETTINGS, PERFORMANCE, BOUNDS
from .data_manager import DataManager

class FireVisualizer:
    def __init__(self, data_manager: Optional[DataManager] = None):
        """
        Initialize the visualizer
        
        Args:
            data_manager: Optional DataManager instance. If not provided, will create new one.
        """
        self.data_manager = data_manager or DataManager()
        self.map = None
        self.features = []
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Define seasons for aggregation
        self.seasons = {
            'Winter': [12, 1, 2],
            'Spring': [3, 4, 5],
            'Summer': [6, 7, 8],
            'Fall': [9, 10, 11]
        }
        
        # Constants for size scaling
        self.EARTH_RADIUS_KM = 6371  # Earth's radius in kilometers
        self.PIXELS_PER_KM = 0.05  # Reduced base scale for better visibility
    
    def _create_base_map(self) -> folium.Map:
        """Create the base map with initial configuration"""
        m = folium.Map(
            location=VIS_SETTINGS['map']['default_center'],
            zoom_start=VIS_SETTINGS['map']['default_zoom'],
            tiles='cartodbpositron',
            min_zoom=VIS_SETTINGS['map']['min_zoom'],
            max_zoom=VIS_SETTINGS['map']['max_zoom']
        )
        
        # Add state/province boundaries for context
        folium.GeoJson(
            'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json',
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#666',
                'weight': 1
            }
        ).add_to(m)
        
        return m
    
    def _add_state_choropleth(self, state_data: List[Dict]) -> None:
        """Add state-level choropleth layer"""
        # Convert state data to proper format for choropleth
        state_df = pd.DataFrame(state_data)
        
        # Convert fire area to numeric and handle any missing values
        state_df['fire_area'] = pd.to_numeric(state_df['fire_area'], errors='coerce')
        state_df = state_df.dropna()
        
        # Create choropleth layer for fire area
        folium.Choropleth(
            geo_data='https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json',
            name='Fire Area Heat Map',
            data=state_df,
            columns=['state', 'fire_area'],
            key_on='feature.id',
            fill_color='YlOrRd',
            fill_opacity=0.3,
            line_opacity=0.2,
            legend_name='Total Fire Area (km²)',
            highlight=True,
            bins=8
        ).add_to(self.map)
        
        # Add hover tooltips with detailed information
        style_function = lambda x: {'fillColor': '#ffffff', 
                                  'color':'#000000', 
                                  'fillOpacity': 0.1, 
                                  'weight': 0.1}
        highlight_function = lambda x: {'fillColor': '#000000', 
                                      'color':'#000000', 
                                      'fillOpacity': 0.50, 
                                      'weight': 0.1}
        
        # Create tooltip HTML with both area and count
        state_info = {}
        for _, row in state_df.iterrows():
            state_info[row['state']] = {
                'area': f"{row['fire_area']:,.0f}",
                'count': f"{row['fire_count']:,}"
            }
        
        def tooltip_html(feature):
            state_id = feature['id']
            if state_id in state_info:
                info = state_info[state_id]
                return f"""
                    <div style="font-family: Arial; font-size: 12px; padding: 10px;">
                        <b>{feature['properties']['name']}</b><br>
                        Total Fire Area: {info['area']} km²<br>
                        Number of Fires: {info['count']}
                    </div>
                """
            return feature['properties']['name']
        
        NIL = folium.features.GeoJson(
            'https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json',
            style_function=style_function,
            control=False,
            highlight_function=highlight_function,
            tooltip=folium.features.GeoJsonTooltip(
                fields=['name'],
                aliases=[''],
                style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
            )
        )
        self.map.add_child(NIL)
        self.map.keep_in_front(NIL)
    
    def _calculate_pixel_radius(self, area_km2: float, latitude: float, zoom: int) -> float:
        """
        Calculate the pixel radius for a fire based on its actual area
        
        Args:
            area_km2: Fire area in square kilometers
            latitude: Latitude of the fire (for Mercator projection correction)
            zoom: Current zoom level
        
        Returns:
            Radius in pixels
        """
        # Convert area to radius in km (assuming circular fire)
        radius_km = np.sqrt(area_km2 / np.pi)
        
        # Correct for Mercator projection distortion
        lat_rad = np.radians(latitude)
        mercator_correction = np.cos(lat_rad)
        
        # Convert to pixels with zoom scaling
        # At zoom level 0, one pixel represents 156.543 km at the equator
        # Each zoom level doubles the number of pixels
        km_per_pixel = 156.543 / (2 ** zoom)
        pixel_radius = (radius_km * mercator_correction) / km_per_pixel * self.PIXELS_PER_KM
        
        # Apply minimum and maximum limits
        return np.clip(pixel_radius, 
                      VIS_SETTINGS['fire_markers']['min_radius'],
                      VIS_SETTINGS['fire_markers']['max_radius'])
    
    def _create_fire_feature(self, row: pd.Series, base_zoom: int = 4) -> Dict:
        """Create a GeoJSON feature for a single fire record"""
        # Calculate normalized intensity (0-1 scale)
        intensity = (row['intensity'] - self.intensity_min) / (self.intensity_max - self.intensity_min)
        intensity = max(0, min(1, intensity))  # Clip to [0,1]
        
        # Determine color based on intensity
        if intensity < 0.33:
            color = VIS_SETTINGS['colors']['low_intensity']
        elif intensity < 0.66:
            color = VIS_SETTINGS['colors']['medium_intensity']
        else:
            color = VIS_SETTINGS['colors']['high_intensity']
        
        # Calculate radius based on actual fire area
        radius = self._calculate_pixel_radius(row['fire_area'], row['LATITUDE'], base_zoom)
        
        return {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [float(row['LONGITUDE']), float(row['LATITUDE'])]
            },
            'properties': {
                'time': row['period_start'].strftime('%Y-%m-%d'),
                'style': {
                    'color': color,
                    'fillColor': color,
                    'fillOpacity': VIS_SETTINGS['fire_markers']['base_opacity'],
                    'weight': 1,
                    'radius': radius
                },
                'icon': 'circle',
                'popup': (
                    f"<div style='font-family: Arial; font-size: 12px;'>"
                    f"<b>Fire Activity</b><br>"
                    f"Period: {row['season']} {row['year']}<br>"
                    f"Average Temperature: {row['BRIGHTNESS']:.1f}K<br>"
                    f"Total Area: {row['fire_area']:.2f} km²<br>"
                    f"Intensity: {row['intensity']:.2f}<br>"
                    f"Fires in Location: {int(row['fire_count']):,}"
                    f"</div>"
                )
            }
        }
    
    def _aggregate_by_location_and_season(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Aggregate data by unique location and season"""
        # Round coordinates to reduce noise while maintaining distinct locations
        data['LATITUDE_BIN'] = (data['LATITUDE'] * 10).round() / 10
        data['LONGITUDE_BIN'] = (data['LONGITUDE'] * 10).round() / 10
        
        # Aggregate by location bins and season
        seasonal_data = data.groupby([
            'year', 'season', 'period_start', 
            'LATITUDE_BIN', 'LONGITUDE_BIN'
        ]).agg({
            'LATITUDE': 'first',
            'LONGITUDE': 'first',
            'BRIGHTNESS': 'mean',
            'fire_area': 'sum',
            'intensity': 'mean',
            'state': 'first',
            'datetime': 'count'  # Use datetime for count instead of LATITUDE
        }).reset_index()
        
        # Rename count column to fire_count
        seasonal_data = seasonal_data.rename(columns={'datetime': 'fire_count'})
        
        # Calculate state-level statistics
        state_data = data.groupby('state').agg({
            'fire_area': 'sum',
            'datetime': 'count'  # Use datetime for count
        }).reset_index()
        
        # Rename count column and format for choropleth
        state_data = state_data.rename(columns={'datetime': 'fire_count'})
        
        return seasonal_data, state_data.to_dict('records')
    
    def create_visualization(self, output_file: str = "fire_visualization.html") -> None:
        """
        Create an interactive visualization of fire data
        
        Args:
            output_file: Path to save the output HTML file
        """
        logging.info("Creating visualization...")
        
        # Load data if not already loaded
        if self.data_manager.processed_data is None:
            self.data_manager.load_processed_data()
            if self.data_manager.processed_data is None:
                raise ValueError("No processed data available")
        
        # Get time series data and aggregate by season
        data = self.data_manager.get_time_series()
        logging.info(f"Processing {len(data):,} fire records")
        
        # Add season information
        data['season'] = data['datetime'].dt.month.map(
            lambda m: next(season for season, months in self.seasons.items() if m in months)
        )
        data['year'] = data['datetime'].dt.year
        
        # Create seasonal periods
        data['period_start'] = data.apply(
            lambda row: pd.Timestamp(f"{row['year']}-{self.seasons[row['season']][0]:02d}-01"),
            axis=1
        )
        
        # Aggregate data while maintaining location granularity
        seasonal_data, state_data = self._aggregate_by_location_and_season(data)
        logging.info(f"Created {len(seasonal_data)} location-based seasonal records")
        
        # Calculate global statistics for normalization
        self.intensity_min = seasonal_data['intensity'].min()
        self.intensity_max = seasonal_data['intensity'].max()
        self.area_min = seasonal_data['fire_area'].min()
        self.area_max = seasonal_data['fire_area'].max()
        
        # Create base map
        self.map = self._create_base_map()
        self._add_state_choropleth(state_data)
        
        # Create features for each location-season combination
        features = []
        for _, group in seasonal_data.groupby(['year', 'season']):
            if len(group) > PERFORMANCE['max_points_per_frame']:
                # Sample points, weighted by intensity and area
                weights = group['intensity'].abs() * group['fire_area']
                group = group.sample(
                    n=PERFORMANCE['max_points_per_frame'],
                    weights=weights,
                    random_state=42
                )
            features.extend(self._create_fire_feature(row) for _, row in group.iterrows())
        
        logging.info(f"Created {len(features)} visualization features")
        
        # Add the time slider with fire points
        TimestampedGeoJson(
            {
                'type': 'FeatureCollection',
                'features': features
            },
            period='P3M',  # 3 months per period
            duration='P3M',  # Show each season for its full duration
            transition_time=VIS_SETTINGS['animation']['transition_time'],
            auto_play=True,
            loop=True
        ).add_to(self.map)
        
        # Add layer control
        folium.LayerControl().add_to(self.map)
        
        # Add enhanced playback controls
        self._add_playback_controls()
        
        # Add zoom-based scaling for circles
        self._add_zoom_scaling()
        
        logging.info(f"Saving visualization to {output_file}...")
        self.map.save(output_file)
        logging.info("Visualization created successfully!")
    
    def _add_playback_controls(self) -> None:
        """Add enhanced playback controls to the map"""
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
        '''
        
        # Add JavaScript for control functionality
        control_js = '''
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
                        // Extract start and end dates
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
        
        self.map.get_root().html.add_child(folium.Element(playback_html + control_js))
    
    def _add_zoom_scaling(self) -> None:
        """Add zoom-based scaling for fire markers"""
        zoom_scale_js = '''
            <script>
            var map = document.querySelector('#map');
            
            // Add zoom handler
            map.addEventListener('zoomend', function() {
                var zoom = document._leaflet_map.getZoom();
                var circles = document.querySelectorAll('.leaflet-marker-icon');
                
                circles.forEach(function(circle) {
                    var baseRadius = parseFloat(circle.style.width) / 2;
                    var scale = Math.pow(1.5, zoom - 4);
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
        
        self.map.get_root().html.add_child(folium.Element(zoom_scale_js)) 