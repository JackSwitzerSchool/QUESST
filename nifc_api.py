import requests
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

class NIFCApi:
    """Client for the NIFC ArcGIS REST API"""
    
    def __init__(self, api_key: str):
        """Initialize the NIFC API client"""
        self.endpoint = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Public_Wildfire_Perimeters_View/FeatureServer/0"
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)

    def _make_request(
        self, 
        params: Dict,
        is_query: bool = True
    ) -> Optional[Dict]:
        """Make a request to the API with error handling"""
        try:
            # Add API key to parameters
            params.update({
                'f': 'json',
                'token': self.api_key
            })
            
            url = f"{self.endpoint}/query" if is_query else self.endpoint
            self.logger.info(f"Making request to: {url}")
            self.logger.debug(f"Parameters: {params}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'error' in data:
                self.logger.error(f"API Error: {data['error']}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return None

    def get_yearly_summary(self, year: int) -> Optional[Dict]:
        """Get summary statistics for a specific year"""
        params = {
            'where': f'FIRE_YEAR = {year}',
            'outStatistics': [
                {
                    'statisticType': 'count',
                    'onStatisticField': 'OBJECTID',
                    'outStatisticFieldName': 'TOTAL_FIRES'
                },
                {
                    'statisticType': 'sum',
                    'onStatisticField': 'FIRE_SIZE',
                    'outStatisticFieldName': 'TOTAL_ACRES'
                },
                {
                    'statisticType': 'avg',
                    'onStatisticField': 'FIRE_SIZE',
                    'outStatisticFieldName': 'AVG_FIRE_SIZE'
                }
            ],
            'returnGeometry': 'false'
        }
        
        return self._make_request(params)

    def get_fires(
        self,
        year: int,
        fields: List[str] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> Optional[List[Dict]]:
        """Get fire records for a specific year"""
        if fields is None:
            fields = [
                'FIRE_YEAR', 'DISCOVERY_DATE', 'CONT_DATE',
                'FIRE_SIZE', 'STATE', 'INCIDENT_NAME',
                'FIRE_SIZE_CLASS'
            ]
            
        params = {
           