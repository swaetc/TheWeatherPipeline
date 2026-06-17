# pipeline/extractor.py
import requests
import pandas as pd
from typing import Optional
from config.settings import API_BASE_URL, LOCATIONS
from pipeline.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
class WeatherExtractor:
    """Extracts hourly weather data from the Open-Meteo API for all configured locations."""
 
    HOURLY_VARS = [
        'temperature_2m', 'relative_humidity_2m', 'precipitation',
        'wind_speed_10m', 'wind_direction_10m', 'surface_pressure',
        'cloud_cover', 'weather_code',
    ]
 
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url
 
    def _build_params(self, latitude: float, longitude: float) -> dict:
        return {
            'latitude':      latitude,
            'longitude':     longitude,
            'hourly':        ','.join(self.HOURLY_VARS),
            'timezone':      'Africa/Johannesburg',
            'forecast_days': 1,
        }
 
    def fetch_location(self, location: dict) -> Optional[pd.DataFrame]:
        """Fetch hourly data for a single location. Returns DataFrame or None."""
        name   = location['name']
        params = self._build_params(location['latitude'], location['longitude'])
 
        try:
            logger.info(f'Extracting data for {name} ...')
            resp = requests.get(self.base_url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            logger.error(f'Connection error for {name}: {e}')
            return None
        except requests.exceptions.Timeout:
            logger.error(f'Request timed out for {name}')
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f'HTTP error for {name}: {e}')
            return None
 
        data = resp.json()
        if 'hourly' not in data:
            logger.warning(f'Unexpected API response structure for {name}')
            return None
 
        df = pd.DataFrame(data['hourly'])
        df['location_name'] = name
        df['latitude']      = location['latitude']
        df['longitude']     = location['longitude']
        logger.info(f'Extracted {len(df)} rows for {name}')
        return df
 
    def extract_all(self) -> pd.DataFrame:
        """Extract data for all locations. Returns combined DataFrame."""
        frames = []
        for loc in LOCATIONS:
            df = self.fetch_location(loc)
            if df is not None:
                frames.append(df)
 
        if not frames:
            raise RuntimeError('No data extracted — check API connectivity.')
 
        combined = pd.concat(frames, ignore_index=True)
        logger.info(f'Total rows extracted: {len(combined)}')
        return combined
