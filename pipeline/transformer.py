# pipeline/transformer.py
import pandas as pd
from pipeline.utils.logger import get_logger
from pipeline.utils.validators import validate_schema, validate_ranges
 
logger = get_logger(__name__)
 
 
class WeatherTransformer:
    """Cleans, validates, and enriches raw weather data."""
 
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info('Starting transformation ...')
        df = self._clean_column_names(df)
        if not validate_schema(df):
            raise ValueError('Schema validation failed — aborting transform.')
        df = self._convert_types(df)
        df = self._handle_missing(df)
        df = self._remove_duplicates(df)
        df = validate_ranges(df)
        df = self._add_derived_fields(df)
        df = self._standardize_location(df)
        logger.info(f'Transformation complete. Output rows: {len(df)}')
        return df
 
    def _clean_column_names(self, df):
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        return df
 
    def _convert_types(self, df):
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        numeric_cols = [
            'temperature_2m', 'relative_humidity_2m', 'precipitation',
            'wind_speed_10m', 'wind_direction_10m', 'surface_pressure',
            'cloud_cover', 'weather_code', 'latitude', 'longitude',
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
 
    def _handle_missing(self, df):
        before = len(df)
        df = df.dropna(subset=['time'])
        logger.info(f'Dropped {before - len(df)} rows with null timestamps')
        numeric_cols = df.select_dtypes(include='number').columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
        return df
 
    def _remove_duplicates(self, df):
        before = len(df)
        df = df.drop_duplicates(subset=['time', 'location_name'])
        logger.info(f'Removed {before - len(df)} duplicate rows')
        return df
 
    def _add_derived_fields(self, df):
        df['date']  = df['time'].dt.date
        df['hour']  = df['time'].dt.hour
        df['month'] = df['time'].dt.month
        df['year']  = df['time'].dt.year
        if 'temperature_2m' in df.columns and 'relative_humidity_2m' in df.columns:
            T = df['temperature_2m']
            H = df['relative_humidity_2m']
            df['heat_index'] = (-8.78469475556
                + 1.61139411  * T + 2.33854883889 * H
                - 0.14611605  * T * H - 0.012308094  * T**2
                - 0.0164248277778 * H**2 + 0.002211732 * T**2 * H
                + 0.00072546  * T * H**2 - 0.000003582 * T**2 * H**2
            ).round(2)
        return df
 
    def _standardize_location(self, df):
        df['location_name'] = df['location_name'].str.strip().str.title()
        return df
