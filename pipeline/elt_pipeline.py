# pipeline/elt_pipeline.py
import pandas as pd
from sqlalchemy import create_engine, text
from config.settings import DATABASE_URL
from pipeline.extractor import WeatherExtractor
from pipeline.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
class ELTPipeline:
    """ELT: Extract raw -> Load to staging -> Transform inside DB via SQL."""
 
    def __init__(self):
        self.engine    = create_engine(DATABASE_URL)
        self.extractor = WeatherExtractor()
 
    def _ensure_staging_table(self) -> None:
        ddl = '''
        CREATE TABLE IF NOT EXISTS staging_weather (
            id            SERIAL PRIMARY KEY,
            time          TEXT, location_name TEXT,
            latitude      TEXT, longitude     TEXT,
            temperature_2m TEXT, relative_humidity_2m TEXT,
            precipitation TEXT, wind_speed_10m TEXT,
            wind_direction_10m TEXT, surface_pressure TEXT,
            cloud_cover TEXT, weather_code TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        '''
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
        logger.info('Staging table verified / created.')
 
    def extract_and_stage(self) -> None:
        df = self.extractor.extract_all()
        self._ensure_staging_table()
        df.astype(str).to_sql('staging_weather', self.engine, if_exists='append', index=False)
        logger.info(f'Staged {len(df)} raw rows into staging_weather.')
 
    def transform_in_db(self) -> None:
        with open('sql/elt_transforms.sql', 'r') as f:
            sql_script = f.read()
        statements = [s.strip() for s in sql_script.split(';') if s.strip()]
        with self.engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info('In-database ELT transforms complete.')
 
    def run(self) -> None:
        logger.info('=== ELT Pipeline Start ===')
        self.extract_and_stage()
        self.transform_in_db()
        logger.info('=== ELT Pipeline Complete ===')
