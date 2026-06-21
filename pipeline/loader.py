# pipeline/loader.py
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.settings import DATABASE_URL
from pipeline.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
class WeatherLoader:
    """Loads transformed weather data into the PostgreSQL star schema."""
 
    def __init__(self, db_url: str = DATABASE_URL):
        try:
            self.engine = create_engine(db_url, echo=False)
            logger.info('Database engine created successfully.')
        except Exception as e:
            logger.error(f'Failed to create database engine: {e}')
            raise
 
    def create_tables(self) -> None:
        try:
            with open('sql/schema.sql', 'r') as f:
                ddl = f.read()
            with self.engine.begin() as conn:
                conn.execute(text(ddl))
            logger.info('Tables verified / created from schema.sql.')
        except (FileNotFoundError, SQLAlchemyError) as e:
            logger.error(f'Error creating tables: {e}')
            raise
 
    def load_dim_location(self, df: pd.DataFrame) -> None:
        locations = df[['location_name', 'latitude', 'longitude']].drop_duplicates()
        try:
            with self.engine.begin() as conn:
                for _, row in locations.iterrows():
                    conn.execute(text(
                        'INSERT INTO dim_location (location_name, latitude, longitude) '
                        'VALUES (:name, :lat, :lon) '
                        'ON CONFLICT (location_name) DO NOTHING'
                    ), {'name': row.location_name, 'lat': row.latitude, 'lon': row.longitude})
            logger.info(f'Loaded {len(locations)} records into dim_location.')
        except SQLAlchemyError as e:
            logger.error(f'Error loading dim_location: {e}')
            raise
 
    def load_dim_date(self, df: pd.DataFrame) -> None:
        dates = df[['date', 'year', 'month', 'hour']].drop_duplicates(subset=['date', 'hour'])
        try:
            with self.engine.begin() as conn:
                for _, row in dates.iterrows():
                    conn.execute(text(
                        'INSERT INTO dim_date (date, year, month, hour) '
                        'VALUES (:d, :y, :m, :h) '
                        'ON CONFLICT (date, hour) DO NOTHING'
                    ), {'d': str(row.date), 'y': int(row.year), 'm': int(row.month), 'h': int(row.hour)})
            logger.info(f'Loaded {len(dates)} records into dim_date.')
        except SQLAlchemyError as e:
            logger.error(f'Error loading dim_date: {e}')
            raise
 
    def load_fact_weather(self, df: pd.DataFrame) -> None:
        fact_cols = [
            'time', 'location_name', 'temperature_2m', 'relative_humidity_2m',
            'precipitation', 'wind_speed_10m', 'wind_direction_10m',
            'surface_pressure', 'cloud_cover', 'weather_code', 'heat_index',
        ]
        fact_df = df[[c for c in fact_cols if c in df.columns]].copy()
        fact_df['time'] = fact_df['time'].astype(str)
        try:
            fact_df.to_sql('fact_weather', self.engine, if_exists='append',
                           index=False, method='multi')
            logger.info(f'Loaded {len(fact_df)} rows into fact_weather.')
        except SQLAlchemyError as e:
            logger.error(f'Error loading fact_weather: {e}')
            raise

    def load(self, df: pd.DataFrame) -> None:
        """Orchestrates all load steps in order."""
        self.create_tables()
        self.load_dim_location(df)
        self.load_dim_date(df)
        self.load_fact_weather(df)
        logger.info('All data loaded successfully.')
