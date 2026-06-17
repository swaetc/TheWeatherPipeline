# config/settings.py
import os
from dotenv import load_dotenv
 
load_dotenv()  # reads variables from .env
 
# ── Database ──────────────────────────────────────────────────────
DB_HOST     = os.getenv('DB_HOST',     'localhost')
DB_PORT     = int(os.getenv('DB_PORT', '5432'))
DB_NAME     = os.getenv('DB_NAME',     'weather_analytics')
DB_USER     = os.getenv('DB_USER',     'weather_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'weather_pass123')
 
DATABASE_URL = (
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}'
    f'@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)
 
# ── API ───────────────────────────────────────────────────────────
API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.open-meteo.com/v1/forecast')
 
# ── Locations to pull data for (South African cities) ─────────────
LOCATIONS = [
    {'name': 'Johannesburg', 'latitude': -26.2041, 'longitude': 28.0473},
    {'name': 'Cape Town',    'latitude': -33.9249, 'longitude': 18.4241},
    {'name': 'Durban',       'latitude': -29.8587, 'longitude': 31.0218},
    {'name': 'Pretoria',     'latitude': -25.7479, 'longitude': 28.2293},
]
 
# ── Logging ───────────────────────────────────────────────────────
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_DIR   = os.getenv('LOG_DIR',   'logs')
