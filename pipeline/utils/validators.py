# pipeline/utils/validators.py
import pandas as pd
from pipeline.utils.logger import get_logger
 
logger = get_logger(__name__)
 
REQUIRED_COLUMNS = [
    'time', 'temperature_2m', 'relative_humidity_2m',
    'precipitation', 'wind_speed_10m', 'location_name',
]
 
VALID_RANGES = {
    'temperature_2m':       (-60,  60),
    'relative_humidity_2m': (  0, 100),
    'precipitation':        (  0, 500),
    'wind_speed_10m':       (  0, 250),
    'surface_pressure':     (800, 1085),  
    'cloud_cover':          (  0, 100),
}
 
def validate_schema(df: pd.DataFrame) -> bool:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        logger.error(f'Missing required columns: {missing}')
        return False
    logger.info('Schema validation passed.')
    return True
 
def validate_ranges(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)
    for col, (lo, hi) in VALID_RANGES.items():
        if col in df.columns:
            mask = df[col].between(lo, hi) | df[col].isna()
            dropped = (~mask).sum()
            if dropped:
                logger.warning(f'{col}: dropped {dropped} out-of-range rows')
            df = df[mask]
    logger.info(f'Range validation: kept {len(df)} of {initial} rows')
    return df
