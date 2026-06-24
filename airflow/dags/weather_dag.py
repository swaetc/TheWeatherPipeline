# airflow/dags/weather_dag.py
import sys
import os


PIPELINE_SITE_PACKAGES = '/mnt/c/Users/Yabon/Desktop/TheWeatherPipeline/venv_pipeline/lib/python3.12/site-packages'
PROJECT_ROOT = '/mnt/c/Users/Yabon/Desktop/TheWeatherPipeline'

if PIPELINE_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, PIPELINE_SITE_PACKAGES)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':            'data_engineering_team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=2),
}

dag = DAG(
    dag_id            = 'weather_etl_daily',
    description       = 'Daily ETL pipeline: Open-Meteo API to PostgreSQL star schema',
    default_args      = default_args,
    schedule_interval = '@daily',
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    tags              = ['weather', 'etl', 'capstone'],
)

# ── Each task function re-injects the path to be safe ────────────────────────
def _inject_path():
    if PIPELINE_SITE_PACKAGES not in sys.path:
        sys.path.insert(0, PIPELINE_SITE_PACKAGES)
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

def task_extract(**context):
    _inject_path()
    from pipeline.extractor import WeatherExtractor
    extractor = WeatherExtractor()
    df = extractor.extract_all()
    context['ti'].xcom_push(key='raw_df', value=df.to_json())

def task_transform(**context):
    _inject_path()
    import pandas as pd
    from pipeline.transformer import WeatherTransformer
    raw_json = context['ti'].xcom_pull(key='raw_df', task_ids='extract_weather')
    if not raw_json:
        raise ValueError('No data received from extract_weather task via XCom.')
    df = pd.read_json(raw_json)
    transformer = WeatherTransformer()
    clean_df = transformer.transform(df)
    context['ti'].xcom_push(key='clean_df', value=clean_df.to_json())

def task_validate(**context):
    _inject_path()
    import pandas as pd
    from pipeline.utils.validators import validate_schema
    clean_json = context['ti'].xcom_pull(key='clean_df', task_ids='transform_weather')
    if not clean_json:
        raise ValueError('No data received from transform_weather task via XCom.')
    df = pd.read_json(clean_json)
    if not validate_schema(df):
        raise ValueError('Validation failed — pipeline halted.')
    context['ti'].xcom_push(key='clean_df', value=df.to_json())

def task_load(**context):
    _inject_path()
    import pandas as pd
    from pipeline.loader import WeatherLoader
    clean_json = context['ti'].xcom_pull(key='clean_df', task_ids='validate_weather')
    if not clean_json:
        raise ValueError('No data received from validate_weather task via XCom.')
    df = pd.read_json(clean_json)
    loader = WeatherLoader()
    loader.load(df)

# ── Operators ─────────────────────────────────────────────────────────────────
t_extract = PythonOperator(
    task_id         = 'extract_weather',
    python_callable = task_extract,
    provide_context = True,
    dag             = dag,
)

t_transform = PythonOperator(
    task_id         = 'transform_weather',
    python_callable = task_transform,
    provide_context = True,
    dag             = dag,
)

t_validate = PythonOperator(
    task_id         = 'validate_weather',
    python_callable = task_validate,
    provide_context = True,
    dag             = dag,
)

t_load = PythonOperator(
    task_id         = 'load_weather',
    python_callable = task_load,
    provide_context = True,
    dag             = dag,
)

# ── Task execution order ──────────────────────────────────────────────────────
t_extract >> t_transform >> t_validate >> t_load