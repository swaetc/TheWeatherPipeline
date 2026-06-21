# airflow/dags/weather_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys, os
 
# Add the project directories to Python path so Airflow scheduler/worker can find pipeline modules
sys.path.insert(0, '/mnt/c/Users/Yabon/Desktop/TheWeatherPipeline')
sys.path.insert(0, os.path.expanduser('~/weather_pipeline'))
sys.path.insert(0, '/mnt/c/Users/Yabon/Desktop/TheWeatherPipeline/venv_pipeline/lib/python3.12/site-packages')


from pipeline.extractor   import WeatherExtractor
from pipeline.transformer import WeatherTransformer
from pipeline.loader      import WeatherLoader
from pipeline.utils.validators import validate_schema
 
default_args = {
    'owner':            'data_engineering_team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
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
 
def task_extract(**context):
    df = WeatherExtractor().extract_all()
    context['ti'].xcom_push(key='raw_df', value=df.to_json())
 
def task_transform(**context):
    import pandas as pd
    df = pd.read_json(context['ti'].xcom_pull(key='raw_df', task_ids='extract_weather'))
    clean_df = WeatherTransformer().transform(df)
    context['ti'].xcom_push(key='clean_df', value=clean_df.to_json())
 
def task_validate(**context):
    import pandas as pd
    df = pd.read_json(context['ti'].xcom_pull(key='clean_df', task_ids='transform_weather'))
    if not validate_schema(df):
        raise ValueError('Validation failed — pipeline halted.')
    context['ti'].xcom_push(key='clean_df', value=df.to_json())
 
def task_load(**context):
    import pandas as pd
    df = pd.read_json(context['ti'].xcom_pull(key='clean_df', task_ids='validate_weather'))
    WeatherLoader().load(df)
 
t_extract   = PythonOperator(task_id='extract_weather',   python_callable=task_extract,   provide_context=True, dag=dag)
t_transform = PythonOperator(task_id='transform_weather', python_callable=task_transform, provide_context=True, dag=dag)
t_validate  = PythonOperator(task_id='validate_weather',  python_callable=task_validate,  provide_context=True, dag=dag)
t_load      = PythonOperator(task_id='load_weather',      python_callable=task_load,      provide_context=True, dag=dag)
 
# Task execution order
t_extract >> t_transform >> t_validate >> t_load
