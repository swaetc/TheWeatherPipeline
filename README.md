# 🌤️ TheWeatherPipeline

A production-grade **ELT / ETL weather data pipeline** that pulls hourly weather data
from the [Open-Meteo API](https://open-meteo.com/) for four South African cities,
transforms and validates it in Python, and loads it into a PostgreSQL star-schema data warehouse.
The pipeline is orchestrated nightly by **Apache Airflow** running inside WSL (Ubuntu).

---

## 📐 Architecture Overview

```
Open-Meteo API  ──►  Extractor  ──►  Transformer  ──►  Loader  ──►  PostgreSQL
(free, no key)         (HTTP)        (pandas ETL)     (SQLAlchemy)  (star schema)
                                                                         │
                                                            ┌────────────┴────────────┐
                                                            │   dim_location           │
                                                            │   dim_date               │
                                                            │   fact_weather           │
                                                            │   staging_weather (ELT) │
                                                            └─────────────────────────┘
                         Apache Airflow DAG (nightly @ 06:00 SAST)
```

### Two pipeline modes
| Mode | Entry Point | Description |
|------|-------------|-------------|
| **ETL** | `pipeline/pipeline_runner.py` | Extract → Transform in Python → Load |
| **ELT** | `pipeline/elt_pipeline.py`    | Extract → Load raw to staging → Transform in DB via SQL |

---

## 🗂️ Project Structure

```
TheWeatherPipeline/
├── airflow/
│   └── dags/
│       └── weather_pipeline_dag.py   # Airflow DAG definition
├── config/
│   └── settings.py                   # DB URL, API URL, locations, log config
├── logs/                             # Runtime log files (git-ignored)
├── pipeline/
│   ├── __init__.py
│   ├── extractor.py                  # WeatherExtractor — Open-Meteo HTTP client
│   ├── transformer.py                # WeatherTransformer — pandas clean & enrich
│   ├── loader.py                     # WeatherLoader — SQLAlchemy → PostgreSQL
│   ├── elt_pipeline.py               # ELTPipeline — stage-first variant
│   ├── pipeline_runner.py            # WeatherPipelineRunner — ETL orchestrator
│   └── utils/
│       ├── logger.py                 # Centralised logging setup
│       └── validators.py             # Schema + range validators
├── sql/
│   ├── schema.sql                    # DDL for star schema tables
│   └── elt_transforms.sql            # SQL transforms for ELT mode
├── tests/
│   ├── __init__.py
│   ├── test_extractor.py             # 20 + pytest tests — WeatherExtractor
│   ├── test_transformer.py           # 25 + pytest tests — WeatherTransformer
│   └── test_loader.py                # 15 + pytest tests — WeatherLoader
├── .env                              # Local secrets (git-ignored)
├── .env.example                      # Template for .env
├── .gitignore
├── requirements.txt                  # Pinned runtime dependencies
├── requirements_pipeline.txt         # Frozen pipeline venv deps
└── requirements_airflow.txt          # Frozen Airflow venv deps
```

---

## 🚀 Quick Start

### Prerequisites
- Ubuntu / WSL (Windows Subsystem for Linux)
- Python 3.10 +
- PostgreSQL 14 +
- Git

### 1 — Clone the repository
```bash
git clone https://github.com/<your-username>/TheWeatherPipeline.git
cd TheWeatherPipeline
```

### 2 — Create the pipeline virtual environment

```bash
python3 -m venv venv_pipeline
source venv_pipeline/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
```

### 3 — Create the Airflow virtual environment

```bash
python3 -m venv venv_airflow
source venv_airflow/bin/activate
pip install --upgrade pip
pip install "apache-airflow==2.8.4" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.10.txt"
deactivate
```

### 4 — Set up PostgreSQL

Start the PostgreSQL service and create the database and role:

```bash
sudo service postgresql start

# Open the postgres superuser shell
sudo -u postgres psql <<'SQL'
CREATE ROLE weather_user WITH LOGIN PASSWORD 'weather_pass123';
CREATE DATABASE weather_analytics OWNER weather_user;
GRANT ALL PRIVILEGES ON DATABASE weather_analytics TO weather_user;
\q
SQL
```

Apply the star-schema DDL:

```bash
source venv_pipeline/bin/activate
PGPASSWORD=weather_pass123 psql \
    -h localhost -U weather_user -d weather_analytics \
    -f sql/schema.sql
deactivate
```

### 5 — Initialise Airflow

```bash
source venv_airflow/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
# Create the admin web-UI user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
deactivate
```

### 6 — Configure credentials
```bash
cp .env.example .env
nano .env          # fill in DB_PASSWORD (and any overrides)
```

### 7 — Run the ETL pipeline manually
```bash
source venv_pipeline/bin/activate
python -m pipeline.pipeline_runner
```

### 8 — Run the ELT pipeline manually
```bash
source venv_pipeline/bin/activate
python -c "from pipeline.elt_pipeline import ELTPipeline; ELTPipeline().run()"
```

### 9 — Run the full test suite
```bash
source venv_pipeline/bin/activate
python -m pytest tests/ -v
```

---

## ⚙️ Configuration

All runtime settings live in [`config/settings.py`](config/settings.py) and can be
overridden via environment variables in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `weather_analytics` | Database name |
| `DB_USER` | `weather_user` | Database role |
| `DB_PASSWORD` | — | **Required** — set in `.env` |
| `API_BASE_URL` | `https://api.open-meteo.com/v1/forecast` | Open-Meteo base URL |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `LOG_DIR` | `logs` | Log file output directory |

### Monitored locations
| City | Latitude | Longitude |
|------|----------|-----------|
| Johannesburg | −26.2041 | 28.0473 |
| Cape Town | −33.9249 | 18.4241 |
| Durban | −29.8587 | 31.0218 |
| Pretoria | −25.7479 | 28.2293 |

---

## 🗄️ Database Schema

### `dim_location`
| Column | Type | Description |
|--------|------|-------------|
| `location_id` | SERIAL PK | Surrogate key |
| `location_name` | TEXT UNIQUE | City name |
| `latitude` | NUMERIC | Geographic latitude |
| `longitude` | NUMERIC | Geographic longitude |

### `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| `date_id` | SERIAL PK | Surrogate key |
| `date` | DATE | Calendar date |
| `year` | INT | Year |
| `month` | INT | Month (1–12) |
| `hour` | INT | Hour (0–23) |

### `fact_weather`
| Column | Type | Description |
|--------|------|-------------|
| `fact_id` | SERIAL PK | Surrogate key |
| `time` | TEXT | ISO-8601 observation timestamp |
| `location_name` | TEXT FK → dim_location | City |
| `temperature_2m` | NUMERIC | Air temperature (°C) at 2 m |
| `relative_humidity_2m` | NUMERIC | Relative humidity (%) at 2 m |
| `precipitation` | NUMERIC | Precipitation (mm) |
| `wind_speed_10m` | NUMERIC | Wind speed (km/h) at 10 m |
| `wind_direction_10m` | NUMERIC | Wind direction (°) |
| `surface_pressure` | NUMERIC | Surface pressure (hPa) |
| `cloud_cover` | NUMERIC | Cloud cover (%) |
| `weather_code` | INT | WMO weather code |
| `heat_index` | NUMERIC | Computed heat-index (°C) |
| `loaded_at` | TIMESTAMP | Row insert timestamp |

### `staging_weather` (ELT mode only)
Raw string-typed staging table populated before SQL transforms run.

---

## 🛩️ Apache Airflow DAG

**DAG ID:** `weather_pipeline_daily`  
**Schedule:** `0 6 * * *` (06:00 SAST / 04:00 UTC)  
**Catchup:** disabled

### Tasks
```
extract_weather  ──►  transform_weather  ──►  load_weather  ──►  validate_load
```

### Starting Airflow
```bash
source venv_airflow/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8090 &
airflow scheduler &
```
Then open [http://localhost:8090](http://localhost:8090) (default login: `admin` / `admin`).

---

## 🧪 Testing

Tests are written with **pytest** and require no live database connection —
all PostgreSQL interactions are fully mocked with `unittest.mock`.

```bash
# Run all tests with verbose output
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_extractor.py -v

# Run tests matching a keyword
python -m pytest tests/ -v -k "transformer"

# Run with coverage report
pip install pytest-cov
python -m pytest tests/ --cov=pipeline --cov-report=term-missing
```

### Test coverage summary
| Module | Test File | Tests |
|--------|-----------|-------|
| `extractor.py` | `test_extractor.py` | 16 |
| `transformer.py` | `test_transformer.py` | 22 |
| `loader.py` | `test_loader.py` | 14 |



## 📦 Dependencies

### Pipeline
- `pandas` — data manipulation
- `requests` — Open-Meteo HTTP client
- `sqlalchemy` — database abstraction layer
- `psycopg2-binary` — PostgreSQL driver
- `python-dotenv` — `.env` file loading

### Testing
- `pytest` — test runner

### Airflow
- `apache-airflow==2.8.*`

See `requirements_pipeline.txt` and `requirements_airflow.txt` for pinned versions.

---

## Screenshots included on project folder
- `see Screenshot folder on main project folder`

## 📝 License

This project is submitted as part of a Data Engineering portfolio assessment from AI Community Africa (AICA).  
© 2026 — All rights reserved.
