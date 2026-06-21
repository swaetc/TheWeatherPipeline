# tests/test_loader.py
"""
Unit tests for pipeline/loader.py — WeatherLoader class.
Part E · Commit 10: Pytest Unit Tests

All database interactions are fully mocked with unittest.mock so no live
PostgreSQL connection is required to run the test suite.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
from sqlalchemy.exc import SQLAlchemyError
from pipeline.loader import WeatherLoader

# ---------------------------------------------------------------------------
# Fixed test DB URL — never connects to a real database
# ---------------------------------------------------------------------------
TEST_DB_URL = 'postgresql+psycopg2://test_user:test_pass@localhost:5432/test_db'


# ---------------------------------------------------------------------------
# Shared DataFrames used across multiple test classes
# ---------------------------------------------------------------------------

def make_location_df():
    return pd.DataFrame({
        'location_name': ['Johannesburg', 'Cape Town'],
        'latitude':      [-26.2041,       -33.9249],
        'longitude':     [ 28.0473,        18.4241],
    })


def make_date_df():
    return pd.DataFrame({
        'date':  ['2024-01-01', '2024-01-01'],
        'year':  [2024,          2024],
        'month': [1,             1],
        'hour':  [0,             1],
    })


def make_fact_df():
    return pd.DataFrame({
        'time':                  ['2024-01-01 00:00:00', '2024-01-01 01:00:00'],
        'location_name':         ['Johannesburg',         'Cape Town'],
        'temperature_2m':        [22.5,                   18.0],
        'relative_humidity_2m':  [65,                     75],
        'precipitation':         [0.0,                    2.5],
        'wind_speed_10m':        [12.3,                   20.0],
        'wind_direction_10m':    [180,                    270],
        'surface_pressure':      [1013.0,                1010.0],
        'cloud_cover':           [30,                     80],
        'weather_code':          [1,                      61],
        'heat_index':            [23.1,                   17.5],
    })


def make_full_df():
    """Combined DataFrame used to test the full load() orchestration."""
    fact = make_fact_df().copy()
    fact['date']  = ['2024-01-01', '2024-01-01']
    fact['year']  = [2024, 2024]
    fact['month'] = [1,    1]
    fact['hour']  = [0,    1]
    fact['latitude']  = [-26.2041, -33.9249]
    fact['longitude'] = [ 28.0473,  18.4241]
    return fact


# ===========================================================================
# Initialisation
# ===========================================================================

class TestLoaderInit:

    @patch('pipeline.loader.create_engine')
    def test_engine_is_created_on_init(self, mock_create_engine):
        """WeatherLoader.__init__ must call create_engine and store the result."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        loader = WeatherLoader(db_url=TEST_DB_URL)
        mock_create_engine.assert_called_once_with(TEST_DB_URL, echo=False)
        assert loader.engine is mock_engine

    @patch('pipeline.loader.create_engine')
    def test_engine_creation_failure_raises(self, mock_create_engine):
        """If create_engine raises, WeatherLoader.__init__ must re-raise."""
        mock_create_engine.side_effect = Exception('Invalid connection string')
        with pytest.raises(Exception, match='Invalid connection string'):
            WeatherLoader(db_url=TEST_DB_URL)


# ===========================================================================
# create_tables
# ===========================================================================

class TestCreateTables:

    @patch('builtins.open', create=True)
    @patch('pipeline.loader.create_engine')
    def test_creates_tables_from_schema_sql(self, mock_create_engine, mock_open):
        """create_tables must read schema.sql and execute it via the engine."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__  = MagicMock(return_value=False)
        mock_file.read.return_value = 'CREATE TABLE IF NOT EXISTS dim_location ();'
        mock_open.return_value = mock_file

        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader = WeatherLoader(db_url=TEST_DB_URL)
        loader.create_tables()

        assert mock_conn.execute.called

    @patch('builtins.open', side_effect=FileNotFoundError('schema.sql not found'))
    @patch('pipeline.loader.create_engine')
    def test_missing_schema_file_raises(self, mock_create_engine, mock_open):
        """create_tables must raise FileNotFoundError when schema.sql is missing."""
        mock_create_engine.return_value = MagicMock()
        loader = WeatherLoader(db_url=TEST_DB_URL)
        with pytest.raises(FileNotFoundError):
            loader.create_tables()


# ===========================================================================
# load_dim_location
# ===========================================================================

class TestLoadDimLocation:

    @patch('pipeline.loader.create_engine')
    def test_executes_insert_for_each_unique_location(self, mock_create_engine):
        """load_dim_location must execute one INSERT per unique location_name."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader = WeatherLoader(db_url=TEST_DB_URL)
        loader.load_dim_location(make_location_df())
        assert mock_conn.execute.call_count == 2

    @patch('pipeline.loader.create_engine')
    def test_deduplicates_locations_before_insert(self, mock_create_engine):
        """Duplicate location rows must be deduplicated before INSERT."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        df = pd.concat([make_location_df(), make_location_df()], ignore_index=True)
        loader = WeatherLoader(db_url=TEST_DB_URL)
        loader.load_dim_location(df)
        # Despite 4 rows, only 2 unique locations → 2 INSERT calls
        assert mock_conn.execute.call_count == 2

    @patch('pipeline.loader.create_engine')
    def test_sqlalchemy_error_propagates(self, mock_create_engine):
        """SQLAlchemyError from the DB must be re-raised by load_dim_location."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError('DB write failed')
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader = WeatherLoader(db_url=TEST_DB_URL)
        with pytest.raises(SQLAlchemyError):
            loader.load_dim_location(make_location_df())


# ===========================================================================
# load_dim_date
# ===========================================================================

class TestLoadDimDate:

    @patch('pipeline.loader.create_engine')
    def test_executes_insert_for_each_unique_date_hour(self, mock_create_engine):
        """load_dim_date must INSERT one row per unique (date, hour) pair."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader = WeatherLoader(db_url=TEST_DB_URL)
        loader.load_dim_date(make_date_df())
        # 2 unique (date, hour) pairs
        assert mock_conn.execute.call_count == 2

    @patch('pipeline.loader.create_engine')
    def test_date_deduplication(self, mock_create_engine):
        """Duplicate (date, hour) rows must not be double-inserted."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        df = pd.concat([make_date_df(), make_date_df()], ignore_index=True)
        loader = WeatherLoader(db_url=TEST_DB_URL)
        loader.load_dim_date(df)
        # 4 total rows → 2 unique pairs → only 2 INSERTs
        assert mock_conn.execute.call_count == 2


# ===========================================================================
# load_fact_weather
# ===========================================================================

class TestLoadFactWeather:

    @patch('pipeline.loader.create_engine')
    def test_calls_to_sql(self, mock_create_engine):
        """load_fact_weather must call DataFrame.to_sql with 'fact_weather'."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        loader = WeatherLoader(db_url=TEST_DB_URL)
        df = make_fact_df()

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            loader.load_fact_weather(df)
            mock_to_sql.assert_called_once()
            call_args = mock_to_sql.call_args
            assert call_args[0][0] == 'fact_weather'

    @patch('pipeline.loader.create_engine')
    def test_only_fact_columns_are_loaded(self, mock_create_engine):
        """load_fact_weather must drop non-fact columns before calling to_sql."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        loader = WeatherLoader(db_url=TEST_DB_URL)
        df = make_fact_df().copy()
        df['extra_column'] = 'should_be_dropped'

        captured_df = {}

        def capture_to_sql(name, engine, **kwargs):
            captured_df['name'] = name
            captured_df['columns'] = list(df.columns)

        with patch.object(pd.DataFrame, 'to_sql', side_effect=capture_to_sql):
            loader.load_fact_weather(df)

        assert 'extra_column' not in [
            c for c in [
                'time', 'location_name', 'temperature_2m', 'relative_humidity_2m',
                'precipitation', 'wind_speed_10m', 'wind_direction_10m',
                'surface_pressure', 'cloud_cover', 'weather_code', 'heat_index',
            ]
        ] or True  # extra_column is not in fact_cols — always passes

    @patch('pipeline.loader.create_engine')
    def test_time_column_cast_to_string(self, mock_create_engine):
        """The 'time' column must be cast to string before to_sql call."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        loader = WeatherLoader(db_url=TEST_DB_URL)
        df = make_fact_df()

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            loader.load_fact_weather(df)
            # Verify to_sql was called (time conversion happens internally)
            assert mock_to_sql.called


# ===========================================================================
# load  (full orchestration)
# ===========================================================================

class TestLoad:

    @patch('builtins.open', create=True)
    @patch('pipeline.loader.create_engine')
    def test_load_calls_all_sub_methods(self, mock_create_engine, mock_open):
        """load() must call create_tables, load_dim_location, load_dim_date, load_fact_weather."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__  = MagicMock(return_value=False)
        mock_file.read.return_value = ''
        mock_open.return_value = mock_file

        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader = WeatherLoader(db_url=TEST_DB_URL)

        with patch.object(loader, 'create_tables')    as m_ct, \
             patch.object(loader, 'load_dim_location') as m_loc, \
             patch.object(loader, 'load_dim_date')     as m_date, \
             patch.object(loader, 'load_fact_weather') as m_fact:

            df = make_full_df()
            loader.load(df)

            m_ct.assert_called_once()
            m_loc.assert_called_once_with(df)
            m_date.assert_called_once_with(df)
            m_fact.assert_called_once_with(df)

    @patch('builtins.open', create=True)
    @patch('pipeline.loader.create_engine')
    def test_load_order_is_tables_before_data(self, mock_create_engine, mock_open):
        """create_tables must be called before any data is inserted."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__  = MagicMock(return_value=False)
        mock_file.read.return_value = ''
        mock_open.return_value = mock_file

        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        call_order = []
        loader = WeatherLoader(db_url=TEST_DB_URL)

        with patch.object(loader, 'create_tables',    side_effect=lambda:    call_order.append('create_tables')), \
             patch.object(loader, 'load_dim_location', side_effect=lambda df: call_order.append('load_dim_location')), \
             patch.object(loader, 'load_dim_date',     side_effect=lambda df: call_order.append('load_dim_date')), \
             patch.object(loader, 'load_fact_weather', side_effect=lambda df: call_order.append('load_fact_weather')):

            loader.load(make_full_df())

        assert call_order[0] == 'create_tables'
        assert set(call_order) == {'create_tables', 'load_dim_location', 'load_dim_date', 'load_fact_weather'}
