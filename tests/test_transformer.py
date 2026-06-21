# tests/test_transformer.py
"""
Unit tests for pipeline/transformer.py — WeatherTransformer class.
Part E · Commit 10: Pytest Unit Tests
"""
import pytest
import pandas as pd
from pipeline.transformer import WeatherTransformer


# ---------------------------------------------------------------------------
# Shared test fixture factory
# ---------------------------------------------------------------------------

def make_sample_df(**overrides):
    """
    Returns a minimal valid raw DataFrame that matches the extractor output schema.
    Pass keyword arguments to override specific column values (must be lists of length 3).
    """
    data = {
        'time':                  ['2024-01-01T12:00', '2024-01-01T12:00', '2024-01-01T13:00'],
        'location_name':         ['johannesburg',      'johannesburg',      'cape town'],
        'latitude':              [-26.2,               -26.2,               -33.9],
        'longitude':             [28.0,                28.0,                18.4],
        'temperature_2m':        [25.0,                25.0,                18.0],
        'relative_humidity_2m':  [60,                  60,                  75],
        'precipitation':         [0.0,                 0.0,                 2.5],
        'wind_speed_10m':        [10.0,                10.0,                20.0],
        'wind_direction_10m':    [180,                 180,                 270],
        'surface_pressure':      [1013.0,              1013.0,              1010.0],
        'cloud_cover':           [20,                  20,                  80],
        'weather_code':          [1,                   1,                   61],
    }
    data.update(overrides)
    return pd.DataFrame(data)


# ===========================================================================
# Deduplication
# ===========================================================================

class TestDeduplication:

    def test_removes_exact_duplicates(self):
        """Duplicate (time, location_name) pairs must be collapsed to one row."""
        df = make_sample_df()          # rows 0 & 1 share time + location
        result = WeatherTransformer().transform(df)
        assert len(result) == 2

    def test_unique_pairs_are_kept(self):
        """Different (time, location) combos must all be retained."""
        data = make_sample_df(
            time=['2024-01-01T10:00', '2024-01-01T11:00', '2024-01-01T10:00'],
            location_name=['johannesburg', 'johannesburg', 'cape town'],
        )
        result = WeatherTransformer().transform(data)
        assert len(result) == 3


# ===========================================================================
# Type conversions
# ===========================================================================

class TestTypeConversions:

    def test_time_is_datetime64(self):
        """'time' column must be parsed to datetime64."""
        result = WeatherTransformer().transform(make_sample_df())
        assert pd.api.types.is_datetime64_any_dtype(result['time'])

    def test_numeric_columns_are_float(self):
        """All numeric weather columns must be numeric dtype after transform."""
        result = WeatherTransformer().transform(make_sample_df())
        for col in ['temperature_2m', 'relative_humidity_2m', 'precipitation',
                    'wind_speed_10m', 'surface_pressure', 'cloud_cover']:
            assert pd.api.types.is_numeric_dtype(result[col]), \
                f"Expected numeric dtype for '{col}', got {result[col].dtype}"


# ===========================================================================
# Missing / null handling
# ===========================================================================

class TestMissingHandling:

    def test_null_timestamps_dropped(self):
        """Rows with null 'time' values must be dropped."""
        df = make_sample_df()
        df.loc[0, 'time'] = None
        result = WeatherTransformer().transform(df)
        assert result['time'].isna().sum() == 0

    def test_numeric_nulls_filled(self):
        """NaN numeric values must be filled (median) — no NaN remaining."""
        df = make_sample_df()
        df.loc[0, 'temperature_2m'] = None
        result = WeatherTransformer().transform(df)
        assert result['temperature_2m'].isna().sum() == 0


# ===========================================================================
# Derived / enriched fields
# ===========================================================================

class TestDerivedFields:

    def test_date_column_added(self):
        """A 'date' column (dt.date) must be added by transform."""
        result = WeatherTransformer().transform(make_sample_df())
        assert 'date' in result.columns

    def test_hour_column_added(self):
        """An 'hour' column must be added by transform."""
        result = WeatherTransformer().transform(make_sample_df())
        assert 'hour' in result.columns

    def test_month_column_added(self):
        """A 'month' column must be added by transform."""
        result = WeatherTransformer().transform(make_sample_df())
        assert 'month' in result.columns

    def test_year_column_added(self):
        """A 'year' column must be added by transform."""
        result = WeatherTransformer().transform(make_sample_df())
        assert 'year' in result.columns

    def test_heat_index_column_added(self):
        """A 'heat_index' column must be computed and added."""
        result = WeatherTransformer().transform(make_sample_df())
        assert 'heat_index' in result.columns

    def test_heat_index_is_numeric(self):
        """'heat_index' values must be numeric (float)."""
        result = WeatherTransformer().transform(make_sample_df())
        assert pd.api.types.is_numeric_dtype(result['heat_index'])

    def test_hour_value_correct(self):
        """Hour extracted from '2024-01-01T12:00' must equal 12."""
        result = WeatherTransformer().transform(make_sample_df())
        assert result['hour'].iloc[0] == 12

    def test_year_value_correct(self):
        """Year extracted from '2024-01-01T12:00' must equal 2024."""
        result = WeatherTransformer().transform(make_sample_df())
        assert result['year'].iloc[0] == 2024

    def test_month_value_correct(self):
        """Month extracted from '2024-01-01T12:00' must equal 1 (January)."""
        result = WeatherTransformer().transform(make_sample_df())
        assert result['month'].iloc[0] == 1


# ===========================================================================
# Location standardisation
# ===========================================================================

class TestLocationStandardisation:

    def test_location_name_title_case(self):
        """'location_name' must be converted to Title Case."""
        result = WeatherTransformer().transform(make_sample_df())
        assert result['location_name'].iloc[0] == 'Johannesburg'

    def test_multi_word_location_title_case(self):
        """Multi-word location name ('cape town') must become 'Cape Town'."""
        result = WeatherTransformer().transform(make_sample_df())
        cape_town = result[result['location_name'] == 'Cape Town']
        assert len(cape_town) == 1

    def test_location_name_no_leading_trailing_spaces(self):
        """Location names must not have leading or trailing whitespace."""
        df = make_sample_df(location_name=['  johannesburg  ', '  johannesburg  ', ' cape town '])
        result = WeatherTransformer().transform(df)
        for name in result['location_name']:
            assert name == name.strip()


# ===========================================================================
# Column name cleaning
# ===========================================================================

class TestColumnCleaning:

    def test_column_names_lowercase(self):
        """All column names must be lowercase after transform."""
        result = WeatherTransformer().transform(make_sample_df())
        for col in result.columns:
            assert col == col.lower(), f"Column name '{col}' is not lowercase"

    def test_column_names_no_spaces(self):
        """Column names must not contain spaces."""
        result = WeatherTransformer().transform(make_sample_df())
        for col in result.columns:
            assert ' ' not in col, f"Column name '{col}' contains spaces"


# ===========================================================================
# Schema validation
# ===========================================================================

class TestSchemaValidation:

    def test_missing_required_column_raises(self):
        """Transform must raise ValueError when a required column is missing."""
        df = make_sample_df().drop(columns=['temperature_2m'])
        with pytest.raises(ValueError, match='Schema validation failed'):
            WeatherTransformer().transform(df)

    def test_valid_schema_does_not_raise(self):
        """Transform must NOT raise when all required columns are present."""
        try:
            WeatherTransformer().transform(make_sample_df())
        except ValueError:
            pytest.fail("transform() raised ValueError on a valid DataFrame")


# ===========================================================================
# Out-of-range value filtering
# ===========================================================================

class TestRangeValidation:

    def test_extreme_temperature_row_removed(self):
        """Rows with temperature outside [-60, 60] must be dropped."""
        df = make_sample_df(
            time=['2024-01-01T10:00', '2024-01-01T11:00', '2024-01-01T12:00'],
            location_name=['johannesburg', 'cape town', 'durban'],
            temperature_2m=[25.0, 999.0, 18.0],   # 999 is invalid
        )
        result = WeatherTransformer().transform(df)
        assert (result['temperature_2m'] > 60).sum() == 0

    def test_valid_rows_retained_after_range_filter(self):
        """Rows with in-range values must not be removed by range validation."""
        df = make_sample_df(
            time=['2024-01-01T10:00', '2024-01-01T11:00', '2024-01-01T12:00'],
            location_name=['johannesburg', 'cape town', 'durban'],
        )
        result = WeatherTransformer().transform(df)
        assert len(result) == 3
