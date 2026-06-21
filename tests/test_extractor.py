# tests/test_extractor.py
"""
Unit tests for pipeline/extractor.py — WeatherExtractor class.
Part E · Commit 10: Pytest Unit Tests
"""
import pytest
import pandas as pd
import requests
from unittest.mock import patch, MagicMock
from pipeline.extractor import WeatherExtractor

# ---------------------------------------------------------------------------
# Shared mock API response that mirrors the Open-Meteo schema
# ---------------------------------------------------------------------------
MOCK_RESPONSE = {
    'hourly': {
        'time':                  ['2024-01-01T00:00', '2024-01-01T01:00'],
        'temperature_2m':        [22.5, 21.0],
        'relative_humidity_2m':  [65,   68],
        'precipitation':         [0.0,  0.1],
        'wind_speed_10m':        [12.3, 14.0],
        'wind_direction_10m':    [180,  190],
        'surface_pressure':      [1013, 1012],
        'cloud_cover':           [30,   45],
        'weather_code':          [1,    2],
    }
}

SAMPLE_LOCATION = {'name': 'Johannesburg', 'latitude': -26.2041, 'longitude': 28.0473}


# ---------------------------------------------------------------------------
# Helper: build a mock requests.Response
# ---------------------------------------------------------------------------
def _make_mock_response(json_data, status_code=200):
    mock_r = MagicMock()
    mock_r.json.return_value = json_data
    mock_r.status_code = status_code
    mock_r.raise_for_status.return_value = None
    return mock_r


# ===========================================================================
# fetch_location — success cases
# ===========================================================================

class TestFetchLocationSuccess:

    @patch('pipeline.extractor.requests.get')
    def test_returns_dataframe(self, mock_get):
        """Successful call returns a non-None DataFrame."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        result = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    @patch('pipeline.extractor.requests.get')
    def test_row_count_matches_api(self, mock_get):
        """Returned DataFrame must have the same number of rows as API hourly entries."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert len(df) == 2

    @patch('pipeline.extractor.requests.get')
    def test_location_name_column_populated(self, mock_get):
        """The location_name column is added and matches the location dict."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert 'location_name' in df.columns
        assert df['location_name'].iloc[0] == 'Johannesburg'

    @patch('pipeline.extractor.requests.get')
    def test_latitude_longitude_columns_added(self, mock_get):
        """latitude and longitude columns are appended to the returned DataFrame."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert 'latitude' in df.columns
        assert 'longitude' in df.columns
        assert df['latitude'].iloc[0] == pytest.approx(-26.2041)
        assert df['longitude'].iloc[0] == pytest.approx(28.0473)

    @patch('pipeline.extractor.requests.get')
    def test_all_hourly_variables_present(self, mock_get):
        """All configured HOURLY_VARS appear as columns in the result."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        for col in WeatherExtractor.HOURLY_VARS:
            assert col in df.columns, f"Column '{col}' missing from result"

    @patch('pipeline.extractor.requests.get')
    def test_temperature_values_correct(self, mock_get):
        """Temperature values from the mock response are preserved exactly."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert df['temperature_2m'].tolist() == [22.5, 21.0]


# ===========================================================================
# fetch_location — error / edge cases
# ===========================================================================

class TestFetchLocationErrors:

    @patch('pipeline.extractor.requests.get')
    def test_connection_error_returns_none(self, mock_get):
        """ConnectionError must be caught and None returned."""
        mock_get.side_effect = requests.exceptions.ConnectionError('Network unreachable')
        result = WeatherExtractor().fetch_location(
            {'name': 'Cape Town', 'latitude': -33.9249, 'longitude': 18.4241}
        )
        assert result is None

    @patch('pipeline.extractor.requests.get')
    def test_timeout_returns_none(self, mock_get):
        """Timeout exception must be caught and None returned."""
        mock_get.side_effect = requests.exceptions.Timeout()
        result = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert result is None

    @patch('pipeline.extractor.requests.get')
    def test_http_error_returns_none(self, mock_get):
        """HTTP 500 error must be caught and None returned."""
        mock_r = MagicMock()
        mock_r.raise_for_status.side_effect = requests.exceptions.HTTPError('500 Server Error')
        mock_get.return_value = mock_r
        result = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert result is None

    @patch('pipeline.extractor.requests.get')
    def test_missing_hourly_key_returns_none(self, mock_get):
        """API response without 'hourly' key must return None."""
        mock_get.return_value = _make_mock_response({'error': True, 'reason': 'invalid request'})
        result = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert result is None

    @patch('pipeline.extractor.requests.get')
    def test_empty_hourly_data_returns_empty_df(self, mock_get):
        """An 'hourly' key present but empty returns an empty DataFrame (not None)."""
        empty_response = {'hourly': {k: [] for k in ['time'] + list(WeatherExtractor.HOURLY_VARS)}}
        mock_get.return_value = _make_mock_response(empty_response)
        df = WeatherExtractor().fetch_location(SAMPLE_LOCATION)
        assert df is not None
        assert len(df) == 0


# ===========================================================================
# extract_all — aggregation over multiple locations
# ===========================================================================

class TestExtractAll:

    @patch('pipeline.extractor.requests.get')
    def test_combines_multiple_locations(self, mock_get):
        """extract_all must concatenate data from all configured locations."""
        mock_get.return_value = _make_mock_response(MOCK_RESPONSE)
        df = WeatherExtractor().extract_all()
        # 4 configured locations × 2 hourly rows each = 8 rows minimum
        assert len(df) >= 2

    @patch('pipeline.extractor.requests.get')
    def test_raises_if_all_locations_fail(self, mock_get):
        """RuntimeError raised when every location fetch returns None."""
        mock_get.side_effect = requests.exceptions.ConnectionError()
        with pytest.raises(RuntimeError, match='No data extracted'):
            WeatherExtractor().extract_all()

    @patch('pipeline.extractor.requests.get')
    def test_partial_failure_still_returns_data(self, mock_get):
        """If at least one location succeeds, extract_all must succeed."""
        # First call succeeds, rest raise ConnectionError
        mock_get.side_effect = [
            _make_mock_response(MOCK_RESPONSE),
            requests.exceptions.ConnectionError(),
            requests.exceptions.ConnectionError(),
            requests.exceptions.ConnectionError(),
        ]
        df = WeatherExtractor().extract_all()
        assert df is not None
        assert len(df) == 2


# ===========================================================================
# Custom base URL / _build_params
# ===========================================================================

class TestBuildParams:

    def test_custom_base_url(self):
        """WeatherExtractor should accept a custom base URL."""
        extractor = WeatherExtractor(base_url='https://custom.api.example.com/forecast')
        assert extractor.base_url == 'https://custom.api.example.com/forecast'

    def test_build_params_includes_hourly_vars(self):
        """_build_params must include all HOURLY_VARS joined by comma."""
        extractor = WeatherExtractor()
        params = extractor._build_params(-26.2041, 28.0473)
        expected_hourly = ','.join(WeatherExtractor.HOURLY_VARS)
        assert params['hourly'] == expected_hourly

    def test_build_params_lat_lon(self):
        """_build_params must include the correct latitude and longitude."""
        params = WeatherExtractor()._build_params(-33.9249, 18.4241)
        assert params['latitude']  == pytest.approx(-33.9249)
        assert params['longitude'] == pytest.approx(18.4241)

    def test_build_params_timezone(self):
        """Default timezone in params must be 'Africa/Johannesburg'."""
        params = WeatherExtractor()._build_params(0.0, 0.0)
        assert params['timezone'] == 'Africa/Johannesburg'
