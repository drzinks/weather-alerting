import json
from ingester import WeatherIngester
from unittest import mock

correct_pulled_data = [{'day': '24-11-2024', 'hour': '06', 'temp': 1.5},
                       {'day': '24-11-2024', 'hour': '12', 'temp': 4.5},
                       {'day': '25-11-2024', 'hour': '06', 'temp': 4.3},
                       {'day': '25-11-2024', 'hour': '12', 'temp': 10.6},
                       {'day': '26-11-2024', 'hour': '06', 'temp': 5.0},
                       {'day': '26-11-2024', 'hour': '12', 'temp': 7.8},
                       {'day': '27-11-2024', 'hour': '06', 'temp': 3.5},
                       {'day': '27-11-2024', 'hour': '12', 'temp': 5.9},
                       {'day': '28-11-2024', 'hour': '06', 'temp': 4.9},
                       {'day': '28-11-2024', 'hour': '12', 'temp': 8.3},
                       {'day': '29-11-2024', 'hour': '06', 'temp': 3.8},
                       {'day': '29-11-2024', 'hour': '12', 'temp': 3.4},
                       {'day': '30-11-2024', 'hour': '06', 'temp': 0.5},
                       {'day': '30-11-2024', 'hour': '12', 'temp': 3.2},
                       {'day': '01-12-2024', 'hour': '06', 'temp': 1.7},
                       {'day': '01-12-2024', 'hour': '12', 'temp': 3.9},
                       {'day': '02-12-2024', 'hour': '06', 'temp': -2.0},
                       {'day': '02-12-2024', 'hour': '12', 'temp': 4.1}]


def test_fetch_only_temperature_and_date():
    with open("weather_data.json", "r", encoding="UTF-8") as data_file:
        data = json.load(data_file)

    with mock.patch('ingester.WeatherIngester.get_logger') as mock_get_logger:
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        # Mock the FileHandler so that no files are created
        mock_file_handler = mock.Mock()
        mock_logger.addHandler(mock_file_handler)
        ingester = WeatherIngester()
        assert correct_pulled_data == ingester.extract_day_night_temperatures(data)
