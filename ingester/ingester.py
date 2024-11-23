import requests
import yaml
import json
import logging
from datetime import datetime
from requests import Response

ERROR_LOG_PATH = "logs/error.log"
LOG_PATH = "logs/regular.log"
LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DAY_MONTH_YEAR_FORMAT = "%d-%m-%Y"
YR_NO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class WeatherIngester:

    def __init__(self):
        self.logger = self.get_logger()
        self.config = self.load_config()

    def get_logger(self):
        """Sets up logging for the application."""
        logger = logging.getLogger("ingester_logger")
        logger.setLevel(logging.INFO)

        regular_handler = logging.FileHandler(LOG_PATH)
        error_handler = logging.FileHandler(ERROR_LOG_PATH)

        regular_handler.setLevel(logging.INFO)
        error_handler.setLevel(logging.ERROR)

        formatter = logging.Formatter(LOGGING_FORMAT)
        regular_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)

        logger.addHandler(regular_handler)
        logger.addHandler(error_handler)
        return logger

    def load_config(self):
        try:
            with open("config/ingester-config.yaml", "r", encoding="UTF-8") as config_file:
                return yaml.safe_load(config_file)
        except FileNotFoundError:
            self.logger.error("Config file does not exist.")
            raise ValueError("Configuration file is missing.")

    def request_data(self) -> Response:
        """Makes an HTTP request to fetch data."""
        api_url = self.config.get("base_api_url", "")
        position = self.config.get("position", [0, 0])
        params = {
            "lat": position[0],
            "lon": position[1],
            "altitude": self.config.get("altitude", 0)
        }
        headers = {
            "User-Agent": self.config.get("user-agent", "default-agent")
        }
        return requests.get(api_url, params=params, headers=headers)

    def extract_day_night_temperatures(self, data: dict):
        """Extracts day/night temperature data from the API response."""
        day_hour_temp = []
        for time_serie in data["properties"]["timeseries"]:
            json_date = time_serie["time"]
            date = datetime.strptime(json_date, YR_NO_DATE_FORMAT)
            day_month_year = date.strftime(DAY_MONTH_YEAR_FORMAT)
            hour = date.strftime("%H")
            temp = time_serie["data"]["instant"]["details"]["air_temperature"]
            if hour == self.config["hours_wanted"][0] or hour == self.config["hours_wanted"][1]:
                day_hour_temp.append({"day": day_month_year, "hour": hour, "temp": temp})
        return day_hour_temp

    def process_and_store_weather_data(self):
        """Main function to fetch, process, and store weather data."""
        try:
            response = self.request_data()
            response.raise_for_status()  # Raise an error for HTTP codes >= 400
            if(response.status_code == 203):
                self.logger.warning("API is going to be deprecated!")
                """
                When new versions of products are introduced (usually after a beta period), the older versions will be deprecated.
                This can be monitored by checking for a 203 status code (instead of the usual 200);
                if so this should be logged and/or shown as a warning. Deprecated versions will be terminated after a reasonable time period (usually 1 month).
                """
            data = response.json()
            short_data = self.extract_day_night_temperatures(data)
            # Write data to a local JSON file
            with open("processed_weather_data.json", "w") as json_file:
                json.dump(short_data, json_file, indent=4)
                self.logger.info("Weather data fetched successfully.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed: {e}")
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")


if __name__ == "__main__":
    ingester = WeatherIngester()
    ingester.process_and_store_weather_data()
