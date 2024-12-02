import requests
import yaml
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
import time
import schedule

ERROR_LOG_PATH = "logs/error.log"
LOG_PATH = "logs/regular.log"
LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DAY_MONTH_YEAR_FORMAT = "%d-%m-%Y"
YR_NO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
KAFKA_TOPIC = "weather"
KAFKA_BROKER = "localhost:9092"

class WeatherIngester:

    def __init__(self):
        self.logger = self.get_logger()
        self.config = self.load_config()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

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

    def request_data(self) -> requests.Response:
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
        try:
            for time_serie in data["properties"]["timeseries"]:
                hours_wanted = self.config.get("hours_wanted", ["00", "12"])
                json_date = time_serie["time"]
                date = datetime.strptime(json_date, YR_NO_DATE_FORMAT)
                day_month_year = date.strftime(DAY_MONTH_YEAR_FORMAT)
                hour = date.strftime("%H")
                temp = time_serie["data"]["instant"]["details"]["air_temperature"]
                if hour in hours_wanted:
                    day_hour_temp.append({"day": day_month_year, "hour": hour, "temp": temp})
        except KeyError as e:
            self.logger.error(f"Missing key {e} in record: {time_serie}")
        return day_hour_temp

    def send_to_kafka(self, data):
        """Sends data to the Kafka topic."""
        try:
            self.producer.send(KAFKA_TOPIC, data)
            self.logger.info("Weather data sent to Kafka.")
        except Exception as e:
            self.logger.error(f"Failed to send data to Kafka: {e}")

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
            self.logger.info("Weather data fetched successfully.")
            short_data = self.extract_day_night_temperatures(data)
            self.logger.info("Weather data sent to kafka.")
            self.send_to_kafka(data)
            # with open("processed_weather_data.json", "w") as json_file:
            #     json.dump(short_data, json_file, indent=4)
            #     self.logger.info("Weather data fetched successfully.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed: {e}")
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")


def scheduled_job():
    ingester = WeatherIngester()
    ingester.process_and_store_weather_data()

if __name__ == "__main__":
    schedule.every().day.at("00:01").do(scheduled_job)
    schedule.every().day.at("12:01").do(scheduled_job)
    scheduled_job() #first run at once
    print("Weather ingestion service started. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(1)

