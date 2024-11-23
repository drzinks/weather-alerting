import requests, yaml, json, logging


def get_logger():
    logger = logging.getLogger("ingester_logger")
    logger.setLevel(logging.INFO)

    regular_handler = logging.FileHandler("regular.log")
    error_handler = logging.FileHandler("error.log")

    regular_handler.setLevel(logging.INFO)
    error_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    regular_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    logger.addHandler(regular_handler)
    logger.addHandler(error_handler)
    return logger


def load_config():
    try:
        with open("config/ingester-config.yaml", "r", encoding="UTF-8") as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        logger.error("File does not exist.")


def request_data():
    global response
    api_url = config["base_api_url"]
    position = config["position"]
    params = {
        "lat": position[0],
        "lon": position[1],
        "altitude": config["altitude"]
    }
    headers = {
        "User-Agent": config["user-agent"]
    }
    response = requests.get(api_url, params=params, headers=headers)


logger = get_logger()
config = load_config()

try:
    request_data()
    response.raise_for_status()  # Raise an error for HTTP codes >= 400
    data = response.json()
    with open("weather_data.json", "w") as json_file:
        json.dump(data, json_file, indent=4)
        logger.info("Weather data fetched successfully.")
except requests.exceptions.RequestException as e:
    logger.error(f"HTTP Request failed: {e}")
except json.JSONDecodeError:
    logger.error("Failed to parse JSON response")
