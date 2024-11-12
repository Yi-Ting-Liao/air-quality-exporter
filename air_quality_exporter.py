from typing import Optional
from prometheus_client import start_http_server, Gauge
from loguru import logger
from types import FrameType
import sys
import signal
import time
import requests
import json

CONFIG_FILE = "config.json"

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

EXPORTER_PORT = config["exporter_port"]
API_URL = config["api_url"]
API_KEY = config["api_key"]
API_UPDATE_INTERVAL_SECONDS = config["api_update_interval_seconds"]

REQUEST_HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
    "charset": "utf-8",
    "Authorization": "Basic " + API_KEY,
}

# setting up logging
_ = logger.add(sys.stderr, level="DEBUG")


class Pollutants:
    def __init__(
        self,
        temperature: Optional[float] = None,
        humidity: Optional[float] = None,
        co2: Optional[float] = None,
        co: Optional[float] = None,
        pm25: Optional[float] = None,
        pm10: Optional[float] = None,
        o3: Optional[float] = None,
        voc: Optional[float] = None,
        hc: Optional[float] = None,
    ):
        self.temperature = temperature
        self.humidity = humidity
        self.co2 = co2
        self.co = co
        self.pm25 = pm25
        self.pm10 = pm10
        self.o3 = o3
        self.voc = voc
        self.hc = hc

    def get_all_pollutants_type(self):
        return [
            "temperature",
            "humidity",
            "co2",
            "co",
            "pm25",
            "pm10",
            "o3",
            "voc",
            "hc",
        ]

    def update_pollutant_data(self, pollutants_type: str, value: float):
        if pollutants_type == "temperature":
            self.temperature = value
        elif pollutants_type == "humidity":
            self.humidity = value
        elif pollutants_type == "co2":
            self.co2 = value
        elif pollutants_type == "co":
            self.co = value
        elif pollutants_type == "pm25":
            self.pm25 = value
        elif pollutants_type == "pm10":
            self.pm10 = value
        elif pollutants_type == "o3":
            self.o3 = value
        elif pollutants_type == "voc":
            self.voc = value
        elif pollutants_type == "hc":
            self.hc = value
        else:
            logger.error(f"Unknown pollutants type: {pollutants_type}")


class AirQualityStation:
    def __init__(
        self,
        station_id: str,
        station_name: str,
        latitude: float,
        longitude: float,
        status: str,
        api_names: dict[str, str],
    ):
        self.station_id = station_id  # the name of url
        self.station_name = station_name  # describe the station
        # self.location = {
        #     "latitude": latitude,
        #     "longitude": longitude,
        # }  # location of the station(latitude, longitude)
        self.status = status  # UP, DOWN
        self.api_names = api_names
        self.latitude = latitude
        self.longitude = longitude
        self.pollutants = Pollutants()

        keys_to_delete: list[str] = []
        for key, value in api_names.items():
            if value == "None":
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del api_names[key]

    # update the pollutants data by fetch the data from the API
    def update_pollutant_data(self) -> dict[str, float]:

        new_values: dict[str, float] = {}

        for pollutants_type, api_names in self.api_names.items():
            request_data = {"Tags": [{"Name": api_names}]}

            r = requests.post(
                API_URL,
                data=json.dumps(request_data),
                headers=REQUEST_HEADERS,
            ).json()

            new_value = r["Values"][0]["Value"]

            self.pollutants.update_pollutant_data(pollutants_type, new_value)

            new_values[pollutants_type] = new_value

        return new_values


metrics = {
    "temperature": Gauge(
        "air_quality_temperature_celsius",
        "Temperature in Celsius",
        ["station_id", "latitude", "longitude"],
    ),
    "humidity": Gauge(
        "air_quality_humidity_percentage",
        "Relative humidity in percentage",
        ["station_id", "latitude", "longitude"],
    ),
    "co2": Gauge(
        "air_quality_co2_concentration_ppm",
        "CO2 concentration in parts per million",
        ["station_id", "latitude", "longitude"],
    ),
    "co": Gauge(
        "air_quality_co_concentration_ppm",
        "CO concentration in parts per million",
        ["station_id", "latitude", "longitude"],
    ),
    "pm25": Gauge(
        "air_quality_pm25_concentration_micrograms",
        "PM2.5 concentration in micrograms per cubic meter",
        ["station_id", "latitude", "longitude"],
    ),
    "pm10": Gauge(
        "air_quality_pm10_concentration_micrograms",
        "PM10 concentration in micrograms per cubic meter",
        ["station_id", "latitude", "longitude"],
    ),
    "o3": Gauge(
        "air_quality_o3_concentration_ppb",
        "O3 concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
    "voc": Gauge(
        "air_quality_voc_concentration_ppb",
        "VOC concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
    "hc": Gauge(
        "air_quality_hc_concentration_ppb",
        "HC concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
}


air_quailty_stations_list = config["AirQualityStation"]

air_quailty_stations: list[AirQualityStation] = []

# warning if no station is found

for station in air_quailty_stations_list:
    air_quailty_stations.append(
        AirQualityStation(
            station["station_id"],
            station["station_name"],
            station["location"]["latitude"],
            station["location"]["longitude"],
            station["status"],
            station["api_names"],
        )
    )


def collect_data():
    for station in air_quailty_stations:
        new_values = station.update_pollutant_data()

        for pollutants_type, value in new_values.items():
            if value == -1 or value == -2:
                output_value = float("NaN")
                logger.warning(f"Station {station.station_id}: {pollutants_type} = {value}")
            else:
                output_value = value
                logger.info(f"Station {station.station_id}: {pollutants_type} = {value}")

            metrics[pollutants_type].labels(
                station_id=station.station_id,
                latitude=station.latitude,
                longitude=station.longitude,
            ).set(output_value)


def handler(signum: int, frame: Optional[FrameType]):
    _ = signal.alarm(
        API_UPDATE_INTERVAL_SECONDS
    )  # set the alarm signal to call collect_data() every API_UPDATE_INTERVAL_SECONDS
    collect_data()  # collect the data


if __name__ == "__main__":
    _ = start_http_server(EXPORTER_PORT)  # start the http server at EXPORTER_PORT

    # set the alarm signal to call collect_data() every API_UPDATE_INTERVAL_SECONDS
    _ = signal.alarm(API_UPDATE_INTERVAL_SECONDS)
    # set the signal handler for SIGALRM
    _ = signal.signal(signal.SIGALRM, handler)

    # collect the data for the first time
    collect_data()

    while True:
        time.sleep(2 * API_UPDATE_INTERVAL_SECONDS)
