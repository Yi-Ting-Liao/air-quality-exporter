from prometheus_client import start_http_server, Gauge
from loguru import logger
from dotenv import load_dotenv
from typing import Optional
from types import FrameType
import re
import numpy as np
import pandas as pd
import requests
import json
import sys
import signal
import time
import os

_ = load_dotenv()

CONFIG_FILE = os.getenv("STATIONS_CONFIG_FILE", "config.json")  # default config file is config.json
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", 9100))  # default port is 9100
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
API_UPDATE_INTERVAL_SEC = int(os.getenv("API_UPDATE_INTERVAL_SEC", 300))  # default interval is 300 seconds
PROMETHEUS_QUERY_URL = os.getenv(
    "PROMETHEUS_QUERY_URL", "http://localhost:9090/api/v1/query"
)  # default url is localhost
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")  # default log level is WARNING


# setting up logging
logger.remove()  # remove the default logger
_ = logger.add(sys.stderr)  # add a new logger to stderr

if not CONFIG_FILE:
    logger.warning("STATIONS_CONFIG_FILE environment variable is not set, using default value config.json")
if not EXPORTER_PORT:
    logger.warning("EXPORTER_PORT environment variable is not set, using default value 9100")
if not API_URL:
    logger.error("API_URL environment variable is not set")
    sys.exit(1)
if not API_KEY:
    logger.error("API_KEY environment variable is not set")
    sys.exit(1)
if not API_UPDATE_INTERVAL_SEC:
    logger.warning("API_UPDATE_INTERVAL_SEC environment variable is not set, using default value 300")
if not PROMETHEUS_QUERY_URL:
    logger.warning(
        "PROMETHEUS_QUERY_URL environment variable is not set, using default value http://localhost:9090/api/v1/query"
    )
if not LOG_LEVEL:
    logger.warning("LOG_LEVEL environment variable is not set, using default value WARNING")

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

REQUEST_HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
    "charset": "utf-8",
    "Authorization": "Basic " + API_KEY,
}


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

            if API_URL is None:
                logger.error("API_URL is not set")
                continue

            try:
                response = requests.post(
                    API_URL,
                    data=json.dumps(request_data),
                    headers=REQUEST_HEADERS,
                )
                response.raise_for_status()
                r = response.json()

                if "Values" in r and len(r["Values"]) > 0 and "Value" in r["Values"][0]:
                    new_value = r["Values"][0]["Value"]
                else:
                    logger.error(f"Invalid response format for {pollutants_type}: {r}")
                    new_value = float("NaN")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {pollutants_type}: {e}")
                new_value = float("NaN")
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing response for {pollutants_type}: {e}")
                new_value = float("NaN")

            self.pollutants.update_pollutant_data(pollutants_type, new_value)

            new_values[pollutants_type] = new_value

        return new_values


# Setting up the metrics
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
        "air_quality_co2_ppm",
        "CO2 concentration in parts per million",
        ["station_id", "latitude", "longitude"],
    ),
    "co": Gauge(
        "air_quality_co_ppm",
        "CO concentration in parts per million",
        ["station_id", "latitude", "longitude"],
    ),
    "pm25": Gauge(
        "air_quality_pm25_microgram_cubic_meter",
        "PM2.5 concentration in micrograms per cubic meter",
        ["station_id", "latitude", "longitude"],
    ),
    "pm10": Gauge(
        "air_quality_pm10_microgram_cubic_meter",
        "PM10 concentration in micrograms per cubic meter",
        ["station_id", "latitude", "longitude"],
    ),
    "o3": Gauge(
        "air_quality_o3_ppb",
        "O3 concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
    "voc": Gauge(
        "air_quality_voc_ppb",
        "VOC concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
    "hc": Gauge(
        "air_quality_hc_ppb",
        "HC concentration in parts per billion",
        ["station_id", "latitude", "longitude"],
    ),
    "aqi": Gauge(
        "air_quality_aqi",
        "Air Quality Index",
        ["station_id", "latitude", "longitude"],
    ),
}


air_quality_stations_list = config["AirQualityStation"]

air_quality_stations: list[AirQualityStation] = []

# warning if no station is found
if not air_quality_stations_list:
    logger.warning("No station found in the config file")

for station in air_quality_stations_list:
    air_quality_stations.append(
        AirQualityStation(
            station["station_id"],
            station["station_name"],
            station["location"]["latitude"],
            station["location"]["longitude"],
            station["status"],
            station["api_names"],
        )
    )


def interpolate(xp, yp):
    return lambda a: np.interp(a, xp, yp)


def get_mean(url, hr):
    raw = requests.get(url).json()["data"]["result"]
    res = {s: np.float64(np.nan) for s in ["B1F", "M", "106", "A1F", "PL", "PR", "ARF"]}
    for i in raw:
        df = pd.DataFrame(i["values"])
        (data := df.iloc[:, 1].apply(lambda x: int(x) if x.isdigit() else np.nan)).index = pd.to_datetime(
            df.iloc[:, 0], unit="s"
        )
        res.update([(i["metric"]["station_id"], data.rolling(f"{hr}h", min_periods=1).mean().iloc[-1])])
    return res


def get_aqi_data(threshold=151):
    df = pd.DataFrame(
        {
            sid: get_mean(f"{PROMETHEUS_QUERY_URL}?query=air_quality_{name}%5B8h%5D", hr)
            for sid, name, hr in [
                ("O3_8", "o3_ppb", 8),
                ("O3_1", "o3_ppb", 1),
                ("PM25", "pm25_microgram_cubic_meter", 24),
                ("PM10", "pm10_microgram_cubic_meter", 24),
                ("CO", "co_ppm", 8),
            ]
        }
    )
    df[(df < AQI_min) | (read_error := (df > AQI_max) | (df < 0))] = np.nan
    df["AQI"] = np.nanmax(AQI_eval(funcs, df.T), axis=0)
    # df.loc["B1F", "AQI"] = np.nan  # 測試用
    df["risk"] = AQI_map.index[np.minimum(np.searchsorted(y, df["AQI"]) // 2, 6)]
    df.loc[pd.isna(df["AQI"]), "risk"] = np.nan
    df["abnormal"] = df["AQI"] >= threshold
    return df.iloc[:, 5:], read_error


AQI_map = pd.read_csv("AQI.csv", index_col=0)
AQI_map = AQI_map.apply(
    np.vectorize(lambda s: list(map(float, re.search(r"(\d+\.?\d*)\s*-\s*(\d+\.?\d*)", s).groups())), otypes=[list]),
    raw=True,
)[AQI_map != "0 - 0"]
(
    intervals := AQI_map.apply(
        lambda s: pd.Series(
            (np.vstack(s.dropna()).ravel(), np.repeat(~pd.isna(s).to_numpy(), 2)), index=["val", "mask"]
        )
    ).T
).iloc[:2, 0] *= 1000
y = np.vstack(AQI_map.index.to_series().apply(lambda s: list(map(int, re.search(r"(\d+)～(\d+)", s).groups())))).ravel()
AQI_min, AQI_max = intervals["val"].apply(lambda arr: pd.Series(arr[[0, -1]])).to_numpy().T
funcs = intervals.apply(lambda arr: interpolate(arr["val"], y[arr["mask"]]), axis=1).to_numpy()
AQI_eval = np.vectorize(lambda f, x: f(x), signature="(), (x)->(x)")


def collect_data():
    for station in air_quality_stations:
        new_values = station.update_pollutant_data()

        for pollutants_type, value in new_values.items():
            if value == -1 or value == -2:  # -1 or -2 indicates invalid data
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

    AQI, read_error = get_aqi_data()

    for station in air_quality_stations:
        aqi_value = AQI.loc[station.station_id, "AQI"]
        if aqi_value == np.nan:
            aqi_output_value = float("NaN")
            logger.warning(f"Station {station.station_id}: AQI = {aqi_value}")
        else:
            aqi_output_value = aqi_value
            logger.info(f"Station {station.station_id}: AQI = {aqi_value}")

        metrics["aqi"].labels(
            station_id=station.station_id,
            latitude=station.latitude,
            longitude=station.longitude,
        ).set(aqi_output_value)


def handler(signum: int, frame: Optional[FrameType]):
    """
    Signal handler function to collect data at regular intervals.

    Args:
        signum (int): The signal number.
        frame (Optional[FrameType]): The current stack frame (or None).
    """
    # set the alarm signal to call collect_data() every API_UPDATE_INTERVAL_SECONDS
    _ = signal.alarm(API_UPDATE_INTERVAL_SEC)

    collect_data()  # collect the data


if __name__ == "__main__":
    _ = start_http_server(EXPORTER_PORT)  # start the http server at EXPORTER_PORT

    # set the alarm signal to call collect_data() every API_UPDATE_INTERVAL_SECONDS
    _ = signal.alarm(API_UPDATE_INTERVAL_SEC)
    # set the signal handler for SIGALRM
    _ = signal.signal(signal.SIGALRM, handler)

    # collect the data for the first time
    collect_data()

    while True:
        time.sleep(2 * API_UPDATE_INTERVAL_SEC)
