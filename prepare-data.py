#!/usr/bin/env python3

"""Download and organize the forecast data from the GitHub repo "jhrcook/weather-forecast-data"."""

import json
import logging
import pickle
import urllib.parse as urlparse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Type

import pandas as pd
import requests
import typer
from github import Github
from pydantic import BaseModel
from weather_forecast_collection.apis import accuweather_api as accu
from weather_forecast_collection.apis import climacell_api as cc
from weather_forecast_collection.apis import national_weather_service_api as nws
from weather_forecast_collection.apis import openweathermap_api as owm

from keys import GITHUB_ACCESS_TOKEN
from src.data_conversions.accuweather_to_dataframe import accu_to_dataframe
from src.data_conversions.climacell_to_dataframe import climacell_to_dataframe
from src.data_conversions.nws_to_dataframe import nws_to_dataframe
from src.data_conversions.owm_to_dataframe import owm_to_dataframe

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] - %(message)s")

json_data_dir = Path("data", "json-data")
pkl_dir = Path("data", "pkl-data")
df_dir = Path("data", "dataframes")
for dir in [json_data_dir, pkl_dir, df_dir]:
    if not dir.exists():
        dir.mkdir()

data_branch = "weather-data"

#### ---- Download raw data from GitHub repo ---- ####


def make_file_name(url: str) -> Path:
    return json_data_dir / urlparse.unquote(Path(url).name)


def download_data_file(url: str, force: bool = False, n_attempt: int = 1) -> Path:
    MAX_ATTEMPTS = 3
    path = make_file_name(url)

    if path.exists() and not force:
        return path

    response = requests.get(url)
    if response.status_code == 200:
        with open(path, "wb") as file:
            file.write(response.content)
    elif n_attempt <= MAX_ATTEMPTS:
        download_data_file(url=url, n_attempt=n_attempt + 1)
    else:
        logging.error(f"Failed to aquire {url} after {MAX_ATTEMPTS} attempts.")

    return path


def get_data_urls() -> List[str]:
    g = Github(GITHUB_ACCESS_TOKEN)
    urls: List[str] = []
    data_repo = g.get_repo("jhrcook/weather-forecast-data")
    for first_lvl_dir in data_repo.get_contents("data", data_branch):
        for second_lvl_dir in data_repo.get_contents(first_lvl_dir.path, data_branch):
            for data_file in data_repo.get_contents(second_lvl_dir.path, data_branch):
                urls.append(data_file.download_url)
    return urls


def download_forecast_data_files(force_download: bool = False):
    data_file_urls = get_data_urls()
    logging.info(f"Found {len(data_file_urls)} data files in GitHub repo.")
    with typer.progressbar(data_file_urls) as progress:
        for url in progress:
            _ = download_data_file(url, force=force_download)


#### ---- Convert JSON data into pickled models ---- ####


class MetaData(BaseModel):
    source: str
    city: str
    timestamp: datetime
    json_path: Path
    pkl_path: Path


def make_pickle_path(json_path: Path) -> Path:
    return pkl_dir / urlparse.unquote(json_path.name.replace(".json", ".pkl"))


def parse_json_filepath(path: Path) -> MetaData:
    name = path.name.replace(".json", "")
    pieces = name.split("_")
    return MetaData(
        source=pieces[0],
        city=pieces[1],
        timestamp=pieces[2],
        json_path=path,
        pkl_path=make_pickle_path(json_path=path),
    )


def get_data_model(name: str) -> Type[BaseModel]:
    d: Dict[str, Type[BaseModel]] = {
        "climacell": cc.CCForecastData,
        "national-weather-service": nws.NWSForecast,
        "accuweather": accu.AccuForecast,
        "open-weather-map": owm.OWMForecast,
    }
    return d[name]


def parse_json(meta_data: MetaData) -> BaseModel:
    model = get_data_model(meta_data.source)
    with open(meta_data.json_path, "r") as json_f:
        return model(**json.loads(json.load(json_f)))


def pickle_data(d: BaseModel, meta_data: MetaData, force: bool = False) -> None:
    if meta_data.pkl_path.exists() and not force:
        return
    with open(meta_data.pkl_path, "wb") as f:
        pickle.dump(d, f)
    return


def pickle_data_types(force: bool = False) -> List[MetaData]:
    meta_datas: List[MetaData] = []
    all_json_files = list(json_data_dir.iterdir())
    with typer.progressbar(all_json_files) as progress:
        for json_data in progress:
            meta_data = parse_json_filepath(json_data)
            meta_datas.append(meta_data)
            if not meta_data.pkl_path.exists() or force:
                data = parse_json(meta_data)
                pickle_data(data, meta_data)
    return meta_datas


#### ---- Compile data into DataFrames ---- ####


def read_pickle(p: Path) -> Any:
    with open(p, "rb") as f:
        return pickle.load(f)


def compile_data(meta_data: List[MetaData], source: str) -> pd.DataFrame:
    fxns: Dict[str, Callable] = {
        "accuweather": accu_to_dataframe,
        "climacell": climacell_to_dataframe,
        "national-weather-service": nws_to_dataframe,
        "open-weather-map": owm_to_dataframe,
    }
    data = [read_pickle(md.pkl_path) for md in meta_data]
    fxn = fxns[source]
    return fxn(data, cities=[d.city for d in meta_data])


def save_compiled_data(source: str, data: pd.DataFrame) -> Path:
    pkl_path = df_dir / (source + ".pkl")
    csv_path = df_dir / (source + ".csv")
    data.to_pickle(path=pkl_path.as_posix())
    data.to_csv(csv_path)
    return csv_path


def main(force_download: bool = False, force_pickle: bool = False) -> None:
    logging.info("Downloading data from GitHub repo.")
    download_forecast_data_files(force_download=force_download)
    logging.info("Converting JSON to data types and pickling.")
    meta_data = pickle_data_types(force=force_pickle)
    logging.info(f"Downloaded and pickled {len(meta_data)} data points.")
    sources = set([d.source for d in meta_data])
    for source in sources:
        if source == "accuweather":
            logging.info("Skipping AccuWeather data preparation.")
            continue
        logging.info(f"Compiling '{source}' data...")
        md = [d for d in meta_data if d.source == source]
        data = compile_data(md, source)
        csv_path = save_compiled_data(source=source, data=data)
        logging.info(f"Data from '{source}' saved to '{csv_path.as_posix()}'")


if __name__ == "__main__":
    typer.run(main)
