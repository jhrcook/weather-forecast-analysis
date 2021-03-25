#!/usr/bin/env python3

"""Download and organize the forecast data from the GitHub repo "jhrcook/weather-forecast-data"."""

import json
import pickle
import urllib.parse as urlparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Type

import requests
from github import Github
from pydantic import BaseModel
from weather_forecast_collection.apis import accuweather_api as accu
from weather_forecast_collection.apis import climacell_api as cc
from weather_forecast_collection.apis import national_weather_service_api as nws
from weather_forecast_collection.apis import openweathermap_api as owm

from keys import GITHUB_ACCESS_TOKEN

json_data_dir = Path("json-data")
pkl_dir = Path("data")
for dir in [json_data_dir, pkl_dir]:
    if not dir.exists():
        dir.mkdir()


def make_file_name(url: str) -> Path:
    return json_data_dir / urlparse.unquote(Path(url).name)


def download_data_file(url: str, force: bool = False) -> Path:
    path = make_file_name(url)

    if path.exists() and not force:
        return path

    response = requests.get(url)
    with open(path, "wb") as file:
        file.write(response.content)

    return path


def download_forecast_data_files():
    g = Github(GITHUB_ACCESS_TOKEN)
    data_repo = g.get_repo("jhrcook/weather-forecast-data")
    contents = data_repo.get_contents("data", "weather-data")
    for data_file in contents:
        _ = download_data_file(data_file.download_url, force=False)


class MetaData(BaseModel):
    source: str
    city: str
    timestamp: datetime
    file_path: Path


def parse_json_filepath(path: Path) -> MetaData:
    name = path.name.replace(".json", "")
    pieces = name.split("_")
    return MetaData(
        source=pieces[0], city=pieces[1], timestamp=pieces[2], file_path=path
    )


def get_data_model(name: str) -> Type[BaseModel]:
    d: Dict[str, Type[BaseModel]] = {
        "climacell": cc.CCForecastData,
        "national-weather-service": nws.NSWForecast,
        "accuweather": accu.AccuForecast,
        "open-weather-map": owm.OWMForecast,
    }
    return d[name]


def parse_json(meta_data: MetaData) -> BaseModel:
    model = get_data_model(meta_data.source)
    with open(meta_data.file_path, "r") as json_f:
        return model(**json.loads(json.load(json_f)))


def make_pickle_path(meta_data: MetaData) -> Path:
    return pkl_dir / urlparse.unquote(meta_data.file_path.name.replace(".json", ".pkl"))


def pickle_data(d: BaseModel, meta_data: MetaData, force: bool = False) -> Path:
    p = make_pickle_path(meta_data=meta_data)
    if p.exists() and not force:
        return p
    with open(p, "wb") as f:
        pickle.dump(d, f)
    return p


def pickle_data_types():
    for json_data in json_data_dir.iterdir():
        meta_data = parse_json_filepath(json_data)
        # TODO: Currently only works for NWS. (issue opened in collection package)
        if meta_data.source != "national-weather-service":
            continue
        data = parse_json(meta_data)
        pickle_data(data, meta_data)


if __name__ == "__main__":
    download_forecast_data_files()
    pickle_data_types()
