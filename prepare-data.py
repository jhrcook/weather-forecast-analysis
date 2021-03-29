#!/usr/bin/env python3

"""Download and organize the forecast data from the GitHub repo "jhrcook/weather-forecast-data"."""

import json
import pickle
import urllib.parse as urlparse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Type

import pandas as pd
import requests
from github import Github
from pandas.core.frame import DataFrame
from pydantic import BaseModel
from weather_forecast_collection.apis import accuweather_api as accu
from weather_forecast_collection.apis import climacell_api as cc
from weather_forecast_collection.apis import national_weather_service_api as nws
from weather_forecast_collection.apis import openweathermap_api as owm

from keys import GITHUB_ACCESS_TOKEN
from src.data_conversions.accuweather_to_dataframe import accu_to_dataframe
from src.data_conversions.climacell_to_dataframe import climacell_to_dataframe
from src.data_conversions.nws_to_dataframe import nws_to_dataframe

json_data_dir = Path("data", "json-data")
pkl_dir = Path("data", "pkl-data")
df_dir = Path("data", "dataframes")
for dir in [json_data_dir, pkl_dir, df_dir]:
    if not dir.exists():
        dir.mkdir()


#### ---- Download raw data from GitHub repo ---- ####


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


def pickle_data_types() -> List[MetaData]:
    meta_datas: List[MetaData] = []
    for json_data in json_data_dir.iterdir():
        meta_data = parse_json_filepath(json_data)
        meta_datas.append(meta_data)
        data = parse_json(meta_data)
        pickle_data(data, meta_data)
    return meta_datas


#### ---- Compile data into DataFrames ---- ####


def read_pickle(p: Path) -> Any:
    with open(p, "rb") as f:
        return pickle.load(f)


def compile_data(meta_data: List[MetaData], source: str) -> pd.DataFrame:
    # TODO: Add other services when data types are fixed.
    fxns: Dict[str, Callable] = {
        "accuweather": accu_to_dataframe,
        "climacell": climacell_to_dataframe,
        "national-weather-service": nws_to_dataframe,
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


if __name__ == "__main__":
    download_forecast_data_files()
    meta_data = pickle_data_types()
    sources = set([d.source for d in meta_data])
    for source in sources:
        if source == "accuweather":
            continue
        md = [d for d in meta_data if d.source == source]
        data = compile_data(md, source)
        csv_path = save_compiled_data(source=source, data=data)
        print(f"Data from '{source}' saved to '{csv_path.as_posix()}'")
