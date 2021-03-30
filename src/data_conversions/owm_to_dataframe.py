"""Convert National Weather Service data to pandas DataFrames."""

from datetime import date, datetime
from pprint import pprint
from typing import Any, Dict, List

import pandas as pd
from weather_forecast_collection.apis import openweathermap_api as owm


def make_onerow_df(d: Dict[str, Any], city: str, timestamp: datetime) -> pd.DataFrame:
    df = pd.DataFrame(d, index=[0])
    df["city"] = city
    df["timestamp"] = timestamp
    return df


def split_weather_description(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    weather_description = data_dict.pop("weather")
    if weather_description is None:
        return data_dict
    data_dict["weather"] = weather_description["main"]
    data_dict["weather_description"] = weather_description["description"]
    return data_dict


# def combine_and_lengthen_daily_temperatures(temp: Dict[str, float], feels_like: Dict[str, float]) -> pd.DataFrame:


def daily_to_df(
    daily_forecast: owm.OWMWeatherDaily, city: str, timestamp: datetime
) -> pd.DataFrame:
    daily_dict = split_weather_description(daily_forecast.dict())
    feels_like = daily_dict.pop("feels_like")
    temp = daily_dict.pop("temp")

    base_df = make_onerow_df(daily_dict, city=city, timestamp=timestamp)
    final_df = pd.DataFrame()
    for k in feels_like.keys():
        new_row = base_df.copy().merge(
            pd.DataFrame(
                {"time_of_day": k, "temp": temp[k], "feels_like": feels_like[k]},
                index=[0],
            ),
            left_index=True,
            right_index=True,
        )
        final_df = pd.concat([final_df, new_row])

    return final_df


def daily_forecasts_to_df(
    daily_forecasts: List[owm.OWMWeatherDaily], city: str, timestamp: datetime
) -> pd.DataFrame:
    return pd.concat(
        [daily_to_df(f, city, timestamp) for f in daily_forecasts]
    ).reset_index(drop=True)


def hourly_to_df(
    hourly_forecast: owm.OWMWeatherHourly, city: str, timestamp: datetime
) -> pd.DataFrame:
    hourly_dict = split_weather_description(hourly_forecast.dict())
    return make_onerow_df(hourly_dict, city=city, timestamp=timestamp)


def hourly_forecasts_to_df(
    hourly_forecasts: List[owm.OWMWeatherHourly], city: str, timestamp: datetime
) -> pd.DataFrame:
    return pd.concat(
        [hourly_to_df(f, city, timestamp) for f in hourly_forecasts]
    ).reset_index(drop=True)


def current_to_df(
    current_forecast: owm.OWMWeatherCurrent, city: str, timestamp: datetime
) -> pd.DataFrame:
    current_dict = split_weather_description(current_forecast.dict())
    return make_onerow_df(current_dict, city=city, timestamp=timestamp)


def owm_to_dataframe(
    forecasts: List[owm.OWMForecast], cities: List[str]
) -> pd.DataFrame:
    current_df = pd.concat(
        [current_to_df(f.current, c, f.timestamp) for f, c in zip(forecasts, cities)]
    )
    hourly_df = pd.concat(
        [
            hourly_forecasts_to_df(f.hourly, c, f.timestamp)
            for f, c in zip(forecasts, cities)
        ]
    )
    daily_df = pd.concat(
        [
            daily_forecasts_to_df(f.daily, c, f.timestamp)
            for f, c in zip(forecasts, cities)
        ]
    )
    current_df["forecast_type"] = "current"
    hourly_df["forecast_type"] = "hourly"
    daily_df["forecast_type"] = "daily"
    df = pd.concat([current_df, hourly_df, daily_df]).reset_index(drop=True)
    return df
