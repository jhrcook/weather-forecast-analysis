"""Convert National Weather Service data to pandas DataFrames."""

from datetime import datetime
from pprint import pprint
from typing import List

import pandas as pd
from weather_forecast_collection.apis import climacell_api as cc


def ccdata_to_dataframe(data: cc.CCData) -> pd.DataFrame:
    df = pd.DataFrame(data.values.dict(), index=[0])
    df["starttime"] = data.startTime
    return df


def intervals_to_dataframe(
    intervals: List[cc.CCData], forecast_type: str
) -> pd.DataFrame:
    df = pd.concat([ccdata_to_dataframe(d) for d in intervals])
    df["forecast_type"] = forecast_type
    return df


def forecast_to_dataframe(forecast: cc.CCForecastData, city: str) -> pd.DataFrame:
    df = pd.concat(
        [
            intervals_to_dataframe(forecast.current.intervals, forecast_type="current"),
            intervals_to_dataframe(forecast.oneHour.intervals, forecast_type="hourly"),
            intervals_to_dataframe(forecast.oneDay.intervals, forecast_type="daily"),
        ]
    )
    df["collection_timestamp"] = forecast.timestamp
    df["city"] = city.lower()
    return df


def climacell_to_dataframe(
    forecasts: List[cc.CCForecastData], cities: List[str]
) -> pd.DataFrame:
    return pd.concat(
        [
            forecast_to_dataframe(forecast, city)
            for forecast, city in zip(forecasts, cities)
        ]
    )
