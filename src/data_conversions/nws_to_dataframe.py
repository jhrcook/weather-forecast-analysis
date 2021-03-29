"""Convert National Weather Service data to pandas DataFrames."""

from datetime import datetime
from typing import List

import pandas as pd
from weather_forecast_collection.apis import national_weather_service_api as nws


def nws_period_to_dataframe(period: nws.NWSPeriodForecast) -> pd.DataFrame:
    d = period.dict()
    return pd.DataFrame(d, index=[0])


def nws_periods_to_dataframe(
    periods: List[nws.NWSPeriodForecast], timestamp: datetime, city: str
) -> pd.DataFrame:
    df = pd.concat([nws_period_to_dataframe(p) for p in periods])
    df["collection_timestamp"] = timestamp
    df["city"] = city
    return df


def nws_to_dataframe(
    forecasts: List[nws.NWSForecast], cities: List[str]
) -> pd.DataFrame:
    daily_df = pd.concat(
        [
            nws_periods_to_dataframe(f.seven_day.periods, timestamp=f.timestamp, city=c)
            for f, c in zip(forecasts, cities)
        ]
    )
    hourly_df = pd.concat(
        [
            nws_periods_to_dataframe(
                f.hourly_forecast.periods, timestamp=f.timestamp, city=c
            )
            for f, c in zip(forecasts, cities)
        ]
    )
    daily_df["forecast_type"] = "daily"
    hourly_df["forecast_type"] = "hourly"
    df = pd.concat([daily_df, hourly_df]).reset_index(drop=True)
    return df
