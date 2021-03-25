"""Convert National Weather Service data to pandas DataFrames."""

from datetime import datetime
from typing import List

import pandas as pd
from weather_forecast_collection.apis import national_weather_service_api as nws


def nws_period_to_dataframe(period: nws.NWSPeriodForecast) -> pd.DataFrame:
    d = period.dict()
    return pd.DataFrame(d, index=[0])


def nws_periods_to_dataframe(periods: List[nws.NWSPeriodForecast]) -> pd.DataFrame:
    return pd.concat([nws_period_to_dataframe(p) for p in periods])


def nws_to_dataframe(forecasts: List[nws.NSWForecast]) -> pd.DataFrame:
    # TODO: Add collection timestamp when added to data object.
    daily_df = pd.concat(
        [nws_periods_to_dataframe(f.seven_day.periods) for f in forecasts]
    )
    hourly_df = pd.concat(
        [nws_periods_to_dataframe(f.hourly_forecast.periods) for f in forecasts]
    )
    daily_df["forecast_type"] = "daily"
    hourly_df["forecast_type"] = "hourly"
    df = pd.concat([daily_df, hourly_df]).reset_index(drop=True)
    return df
