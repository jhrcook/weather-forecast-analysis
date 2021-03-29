"""Convert Accuweather data to a panda DataFrame."""

from datetime import datetime
from pprint import pprint
from typing import List

import pandas as pd
from weather_forecast_collection.apis import accuweather_api as accu


def current_conditions_to_dataframe(
    data: accu.AccuConditions, timestamp: datetime, city: str
) -> pd.DataFrame:
    data_dict = data.dict()
    temperature_keys = [k for k in data_dict.keys() if "Temperature" in k]
    for k in temperature_keys:
        data_dict[k] = data_dict[k]["Metric"]["Value"]
    df = pd.DataFrame(data_dict, index=[0])
    df["collection_timestamp"] = timestamp
    df["city"] = city
    return df


def weathersummary_to_dataframe(summary_f: accu.AccuSummary) -> pd.DataFrame:
    data_dict = summary_f.dict()
    total_liquid = data_dict.pop("TotalLiquid")
    data_dict["TotalLiquid"] = total_liquid["Value"]
    data_dict["TotalLiquidUnit"] = total_liquid["Unit"]
    return pd.DataFrame(data_dict, index=[0])


def day_to_dataframe(day_f: accu.AccuDayForecast) -> pd.DataFrame:
    data_dict = day_f.dict()
    temperature_data = {}
    temperature_keys = [k for k in data_dict.keys() if "Temperature" in k]
    for k in temperature_keys:
        d = data_dict.pop(k)
        temperature_data[k + "Min"] = d["Minimum"]["Value"]
        temperature_data[k + "MinUnit"] = d["Minimum"]["Unit"]
        temperature_data[k + "Max"] = d["Maximum"]["Value"]
        temperature_data[k + "MaxUnit"] = d["Maximum"]["Unit"]
    temp_df = pd.DataFrame(temperature_data, index=[0])

    day_df = weathersummary_to_dataframe(day_f.Day)
    day_df["IsDaylight"] = True
    night_df = weathersummary_to_dataframe(day_f.Night)
    night_df["IsDaylight"] = False

    return pd.concat([temp_df, day_df, night_df]).reset_index(drop=True)


def fiveday_to_dataframe(
    data: accu.AccuFiveDayForecast, timestamp: datetime, city: str
) -> pd.DataFrame:
    df = pd.concat([day_to_dataframe(f) for f in data.DailyForecasts])
    df["collection_timestamp"] = timestamp
    df["city"] = city
    return df


# def accu_to_dataframe(
#     forecasts: List[accu.AccuForecast], cities: List[str]
# ) -> pd.DataFrame:
#     conditions_df = pd.concat(
#         [
#             current_conditions_to_dataframe(f.conditions, f.timestamp, c)
#             for f, c in zip(forecasts, cities)
#         ]
#     ).reset_index(drop=True)
#     conditions_df["forecast_type"] = "current_conditions"

#     fiveday_df = pd.concat(
#         [
#             fiveday_to_dataframe(f.fiveday, f.timestamp, c)
#             for f, c in zip(forecasts, cities)
#         ]
#     ).reset_index(drop=True)
#     fiveday_df["forecast_type"] = "daily"

#     df = pd.concat([conditions_df, fiveday_df])
#     print(df.head())
#     return df


def accu_to_dataframe(*args, **kwargs) -> pd.DataFrame:
    raise Exception("Accuweather data preparation pipeline incomplete.")
    return pd.DataFrame({"a": [1, 2, 3]})


# ! How the Accuweather data should be prepared is not currently clear.
# ! Since it is only collected for Boston and every 2 hours, I will skip
# ! the processing of this data for now and worry about it when I have a
# ! use for it.
