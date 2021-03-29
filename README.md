# Weather Forecast Data

[![python](https://img.shields.io/badge/Python-3.9-3776AB.svg?style=flat&logo=python)](https://www.python.org)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![License: GPLv3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

An analysis of weather forecast and nowcast data collected in the ['weather-forecast-data'](https://github.com/jhrcook/weather-forecast-data) repository.

The goal is to understand and compare weather predictions from multiple data sources.
Hopefully, I'll be able to identify trends in error that will help me decide which source is the most reliable.
It may also be possible to create a simple machine learning model to make predictions using data from all of the sources.

## Data

The data are collected and converted to ['pydantic'](https://pydantic-docs.helpmanual.io) models using the ['weather_forecast_collection'](https://github.com/jhrcook/weather_forecast_collection) package I built for this project.

The data collected are available in the "data/" directory of the [`weather-data`](https://github.com/jhrcook/weather-forecast-data/tree/weather-data) branch of the GitHub repo.
Check out that repo for details on how the data are collected.
