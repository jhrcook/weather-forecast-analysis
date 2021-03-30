import os

from setuptools import find_packages, setup

setup(
    name="weather-forecast-analysis",
    version="0.1.0",
    author="Joshua Cook",
    description=("Analysis of the accuracy of weather forecasts."),
    keywords="weather forecast data analysis",
    url="https://github.com/jhrcook/weather-forecast-analysis",
    project_urls={
        "Issue Tracker": "https://github.com/jhrcook/weather-forecast-analysis/issues",
    },
    packages=find_packages(),
)
