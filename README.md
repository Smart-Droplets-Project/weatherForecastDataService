# Weather Service - README

This script collects weather data for agricultural parcels using the OpenMeteo API, processes it into WeatherObserved and WeatherForecast entities, and uploads it to a data service.

## Features

    Fetches current weather and forecast data (temperature, humidity, wind speed, etc.) for specific locations.

    Uses scheduled tasks to update weather data daily at 10:00 AM.

    Uploads the data to a specified service (using sd_data_adapter).

## Libraries

    schedule: For scheduling tasks.

    time: For adding delays.

    openmeteo_requests: For fetching weather data.

    geojson: For handling geospatial data.

    sd_data_adapter: For interacting with the data service.

## Main Methods

    _get_weather_data(latitude, longitude, agri_parcel_id): Fetches weather data for a given location and creates weather entities.

    update_weather_for_parcels(): Fetches and processes weather data for all agricultural parcels.

    job(): Scheduled task that runs the update function daily at 10:00 AM.

## Usage

    Install dependencies:

    pip install schedule geojson openmeteo_requests sd_data_adapter

    Run the script to start the service, which will update the weather data daily.

## License

Copyright 2024 VizLore Labs Foundation

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


