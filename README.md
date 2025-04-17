#Weather Service - README

This script collects weather data for agricultural parcels using the OpenMeteo API, processes it into WeatherObserved and WeatherForecast entities, and uploads it to a data service.
Features

    Fetches current weather and forecast data (temperature, humidity, wind speed, etc.) for specific locations.

    Uses scheduled tasks to update weather data daily at 10:00 AM.

    Uploads the data to a specified service (using sd_data_adapter).

Libraries

    schedule: For scheduling tasks.

    time: For adding delays.

    openmeteo_requests: For fetching weather data.

    geojson: For handling geospatial data.

    sd_data_adapter: For interacting with the data service.

Main Methods

    _get_weather_data(latitude, longitude, agri_parcel_id): Fetches weather data for a given location and creates weather entities.

    update_weather_for_parcels(): Fetches and processes weather data for all agricultural parcels.

    job(): Scheduled task that runs the update function daily at 10:00 AM.

Usage

    Install dependencies:

    pip install schedule geojson openmeteo_requests sd_data_adapter

    Run the script to start the service, which will update the weather data daily.

License

MIT License. See the LICENSE file for details.

This README provides a brief overview of how the service works, its key features, and usage instructions.