import schedule
import time
import openmeteo_requests
from datetime import datetime, timedelta
from geojson import Point
from sd_data_adapter.api import search
from sd_data_adapter.api import upsert
from sd_data_adapter.client import DAClient
from sd_data_adapter.models.weather import WeatherObserved, WeatherForecast
import os

class WeatherService:
    def __init__(self):
        pass

    def update_weather_forecast_data(self, latitude, longitude, agri_parcel_id):
        om = openmeteo_requests.Client()


        start_date = datetime.utcnow()
        forecast_days = 14

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": [
                "temperature_2m", "relative_humidity_2m", "apparent_temperature",
                "precipitation", "weather_code",
                "surface_pressure", "wind_speed_10m",
                "wind_direction_10m", "wind_gusts_10m"
            ],
            "daily": [
                "temperature_2m_max", "temperature_2m_min",
                "uv_index_max"
            ],
            "hourly": [
                "relative_humidity_2m", "dew_point_2m",
                "apparent_temperature", "visibility"
            ],
            "timezone": "auto"
        }

        try:
            for day_offset in range(forecast_days):

                current_date = start_date + timedelta(days=day_offset)

                start_date_str = current_date.strftime('%Y-%m-%d')
                end_date_str = current_date.strftime('%Y-%m-%d')

                params["start_date"] = start_date_str
                params["end_date"] = end_date_str

                responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
                if not responses or len(responses) == 0:
                    raise ValueError(f"No data returned from the weather API for {start_date_str}.")

                for i in range(len(responses)):
                    data = responses[i]

                    current = data.Current()
                    hourly = data.Hourly()
                    daily = data.Daily()

                    current_temperature_2m = current.Variables(0).Value()
                    current_relative_humidity_2m = current.Variables(1).Value()
                    current_apparent_temperature = current.Variables(2).Value()
                    current_precipitation = current.Variables(3).Value()
                    current_weather_code = current.Variables(4).Value()
                    current_surface_pressure = current.Variables(5).Value()
                    current_wind_speed_10m = current.Variables(6).Value()
                    current_wind_direction_10m = current.Variables(7).Value()
                    current_wind_gusts_10m = current.Variables(8).Value()

                    daily_max_temperature = daily.Variables(0).ValuesAsNumpy()
                    daily_min_temperature = daily.Variables(1).ValuesAsNumpy()
                    daily_uv_index = daily.Variables(2).ValuesAsNumpy()

                    hourly_relative_humidity_2m = hourly.Variables(0).ValuesAsNumpy()
                    hourly_dew_point_2m = hourly.Variables(1).ValuesAsNumpy()
                    hourly_apparent_temperature = hourly.Variables(2).ValuesAsNumpy()
                    hourly_visibility = hourly.Variables(3).ValuesAsNumpy()

                    def get_weather_type(weather_code):
                        weather_mapping = {
                            0: "Sunny",
                            1: "Partly Cloudy",
                            2: "Cloudy",
                            3: "Rainy",
                            4: "Snowy",
                            5: "Foggy",
                            6: "Windy",
                            7: "Sensitive",
                            8: "Very Cloudy",
                            10: "Approaching Rain",
                            11: "Approaching Snow",
                        }

                        return weather_mapping.get(weather_code, f"Unknown weatherCode: {weather_code}")

                    # Generisanje objekta WeatherForecast za svaki dan
                    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                    weather_forecast = WeatherForecast(
                        id=f"urn:ngsi-ld:WeatherForecast:{agri_parcel_id}:{start_date_str}",
                        dateIssued=start_date_str + "Z",
                        location=Point((longitude, latitude))
                    )

                    weather_forecast.temperature = current_temperature_2m
                    weather_forecast.feelLikesTemperature = current_apparent_temperature
                    weather_forecast.relativeHumidity = current_relative_humidity_2m
                    weather_forecast.windSpeed = current_wind_speed_10m
                    weather_forecast.windDirection = current_wind_direction_10m
                    weather_forecast.gustSpeed = current_wind_gusts_10m
                    weather_forecast.precipitation = current_precipitation
                    weather_forecast.atmosphericPressure = current_surface_pressure
                    weather_forecast.weatherType = get_weather_type(int(current_weather_code))
                    weather_forecast.visibility = float(hourly_visibility[0])
                    weather_forecast.uVIndexMax = float(daily_uv_index[0]) if len(daily_uv_index) > 0 else None
                    weather_forecast.refPointOfInterest = agri_parcel_id

                    weather_forecast.dayMaximum = {
                        "temperature": float(daily_max_temperature[0]),
                        "feelLikesTemperature": float(hourly_apparent_temperature.max()),
                        "relativeHumidity": float(hourly_relative_humidity_2m.max())
                    }

                    weather_forecast.dayMinimum = {
                        "temperature": float(daily_min_temperature[0]),
                        "feelLikesTemperature": float(hourly_apparent_temperature.min()),
                        "relativeHumidity": float(hourly_relative_humidity_2m.min())
                    }

                    weather_forecast.description = "Data from Openmeteo API response"
                    weather_forecast.dataProvider = "OpenMeteo"
                    weather_forecast.dateCreated = datetime.utcnow().isoformat() + "Z"
                    weather_forecast.dateModified = datetime.utcnow().isoformat() + "Z"
                    weather_forecast.dateIssued = start_date_str

                    upsert(weather_forecast)

        except Exception as e:
            print(f"Error fetching weather data: {str(e)}")
            raise ValueError(f"Weather API error: {str(e)}")

    def save_weather_observed_data(self, latitude, longitude, agri_parcel_id):
        #UNUSED
        #Left in case of adding WeatherObserved objects
        try:

            om = openmeteo_requests.Client()

            date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": [
                    "temperature_2m", "relative_humidity_2m", "apparent_temperature",
                    "precipitation", "weather_code",
                    "surface_pressure", "wind_speed_10m",
                    "wind_direction_10m", "wind_gusts_10m"
                ],
                "daily": [
                    "temperature_2m_max", "temperature_2m_min",
                    "uv_index_max"
                ],
                "hourly": [
                    "relative_humidity_2m", "dew_point_2m",
                    "apparent_temperature", "visibility"
                ],
                "timezone": "auto",
                "start_date": date,
                "end_date": date
            }

            responses = om.weather_api("https://historical-forecast-api.open-meteo.com/v1/forecast", params=params)
            if not responses or len(responses) == 0:
                raise ValueError("No data returned from the weather API.")

            for i in range(len(responses)):
                data = responses[i]

                current = data.Current()
                hourly = data.Hourly()
                daily = data.Daily()

                current_temperature_2m = current.Variables(0).Value()
                current_surface_pressure = current.Variables(5).Value()

                daily_max_temperature = daily.Variables(0).ValuesAsNumpy()
                daily_min_temperature = daily.Variables(1).ValuesAsNumpy()

                hourly_dew_point_2m = hourly.Variables(1).ValuesAsNumpy()

                now_utc = datetime.utcnow()
                iso_timestamp = now_utc.isoformat() + "Z"

                weather_observed = WeatherObserved(
                    id=f"urn:ngsi-ld:WeatherObserved:{agri_parcel_id}:{date}",
                    dateObserved=date,
                    location=Point((longitude, latitude))
                )

                weather_observed.airTemperatureTSA = {
                    "averageValue": current_temperature_2m,
                    "instValue": current_temperature_2m,
                    "maxOverTime": daily_max_temperature.tolist(),
                    "minOverTime": daily_min_temperature.tolist()
                }
                weather_observed.atmosphericPressure = current_surface_pressure
                weather_observed.dewPoint = hourly_dew_point_2m.tolist()
                weather_observed.description = "Data from Openmeteo API response"
                weather_observed.dataProvider = "OpenMeteo"
                weather_observed.dateCreated = iso_timestamp
                weather_observed.dateModified = iso_timestamp

                upsert(weather_observed)

        except Exception as e:
            print(f"Error fetching or processing weather data: {e}")

    def update_weather_for_parcels(self):
        try:
            DAClient.get_instance(
                host=os.environ.get("ORION_HOST", "orion"),
                port=int(os.environ.get("ORION_PORT", "1026")))
            #DAClient.get_instance() #Terminal
            #DAClient.get_instance(host="orion", port=1026) #when starting without environment variables


            try:
                ctx = "https://smartdatamodels.org/context.jsonld"
                agri_parcels = search(params={"type": "https://smartdatamodels.org/dataModel.Agrifood/AgriParcel"}, ctx=ctx)
            except Exception as e:
                print("Error: Orion is not connected")

            for agri_parcel in agri_parcels:
                if agri_parcel.type != "https://smartdatamodels.org/dataModel.Agrifood/AgriParcel":
                    continue

                try:
                    coords = agri_parcel.location["coordinates"][0][:-1]
                    centroid_lon = sum(c[0] for c in coords) / len(coords)
                    centroid_lat = sum(c[1] for c in coords) / len(coords)
                except Exception as e:
                    continue


                #self.save_weather_observed_data(centroid_lat, centroid_lon, agri_parcel.id)
                self.update_weather_forecast_data(
                    centroid_lat, centroid_lon, agri_parcel.id
                )
        except Exception as e:
            print(f"Server error: {str(e)}")

def job():
    ws = WeatherService()
    ws.update_weather_for_parcels()

schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

