from http.server import BaseHTTPRequestHandler, HTTPServer
import openmeteo_requests
import json
from datetime import datetime
import numpy as np
import requests
import pandas as pd
from geojson import Point
from sd_data_adapter.api.search import search
from sd_data_adapter.api.upload import upload
from sd_data_adapter.client import DAClient
from sd_data_adapter.models.weather import WeatherObserved


try:
    SD_ADAPTER_AVAILABLE = True
    DAClient.get_instance()
except ImportError as e:
    SD_ADAPTER_AVAILABLE = False


class WeatherHandler(BaseHTTPRequestHandler):
    def _send_response(self, status_code, content_type, data):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')  # Dozvoli bilo koji domen
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')  # Dozvoljene metode
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')  # Dozvoljeni headeri
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=self._json_serializer).encode())

    def _json_serializer(self, obj):
        if isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        if isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        # Dodajte proveru za OpenMeteo objekte:
        if hasattr(obj, 'Value'):  # Ako objekat ima .Value() metodu (npr. VariablesWithTime)
            return obj.Value()
        if hasattr(obj, '__dict__'):  # Ako je običan Python objekat
            return obj.__dict__
        raise TypeError(f"Tip {type(obj)} nije serijalizabilan")

    def _get_weather_data(self, latitude, longitude, agri_parcel_id):
        """Dobavlja vremenske podatke za datu lokaciju i kreira WeatherObserved entitet"""

        om = openmeteo_requests.Client()

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": [
                "temperature_2m", "relative_humidity_2m", "apparent_temperature",
                "precipitation", "rain", "showers", "snowfall", "weather_code",
                "surface_pressure", "cloud_cover", "visibility", "wind_speed_10m",
                "wind_direction_10m", "wind_gusts_10m", "is_day"
            ],
            "daily": [
                "temperature_2m_max", "temperature_2m_min",
                "sunrise", "sunset",
                "precipitation_sum", "rain_sum", "showers_sum", "snowfall_sum", "uv_index_max"
            ],
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "dew_point_2m",
                "apparent_temperature", "precipitation", "rain", "showers",
                "snowfall", "weather_code", "surface_pressure", "cloud_cover",
                "visibility", "evapotranspiration", "et0_fao_evapotranspiration",
                "vapour_pressure_deficit", "wind_speed_10m", "wind_direction_10m",
                "wind_gusts_10m"
            ],
            "timezone": "auto"
        }

        try:
            responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
            if not responses or len(responses) == 0:
                raise ValueError("No data returned from the weather API.")

            for i in range(len(responses)):
                data = responses[i]

                current = data.Current()
                hourly = data.Hourly()
                daily = data.Daily()

                current_temperature_2m = current.Variables(0).Value()
                current_relative_humidity_2m = current.Variables(1).Value()
                current_apparent_temperature = current.Variables(2).Value()
                current_is_day = current.Variables(3).Value()
                current_wind_speed_10m = current.Variables(4).Value()
                current_wind_direction_10m = current.Variables(5).Value()
                current_precipitation = current.Variables(6).Value()
                current_rain = current.Variables(7).Value()
                current_showers = current.Variables(8).Value()
                current_snowfall = current.Variables(9).Value()
                current_wind_gusts_10m = current.Variables(10).Value()
                current_weather_code = current.Variables(11).Value()
                current_cloud_cover = current.Variables(12).Value()
                current_pressure_msl = current.Variables(13).Value()
                current_surface_pressure = current.Variables(14).Value()

                daily_max_temperature = daily.Variables(0).ValuesAsNumpy()
                daily_min_temperature = daily.Variables(1).ValuesAsNumpy()
                daily_sunrise = daily.Variables(2).ValuesInt64AsNumpy()
                daily_sunset = daily.Variables(3).ValuesInt64AsNumpy()
                daily_precipitation_sum = daily.Variables(4).ValuesAsNumpy()
                daily_rain_sum = daily.Variables(5).ValuesAsNumpy()
                daily_showers_sum = daily.Variables(6).ValuesAsNumpy()
                daily_snowfall_sum = daily.Variables(7).ValuesAsNumpy()
                daily_uv_index = daily.Variables(8).ValuesAsNumpy()

                daily_data = {"date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )}

                daily_data["maxTemperature"] = daily_max_temperature
                daily_data["minTemperature"] = daily_min_temperature
                daily_data["sunrise"] = daily_sunrise
                daily_data["sunset"] = daily_sunset
                daily_data["precipitationSum"] = daily_precipitation_sum
                daily_data["rainSum"] = daily_rain_sum
                daily_data["showersSum"] = daily_showers_sum
                daily_data["snowfallSum"] = daily_snowfall_sum
                daily_data["uvIndex"] = daily_uv_index


                hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
                hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
                hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
                hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
                hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
                hourly_rain = hourly.Variables(5).ValuesAsNumpy()
                hourly_showers = hourly.Variables(6).ValuesAsNumpy()
                hourly_snowfall = hourly.Variables(7).ValuesAsNumpy()
                hourly_weather_code = hourly.Variables(8).ValuesAsNumpy()
                hourly_surface_pressure = hourly.Variables(9).ValuesAsNumpy()
                hourly_cloud_cover = hourly.Variables(10).ValuesAsNumpy()
                hourly_visibility = hourly.Variables(11).ValuesAsNumpy()
                hourly_evapotranspiration = hourly.Variables(12).ValuesAsNumpy()
                hourly_et0_fao_evapotranspiration = hourly.Variables(13).ValuesAsNumpy()
                hourly_vapour_pressure_deficit = hourly.Variables(14).ValuesAsNumpy()
                hourly_wind_speed_10m = hourly.Variables(15).ValuesAsNumpy()
                hourly_wind_direction_10m = hourly.Variables(16).ValuesAsNumpy()
                hourly_wind_gusts_10m = hourly.Variables(17).ValuesAsNumpy()

                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

                weather_observed = WeatherObserved(
                    id=f"urn:ngsi-ld:WeatherObserved:{agri_parcel_id}:{timestamp}",
                    dateObserved=datetime.utcnow().isoformat() + "Z",
                    location=Point((longitude, latitude))
                )

                weather_observed.airTemperatureTSA = {
                    "averageValue": current_temperature_2m,
                    "instValue": current_temperature_2m,
                    "maxOverTime": daily_data["maxTemperature"].tolist(),
                    "minOverTime": daily_data["minTemperature"].tolist()
                }
                weather_observed.atmosphericPressure = current_surface_pressure
                weather_observed.dewPoint = hourly_dew_point_2m.tolist()
                weather_observed.description = "Podaci generisani na osnovu OpenMeteo API odgovora"
                weather_observed.dataProvider = "OpenMeteo"
                weather_observed.dateCreated = datetime.utcnow().isoformat() + "Z"
                weather_observed.dateModified = datetime.utcnow().isoformat() + "Z"

                try:
                    upload_result = upload(weather_observed)
                except Exception as upload_err:
                    print(f"Greška pri upisu WeatherObserved za parcelu {agri_parcel_id}: {upload_err}")

                return weather_observed

        except Exception as e:
            print(f"Error fetching weather data: {str(e)}")
            raise ValueError(f"Weather API error: {str(e)}")

    def do_GET(self):
        if self.path == '/weather-for-parcels':
            try:
                if SD_ADAPTER_AVAILABLE:
                    try:
                        ctx = "https://smartdatamodels.org/context.jsonld"
                        agri_parcels = search(params={"type": "https://smartdatamodels.org/dataModel.Agrifood/AgriParcel"}, ctx=ctx)
                    except Exception as e:
                        orion_url = "http://localhost:1026/v2/entities?type=AgriParcel"
                        headers = {"Accept": "application/json"}
                        res = requests.get(orion_url, headers=headers)
                        if res.status_code != 200:
                            self._send_response(500, 'application/json', {"error": "Neuspešno dobavljanje AgriParcel entiteta iz Orion-a"})
                            return
                        agri_parcels = res.json()
                else:
                    print("GRESKA ORION NIJE DOBRO POVEZAN\n")

                weather_data = []

                for agri_parcel in agri_parcels:
                    if agri_parcel.type != "https://smartdatamodels.org/dataModel.Agrifood/AgriParcel":
                        continue

                    try:
                        coords = agri_parcel.location["coordinates"][0][:-1]
                        centroid_lon = sum(c[0] for c in coords) / len(coords)
                        centroid_lat = sum(c[1] for c in coords) / len(coords)
                    except Exception as e:
                        continue

                    weather_info = self._get_weather_data(
                        centroid_lat, centroid_lon, agri_parcel.id
                    )
                    weather_data.append(weather_info)

                self._send_response(200, 'application/json', weather_data)

            except Exception as e:
                self._send_response(500, 'application/json', {"error": f"Greška na serveru: {str(e)}"})

if __name__ == '__main__':
    server = HTTPServer(('localhost', 31087), WeatherHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
