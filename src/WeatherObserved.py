from http.server import BaseHTTPRequestHandler, HTTPServer
import openmeteo_requests
import json
from datetime import datetime
import numpy as np

class WeatherHandler(BaseHTTPRequestHandler):
    def _send_response(self, status_code, content_type, data):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, default=self._json_serializer).encode())

    def _json_serializer(self, obj):
        """Helper function for numpy type serialization"""
        if isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        if isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    def do_POST(self):
        if self.path == '/weather-observed':
            try:
                # Read and parse the request body
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)

                try:
                    agri_parcel = json.loads(post_data)
                except json.JSONDecodeError:
                    self._send_response(400, 'application/json', {"error": "Invalid JSON in request body"})
                    return

                if not isinstance(agri_parcel, dict) or agri_parcel.get('type') != 'AgriParcel':
                    self._send_response(400, 'application/json', {"error": "Invalid AgriParcel entity"})
                    return

                # Extract coordinates from location
                if not agri_parcel.get('location') or not agri_parcel['location'].get('coordinates'):
                    self._send_response(400, 'application/json', {"error": "AgriParcel must have location with coordinates"})
                    return

                try:
                    polygon_coords = agri_parcel['location']['coordinates'][0]  # Get the polygon coordinates
                    unique_coords = polygon_coords[:-1]  # Exclude the last (duplicate) point

                    centroid_lon = sum(coord[0] for coord in unique_coords) / len(unique_coords)
                    centroid_lat = sum(coord[1] for coord in unique_coords) / len(unique_coords)
                    lat, lon = centroid_lat, centroid_lon
                except Exception as e:
                    self._send_response(400, 'application/json', {"error": f"Invalid coordinates in AgriParcel location: {str(e)}"})
                    return

                # Get weather data from Open-Meteo
                om = openmeteo_requests.Client()
                weather_params = {
                    "latitude": lat,
                    "longitude": lon,
                    "current": ["temperature_2m", "relative_humidity_2m", "dew_point_2m",
                                "pressure_msl", "wind_speed_10m", "weather_code", "wind_direction_10m"],
                    "hourly": ["temperature_2m", "precipitation", "snow_height"],
                    "timezone": "auto"
                }

                responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=weather_params)
                response = responses[0]
                current = response.Current()
                hourly = response.Hourly()

                # Convert data
                hourly_temps = [float(x) for x in hourly.Variables(0).ValuesAsNumpy()]
                hourly_precipitation = [float(x) for x in hourly.Variables(1).ValuesAsNumpy()]
                hourly_snow = [float(x) for x in hourly.Variables(2).ValuesAsNumpy()]

                # Create NGSI-LD weather entity
                weather_data = {
                    "id": f"urn:ngsi-ld:WeatherObserved:{agri_parcel['id'].split(':')[-1]}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                    "type": "WeatherObserved",
                    "dateObserved": {
                        "type": "DateTime",
                        "value": datetime.utcnow().isoformat() + "Z"
                    },
                    "location": {
                        "type": "geo:json",
                        "value": {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        }
                    },
                    "temperature": {
                        "type": "Number",
                        "value": float(current.Variables(0).Value())
                    },
                    "relativeHumidity": {
                        "type": "Number",
                        "value": float(current.Variables(1).Value())
                    },
                    "dewPoint": {
                        "type": "Number",
                        "value": float(current.Variables(2).Value())
                    },
                    "atmosphericPressure": {
                        "type": "Number",
                        "value": float(current.Variables(3).Value())
                    },
                    "windSpeed": {
                        "type": "Number",
                        "value": float(current.Variables(4).Value())
                    },
                    "windDirection": {
                        "type": "Number",
                        "value": float(current.Variables(6).Value())
                    },
                    "weatherCode": {
                        "type": "Number",
                        "value": int(current.Variables(5).Value())
                    },
                    "precipitation": {
                        "type": "Number",
                        "value": float(np.mean(hourly_precipitation))
                    },
                    "snowHeight": {
                        "type": "Number",
                        "value": float(np.mean(hourly_snow))
                    },
                    "dataProvider": {
                        "type": "Text",
                        "value": "OpenMeteo"
                    },
                    "source": {
                        "type": "Text",
                        "value": "https://open-meteo.com"
                    },
                    "refAgriParcel": {
                        "type": "Text",
                        "value": agri_parcel['id']
                    },
                    #"@context": [
                    #    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                    #    "https://smart-data-models.github.io/dataModel.Weather/context.jsonld",
                    #    "https://smart-data-models.github.io/dataModel.Agrifood/context.jsonld"
                    #]
                }

                self._send_response(200, 'application/ld+json', weather_data)

            except Exception as e:
                self._send_response(500, 'application/json', {"error": f"Server error: {str(e)}"})

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8000), WeatherHandler)
    print("Server running at http://localhost:8000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer is shutting down gracefully...")
        server.server_close()
        print("Server stopped.")