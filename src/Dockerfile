FROM python:3.11-slim

RUN apt-get update && apt-get install -y git  # Install git

WORKDIR /weather_service

COPY . /weather_service

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "WeatherService.py"]
