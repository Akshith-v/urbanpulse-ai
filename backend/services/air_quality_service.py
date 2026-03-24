import requests
from backend.config import settings

GEOCODE_URL = "http://api.openweathermap.org/geo/1.0/direct"
AIR_QUALITY_URL = "http://api.openweathermap.org/data/2.5/air_pollution"


def get_city_coordinates(city_name: str):
    params = {
        "q": f"{city_name},IN",
        "limit": 1,
        "appid": settings.openweather_api_key
    }

    response = requests.get(GEOCODE_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    if not data:
        raise ValueError(f"Coordinates not found for city: {city_name}")

    return {
        "lat": data[0]["lat"],
        "lon": data[0]["lon"]
    }


def fetch_air_quality(city_name: str):
    coords = get_city_coordinates(city_name)

    params = {
        "lat": coords["lat"],
        "lon": coords["lon"],
        "appid": settings.openweather_api_key
    }

    response = requests.get(AIR_QUALITY_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    air = data["list"][0]

    return {
        "city": city_name,
        "aqi": air["main"]["aqi"],
        "components": air["components"],
        "timestamp": air["dt"],
        "lat": coords["lat"],
        "lon": coords["lon"]
    }