import requests
from backend.config import settings


BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(city_name: str):
    params = {
        "q": f"{city_name},IN",
        "appid": settings.openweather_api_key,
        "units": "metric"
    }

    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    return {
        "city": data["name"],
        "temperature_c": data["main"]["temp"],
        "feels_like_c": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"],
        "timestamp": data["dt"]
    }