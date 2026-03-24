from sqlalchemy.orm import Session

from backend.models.city_weather import CityWeather
from backend.models.city_air_quality import CityAirQuality


def save_weather(db: Session, weather_data: dict):
    record = CityWeather(
        city=weather_data["city"],
        temperature_c=weather_data["temperature_c"],
        feels_like_c=weather_data["feels_like_c"],
        humidity=weather_data["humidity"],
        weather=weather_data["weather"],
        weather_description=weather_data["weather_description"],
        wind_speed=weather_data["wind_speed"],
        source_timestamp=weather_data["timestamp"],
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def save_air_quality(db: Session, air_data: dict):
    record = CityAirQuality(
        city=air_data["city"],
        aqi=air_data["aqi"],
        components=air_data["components"],
        source_timestamp=air_data["timestamp"],
        lat=air_data["lat"],
        lon=air_data["lon"],
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def get_latest_weather(db: Session, city_name: str):
    return (
        db.query(CityWeather)
        .filter(CityWeather.city == city_name)
        .order_by(CityWeather.created_at.desc())
        .first()
    )


def get_latest_air_quality(db: Session, city_name: str):
    return (
        db.query(CityAirQuality)
        .filter(CityAirQuality.city == city_name)
        .order_by(CityAirQuality.created_at.desc())
        .first()
    )


def get_recent_weather_history(db: Session, city_name: str, limit: int = 10):
    return (
        db.query(CityWeather)
        .filter(CityWeather.city == city_name)
        .order_by(CityWeather.created_at.desc())
        .limit(limit)
        .all()
    )


def get_recent_air_quality_history(db: Session, city_name: str, limit: int = 10):
    return (
        db.query(CityAirQuality)
        .filter(CityAirQuality.city == city_name)
        .order_by(CityAirQuality.created_at.desc())
        .limit(limit)
        .all()
    )