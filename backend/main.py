from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import time

from data_pipeline.cities import CITIES
from backend.services.weather_service import fetch_weather
from backend.services.air_quality_service import fetch_air_quality
from backend.database.deps import get_db
from backend.database.crud import (
    save_weather,
    save_air_quality,
    get_latest_weather,
    get_latest_air_quality,
    get_recent_weather_history,
    get_recent_air_quality_history,
)
from backend.anomaly.service import run_anomaly_detection
from backend.models.environmental_anomaly import EnvironmentalAnomaly
from backend.models.city_weather import CityWeather
from backend.models.city_air_quality import CityAirQuality

app = FastAPI(title="UrbanPulse API")


@app.get("/")
def read_root():
    return {"message": "UrbanPulse API running"}


@app.get("/cities")
def get_cities():
    return {"cities": CITIES}


@app.post("/ingest/weather/{city_name}")
def ingest_weather(city_name: str, db: Session = Depends(get_db)):
    try:
        weather = fetch_weather(city_name)
        record = save_weather(db, weather)
        return {
            "message": "Weather stored successfully",
            "id": record.id,
            "city": record.city,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ingest/air-quality/{city_name}")
def ingest_air_quality(city_name: str, db: Session = Depends(get_db)):
    try:
        air = fetch_air_quality(city_name)
        record = save_air_quality(db, air)

        anomaly = run_anomaly_detection(db, city_name)

        return {
            "message": "Air quality stored successfully",
            "id": record.id,
            "city": record.city,
            "anomaly": anomaly,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stored/weather/{city_name}")
def stored_weather(city_name: str, db: Session = Depends(get_db)):
    record = get_latest_weather(db, city_name)
    if not record:
        raise HTTPException(status_code=404, detail="No stored weather data found")

    return {
        "id": record.id,
        "city": record.city,
        "temperature_c": record.temperature_c,
        "feels_like_c": record.feels_like_c,
        "humidity": record.humidity,
        "weather": record.weather,
        "weather_description": record.weather_description,
        "wind_speed": record.wind_speed,
        "source_timestamp": record.source_timestamp,
        "created_at": record.created_at,
    }


@app.get("/stored/air-quality/{city_name}")
def stored_air_quality(city_name: str, db: Session = Depends(get_db)):
    record = get_latest_air_quality(db, city_name)
    if not record:
        raise HTTPException(status_code=404, detail="No stored air quality data found")

    return {
        "id": record.id,
        "city": record.city,
        "aqi": record.aqi,
        "components": record.components,
        "source_timestamp": record.source_timestamp,
        "lat": record.lat,
        "lon": record.lon,
        "created_at": record.created_at,
    }


@app.get("/history/weather/{city_name}")
def weather_history(city_name: str, limit: int = 10, db: Session = Depends(get_db)):
    records = get_recent_weather_history(db, city_name, limit)

    return [
        {
            "id": r.id,
            "city": r.city,
            "temperature_c": r.temperature_c,
            "feels_like_c": r.feels_like_c,
            "humidity": r.humidity,
            "weather": r.weather,
            "weather_description": r.weather_description,
            "wind_speed": r.wind_speed,
            "source_timestamp": r.source_timestamp,
            "created_at": r.created_at,
        }
        for r in records
    ]


@app.get("/history/air-quality/{city_name}")
def air_quality_history(city_name: str, limit: int = 10, db: Session = Depends(get_db)):
    records = get_recent_air_quality_history(db, city_name, limit)

    return [
        {
            "id": r.id,
            "city": r.city,
            "aqi": r.aqi,
            "components": r.components,
            "source_timestamp": r.source_timestamp,
            "lat": r.lat,
            "lon": r.lon,
            "created_at": r.created_at,
        }
        for r in records
    ]


@app.get("/anomalies/{city_name}")
def get_anomalies(city_name: str, limit: int = 10, db: Session = Depends(get_db)):
    records = (
        db.query(EnvironmentalAnomaly)
        .filter(EnvironmentalAnomaly.city == city_name)
        .order_by(EnvironmentalAnomaly.created_at.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": r.id,
            "city": r.city,
            "metric_name": r.metric_name,
            "source_timestamp": r.source_timestamp,
            "value": r.value,
            "baseline": r.baseline,
            "z_score": r.z_score,
            "ml_score": r.ml_score,
            "detection_type": r.detection_type,
            "severity": r.severity,
            "created_at": r.created_at,
        }
        for r in records
    ]


@app.get("/anomalies/{city_name}/latest")
def get_latest_anomaly(city_name: str, db: Session = Depends(get_db)):
    record = (
        db.query(EnvironmentalAnomaly)
        .filter(EnvironmentalAnomaly.city == city_name)
        .order_by(EnvironmentalAnomaly.created_at.desc())
        .first()
    )

    if not record:
        raise HTTPException(status_code=404, detail="No anomalies found")

    return {
        "id": record.id,
        "city": record.city,
        "metric_name": record.metric_name,
        "source_timestamp": record.source_timestamp,
        "value": record.value,
        "baseline": record.baseline,
        "z_score": record.z_score,
        "ml_score": record.ml_score,
        "detection_type": record.detection_type,
        "severity": record.severity,
        "created_at": record.created_at,
    }


@app.post("/test/anomaly/{city_name}")
def test_anomaly(city_name: str, db: Session = Depends(get_db)):
    try:
        now_ts = int(time.time())

        weather = CityWeather(
            city=city_name,
            temperature_c=32.0,
            feels_like_c=36.0,
            humidity=55,
            weather="Clear",
            weather_description="clear sky",
            wind_speed=2.0,
            source_timestamp=now_ts,
        )

        air = CityAirQuality(
            city=city_name,
            aqi=999,
            components={
                "co": 1200.0,
                "no": 10.0,
                "no2": 40.0,
                "o3": 20.0,
                "so2": 15.0,
                "pm2_5": 300.0,
                "pm10": 500.0,
                "nh3": 8.0,
            },
            source_timestamp=now_ts,
            lat=17.3850,
            lon=78.4867,
        )

        db.add(weather)
        db.add(air)
        db.commit()

        anomaly = run_anomaly_detection(db, city_name)

        return {
            "message": "Test anomaly record inserted",
            "city": city_name,
            "anomaly": anomaly,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))