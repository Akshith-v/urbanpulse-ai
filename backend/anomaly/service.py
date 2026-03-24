import pandas as pd
from sqlalchemy.orm import Session

from backend.models.city_weather import CityWeather
from backend.models.city_air_quality import CityAirQuality
from backend.models.environmental_anomaly import EnvironmentalAnomaly
from backend.anomaly.detector import detect_anomaly


def fetch_recent_combined_data(db: Session, city: str, limit: int = 20) -> pd.DataFrame | None:
    weather_records = (
        db.query(CityWeather)
        .filter(CityWeather.city == city)
        .order_by(CityWeather.source_timestamp.asc())
        .limit(limit)
        .all()
    )

    air_records = (
        db.query(CityAirQuality)
        .filter(CityAirQuality.city == city)
        .order_by(CityAirQuality.source_timestamp.asc())
        .limit(limit)
        .all()
    )

    if not weather_records or not air_records:
        return None

    weather_df = pd.DataFrame([
        {
            "source_timestamp": w.source_timestamp,
            "temperature": w.temperature_c,
            "feels_like": w.feels_like_c,
            "humidity": w.humidity,
            "wind_speed": w.wind_speed,
        }
        for w in weather_records
    ])

    air_df = pd.DataFrame([
        {
            "source_timestamp": a.source_timestamp,
            "aqi": a.aqi,
        }
        for a in air_records
    ])

    if weather_df.empty or air_df.empty:
        return None

    merged = pd.merge_asof(
        air_df.sort_values("source_timestamp"),
        weather_df.sort_values("source_timestamp"),
        on="source_timestamp",
        direction="nearest",
    )

    merged = merged.dropna().reset_index(drop=True)

    if merged.empty:
        return None

    return merged


def anomaly_already_exists(db: Session, city: str, metric_name: str, source_timestamp: int) -> bool:
    existing = (
        db.query(EnvironmentalAnomaly)
        .filter(EnvironmentalAnomaly.city == city)
        .filter(EnvironmentalAnomaly.metric_name == metric_name)
        .filter(EnvironmentalAnomaly.source_timestamp == source_timestamp)
        .first()
    )
    return existing is not None


def run_anomaly_detection(db: Session, city: str):
    df = fetch_recent_combined_data(db, city)

    if df is None or df.empty:
        return None

    result = detect_anomaly(df)

    if not result:
        return None

    if anomaly_already_exists(db, city, result["metric_name"], result["source_timestamp"]):
        return result

    anomaly = EnvironmentalAnomaly(
        city=city,
        metric_name=result["metric_name"],
        source_timestamp=result["source_timestamp"],
        value=result["value"],
        baseline=result["baseline"],
        z_score=result["z_score"],
        ml_score=result["ml_score"],
        detection_type=result["detection_type"],
        severity=result["severity"],
    )

    db.add(anomaly)
    db.commit()
    db.refresh(anomaly)

    return {
        "id": anomaly.id,
        "city": anomaly.city,
        "metric_name": anomaly.metric_name,
        "source_timestamp": anomaly.source_timestamp,
        "value": anomaly.value,
        "baseline": anomaly.baseline,
        "z_score": anomaly.z_score,
        "ml_score": anomaly.ml_score,
        "detection_type": anomaly.detection_type,
        "severity": anomaly.severity,
        "created_at": anomaly.created_at,
    }