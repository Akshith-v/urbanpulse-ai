from fastapi import FastAPI, HTTPException
from backend.services.weather_service import fetch_weather
from backend.services.air_quality_service import fetch_air_quality
from data_pipeline.cities import CITIES

app = FastAPI(title="UrbanPulse API")


@app.get("/")
def read_root():
    return {"message": "UrbanPulse API running"}


@app.get("/cities")
def get_cities():
    return {"cities": CITIES}


@app.get("/weather/{city_name}")
def get_weather(city_name: str):
    try:
        return fetch_weather(city_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/air-quality/{city_name}")
def get_air_quality(city_name: str):
    try:
        return fetch_air_quality(city_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))