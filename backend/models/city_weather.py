from sqlalchemy import Column, Integer, String, Float, BigInteger, DateTime
from sqlalchemy.sql import func

from backend.database.session import Base


class CityWeather(Base):
    __tablename__ = "city_weather"

    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True, nullable=False)
    temperature_c = Column(Float, nullable=False)
    feels_like_c = Column(Float, nullable=False)
    humidity = Column(Integer, nullable=False)
    weather = Column(String, nullable=False)
    weather_description = Column(String, nullable=False)
    wind_speed = Column(Float, nullable=False)
    source_timestamp = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)