from sqlalchemy import Column, Integer, String, Float, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from backend.database.session import Base


class CityAirQuality(Base):
    __tablename__ = "city_air_quality"

    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True, nullable=False)
    aqi = Column(Integer, nullable=False)
    components = Column(JSONB, nullable=False)
    source_timestamp = Column(BigInteger, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)