from sqlalchemy import Column, Integer, String, Float, BigInteger, DateTime
from sqlalchemy.sql import func

from backend.database.session import Base


class EnvironmentalAnomaly(Base):
    __tablename__ = "environmental_anomalies"

    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, index=True, nullable=False)
    metric_name = Column(String, nullable=False)   # for now: "aqi"
    source_timestamp = Column(BigInteger, nullable=False)

    value = Column(Float, nullable=False)
    baseline = Column(Float, nullable=True)
    z_score = Column(Float, nullable=True)
    ml_score = Column(Float, nullable=True)

    detection_type = Column(String, nullable=False)   # statistical / ml / both
    severity = Column(String, nullable=False)         # low / medium / high

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    