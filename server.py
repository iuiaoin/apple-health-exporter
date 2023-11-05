import os

import dotenv

dotenv.load_dotenv()
import uuid
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import JSON, UUID, Column, DateTime, String, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone

import db  # type: ignore for timescale hook


def convertToUTC(date_string) -> str:
    try:
        dt_with_tz = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %z')
        dt_utc = dt_with_tz.astimezone(timezone.utc)
        return dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    except Exception:
        return date_string

app = FastAPI()
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "timescaledb://postgres:postgres@localhost:5432/postgres"
)
DATABASE_URL = DATABASE_URL.replace("postgresql://", "timescaledb://")

engine = create_engine(DATABASE_URL)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class MetricTable(Base):
    __tablename__ = "metrics"
    id = Column(UUID, default=uuid.uuid4, primary_key=True)
    name = Column(String)
    data = Column(JSON)
    timestamp = Column(DateTime())
    # Add index
    __table_args__ = {
        "timescaledb_hypertable": {
            "time_column_name": "timestamp",
            "partitioning_column": "name",
            "number_partitions": 10,
        }
    }

# AUto migrate
Base.metadata.create_all(engine)

class Datum(BaseModel):
    date: str
    sleepEnd: Optional[str] = None
    inBedStart: Optional[str] = None
    inBedEnd: Optional[str] = None
    sleepStart: Optional[str] = None
    qty: Optional[float] = None
    Avg: Optional[float] = None
    Min: Optional[float] = None
    Max: Optional[float] = None
    deep: Optional[float] = None
    core: Optional[float] = None
    awake: Optional[float] = None
    asleep: Optional[float] = None
    rem: Optional[float] = None
    inBed: Optional[float] = None
    source: Optional[str] = None
class Metric(BaseModel):
    units: str
    data: List[Datum]
    name: str
class Data(BaseModel):
    metrics: List[Metric]
class RequestData(BaseModel):
    data: Data


class Detail(BaseModel):
    units: str
    qty: Optional[float] = None
class DetailWithDate(Detail):
    date: Optional[str] = None
class Elevation(BaseModel):
    units: str
    ascent: Optional[float] = None
    descent: Optional[float] = None
class Workout(BaseModel):
    name: str
    start: str
    end: str
    speed: Optional[Detail] = None
    avgHeartRate: Optional[Detail] = None
    distance: Optional[Detail] = None
    heartRateRecovery: Optional[List[DetailWithDate]] = None
    maxHeartRate: Optional[Detail] = None
    stepCadence: Optional[Detail] = None
    isIndoor: Optional[bool] = None
    activeEnergy: Optional[Detail] = None
    stepCount: Optional[Detail] = None
    totalEnergy: Optional[Detail] = None
    heartRateData: Optional[List[DetailWithDate]] = None
    elevation: Optional[Elevation] = None
    flightsClimbed: Optional[Detail] = None
    temperature: Optional[Detail] = None
    totalSwimmingStrokeCount: Optional[Detail] = None
    swimCadence: Optional[Detail] = None
    humidity: Optional[Detail] = None
    intensity: Optional[Detail] = None
class WorkoutData(BaseModel):
    workouts: List[Workout]
class RequestWorkoutsData(BaseModel):
    data: WorkoutData
    


@app.post("/upload")
def upload_data(request_data: RequestData):
    ps = []
    for metric in request_data.data.metrics:
        for datum in metric.data:
            data = datum.model_dump()
            date = data.get("date", None)
            sleepEnd = data.get("sleepEnd", None)
            inBedStart = data.get("inBedStart", None)
            inBedEnd = data.get("inBedEnd", None)
            sleepStart = data.get("sleepStart", None)
            if(date is not None):
                data["date"] = convertToUTC(date)
            if(sleepEnd is not None):
                data["sleepEnd"] = convertToUTC(sleepEnd)
            if(inBedStart is not None):
                data["inBedStart"] = convertToUTC(inBedStart)
            if(inBedEnd is not None):
                data["inBedEnd"] = convertToUTC(inBedEnd)
            if(sleepStart is not None):
                data["sleepStart"] = convertToUTC(sleepStart)
            utcDate = data.pop("date", None)
            ps.append(dict(name=metric.name, data=data, timestamp=utcDate))
    with SessionLocal() as session:
        insert_ps = (
            insert(MetricTable)
            .values(ps)
            .on_conflict_do_nothing(index_elements=["name", "timestamp"])
        )
        session.execute(insert_ps)
        session.commit()
    return {"status": "Health data uploaded successfully!"}

@app.post("/upload/workouts")
def upload_workouts(request_data: RequestWorkoutsData):
    ps = []
    for workout in request_data.data.workouts:
        data = workout.model_dump()
        start = data.get("start", None)
        end = data.get("end", None)
        heartRateRecovery = data.get("heartRateRecovery", [])
        heartRateData = data.get("heartRateData", [])
        if(start is not None):
          data["start"] = convertToUTC(start)
        if(end is not None):
          data["end"] = convertToUTC(end)
        for hrr in heartRateRecovery:
            d = hrr.get("date", None)
            if(d is not None):
                hrr["date"] = convertToUTC(d)
        for hrd in heartRateData:
            d = hrd.get("date", None)
            if(d is not None):
                hrd["date"] = convertToUTC(d)
        date = data.get("start", None)
        ps.append(dict(name=workout.name, data=data, timestamp=date))
    with SessionLocal() as session:
        insert_ps = (
            insert(MetricTable)
            .values(ps)
            .on_conflict_do_nothing(index_elements=["name", "timestamp"])
        )
        session.execute(insert_ps)
        session.commit()
    return {"status": "Workouts data uploaded successfully!"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
