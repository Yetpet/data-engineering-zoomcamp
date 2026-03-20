import json
import dataclasses
from dataclasses import dataclass
from typing import Optional

@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    # passenger_count: float
    passenger_count: Optional[float]  # Allow None
    trip_distance: float
    tip_amount: float
    total_amount: float

def safe_float(value):
    """Converts value to float, or None if it's NaN/Null."""
    try:
        import pandas as pd
        if pd.isna(value):
            return None
        return float(value)
    except:
        return None


def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
