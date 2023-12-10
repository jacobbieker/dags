import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import dagster

from weather.assets.observations.aeronet import fetch_aeronet_data_for_day
from weather.assets.utils import ObservationConfig

jobs: list[dagster.JobDefinition] = []
schedules: list[dagster.ScheduleDefinition] = []

# --- AeroNet jobs and schedules ----------------------------------------------

AERO_NET_OBSERVATIONS = ["AOD10", "AOD15", "AOD20", "SDA10", "SDA15", "SDA20", "TOT10", "TOT15", "TOT20"]


@dagster.daily_partitioned_config(start_date=dt.datetime(2000, 1, 1))
@dagster.static_partitioned_config(partition_keys=AERO_NET_OBSERVATIONS)
def AeroNetObservationsDailyParitionConfig(start: dt.datetime, _end: dt.datetime, partition_key) -> dict[str, Any]:
    date: dt.datetime = dt.datetime.strptime(start.strftime("%Y-%m-%d"), "%Y-%m-%d")
    date = date - dt.timedelta(days=1)
    config = ObservationConfig(
        # Run it for the previous day
        date=date.strftime("%Y-%m-%d"),
        raw_dir=f"/mnt/storage/aeronet/{partition_key}/raw",
        processed_dir=f"/mnt/storage/aeronet/{partition_key}/processed",
        parameter=partition_key,
    )
    return {"ops": {"fetch_aeronet_data_for_day": {"config": json.loads(config.json())}}}


@dagster.job(
    config=AeroNetObservationsDailyParitionConfig,
    tags={"source": "aeronet", dagster.MAX_RUNTIME_SECONDS_TAG: 345600} # 4 days
)
def aeronet_daily_archive() -> None:
    """Download CAMS data for a given day."""
    fetch_aeronet_data_for_day()


jobs.append(aeronet_daily_archive)
schedules.append(dagster.build_schedule_from_partitioned_job(aeronet_daily_archive, hour_of_day=2))
