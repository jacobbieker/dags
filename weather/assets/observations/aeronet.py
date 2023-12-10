import monetio
import pandas as pd
import xarray as xr
from monetio import aeronet
import dagster
from weather.assets.observations.utils import ObservationConfig

@dagster.op
def fetch_aeronet_data_for_day(context: dagster.OpExecutionContext, config: ObservationConfig):
    """Fetch Aeronet data for a given day."""
    dates = pd.date_range(start=config.start_date, end=config.start_date, freq='H')
    parameter = config.get("parameter", "AOD10")
    df = aeronet.add_data(dates=dates, product=parameter)
    ds = df.to_xarray()
    ds = ds.set_coords(("time", "siteid", "data_quality_level", "latitude", "longitude"))
    # Rename data variables to not have spaces
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    # Save to disk
    ds.to_netcdf(f"{config.processed_dir}/{config.start_date}_{parameter}.nc", engine="h5netcdf")



