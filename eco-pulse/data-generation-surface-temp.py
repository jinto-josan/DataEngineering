# Databricks notebook source
# DBTITLE 1,ERA5 Header
# MAGIC %md
# MAGIC ### ERA5 surface temperature data for India using the Copernicus CDS API.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install dependencies
pip install cdsapi xarray netCDF4 cfgrib

# COMMAND ----------

# DBTITLE 1,Download ERA5 data
import cdsapi

client = cdsapi.Client(
    url="https://cds.climate.copernicus.eu/api",
    key="5c63b6b8-af49-4f7e-aeec-9372eaa415cb"
)

# India bounding box: [North, West, South, East]
INDIA_BBOX = [37.0, 68.0, 6.0, 97.5]

client.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": [
            "2m_temperature",               # Tmean proxy (take daily avg)
            "maximum_2m_temperature_since_previous_post_processing",
            "minimum_2m_temperature_since_previous_post_processing",
        ],
        "year": [str(y) for y in range(2018, 2024)],
        "month": [f"{m:02d}" for m in range(1, 13)],
        "day":   [f"{d:02d}" for d in range(1, 32)],
        "time":  "12:00",         # midday snapshot for Tmean; use all hours if you want true daily avg
        "area":  INDIA_BBOX,      # clips download to India only — much smaller file
        "format": "netcdf",
    },
    "era5_india_temp.nc"          # output file
)

# COMMAND ----------

# DBTITLE 1,Process NetCDF data
import xarray as xr
import numpy as np
import zipfile

# CDS API returns a ZIP archive — extract the NetCDF files inside
with zipfile.ZipFile("era5_india_temp.nc", 'r') as zf:
    zf.extractall("/tmp/era5")

ds_instant = xr.open_dataset("/tmp/era5/data_stream-oper_stepType-instant.nc", engine="netcdf4")
ds_max = xr.open_dataset("/tmp/era5/data_stream-oper_stepType-max.nc", engine="netcdf4")
ds = xr.merge([ds_instant, ds_max])

# Convert Kelvin → Celsius
ds["t2m_c"]  = ds["t2m"]  - 273.15
ds["tmax_c"] = ds["mx2t"] - 273.15
ds["tmin_c"] = ds["mn2t"] - 273.15

print("Dimensions:", dict(ds.sizes))
print("Variables:", list(ds.data_vars))

# COMMAND ----------

# DBTITLE 1,Extract city temperatures
import pandas as pd

# Your city master table — build this once in silver layer
cities = pd.DataFrame({
    "city":      ["Delhi", "Mumbai", "Nagpur", "Chennai", "Kolkata", "Hyderabad"],
    "latitude":  [28.61,   19.08,   21.15,   13.08,   22.57,   17.38],
    "longitude": [77.21,   72.88,   79.09,   80.27,   88.36,   78.49],
})

def extract_city_temps(ds, cities):
    records = []
    for _, row in cities.iterrows():
        # sel with method="nearest" snaps to closest grid point
        city_ds = ds.sel(
            latitude=row["latitude"],
            longitude=row["longitude"],
            method="nearest"
        )
        df = city_ds[["tmax_c", "tmin_c", "t2m_c"]].to_dataframe().reset_index()
        df["city"] = row["city"]
        records.append(df)
    return pd.concat(records, ignore_index=True)

city_temps = extract_city_temps(ds, cities)
city_temps = city_temps.rename(columns={"valid_time": "date", "t2m_c": "tmean_c"})
city_temps["date"] = city_temps["date"].dt.date

# COMMAND ----------

# DBTITLE 1,Inspect results
print(city_temps.head(-20))
print(city_temps.shape)
print(city_temps.dtypes)
