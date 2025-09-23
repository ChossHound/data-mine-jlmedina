import noaa
import matplotlib as mpl
import pandas as pd
import numpy as np

import eda
from noaa import NOAAClient, _cache_path
from datetime import datetime, timedelta, timezone


STATIONS = "GHCND:USS0007M27S"
LOC_ID = "FIPS:08091"
ouray_Climate_Client = NOAAClient(timeout=60)
# print(ouray_Climate_Client._get())
(x, y) = ouray_Climate_Client.station_date_bounds(stationid=STATIONS)

# print(f"x : {x}, y : {y}\n")

test_df = ouray_Climate_Client.data_by_station(stationid=STATIONS, 
                                               datasetid="GHCND", 
                                               startdate=datetime.now() - timedelta(days=7), 
                                               enddate=datetime.now())
print(test_df)

path = _cache_path(STATIONS)
test_df.to_parquet(path=path, index=False)
print(f"Saved {len(test_df)} rows to {path}")
# test_df.to_parquet()
# daily_climate_df = ouray_Climate_Client.data_by_station(stationid=STATIONS, 
#                                      datasetid="GHCND", 
#                                      startdate= x, 
#                                      enddate= datetime.now())
# print(ouray_Climate_Client._get(path="/stations", params={"datasetid":"GHCND", "locationid":"FIPS:08091"}))

# print(daily_climate_df.head())
# _cache_path(station=STATIONS, tag="")
# ouray_Climate_Client.to_parquet