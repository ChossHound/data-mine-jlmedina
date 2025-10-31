from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Optional, Tuple, Dict, Any, List

import os
import time
import requests
import pandas as pd

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
MAX_LIMIT = 1000
TOKEN = os.getenv("NOAA_TOKEN")

DATA_DIR = os.path.join("data")
os.makedirs(DATA_DIR, exist_ok=True)

def _to_ymd(d: date | datetime | str) -> str:
    """Normalize input into 'YYYY-MM-DD' (the format NOAA expects)."""
    if isinstance(d, str):
        # Assume already ISO-like; keep just the date part
        return d[:10]
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    raise TypeError(f"Unsupported date type: {type(d)}")

@dataclass
class NOAAClient:
    token: str = TOKEN
    base_url: str = BASE_URL
    timeout: int = 30
    max_retries: int = 4
    backoff: float = 1.6

    def __post_init__(self):
        if not self.token or not self.token.strip():
            raise ValueError("NOAA_TOKEN is Empty. Set the NOAA_TOKEN env var or pass token=\"your token\"")
        self._session = requests.Session()
        self._headers = {"token": self.token}
    
    
    def dtype_description_by_station(self, dtypes: List[str], station: str):

        for dt in dtypes:
            r = requests.get(f"{self.base_url}/datatypes/{dt}")
            r.raise_for_status()
            js = r.json()
            print(dt, "→", js.get("name"))
        # try:
        #     response = self._get(path=f"/datatypes/{dtype}")
        #     if isinstance(response, dict) and "name" in response:
        #         return {"dtype": dtype, "description": response["name"]}
        #     elif "description" in  response:
        #         return {"dtype": dtype, "description": response["description"]}
        #     else:
        #         raise ValueError(f"Unexpected response structure for datatype '{dtype}': {response}")       
        # except requests.HTTPError as e:
        #     raise ValueError(f"HTTP error when fetching datatype '{dtype}': {e}")
        # except Exception as e:
        #     raise ValueError(f"Failed to retrieve description for datatype '{dtype}': {e}")


    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        '''the main way to retreive data'''
        """GET with basic retry for 429/5xx and JSON fallback to text error."""
        url = f"{self.base_url}{path}"
        params = params or {}
        for attempt in range(self.max_retries):
            resp = self._session.get(url, headers=self._headers, params=params, timeout=self.timeout)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                # Simple backoff; you could also read Retry-After header if present.
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff ** (attempt + 1))
                    continue
            # If not retried, raise on error
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                # Print a helpful message that includes the fully rendered URL and body
                msg = f"HTTP {resp.status_code} for {resp.url}\nBody: {resp.text[:4000]}"
                raise requests.HTTPError(msg) from e
            # Success
            try:
                return resp.json()
            except ValueError:
                # Not JSON; return a dict with raw text
                return {"text": resp.text}
        # If loop exits unexpectedly
        raise RuntimeError("Request failed after retries.")

    def stations(self, limit: int = 25,stationid: Optional[str] = None, **filters) -> List[Dict[str, Any]]:
        params = {"limit":limit, **filters}
        path = "/stations"
        if stationid:
            path += "/" + stationid
        js = self._get(path, params)
        return js.get("results",[])


    def station_date_bounds(self, stationid: str, datasetid: str = "GHCND") -> Tuple[datetime, datetime]:
        """Return (mindate, maxdate) for a specific station as YYYY-MM-DD strings."""
        res = self.stations(limit=1, stationid=stationid)
        if not res:
            raise ValueError(f"Station not found or no metadata: {stationid}")
        # Truncate Output
        # datetime.strptime(s1, "%Y-%m-%d")
        return (datetime.strptime(res[0]["mindate"][:10], "%Y-%m-%d"),
                datetime.strptime(res[0]["maxdate"][:10], "%Y-%m-%d"))
    
    def stations(self, limit: int = 25, **filters) -> List[Dict[str, Any]]:
        '''list stations within given parameters (filters)'''
        params = {"limit": limit, **filters}
        js = self._get("/stations", params)
        return js.get("results", [])

    def data_by_station(
        self,
        stationid: str,
        datasetid: str,
        startdate: datetime,
        enddate: datetime
        ) -> pd.DataFrame:
        # convert datetime to str for request
        start_str, end_str = _to_ymd(startdate), _to_ymd(enddate)
        
        if start_str > end_str:
            raise ValueError(f"startdate {start_str} must be <= enddate {end_str}")
        
        year_offset = timedelta(days=365)
            
        # base_params = {
        #     "datasetid": datasetid,
        #     "stationid":stationid,
        #     "startdate": start_str,
        #     "enddate": end_str,
        #     "limit": MAX_LIMIT
        # }

        all_rows: List[Dict[str, Any]] = []
        cur_start = startdate

        #NOAA only lets you get 1 year of data at a time 
        while cur_start <= enddate:
            cur_end = min(cur_start + year_offset, enddate)

            offset = 1
            while True:
                params = {
                "datasetid": datasetid,
                "stationid": stationid,
                "startdate": cur_start.strftime("%Y-%m-%d"),
                "enddate":   cur_end.strftime("%Y-%m-%d"),
                "limit":     1000,
                "offset":    offset,
                }
                
                js = self._get("/data", params)
                rows = js.get("results", [])
                if not rows:
                    break
                all_rows.extend(rows)

                meta = js.get("metadata", {}).get("resultset", {})
                returned = meta.get("limit", 1000)
                offset += returned

                if meta.get("count") and len(all_rows) >= meta["count"]:
                    break

            cur_start = cur_end + year_offset

        # Build DataFrame
        if not all_rows:
            return pd.DataFrame(columns=["date", "datatype", "station", "value", "attributes"])

        df = pd.DataFrame(all_rows)
        # Normalize date to date-only for convenience
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    
    # def stations_to_parquet(self, 
    #                         stations: List[str], 
    #                         datasetid: str = "GHCND",
    #                         return_combined: bool = False,
    #                         name_of_file: Optional[str] = None
    #                         ) -> Tuple[List[str], Optional[pd.DataFrame]]:
    #     """
    #     Retrieve data from multiple provided station ids and convert them to parquet files. 
            
    #     Returns
    #     -------
    #     (written_paths, combined_df or None)
    #     """

    #     written: List[str] = []
    #     combined_frames: List[pd.DataFrame] = []

    #     for station in stations:
    #         try:
    #             start, end = self.station_date_bounds(stationid=station)
    #             temp_df = self.data_by_station(stationid=station, 
    #                                         datasetid=datasetid, 
    #                                         startdate=start, 
    #                                         enddate=end)
                
    #             if temp_df.empty:
    #                 print(f"[INFO] {station}: no rows for {datasetid} between {start}..{end}; skipping file.")
    #                 continue

    #             if return_combined:
    #                 combined_frames.append(temp_df)

    #             path = _cache_path(station=station,)
    #             temp_df.to_parquet(path=_cache_path(station=station))
            
    #         except:
    #             pass


    def stations_to_parquet(
        self,
        stations: List[str],
        datasetid: str = "GHCND",
        return_combined: bool = False,
        name_of_file: Optional[str] = None,
        outdir: str = "data",
    ) -> Tuple[List[str], Optional[pd.DataFrame]]:
        """
        Fetch data for multiple stations and write a SINGLE Parquet file that includes a 'station' column.

        Returns
        -------
        (written_paths, combined_df or None)
            written_paths will contain a single path to the combined Parquet.
        """
        os.makedirs(outdir, exist_ok=True)

        frames: List[pd.DataFrame] = []
        min_dates: List[str] = []
        max_dates: List[str] = []

        for station in stations:
            try:
                # Use dataset-scoped bounds so coverage matches the dataset you’ll fetch
                start, end = self.station_date_bounds(stationid=station, datasetid=datasetid)

                df = self.data_by_station(
                    stationid=station,
                    datasetid=datasetid,
                    startdate=start,
                    enddate=end,
                )
                if df.empty:
                    print(f"[INFO] {station}: no rows for {datasetid} between {start}..{end}; skipping.")
                    continue

                # Ensure a 'station' column is present and consistent
                df["station"] = station

                frames.append(df)
                min_dates.append(start); max_dates.append(end)

            except Exception as e:
                print(f"[ERROR] {station}: {e}")

        if not frames:
            raise ValueError("No data returned for any station.")

        combined = pd.concat(frames, ignore_index=True)

        # Normalize, sort, and de-duplicate (helps when overlapping pulls happen)
        if "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"]).dt.date
            combined = combined.sort_values(["station", "date"]).reset_index(drop=True)

        keep_cols = [c for c in ["station", "date", "datatype", "value", "attributes"] if c in combined.columns]
        if keep_cols:
            combined = combined.drop_duplicates(subset=keep_cols, keep="first").reset_index(drop=True)

        # File naming
        if name_of_file:
            filename = name_of_file if name_of_file.endswith(".parquet") else f"{name_of_file}.parquet"
        else:
            outer_min = min(min_dates) if min_dates else "unknown_start"
            outer_max = max(max_dates) if max_dates else "unknown_end"
            filename = f"{datasetid.lower()}_{len(stations)}stations_{outer_min}_{outer_max}.parquet"

        outpath = os.path.join(outdir, filename)

        # Write single Parquet
        combined.to_parquet(outpath, index=False)
        print(f"[OK] Wrote {len(combined):,} rows → {outpath}")

        return [outpath], (combined if return_combined else None)




def _cache_path(station: str, tag: Optional[str] = "") -> str:
    """Build a cache filename based on station and a tag."""
    safe = station.replace(":", "-")
    return os.path.join(DATA_DIR, f"{safe}{"_"+tag}.parquet")


def load_station_data(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_parquet(filepath)



def load_multiple_station_data(directory: str, pattern: str = "*.parquet") -> pd.DataFrame:
    """
    Load and combine multiple Parquet files into a single pandas DataFrame.

    Parameters
    ----------
    directory : str
        Path to the folder containing Parquet files.
    pattern : str, optional
        Glob pattern for file matching (default is '*.parquet').

    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing all station data.
    """
    # Find all parquet files matching the pattern
    filepaths = glob.glob(os.path.join(directory, pattern))

    if not filepaths:
        raise FileNotFoundError(f"No Parquet files found in {directory} matching pattern '{pattern}'")

    # Load each parquet file into a DataFrame
    dfs = []
    for path in filepaths:
        print(f"Loading: {path}")
        df = pd.read_parquet(path)
        dfs.append(df)

    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df
