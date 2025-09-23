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
    
def _cache_path(station: str, tag: Optional[str] = "") -> str:
    """Build a cache filename based on station and a tag."""
    safe = station.replace(":", "-")
    return os.path.join(DATA_DIR, f"{safe}{"_"+tag}.parquet")
