import os, pandas as pd, us, glob, re, numpy as np
from requests.exceptions import HTTPError
import clientbackbone as cb
from dotenv import load_dotenv
import time as t

load_dotenv()
class NoaaClient(cb.ParentClient): 

    def __init__(self, key: str, url: str):
        # Limit to 6 calls a min < 10k/day 
        super().__init__("data/noaa_data.db")
        self.url = url.rstrip("/")
        self.HEADERS = {"token": key}
        self.div_map = self.fetch_div_map()

    def _get_retry(self, url: str, endpoint: str, headers: dict, params: dict) -> dict:
 
        executed = HTTPError()
        for attempt in range(3):
            try:
                return self._get(url, params, endpoint, headers)
            except HTTPError as e: 
                status = e.response.status_code if e.response else None 
                if status is not None and 500 <= status < 600: 
                    wait = 3 ** attempt 
                    print(f"HTTP {status}, retry {attempt + 1}/3")
                    t.sleep(wait)
                    executed = e
                    continue 
                raise 
        raise executed

    def fetch_normals(self, state_code: str) -> pd.DataFrame:
        fips = us.states.lookup(state_code).fips # type: ignore 
        raw = self._get_retry(
            self.url, "/data", self.HEADERS,
            {
                "datasetid":  "NORMAL_MLY",
                "locationid": f"FIPS:{fips}",
                "datatypeid": "MLY-TMIN-NORMAL,MLY-TMAX-NORMAL,MLY-PRCP-NORMAL",
                "startdate":  "2010-01-01",
                "enddate":    "2010-12-01",
                "limit":      1000,
            }
        )
        results = raw["results"]
        if not results:
            raise RuntimeError(f"No NORMAL_MLY data for FIPS {fips}")
        return pd.DataFrame(results)

    # Get fips codes for all counties 
    def fetch_div_map(self) -> dict:
        path = "climdiv/county-to-climdivs.txt"
        df = pd.read_csv(
            path,
            sep=r"\s+",
            comment="#",
            header=None,
            names=["state_fips", "county", "climdiv"],
            dtype=str
        )
        return df.groupby("state_fips")["climdiv"].unique().to_dict()

    # Read from local files - credit chatgpt I was not writing this file matching bullshit myself   
    def fetch_climdiv(self) -> pd.DataFrame:
        """
        Parse both the 'norm' and time-series CLIMDIV files
        sitting in ./climdiv/ (no .txt extension) into one DataFrame.
        """
        records = []

        # 1) normals files: climdiv-norm-<datatype>-v...-yyyymmdd
        norm_files = glob.glob("climdiv/climdiv-norm-*")
        if not norm_files:
            raise RuntimeError("No normal files found in climdiv/")
        for path in norm_files:
            print(f"[DEBUG] Reading normal: {path}")
            fname = os.path.basename(path)
            m = re.match(r"climdiv-norm-([a-z]+)-v[\d.]+-(\d{8})$", fname)
            if not m:
                print(f"[WARN] Skipping unexpected normal file: {fname}")
                continue
            datatype = m.group(1).upper()   # e.g. "CDDCCY"

            # fixed-width: 11-char div code, then 12×6-char month fields
            colspecs = [(0,11)] + [(11 + 6*i, 11 + 6*(i+1)) for i in range(12)]
            months   = [f"{i:02d}" for i in range(1,13)]

            df = pd.read_fwf(
                path,
                colspecs=colspecs,
                names=["div"] + months,
                dtype={"div": str},
                na_values=["-","   -","-9999"]
            )
            df = df.melt(
                id_vars=["div"],
                value_vars=months,
                var_name="month",
                value_name="value"
            )
            df["month"]    = df["month"].astype(int)
            df["date"]     = pd.to_datetime(
                dict(year=2000, month=df["month"], day=1)) # type: ignore 
            df["datatype"] = datatype
            records.append(df[["div","datatype","date","value"]])

        # 2) time-series files: climdiv-<datatype>-v...-yyyymmdd
        ts_files = glob.glob("climdiv/climdiv-[a-z]*-v*")
        if not ts_files:
            raise RuntimeError("No time-series files found in climdiv/")
        for path in ts_files:
            print(f"[DEBUG] Reading timeseries: {path}")
            fname = os.path.basename(path)
            m = re.match(r"climdiv-([a-z]+)-v[\d.]+-(\d{8})$", fname)
            if not m:
                print(f"[WARN] Skipping unexpected ts file: {fname}")
                continue
            datatype = m.group(1).upper()   # e.g. "TAVG"

            # fixed-width: 2-char div, 4-char year, then 12×6-char months
            colspecs = [(0,2), (2,6)] + [(6 + 6*i, 12 + 6*i) for i in range(12)]
            months   = [f"{i:02d}" for i in range(1,13)]

            df = pd.read_fwf(
                path,
                colspecs=colspecs,
                names=["div","year"] + months,
                dtype={"div": str, "year": int},
                na_values=["-","   -","-9999"]
            )
            for mon in months:
                df[mon] = pd.to_numeric(df[mon], errors="coerce")

            df = df.melt(
                id_vars=["div","year"],
                value_vars=months,
                var_name="month",
                value_name="value"
            )
            df = df[df["year"].between(1895,2100)]
            df["month"]    = df["month"].astype(int)
            df["date"]     = pd.to_datetime(
                             df[["year","month"]].assign(day=1)) # type: ignore
            df["datatype"] = datatype
            records.append(df[["div","datatype","date","value","month"]])

        if not records:
            raise RuntimeError("fetch_climdiv: no files parsed")
        return pd.concat(records, ignore_index=True)

    # NOAA WORKFLOW 
    def update_states(self, states: list[str]):
        rows = []
        all_climdivs = self.fetch_climdiv()

        # iterate per state 
        for state in states:
            print(f"{state}: fetching…")
            normals = self.fetch_normals(state)

            normals["date"]  = pd.to_datetime(normals["date"])
            normals["month"] = normals["date"].dt.month

            wide_n = (
                normals.pivot_table(
                    index="month",
                    columns="datatype",
                    values="value",
                    aggfunc="mean",
                )
                .rename(columns={
                    "MLY-TMIN-NORMAL": "TMIN",
                    "MLY-TMAX-NORMAL": "TMAX",
                    "MLY-PRCP-NORMAL": "PRCP",
                })
            ).reindex(index=range(1, 13))

            # Fill missing datatypes with nan to retain correct shape 
            for col in ("TMIN", "TMAX", "PRCP"):
                if col not in wide_n.columns:
                    wide_n[col] = np.nan
            flat_normals = wide_n[["TMIN", "TMAX", "PRCP"]].to_numpy().ravel()

            fips = us.states.lookup(state).fips # type: ignore 
            divs = self.div_map.get(fips, [])
            sub  = all_climdivs[all_climdivs["div"].isin(divs)]
            sub["month"] = sub["date"].dt.month # type: ignore 
            wide_c = sub.pivot_table(
                    index="month", columns="datatype", values="value", aggfunc="mean"
            ).reindex(index=range(1,13))

            # Repeat process for climdiv 
            for datatype in ["TAVG","HDD","CDD","SNOW","PDSI","SP12"]:
                if datatype not in wide_c.columns:
                    wide_c[datatype] = np.nan 
            flat_climdiv = wide_c[["TAVG","HDD","CDD","SNOW","PDSI","SP12"]].to_numpy().ravel()

            rows.append({
                "state":   state,
                **{f"norm_{i}": v for i, v in enumerate(flat_normals)},
                **{f"clim_{i}": v for i, v in enumerate(flat_climdiv)}
            })

            t.sleep(0.1) 

        # Create 50x12 dataframe and save to db  
        df = pd.DataFrame(rows)
        self._save(df, "state_climate_raw")


states = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]


if __name__ == "__main__":

    API_KEY  = os.getenv("NOAA_API_KEY")
    BASE_URL = os.getenv("NOAA_BASE_URL")
    if not (API_KEY and BASE_URL): 
        raise RuntimeError("Expected NOAA_KEY and URL_KEY: ")
    
    client = NoaaClient(API_KEY, BASE_URL)
    client.update_states(states)
