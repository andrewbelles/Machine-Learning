from typing import Dict, Optional
import pandas as pd, numpy as np
import clientbackbone as cb

# Imports for state polygons 
import geopandas as gpd, random  
from shapely.ops import unary_union 
from shapely.geometry import Point
import sqlite3

class SoilClient(cb.ParentClient):

    def __init__(self, url: str):
        super().__init__("data/soil.db", 4)
        self.url     = url.rstrip("/")
        self.fields  = ["clay","silt","sand","soc","phh2o","bdod"]
        self.divisor = {"phh2o": 10, "soc": 10, "bdod": 10}
    
    # Pulls wkt bondary data from attom api data
    def load_state_geometry(self, state: str, path="data/attom.db") -> gpd.GeoDataFrame:
        attom = sqlite3.connect(path)
        df    = pd.read_sql(f"SELECT name, bound_wkt FROM state_{state}_boundary", attom)
        attom.close()

        # Gets geometry from wkt file 
        df["geometry"] = df["bound_wkt"].apply(
            lambda w: gpd.GeoSeries.from_wkt([w])[0] if w else None
        )
        # Takes union of all lat lon tuples 
        geometry = unary_union(df.geometry.dropna().tolist())
        return gpd.GeoDataFrame([{"code": state, "geometry": geometry}], crs="EPSG:4326")

    # Grabs data for an individual lat lon tuple 
    def fetch_point(self, lat: float, lon: float ) -> Optional[dict]:
        params = {
            "lat": lat, 
            "lon": lon,
            "property": self.fields, 
            "depth": [
                "0-5cm","0-30cm","5-15cm","15-30cm",
                "30-60cm","60-100cm","100-200cm"],
            "value": ["mean","Q0.05","Q0.5","Q0.95","uncertainty"]
        }
        raw = self._get(self.url, params)
        return raw 

    def sample_grid(self, geometry, n_samples=10):
        minx, miny, maxx, maxy = geometry.bounds
        width, height = maxx - minx, maxy - miny
        min_dist = max(width, height) / np.sqrt(n_samples)

        sampled: list[Point] = []
        attempts = 0
        max_attempts = n_samples * 100

        while len(sampled) < n_samples and attempts < max_attempts:
            attempts += 1
            x = random.uniform(minx, maxx)
            y = random.uniform(miny, maxy)
            p = Point(x, y)
            if not geometry.contains(p):
                continue

            if all(p.distance(q) >= min_dist for q in sampled):
                sampled.append(p)

        return sampled

    def flatten(self, data: dict) -> Dict[str, float]:
        result: Dict[str, float] = {}
        layers = data["properties"]["layers"]

        for layer in layers: 
            name = layer["name"]
            div  = self.divisor.get(name, 1)

            for depth in layer["depths"]:
                rang   = depth["range"]
                values = depth["values"]
                for stat in ["mean","Q0.05","Q0.5","Q0.95","uncertainty"]:
                    key = f"{name}_{rang}_{stat}"
                    raw = values.get(stat)
                    result[key] = None if raw is None else raw / div
        return result 

    def fetch_for_state(self, state: str):
        state_gdf = self.load_state_geometry(state)
        if state_gdf.empty:
            raise ValueError(f"No boundary for {state}...")
    
        geometry = unary_union(state_gdf.geometry)
        points = self.sample_grid(geometry)
        records = []
        for point in points: 
            lat, lon = point.y, point.x 
            data = self.fetch_point(lat, lon)
            if data is None: 
                raise ValueError(f"No data for {state}...")

            record = {"lat": lat, "lon": lon, "state": state}
            record.update(self.flatten(data))
            records.append(record)

        return pd.DataFrame.from_records(records)

    def get_states(self, states: list[str]):

        all_dfs = []
        for state in states:
            print(f"Fetching {state}...")
            all_dfs.append(self.fetch_for_state(state))

        df_final = pd.concat(all_dfs, ignore_index=True)
        self._save(df_final, "state_soil_raw")

if __name__ == "__main__":
    states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
    ]

    base_url = "https://rest.isric.org/soilgrids/v2.0/properties/query" 
    client   = SoilClient(base_url)
    client.get_states(states)
