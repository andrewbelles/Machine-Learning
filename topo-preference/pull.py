import os 
import time 
import requests
import pandas as pd 
from datetime import datetime#, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine  
from apscheduler.schedulers.background import BackgroundScheduler

# Get enviroment into os.environ 
load_dotenv()

API_KEY  = os.getenv("ATTOM_API_KEY")
BASE_URL = os.getenv("ATTOM_BASE_URL")
print(f"Key: {API_KEY} Url: {BASE_URL}")
HEADERS = {
        "accept": "application/json",
        "apikey": API_KEY
}

# Handles API calls to Attom API 
class AttomClient:
    def __init__(self):
        # Hard rate limit
        self.rate_limit  = 200 # per min
        self.calls_made  = 0 
        self.client_time = time.time() 
        # Intrinsic db information and scheduler
        self.engine     = create_engine("sqlite:///attom_data.db")
        self.scheduler = BackgroundScheduler(timezone="America/Chicago")


    def _throttle(self):
        current = time.time()
        elapsed = current - self.client_time

        # Rate and time checks
        if elapsed >= 60: 
            self.client_time = time.time() 
            self.calls_made  = 0 
        elif self.calls_made >= self.rate_limit: 
            wait = 60 - elapsed  
            time.sleep(wait)
            self.client_time = time.time()
            self.calls_made  = 0 

        self.calls_made += 1
    
    # Main api call 
    def _get(self, endpoint: str, params: dict) -> dict: 

        self._throttle() # Get calls made and apply rate limit 
        url  = f"{BASE_URL}{endpoint}"
        print(f"url: {url}")
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    
    # AREA API METHODS 
    def fetch_states(self) -> pd.DataFrame:
        raw = self._get("/areaapi/v2.0.0/state/lookup", {})
        
        items = raw["response"]["result"]["package"]["item"]

        records = []
        for s in items: 
            records.append({
                "code": s.get("abbreviation"),
                "name": s.get("name"),
                "geoIdV4": s.get("geoIdV4")})

        return pd.DataFrame(records)

    def fetch_boundary(self, geoIdV4: str) -> pd.DataFrame:

        endpoint = "/areaapi/v2.0.0/boundary/detail"
        raw      = self._get(endpoint, 
                             {"geoIdV4": geoIdV4, 
                              "format": "wkt"})

        items = (raw["response"]["result"]["package"]["item"])

        # Get properties, record coordinate from geometry field and append 
        records = []
        for item in items: 
            records.append({
                "geoIdV4":   item["geoIdV4"],
                "name":      item["name"],
                "bound_wkt": item["boundary"]
            })
        # Return all props as dataframe
        return pd.DataFrame(records)

    # SQLITE 
    def save(self, df: pd.DataFrame, table: str):
        df["fetched_at"] = datetime.now()
        df.to_sql(table, con=self.engine, if_exists="append", index=False)

    # WORKFLOW 
    def update_states(self):

        # For each requested geography get datasets and insert in db 
        states_df = self.fetch_states()
        for state_id, _, geoIdV4 in states_df[["code","name","geoIdV4"]].itertuples(index=False): 
            boundary_df  = self.fetch_boundary(geoIdV4)
            self.save(boundary_df, f"state_{state_id}_boundary")
            print(f"State: {state_id} Done")

    # TEST API 
    def test_pull(self, state_code: str):
        print(f"\n>> Testing state '{state_code}'")

        # Test lookup 
        states_df = self.fetch_states()
        match = states_df.loc[states_df["code"] == state_code]
        if match.empty:
            print(f"{state_code} not found")
            return 

        geoIdV4 = match.iloc[0]["geoIdV4"]
        print(f">> Found GeoIDv4: {geoIdV4}")

        # Boundary test
        boundary_df = self.fetch_boundary(geoIdV4)
        print("\n>> Boundary DataFrame")
        print(boundary_df.head(3))
        print(f"Total boundary records: {len(boundary_df)}")
        print(">> test_pull complete.\n")

    # SCHEDULER 
    def schedule_geographies(self, interval_hours: int = 24):

        # Setup scheduler, pass fn ptr to update_geographies 
        self.scheduler.add_job(func=self.update_states, 
                               trigger='interval',
                               hours=interval_hours,
                               next_run_time=datetime.now())
        self.scheduler.start()


# Simple call to scheduler 
if __name__ == "__main__":
    client = AttomClient()

    # Test API
    client.test_pull("CA")
    
    # Init scheduler 
    client.update_states()
    client.schedule_geographies()
    print("Ctrl+C to exit API Scheduler.")

    # Do nothing 
    try:
        while True: 
            time.sleep(60)
    # Shutdown scheduler 
    except (KeyboardInterrupt, SystemExit):
        client.scheduler.shutdown()
        print("Ended API Scheduler.")
