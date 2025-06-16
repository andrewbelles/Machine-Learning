import time as t 
import requests
import pandas as pd
from sqlalchemy import create_engine  
from apscheduler.schedulers.background import BackgroundScheduler

class ParentClient: 

    def __init__(self, db_path: str, rate_limit=200):
        # Limit to 6 calls a min < 10k/day 
        self.rate_limit  = rate_limit
        self.calls_made  = 0
        self.client_time = t.time() 
        self.engine      = create_engine(f"sqlite:///{db_path}")
        self.scheduler   = BackgroundScheduler(timezone="America/Chicago")
        self.session     = requests.Session()


    def _throttle(self):
        current = t.time()
        elapsed = current - self.client_time 
        print(f"{elapsed}...")

        # Rate and time checks
        if elapsed >= 60: 
            self.client_time = t.time() 
            self.calls_made  = 0 
        elif self.calls_made >= self.rate_limit: 
            wait = 60 - elapsed  
            t.sleep(wait)
            self.client_time = t.time()
            self.calls_made  = 0 

        self.calls_made += 1

    def _get(self, base_url: str, params: dict, endpoint: str="", headers: dict={}) -> dict:
        self._throttle()
        url = f"{base_url}{endpoint}"
        print(f"url: {url}")
        if headers == {} and endpoint == "":
            resp = self.session.get(url, params=params, timeout=30)
        else: 
            resp = self.session.get(url, headers=headers, params=params, timeout=180)
        print(f"GET {resp.url}: {resp.status_code}")

        resp.raise_for_status()
        return resp.json()


    def _save(self, df: pd.DataFrame, table: str):
        df["fetched_at"] = t.time()
        df.to_sql(table, con=self.engine, if_exists="replace", index=False)
