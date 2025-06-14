import time as t 
import requests
from sqlalchemy import create_engine  
from apscheduler.schedulers.background import BackgroundScheduler

BASE_URL = ""

class ParentClient: 
    def __init__(self, rate_limit=10):
        # Limit to 6 calls a min < 10k/day 
        self.rate_limit  = rate_limit
        self.calls_made  = 0
        self.time = t.time() 
        self.engine      = create_engine("sqlite:///data//noaa_data.db")
        self.scheduler   = BackgroundScheduler(timezone="America/Chicago")

    def _throttle(self):
        current = t.time()
        elapsed = current - self.client_time 

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

    def _get(self, endpoint: str, headers: dict, params: dict) -> dict:
        self._throttle()
        url = f"{BASE_URL}{endpoint}"
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
