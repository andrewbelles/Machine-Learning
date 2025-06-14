
# TODO: 
'''
Pull boundary data for setting minlon,maxlon,etc.
'''

import os
import time as t 
import requests
import pandas as pd 
import api_client as ac
from datetime import datetime#, timezone

from dotenv import load_dotenv

import sqlite3 
from shapely import wkt 

load_dotenv()

API_KEY  = os.getenv("NOAA_API_KEY")
BASE_URL = os.getenv("NOAA_BASE_URL")
``
class NoaaClient(ac.ParentClient): 
    def __init__(self):
        # Limit to 6 calls a min < 10k/day 
        self.client = ac.ParentClient(6)


    # Get boundary from attom database 
    def _state_boundary(self, state_code: str) -> tuple[float, float, float, float]: 
        attom = sqlite3.connect("data/attom_data.db")

        query = f"SELECT bound_wkt FROM state_{state_code}_boundary;" 
        row   = attom.execute(query).fetchone()
        if row is None:
            raise ValueError(f"No boundary table for {state_code}")
        geom = wkt.loads(row[0])
        minx, miny, maxx, maxy = geom.bounds 

        attom.close()
        return minx, miny, maxx, maxy 
