import pandas as pd, os, us 
import clientbackbone as cb

# Imports for state polygons 
from dotenv import load_dotenv

load_dotenv()

class ConsensusClient(cb.ParentClient):

    def __init__(self, url: str, key: str):
        super().__init__("data/socioeconomic.db", 100)
        self.url = url.rstrip("/")
        self.key = key

        self.fields = [
            "B01003_001E",
            "B19013_001E",
            "B15003_022E",
            "B23025_003E",
        ] 

    def fetch_state(self, state: str) -> dict: 
        fips = us.states.lookup(state).fips # type: ignore 
        params = {
                "get": ",".join(self.fields + ["NAME"]),
            "for": f"state:{fips}",
            "key": self.key    
        }

        data = self._get(self.url, params=params)
        header, values = data 
        rec = (dict(zip(header, values)))
        rec["state"] = state
        return rec 

    def get_states(self, states: list[str]):
        records = []
        for state in states: 
            print(f"Fetching {state}...")
            rec = self.fetch_state(state)
            if rec is not None: 
                records.append(rec)

        df = pd.DataFrame.from_records(records)
        df = df.rename(columns={
            "B01003_001E": "total_population",
            "B19013_001E": "median_income",
            "B15003_022E": "bachelors_degree_count",
            "B23025_003E": "employed_count"
        })
        self._save(df, "socio_by_state")


if __name__ == "__main__":

    states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
    ]
    
    API_KEY  = os.getenv("CONSENSUS_API_KEY")
    BASE_URL = os.getenv("CONSENSUS_BASE_URL")
    if not (API_KEY and BASE_URL):
        raise RuntimeError("Expected CONSENSUS KEY and CONSNESUS URL")

    client = ConsensusClient(BASE_URL, API_KEY)
    client.get_states(states)
