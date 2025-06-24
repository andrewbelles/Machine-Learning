<<<<<<< HEAD
import soil as s

sc = s.SoilClient("https://rest.isric.org/soilgrids/v2.0/properties/query")
flat = sc.fetch_point(32.4787, -87.7326)
print(list(flat.items())[:6])
=======
import sqlite3, pandas as pd 

with sqlite3.connect("data/soil.db") as dat: 
    df = pd.read_sql("SELECT * FROM state_soil_raw LIMIT 5", dat)

print(df)
>>>>>>> 8499165 (Porting API Clients to Rust)
