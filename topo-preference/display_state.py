import sqlite3
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import wkt

attom_db = sqlite3.connect("attom_data.db")

# 2. Loop through your state tables and build one DataFrame
frames = []
states = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

# Get each wkt boundary as a geometry and append to frame list 
for state in states:
    table = f"state_{state}_boundary"
    try:
        df = pd.read_sql_query(f"SELECT bound_wkt FROM {table};", attom_db)
    except Exception:
        continue
    df["code"] = state
    df["geometry"] = df["bound_wkt"].apply(wkt.loads)
    frames.append(df[["code","geometry"]])

attom_db.close()

# Geopandas stuff to get the projection to look correct
gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True),
                       geometry="geometry",
                       crs="EPSG:4326")

# Plot projected frames 
gdf_us = gdf.to_crs(epsg=5070)
fig, ax = plt.subplots()
gdf_us.boundary.plot(ax=ax)

ax.set_title("50 States Boundary Maps")
ax.set_axis_off()
plt.tight_layout()
plt.show()
