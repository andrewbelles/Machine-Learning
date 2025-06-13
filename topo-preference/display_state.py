import sqlite3
import pandas as pd 
from shapely import wkt
from shapely import geometry
import matplotlib.pyplot as plt

attom = sqlite3.connect("attom_data.db")
df = pd.read_sql_query("SELECT bound_wkt FROM state_TN_boundary LIMIT 1;", attom)
attom.close()

geom = wkt.loads(df.loc[0, 'bound_wkt'])

plt.figure()
polys = []
if isinstance(geom, geometry.MultiPolygon):
    polys = list(geom.geoms)
elif isinstance(geom, geometry.Polygon):
    polys = [geom]
else: 
    raise ValueError(f"Unexpected Geometry Type: {geom.geom_type}")

for poly in polys:
    x, y = poly.exterior.xy
    plt.plot(x, y)

plt.title("Boundary of TN")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.axis("equal")
plt.show()
