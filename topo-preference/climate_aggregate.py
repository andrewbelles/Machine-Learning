import sqlite3, pandas as pd, numpy as np

with sqlite3.connect("data/noaa.db") as dat: 
    df = pd.read_sql(f"SELECT * FROM state_climate_raw", dat)

norm_cols = [f"norm_{i}" for i in range(36)]

# raw  (n_rows, 36)
raw = df[norm_cols].replace(-9999, np.nan).to_numpy(float)

tmin = raw[:,  0:12] / 10.0           # °F
tmax = raw[:, 12:24] / 10.0           # °F
prcp = raw[:, 24:36] / 100.0          # inches

mean_temp   = (tmin + tmax) / 2.0
annual_prcp = np.nansum(prcp, axis=1)
temp_range  = np.nanmax(mean_temp, axis=1) - np.nanmin(mean_temp, axis=1)

# write the 12 monthly mean columns
for m in range(12):
    df[f"mean_temp_{m:02d}"] = mean_temp[:, m]

df["annual_prcp"] = annual_prcp
df["temp_range"]  = temp_range

with sqlite3.connect("data/noaa.db") as con:
    df.to_sql("state_climate", con, if_exists="replace", index=False)
