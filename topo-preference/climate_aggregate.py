import sqlite3, pandas as pd 

with sqlite3.connect("data/noaa.db") as dat: 
    df = pd.read_sql(f"SELECT * FROM state_climate_raw", dat)

norm_cols = [f"norm_{i}" for i in range(36)]
norm = df[norm_cols].to_numpy(float).reshape(-1, 12, 3)

# Instantiate to expected size we are overwriting 
mean_temp = norm[:, :, 0]

# Do each month individually 
states = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

for state in states:

    st_mask = df["state"] == state
    st_norm = df.loc[st_mask, norm_cols].to_numpy(float).reshape(1, 12, 3)
    tmin_k  = st_norm[:, :, 0] / 10.0
    tmax_k  = st_norm[:, :, 1] / 10.0
    mean_k  = (tmin_k + tmax_k) / 2.0          # shape (1, 12)
    mean_temp[st_mask, :] = mean_k             # align by mask, not by k

prcp = norm[:, :, 2] / 100.0 
print(mean_temp[1, 0])

annual_prcp = prcp.sum(axis=1) 

temp_range = mean_temp.max(axis=1) - mean_temp.min(axis=1) 

for month in range(12):
    df[f"mean_temp_{month:02d}"] = mean_temp[:, month]

df["annual_prcp"] = annual_prcp
df["temp_range"]  = temp_range 

cols = list(df.columns)
path = "data/noaa.db"
with sqlite3.connect(path) as dat: 
    df.to_sql("state_climate", dat, if_exists="replace", index=False)
