import pandas as pd, sqlite3, numpy as np, matplotlib.pyplot as plt

with sqlite3.connect("data/noaa.db") as con:
    raw = pd.read_sql("SELECT * FROM state_climate_raw WHERE state='AR'", con)

# inspect raw CLIMDIV integers
print(raw[[f"norm_{i}" for i in range(6)]].iloc[0].to_list())
# e.g. 389, 431, 513, ...  (should be 3-digit integers near 400–700)

# rebuild Arkansas tensors only
norm_ar = raw[[f"norm_{i}" for i in range(36)]].to_numpy(float).reshape(1, 12, 3)
tmin_ar = norm_ar[:, :, 0] / 10.0
tmax_ar = norm_ar[:, :, 1] / 10.0
mean_ar = (tmin_ar + tmax_ar) / 2.0

print("Arkansas monthly means °F:", np.round(mean_ar[0], 1))
print("Range :", mean_ar.max()-mean_ar.min())

with sqlite3.connect("data/noaa.db") as con:
    ar = pd.read_sql("SELECT * FROM state_climate WHERE state='AR'", con)

monthly = ar.filter(like="mean_temp_").iloc[0]
monthly.index = np.arange(1, 13)       # 1 … 12 for plotting
monthly.plot(marker="o")
plt.ylabel("°F")
plt.title("Arkansas – monthly mean temperature (1991-2020 normals)")
plt.show()
