import soil as s

sc = s.SoilClient("https://rest.isric.org/soilgrids/v2.0/properties/query")
flat = sc.fetch_point(32.4787, -87.7326)
print(list(flat.items())[:6])
