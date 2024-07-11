import geopandas as gpd
from shapely.geometry import MultiPolygon
import pandas as pd
file_name = './houseinfo/secondhandhouse.csv'
df = pd.read_csv(file_name, usecols=[i for i in range(0, 10)])

print(df['total_cnt'].sum())