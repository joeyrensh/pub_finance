import geopandas as gpd
from shapely.geometry import MultiPolygon
import pandas as pd
file_name = './houseinfo/secondhandhouse.csv'
df = pd.read_csv(file_name, usecols=[i for i in range(0, 10)])
df['total_cnt'] = df['total_cnt'].replace(to_replace=r'[\u4e00-\u9fff]', value='', regex=True).astype(int)

print(df['total_cnt'].sum())