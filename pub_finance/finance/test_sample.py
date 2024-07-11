import geopandas as gpd
from shapely.geometry import MultiPolygon
file_name = './houseinfo/shanghaidistrict.json'
data = gpd.read_file(file_name, engine="pyogrio")
print(gpd.list_layers(file_name))