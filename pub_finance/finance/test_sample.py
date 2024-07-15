import geopandas as gpd
from shapely.geometry import MultiPolygon
import pandas as pd
from shapely.ops import unary_union
from xyzservices import TileProvider
import contextily as cx
import xyzservices.providers as xyz

# 定义自定义 TileProvider
xyz.CartoDB.Positron.url = 'https://{s}.basemaps.cartocdn.com/{variant}/{z}/{x}/{y}{r}.png'
xyz.CartoDB.Positron.attribution = '(C) OpenStreetMap contributors (C) CARTO'
# 读取 Shapefile
# gdf = gpd.read_file("path/to/your/file.shp")

geo_path = './houseinfo/shanghaidistrict.json'
gdf = gpd.read_file(geo_path,  engine="pyogrio")
# gdf.to_file('test4.json', driver='GeoJSON')
ax = gdf.plot()
cx.add_basemap(ax, 
               crs="EPSG:4326",
                source = xyz.CartoDB.Positron,
                zoom = 12
                )


# geo_path = './OSMB-743692f42cf1d64dddced1486876337993217878.geojson'
# gdf = gpd.read_file(geo_path,  engine="pyogrio") 
# gdf.to_file('test4.json', driver='GeoJSON')
# gdf.plot()



# gdf_waigaoqiao = gpd.read_file('./test.json',  engine="pyogrio") #waigaoqiao
# gdf_gaohang = gpd.read_file('./test1.json',  engine="pyogrio") #gaohang
# gdf_tangzhen = gpd.read_file('./test2.json',  engine="pyogrio") #tangzhen
# gdf_chuansha = gpd.read_file('./test3.json',  engine="pyogrio") #chuansha
# # 提取 MultiPolygon 几何
# multipolygon_waigaoqiao = gdf_waigaoqiao.unary_union
# multipolygon_gaohang = gdf_gaohang.unary_union
# multipolygon_tangzhen = gdf_tangzhen.unary_union
# multipolygon_chuansha = gdf_chuansha.unary_union

# gdf_inter = multipolygon_waigaoqiao.intersection(multipolygon_chuansha)
# # gaoqiao
# merged_multipolygon = multipolygon_waigaoqiao.difference(gdf_inter)
# # merged_multipolygon = unary_union([multipolygon, gdf_inter])

# # 合并两个 MultiPolygon
# # merged_multipolygon = union_all([multipolygon, multipolygon1])
# # merged_multipolygon = multipolygon.difference(multipolygon3)
# # merged_multipolygon2 = merged_multipolygon1.difference(multipolygon3)

# # 创建一个新的 GeoDataFrame
# merged_gdf = gpd.GeoDataFrame(geometry=[merged_multipolygon])

# merged_gdf.plot()


# # 保存为新的 GeoJSON 文件
# merged_gdf.to_file('test4.json', driver='GeoJSON')