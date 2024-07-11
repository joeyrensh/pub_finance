import geopandas as gpd
from shapely.geometry import MultiPolygon
data = gpd.read_file('./houseinfo/shanghaidistrict.json', engine="pyogrio")
for geom in data['geometry']:
    if isinstance(geom, MultiPolygon):
        overlaps = []
        for poly1 in geom.geoms:
            for poly2 in geom.geoms:
                if poly1 != poly2 and poly1.intersects(poly2):
                    overlaps.append((poly1, poly2))
        if overlaps:
            print("重叠的 MultiPolygon:")
            for overlap in overlaps:
                print(overlap)