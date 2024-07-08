import osmnx
import requests
import json
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import math
from adjustText import adjust_text
from utility.MyEmail import MyEmail
import seaborn as sns
from utility.ToolKit import ToolKit
from matplotlib.transforms import Bbox
import os
import contextily as cx
PLACE_NAME = "China Shanghai"
graph = osmnx.graph_from_place(PLACE_NAME)
type(graph)