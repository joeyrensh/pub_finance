import dash
import dash_table as dt
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import base64
import pandas as pd
import geopandas as gpd


app = dash.Dash(__name__)
server = app.server

file_path = "./houseinfo/newhouse.csv"
geo_path = "./houseinfo/shanghaidistrict.json"
file_path_s = "./houseinfo/secondhandhouse.csv"
png_path = "./houseinfo/map_newhouse.png"
png_path_s = "./houseinfo/map_secondhouse.png"
png_path_s2 = "./houseinfo/map_secondhouse2.png"
png_path_s3 = "./houseinfo/map_secondhouse3.png"
# 读取本地图片文件并编码为 Base64
with open(png_path, "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode()

# 新房数据分析
df_new_house = pd.read_csv(file_path, usecols=[i for i in range(0, 8)])

app.layout = html.Div(
    children=[
        html.Div(
            className="pkcalc-banner",
            children=[
                html.H2("Noncompartmental Pharmacokinetics Analysis"),
                html.A(
                    id="gh-link",
                    children=["View on GitHub"],
                    style={"color": "white", "border": "solid 1px white"},
                ),
            ],
        ),
        # Flex container
        html.Div(
            [
                # Graph container
                dbc.Container(
                    [
                        html.H5("上海房产在售新房联动价中位数板块分布"),
                        html.Img(
                            src=f"data:image/png;base64,{encoded_image}",
                            style={"width": "50%"},
                        ),
                    ]
                ),
                # Table container
                dbc.Container(
                    # 一行代码渲染静态表格
                    dbc.Table.from_dataframe(df_new_house, striped=True),
                    style={
                        "margin-top": "50px",  # 设置顶部留白区域高度
                        "striped": True,
                        "bordered": True,
                    },
                ),
            ],
            style={"display": "flex"},
        ),
    ]
)


if __name__ == "__main__":
    app.run_server()
