import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import pandas as pd


def make_dash_table(df):
    """Return a dash definition of an HTML table for a Pandas dataframe"""
    table = []
    html_row = []
    for row in df.columns:
        html_row.append(html.Th([row]))
    table.append(html.Tr(html_row))
    for index, row in df.iterrows():
        html_row = []
        for i in range(len(row)):
            html_row.append(html.Td([row[i]]))
        table.append(html.Tr(html_row))
    print(table)
    return table


df_new_house = pd.read_csv("./houseinfo/newhouse.csv", usecols=[i for i in range(0, 8)])

make_dash_table(df_new_house)
