import pandas as pd
import plotly.express as px
import pyodide.http

# Load the flights dataset
df = pd.read_csv(
    pyodide.http.open_url(
        "https://raw.githubusercontent.com/jbrownlee/Datasets/master/airline-passengers.csv"
    )
)

# If you are running the code outside of this website, e.g., a notbook on your local computer, you can use pd.read_csv directly
# df = pd.read_csv('https://raw.githubusercontent.com/jbrownlee/Datasets/master/airline-passengers.csv')


df.rename(columns={"Passengers": "passengers", "Month": "month"}, inplace=True)
df["first_day_of_mon"] = pd.to_datetime(df["month"])
df["year"] = df["first_day_of_mon"].dt.year
df["month"] = df["first_day_of_mon"].dt.month

# Pivot the dataset to create a matrix with years as rows and months as columns
df = df.pivot_table(values="passengers", index="year", columns="month")

# Display the first few rows of the dataframe
print(df.head())

# df_trade = pd.read_csv(
#     "./usstockinfo/trade_20241023.csv",
#     header=None,
# )
# df_trade.columns = ["index", "symbol", "date", "action", "base", "volume", "strategy"]

# # 按symbol分组，按date倒序排序，然后保留每个分组中date最大的记录
# trade_result = df_trade.sort_values(
#     by=["symbol", "date"], ascending=[True, False]
# ).drop_duplicates(subset="symbol", keep="first")
# trade_result = trade_result[trade_result["action"] == "buy"]

# std_trade_result = trade_result.groupby(["strategy"])["symbol"].count()


# df_pos = pd.read_csv(
#     "./usstockinfo/position_20241023.csv",
# )

# result = df_pos[df_pos["p&l"] > 0].count()
# print(std_trade_result)
