import streamlit as st
import numpy as np
import pandas as pd


data = pd.read_csv("data/reddit_data.csv")
data["datetime"] = pd.to_datetime(data["datetime"])
data = data.set_index("datetime")
top_five_tickers = list(data["ticker"].value_counts().index[:5])


st.title("Reddit Sentiments")

option = st.sidebar.selectbox(
    "Which ticker would you like to analyse?", top_five_tickers
)
option_2 = st.sidebar.selectbox("Duration?", ["1hr", "12hr", "24hr", "3days", "7days"])


chart_data = data.groupby("ticker").resample("3H")["score"].sum()[option]

st.line_chart(chart_data)