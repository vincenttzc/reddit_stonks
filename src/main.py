import pathlib, re

import praw
import pandas as pd
import yfinance as yf
from datetime import datetime

import data_preprocess as pp

comment_columns = [
    "submission_title",
    "submission_id",
    "comment_author",
    "comment_body",
    "comment_datetime",
    "comment_date",
    "comment_id",
    "comment_score",
]
submission_columns = [
    "submission_title",
    "submission_id",
    "submission_author",
    "submission_body",
    "submission_datetime",
    "submission_date",
    "submission_id",
    "submission_score",
]
exception_list = ["TD", "ANY", "CEO"]


BASE_DIR = pathlib.Path(__file__).resolve().parent.parent

ticker_file_path = BASE_DIR / "data/nasdaq_screener.csv"
ticker_data = pd.read_csv(ticker_file_path)
ticker_list = list(ticker_data["Symbol"])
full_ticker_list = pp.process_ticker_list(ticker_list, exception_list)


df_original = pp.save_subreddit("pennystocks", 10)
df_submission = pp.create_submission_df(
    reddit_df=df_original, submission_columns=submission_columns
)
df_comment = pp.create_comment_df(
    reddit_df=df_original, comment_columns=comment_columns
)
df_combine = pp.combine_submission_comment(df_submission, df_comment)
df_clean = pp.create_ticker_column(
    df_combine, "body", "body_ticker", full_ticker_list
)

print(df_clean.shape)
