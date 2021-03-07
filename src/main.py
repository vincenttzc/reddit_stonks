import pathlib

import praw
import pandas as pd

from data_extraction.ticker_data import TickerData
from data_extraction.reddit_data import RedditData
from model.model import Model

# add into config file
nasdaq_path = "data/nasdaq_screener.csv"
otc_path = "data/otc_screener.csv"
csv_path_list = [nasdaq_path, otc_path]
exception_list = ["TD", "ANY", "CEO", "EV"]
subreddits = ["stocks"]
num_posts = 5
model_name = "albert-base-v2"
batch_size = 2

# Prepare data
ticker_data = TickerData(csv_path_list, exception_list)
ticker_list = ticker_data.create_data()

reddit = praw.Reddit("DEFAULT")
reddit_data = RedditData(reddit, subreddits, num_posts, ticker_list)
reddit_data = reddit_data.create_data()
# reddit_data.to_csv("data/reddit_data.csv", index=False)
print("data shape", reddit_data.shape)

# Make prediction
text = list(reddit_data["body"])
text = text[:4]
model = Model(model_name, batch_size)
preds = model.predict(text)
print("prediction: ", preds)
