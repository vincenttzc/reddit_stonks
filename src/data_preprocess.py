import pandas as pd
import yfinance as yf
import praw
from datetime import datetime


clean_columns = [
    "submission_title",
    "submission_id",
    "author",
    "body",
    "datetime",
    "date",
    "id",
    "score",
    "type",
]

reddit = praw.Reddit('DEFAULT') # credentials stored in praw.ini

def process_ticker_list(ticker_list, exception_list):
    ticker_list_len_1 = [ticker for ticker in ticker_list if len(ticker) == 1]
    full_exception_list = exception_list + ticker_list_len_1

    dollar_ticker_list = ["$" + ticker for ticker in ticker_list]
    ticker_list_no_exception = [
        ticker for ticker in ticker_list if ticker not in full_exception_list
    ]

    full_ticker_list = dollar_ticker_list + ticker_list_no_exception
    return full_ticker_list


def utc_to_datetime_str(utc):
    dt = datetime.fromtimestamp(utc)
    date_time_str = dt.strftime("%m/%d/%Y, %H:%M:%S")
    date_str = dt.strftime("%m/%d/%Y")
    return date_time_str, date_str


def create_reddit_df():
    reddit_columns = [
        "submission_title",
        "submission_author",
        "submission_body",
        "submission_score",
        "submission_id",
        "submission_datetime",
        "submission_date",
        "comment_author",
        "comment_body",
        "comment_datetime",
        "comment_date",
        "comment_id",
        "comment_score",
    ]
    reddit_main_data = pd.DataFrame(columns=reddit_columns)
    return reddit_main_data


def save_comment(comment, submission_info_list):

    # save params
    comment_author = comment.author
    comment_body = comment.body
    comment_utc = comment.created_utc
    comment_id = comment.id
    comment_score = comment.score

    # convert utc to datetime
    comment_datetime, comment_date = utc_to_datetime_str(comment_utc)

    # append to table
    comment_info_list = [
        comment_author,
        comment_body,
        comment_datetime,
        comment_date,
        comment_id,
        comment_score,
    ]
    append_row = submission_info_list + comment_info_list
    reddit_columns = create_reddit_df().columns
    append_series = pd.Series(append_row, index=reddit_columns)

    return append_series


def save_submission(submission):

    # initialise submission df
    submission_df = create_reddit_df()

    # save params
    submission_title = submission.title
    submission_author = submission.author
    submission_body = submission.selftext
    submission_score = submission.score
    submission_id = submission.id
    submission_utc = submission.created_utc

    # convert utc to datetime
    submission_datetime, submission_date = utc_to_datetime_str(submission_utc)

    submission_info_list = [
        submission_title,
        submission_author,
        submission_body,
        submission_score,
        submission_id,
        submission_datetime,
        submission_date,
    ]

    # Access all comments - breadth first
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():

        append_series = save_comment(comment, submission_info_list)
        submission_df = submission_df.append(append_series, ignore_index=True)

    return submission_df


def save_subreddit(subreddit_name, extract_limit):

    # Initialise empty table
    reddit_main_data = create_reddit_df()

    # Create subreddit instance
    subreddit = reddit.subreddit(subreddit_name)

    for submission in subreddit.new(limit=extract_limit):
        submission_df = save_submission(submission)
        reddit_main_data = reddit_main_data.append(submission_df, ignore_index=True)

    return reddit_main_data


def create_comment_df(reddit_df, comment_columns):
    df_comment = reddit_df[comment_columns]
    df_comment["type"] = "comment"

    return df_comment


def create_submission_df(reddit_df, submission_columns):
    df_submission = reddit_df[submission_columns]
    df_submission["type"] = "submission"
    df_submission = df_submission.drop_duplicates()

    return df_submission


def create_clean_df(clean_columns):
    df = pd.DataFrame(columns=clean_columns)
    return df


def combine_submission_comment(df_comment, df_submission):
    """Combine comments and submission df"""
    df = create_clean_df(clean_columns)

    df_comment.columns = clean_columns
    df_submission.columns = clean_columns

    df_combined = df.append(df_submission, ignore_index=True)
    df_combined = df_combined.append(df_comment, ignore_index=True)

    return df_combined


def find_all_ticker(text):
    # Find intersection
    text_set = set(text.split(" "))
    tickers_in_text = text_set.intersection(ticker_list)

    return list(tickers_in_text)


def create_ticker_column(df, text_column, new_column_name, full_ticker_list):
    """Create ticker column from text"""

    def find_all_ticker(text):
        # Find intersection
        text_set = set(text.split(" "))
        tickers_in_text = text_set.intersection(full_ticker_list)
        return list(tickers_in_text)

    df[new_column_name] = df[text_column].apply(find_all_ticker)
    df = df.explode(new_column_name).reset_index(drop=True)

    return df