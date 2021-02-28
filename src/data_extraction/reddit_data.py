import re
from datetime import datetime

import praw
import pandas as pd

from ticker_data import TickerData


class RedditData:
    """Reddit data class to extract and preprocess data from Reddit API

    Args:
        reddit (praw.Reddit): Reddit class to access Reddit's API
        subreddit_list (list): list of subreddit str to extract data
        num_posts (int): number of new submissions to extract
        ticker_list (list): list of ticker symbol str used to extract ticker
    """

    def __init__(
        self,
        reddit: praw.Reddit,
        subreddit_list: list,
        num_posts: int,
        ticker_list: list,
    ):
        """Constructor method"""
        self.reddit = reddit
        self.subreddit_list = subreddit_list
        self.num_post = num_posts
        self.ticker_list = ticker_list
        self.initial_col_names = [
            "submission_title",
            "submission_author",
            "submission_body",
            "submission_score",
            "submission_id",
            "submission_datetime",
            "comment_author",
            "comment_body",
            "comment_datetime",
            "comment_id",
            "comment_score",
        ]
        self.comment_columns = [
            "submission_title",
            "submission_id",
            "comment_author",
            "comment_body",
            "comment_datetime",
            "comment_id",
            "comment_score",
        ]
        self.submission_columns = [
            "submission_title",
            "submission_id",
            "submission_author",
            "submission_body",
            "submission_datetime",
            "submission_id",
            "submission_score",
        ]
        self.final_col_names = [
            "submission_title",
            "submission_id",
            "author",
            "body",
            "datetime",
            "id",
            "score",
            "type",
        ]

    def create_data(self) -> pd.DataFrame:
        """
        1) Extract reddit text from Reddit API
        2) Transform structure of extracted data
        3) Remove unwanted characters in reddit text
        4) Extract ticker symbols found

        Returns:
            pd.DataFrame: DataFrame containing extracted and transformed reddit text
        """
        data = pd.DataFrame(columns=self.final_col_names)
        for subreddit in self.subreddit_list:
            subreddit_data = self.save_subreddit(
                subreddit, self.num_post, self.initial_col_names
            )
            data = data.append(subreddit_data, ignore_index=True)
        data = self.transform_data(data)
        data = self.remove_unwanted_char(data)
        data = self.extract_ticker(data)

        return data

    def utc_to_datetime_str(self, utc: float) -> str:
        """Convert utc to datetime string

        Args:
            utc (float): datetime represented in utc

        Returns:
            str: date time string
        """

        dt = datetime.fromtimestamp(utc)
        date_time_str = dt.strftime("%m/%d/%Y, %H:%M:%S")

        return date_time_str

    def save_comment(self, comment: praw.models.Comment) -> list:
        """Extract and return information of a reddit comment

        Args:
            comment (praw.models.Comment): A class that represents a reddit comment

        Returns:
            list: list of string containing information of comment
        """
        comment_author = comment.author
        comment_body = comment.body
        comment_utc = comment.created_utc
        comment_id = comment.id
        comment_score = comment.score
        comment_datetime = self.utc_to_datetime_str(comment_utc)

        comment_info_list = [
            comment_author,
            comment_body,
            comment_datetime,
            comment_id,
            comment_score,
        ]

        return comment_info_list

    def save_submission(
        self, submission: praw.models.Submission, col_names: list
    ) -> pd.DataFrame:
        """Extract and return information about a reddit submission

        Args:
            submission (praw.models.Submission): A class that represents a
                                                reddit submission
            col_names (list): [description]: column names for the output dataframe

        Returns:
            pd.DataFrame: DataFrame containing information about the
                            comments within the submission
        """

        # Initialise
        submission_df = pd.DataFrame(columns=col_names)

        # save params
        submission_title = submission.title
        submission_author = submission.author
        submission_body = submission.selftext
        submission_score = submission.score
        submission_id = submission.id
        submission_utc = submission.created_utc
        submission_datetime = self.utc_to_datetime_str(submission_utc)

        submission_info_list = [
            submission_title,
            submission_author,
            submission_body,
            submission_score,
            submission_id,
            submission_datetime,
        ]

        # Iterate through each comment to extract info - breadth first search
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():

            comment_info_list = self.save_comment(comment)
            info_list = submission_info_list + comment_info_list
            info_series = pd.Series(info_list, index=col_names)
            submission_df = submission_df.append(info_series, ignore_index=True)

        return submission_df

    def save_subreddit(
        self, subreddit_name: str, extract_limit: int, col_names: list
    ) -> pd.DataFrame:
        """Extract and return information of subreddit specified

        Args:
            subreddit_name (str): subreddit to extract data
            extract_limit (int): number of new posts to extract
            col_names (list): column names of output dataframe

        Returns:
            pd.DataFrame: DataFrame containing information of subreddt
        """
        # Initialise empty table
        subreddit_df = pd.DataFrame(columns=col_names)

        # Create subreddit instance
        subreddit = self.reddit.subreddit(subreddit_name)

        # Iterate through each submission to extract info
        for submission in subreddit.new(limit=extract_limit):
            submission_df = self.save_submission(submission, col_names)
            subreddit_df = subreddit_df.append(submission_df, ignore_index=True)

        return subreddit_df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform structure of data extracted so that it contains a single
        column of body text, regardless if it is a submission or comment text

        Args:
            df (pd.DataFrame): DataFrame to be transformed

        Returns:
            pd.DataFrame: [description]
        """

        # Prepare comment df
        df_comment = df[self.comment_columns]
        df_comment["type"] = "comment"

        # Prepare submission df
        df_submission = df[self.submission_columns]
        df_submission = df_submission.drop_duplicates()
        df_submission["type"] = "submission"

        # Set columns names
        df_comment.columns = self.final_col_names
        df_submission.columns = self.final_col_names

        # Combine submission and comment df
        df_combined = df_submission.append(df_comment, ignore_index=True)

        return df_combined

    def remove_unwanted_char(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove all punctuations and emoji, except $ sign from body text column

        Args:
            df (pd.DataFrame): DataFrame with body column

        Returns:
            pd.DataFrame: DataFrame with unwanted characters removed in body column
        """
        # Remove emoji, unwanted punctuation marks
        regex = "[^\w\d\s\$]+"
        df["body"] = df["body"].apply(lambda text: re.sub(regex, "", text))

        return df

    def extract_ticker(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract ticker symbols from body text column in dataframe

        Args:
            df (pd.DataFrame): Dataframe with body text in body column

        Returns:
            pd.DataFrame: DataFrane with ticker column containing ticker
                        symbols extracted
        """
        # returns column of list
        df["ticker"] = df["body"].apply(self.find_tickers_in_text)

        # create multiple rows if multiple tickers in list
        df = df.explode("ticker").reset_index(drop=True)

        # remove $ sign in extracted ticker
        df["ticker"] = df["ticker"].str.replace("$", "", regex=True)

        return df

    def find_tickers_in_text(self, text: str) -> list:
        """Helper function to extract ticker symbols from string

        Args:
            text (str): string to extract ticker symbols

        Returns:
            list: list of ticker symbols extracted
        """
        # Find intersection
        text_set = set(text.split(" "))
        tickers_in_text = text_set.intersection(self.ticker_list)

        return list(tickers_in_text)


if __name__ == "__main__":
    print("reddit_data")

    nasdaq_path = "data/nasdaq_screener.csv"
    otc_path = "data/otc_screener.csv"
    csv_path_list = [nasdaq_path, otc_path]
    exception_list = ["TD", "ANY", "CEO", "EV"]
    ticker_data = TickerData(csv_path_list, exception_list)
    ticker_list = ticker_data.create_data()

    reddit = praw.Reddit("DEFAULT")
    reddit_data = RedditData(reddit, ["stocks"], 5, ticker_list)
    reddit_data = reddit_data.create_data()

    print(reddit_data)
    print(list(reddit_data["ticker"]))
