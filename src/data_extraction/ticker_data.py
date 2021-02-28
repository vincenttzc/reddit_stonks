import pandas as pd


class TickerData:
    """Ticker data object with csv_path and exception list.
    Used to create list of ticker symbols to extract in reddit data

        Args:
            csv_path (list): list of path of csv files containing ticker info
            exception_list (list): list of ticker symbols to exclude
    """

    def __init__(self, csv_path_list: list, exception_list: list):
        """Constructor method"""

        self.csv_path_list = csv_path_list
        self.exception_list = exception_list

    def create_data(self) -> list:
        """
        1) Read ticker list from csv path specified
        2) Remove tickers in exception list
        3) Create variations of ticker symbols

        Returns:
            list: list of ticker symbols
        """
        ticker_list = self.read_ticker_file(self.csv_path_list, "Symbol")
        ticker_list = self.remove_exceptions(ticker_list, self.exception_list)
        ticker_list = self.create_variation(ticker_list)

        return ticker_list

    def read_ticker_file(self, csv_path_list: list, ticker_column_name: str) -> list:
        """Read .csv file with path specified.
        Extract list of ticker with name of ticker column

        Args:
            csv_path (list): list of path of csv files containing ticker info
            ticker_column_name (str): name of column containing ticker

        Returns:
            list: list of ticker symbols
        """
        ticker_list = []
        for csv_path in csv_path_list:
            ticker_data = pd.read_csv(csv_path)
            ticker_list = ticker_list + list(ticker_data[ticker_column_name])

        # remove duplicates
        ticker_list = list(set(ticker_list))

        return ticker_list

    def remove_exceptions(self, ticker_list: list, exception_list: list) -> list:
        """Remove tickers in excception list

        Args:
            ticker_list (list): initial list of ticker symbols
            exception_list (list): list of ticker symbols to remove

        Returns:
            list: list of ticker symbols without tickers in exception_list
        """

        # 1. Remove tickers in exception list
        ticker_list = [ticker for ticker in ticker_list if ticker not in exception_list]

        # 2. Remove single character tickers
        ticker_list = [ticker for ticker in ticker_list if len(ticker) != 1]

        return ticker_list

    def create_variation(self, ticker_list: list) -> list:
        """Create variations of tickers as it could appear differently
        in reddit text

        Args:
            ticker_list (list): list of ticker symbols

        Returns:
            list: list of ticker symbols with variation added
        """
        # Add dollar sign
        dollar_ticker_list = ["$" + ticker for ticker in ticker_list]
        full_ticker_list = ticker_list + dollar_ticker_list

        return full_ticker_list


if __name__ == "__main__":
    print("ticker_data")

    nasdaq_path = "data/nasdaq_screener.csv"
    otc_path = "data/otc_screener.csv"
    csv_path_list = [nasdaq_path, otc_path]
    exception_list = ["TD", "ANY", "CEO", "EV"]

    ticker_data = TickerData(csv_path_list, exception_list)
    ticker_list = ticker_data.create_data()
    print(len(ticker_list))