import yfinance as yf
import pandas as pd
import logging

from abc import ABC, abstractmethod

logging.basicConfig(
    filename="basic.log",
    encoding="utf-8",
    level=logging.INFO,
    filemode="w",
    format="%(process)d-%(levelname)s-%(message)s",
)
# Simple ETL example using yFinance


class ETL(ABC):
    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def transform_data(self):
        pass

    @abstractmethod
    def load_data(self):
        pass


class yFinanceETL(ETL):
    def __init__(self, tickers):
        self.tickers = tickers

        if len(self.tickers) == 0:
            logging.error("You need to enter a ticker")
            raise ValueError("Need to enter a ticker")

    def extract_data(self):
        logging.info(f"Preparing data extraction for: {self.tickers}")
        start_date = pd.Timestamp.now() - pd.DateOffset(months=12)
        end_date = pd.Timestamp.now()

        all_data = []

        for ticker in self.tickers:
            df_ticker = yf.download(ticker, start=start_date, end=end_date)[["Close"]]
            df_ticker.columns = [ticker]
            all_data.append(df_ticker)

        df_combined = pd.concat(all_data, axis=1)

        logging.info("Completed extraction")
        return df_combined

    def transform_data(self, df):
        # Let's find the % difference between prices each day

        if df.empty:
            logging.warning("Dataframe is empty, no data found")
            return df

        logging.info("Beginning transform")

        for ticker in self.tickers:
            df[f"{ticker}_Difference"] = df[ticker].diff()
            df[f"{ticker}_%_Change"] = (
                df[f"{ticker}_Difference"] / df[ticker].shift(1)
            ) * 100
        # print(df)
        return df

    def load_data(self, df):
        logging.info("Loading CSV")
        df.to_csv("aapl_spy.csv")
        logging.info("CSV complete")


tickers = ["TSLA"]
etl = yFinanceETL(tickers)
data = etl.extract_data()
transformed_data = etl.transform_data(data)
etl.load_data(transformed_data)
