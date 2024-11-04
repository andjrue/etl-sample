import yfinance as yf
import pandas as pd
import logging
from finance_base_etl.base_etl import ETL


class yFinanceETL(ETL):
    def __init__(self, tickers):
        self.tickers = tickers

        if len(self.tickers) == 0:
            logging.error("Ticker not entered")
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
        df.to_csv("stocks.csv")
        logging.info("CSV complete")
