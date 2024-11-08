"""
Simple ETL pipeline that takes data from the yfinance module and converts it to a CSV.
The CSV is then uploaded to S3.
"""

import yfinance as yf
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
import os

from finance_base_etl.base_etl import ETL
from dotenv import load_dotenv


class yFinanceETL(ETL):
    def __init__(self, tickers):
        self.tickers = tickers

        if len(self.tickers) == 0:
            logging.error("Ticker not entered")
            raise ValueError("Need to enter a ticker")

    def extract_data(self):

        """
        Not sure why there is an error showing in my IDE here. Need to investigate that. The code runs no problem.

        The error is showing because the ETL class doesn't specify any return types. Easy enough to add but it may be unnecessary the way this is currently set up.
        """

        # Would want to add some kind of data validation step here.

        logging.info(f"Preparing data extraction for: {self.tickers}")
        start_date = pd.Timestamp.now() - pd.DateOffset(months=12)
        end_date = pd.Timestamp.now()

        all_data = []

        for ticker in self.tickers:

            history = yf.Ticker(ticker).history(period='1mo', interval='1d')

            if history.empty:
                logging.error(f"Data not extracted for ticker: {ticker}")
                return pd.DataFrame()
                # Good to return a blank df here. No need to go further if we can't get all data.

            df_ticker = yf.download(ticker, start=start_date, end=end_date)[["Close"]]
            df_ticker.columns = [ticker]
            all_data.append(df_ticker)
            logging.info(f"Data extracted for {ticker}")

        df_combined = pd.concat(all_data, axis=1)

        logging.info("Completed extraction")
        return df_combined

    def transform_data(self, data):
        # Let's find the % difference between prices each day

        if data.empty:
            logging.warning("Dataframe is empty, no data found")
            return data

        logging.info("Beginning transform")

        for ticker in self.tickers:
            data[f"{ticker}_Difference"] = data[ticker].diff()
            data[f"{ticker}_%_Change"] = (
                data[f"{ticker}_Difference"] / data[ticker].shift(1)
            ) * 100

        return data

    def gen_csv(self, data):
        logging.info("Loading CSV")
        data.to_csv("stocks.csv")
        logging.info("CSV complete")

    def upload_csv_s3(self, file_name, bucket, object_name=None):

        # You could add an exponential backoff here to handle upload failures. Since this is a simple example,
        # I didn't feel the need to go through with it. Its mostly to illustrate possibilities

        load_dotenv()
        ACCESS_KEY = os.getenv("ACCESS_KEY")
        SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

        if object_name is None:
            object_name = os.path.basename(file_name)

        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY)

        try:
            logging.info("Trying to upload to s3")
            response = s3_client.upload_file(file_name, bucket, object_name)
            logging.info("Upload successful")
        except ClientError as e:
            logging.info(f"failed uploading to S3:\n {e}")
            return False
        return True


    def load_data(self, data):
        logging.info("Loading env")
        load_dotenv()
        logging.info("env loaded")

        BUCKET = os.getenv("BUCKET_NAME")

        logging.info("Generating csv")
        self.gen_csv(data)

        logging.info("starting uploading to s3")
        self.upload_csv_s3("stocks.csv", BUCKET)
