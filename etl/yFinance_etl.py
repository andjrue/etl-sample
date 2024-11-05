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

        The error is showing because the ETL class doesn't specify any return types. That would be easy enough to add, I just don't
        think it's necessary for this example.
        """

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
        # print(data)
        return data

    def gen_csv(self, data):
        logging.info("Loading CSV")
        data.to_csv("stocks.csv")
        logging.info("CSV complete")

    def upload_csv_s3(self, file_name, bucket, object_name=None):

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
            logging.info(e)
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
