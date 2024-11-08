import logging
import os

from dotenv.main import load_dotenv
from y_finance_etl.yFinance_etl import yFinanceETL

# Might want to add unit testing.

logging.basicConfig(
    filename="basic.log",
    encoding="utf-8",
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():

    load_dotenv()

    logging.info("*** STARTING yfinance -> CSV -> S3 ***")

    tickers = ["AAPL", "SPY"]
    etl = yFinanceETL(tickers)

    data = etl.extract_data()

    if data.empty:
        logging.error("Error with data - Main")
        raise ValueError("No data extracted")
    else:
        transformed_data = etl.transform_data(data)
        etl.load_data(transformed_data)

if __name__ == "__main__":
    main()
