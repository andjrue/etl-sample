import logging
import datetime
from etl.yFinance_etl import yFinanceETL

logging.basicConfig(
    filename="basic.log",
    encoding="utf-8",
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    tickers = ["AAPL", "SPY"]
    etl = yFinanceETL(tickers)

    data = etl.extract_data()

    if data.empty:
        logging.error("Error with data - Main")
    else:
        transformed_data = etl.transform_data(data)
        etl.load_data(transformed_data)


if __name__ == "__main__":
    main()
