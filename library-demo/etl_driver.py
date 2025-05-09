import argparse
from etl import etl_driver


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
    args = parser.parse_args()

    etl_driver.run(args.config)
