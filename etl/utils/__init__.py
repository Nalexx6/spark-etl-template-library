import argparse


def get_etl_args():
    parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
    parser.add_argument("--registry", required=True, help="Path to additional registry of pipeline entities")
    args = parser.parse_args()

    return args