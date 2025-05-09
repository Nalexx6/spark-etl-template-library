import argparse
import yaml


def get_etl_args():
    parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
    parser.add_argument("--registry", required=False, help="Path to additional registry of pipeline entities", default=None)
    parser.add_argument("--spark-conf", required=False, help="Path to additional spark packages/configurations", default=None)
    args = parser.parse_args()

    return args


def load_yaml(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return yaml.safe_load(f)
