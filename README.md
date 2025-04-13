# ETL Framework for Spark Pipelines

This project is a modular ETL framework designed to simplify the development and deployment of Apache Spark pipelines.  
It defines a 3-step pluggable architecture (`Input → Process → Output`) and is built with extensibility in mind.

## Features (Phase 1 – POC)

- Abstract interfaces for `DataInput`, `DataProcessor`, and `DataOutput`
- Dummy pipeline implementation: S3 → NoOpProcessor → S3
- Implemented in Python, runs on Apache Spark
- Docker support for containerized execution
- GitHub Actions for CI/CD with Docker build
- Basic test structure with `pytest`


## How to Run

### Local (Python)

```bash
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=<your-acc-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-acc-key>
python etl/examples/dummy_pipeline.py
