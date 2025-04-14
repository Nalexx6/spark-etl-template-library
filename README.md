# Metadata-driven ETL Framework for Spark Pipelines

This project is a modular ETL framework designed to simplify the development and deployment of Apache Spark pipelines.  
It defines a 3-step pluggable architecture (`Input → Transfrom → Output`) and is built with extensibility in mind.

## Features (Phase 1 – POC)

- Implemented in Python, runs on Apache Spark
- Abstract interfaces for `DataInput`, `DataTransformer`, and `DataOutput`, which enforce the ETL structure
- Metadata parser to infer input, transformer and output classes from YAML config
- Simple CSV, Parquet implementations for input/output with S3 as source
- Simple transformers to test the metadata workability
- Tested scenario: 
  - read CSV from S3, add timestamp column, write Parquet to S3

## Metadata-Based Pipeline Configuration
The ETL framework supports pipeline definition via external YAML metadata. This enables dynamic construction of ETL pipelines without changing the codebase.

### Structure
A typical pipeline metadata file consists of three parts:

* Input: Defines the data source (e.g., S3, Kafka, HDFS) and it's format (CSV, Parquet)

* Transformations: Defines canonical name of the transformer class

* Output: Defines the data sink (e.g., S3, Redshift, Kafka) and it's format (CSV, Parquet)

### Example YAML

```yaml

pipeline:
  name: example_s3_to_s3_pipeline

  input:
    type: s3
    format: csv
    config:
      path: "s3a://my-bucket/input/"
      header: "true"
      inferSchema: "true"

  transformations:
    klass: "my_module1.my_module2.MyTransformer"
    props: 
      key: "value" 

  output:
    type: s3
    format: parquet
    config:
      path: "s3a://my-bucket/output/"
      mode: "overwrite"


```

### Local (Python)

```bash
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=<your-acc-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-acc-key>
python etl_driver.py --config <path-to-yaml-config> 
