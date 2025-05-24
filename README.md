# Metadata-driven ETL Framework for Spark Pipelines

This project is a modular ETL framework designed to simplify the development and deployment of Apache Spark pipelines.  
It defines a 3-step pluggable architecture (`Read → Transfrom → Write`) and is built with extensibility in mind.

##  Guide to use: 

### 1. Install library

```commandline
pip install git+https://github.com/Nalexx6/spark-etl-template-library.git 
```

### 2. Create entry class 

```python
from etl.etl_driver import ETLDriver


if __name__ == "__main__":
    ETLDriver().run()

```

### 3. Generate metadata files 

Please refer to the etl/examples folder and etl/metadata/pipeline_schema.py file to examine the structure of the file

### 4. Run the job 

* Using python with standalone Spark:

```commandline
python3.11 etl_driver.py  --config config.yaml  --registry registry.yaml --spark-conf spark_conf.yaml
```

* Using Spark submit:

```commandline
spark-submit
  --master <your-master>
  <your-spark-conf>
  etl_driver.py --config config.yaml  --registry registry.yaml --spark-conf spark_conf.yaml
```