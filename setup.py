from setuptools import setup, find_packages

with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name="spark_etl_framework",
    version="1.3.0",
    author="Mykyta Oleksiienko",
    description="A modular Spark-based ETL framework",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.11",
    include_package_data=True,
)


