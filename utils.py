from pyspark.sql import SparkSession


def generate_meta(spark: SparkSession, path: str):
    data = spark.read.option("delimiter", "\\t").option("header", "true").csv(path)
    file_name = path.split('/')[-1]
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': data.columns
    }
    return metadata

def profile_column():
    column_data ={
        'column_name':''
    }
    pass
