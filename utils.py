from pyspark.sql import SparkSession, DataFrame


def generate_meta(spark: SparkSession, path: str):
    df = spark.read.option("delimiter", "\\t").option("header", "true").csv(path)
    file_name = path.split('/')[-1]
    cols = df.columns
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': cols
    }
    columns = []
    for col in cols:
        columns.append(profile_column(df, col))

    metadata['columns'] = columns

    return metadata


def profile_column(df: DataFrame, col: str):
    # Really slow right now!
    empty_num = df.filter(df[col].isNull()).count()
    total_num = df.count()
    freq_list = df.groupBy(col).count().orderBy(col, ascending=False)
    distinct_num = freq_list.count()
    freq = []
    for row in freq_list.take(5):
        freq.append(row[col])
    column_data = {
        'column_name': col,
        'number_non_empty_cells': total_num - empty_num,
        'number_empty_cells': empty_num,
        'number_distinct_values': distinct_num,
        'frequent_values': freq
    }
    return column_data
