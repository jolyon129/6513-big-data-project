from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from collections import Counter
from pyspark import SparkContext, RDD
from csv import reader
import itertools


def mapd(x: List):
    """
    TODO: check date type
    :param x:
    :return:
    """
    # [col_idx, (value, type)]
    res = (x[0], [x[1], None])
    if (x[1] == ''):
        res[1][1] = 'empty'
    elif (is_int(x[1])):
        res[1][1] = 'int'
    elif (is_float(x[1])):
        res[1][1] = 'real'
    else:
        res[1][1] = 'text'
    return res


def is_int(s: str):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_float(value: str):
    if '.' not in value:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def generate_meta(sc: SparkContext, path: str):
    # read dataframe
    # Add index to each row, [([...], 0),([...], 1)...]
    rdd = sc.textFile(path, 1).mapPartitions(lambda x: reader(x, delimiter='\t')).zipWithIndex()
    header = rdd.filter(lambda x: x[1] == 0) \
        .map(lambda x: (x[0])).collect()[0]  # extract the first part, ignore idx
    rows = rdd.filter(lambda x: x[1] != 0).map(lambda x: x[0])
    file_name = path.split('/')[-1]
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': header
    }
    N = len(header)
    # Transform to [(col_idx, value),(col_idx, value)...]
    items = rows.flatMap(
        lambda x, h=header: [(h[i], x[i]) for i in range(N)]).cache()

    # Transform to [(col_idx, (value, type)),(col_idx, (value, type))...]
    mapped_items = items.map(mapd)
    col_map = {}
    for col in header:
        col_map[col] = {}

    res2 = generate_distinct_top5(items)
    res1 = generate_null_empty(mapped_items)
    # [(col,non-empty, empty, total, distinct_num, top5:(col_name,freq))]
    flat_res = res1.join(res2).map(lambda x: (x[0], (*x[1][0], *x[1][1]))).collect()
    columns = []
    for res in flat_res:
        column_data = {
            'column_name': res[0],
            'number_non_empty_cells': res[1][0],
            'number_empty_cells': res[1][1],
            'number_distinct_values': res[1][3],
            'frequent_values': [x[0] for x in res[1][4]]
        }
        columns.append(column_data)
    metadata['columns'] = columns
    return metadata


def generate_null_empty(mapped_items: RDD) -> RDD:
    """
    :param mapped_items: [(col,(value, type)), ...]
    :return: [(col1,[non-empty, empty, total]), (col2,[null-empty, empty, total])]
    """

    def seqFunc(local, x):
        res = [i for i in local]
        if (x[1] != 'empty'):
            res[0] = local[0] + 1
        else:
            res[1] = local[1] + 1
        res[2] = local[2] + 1
        return res

    combFunc = (lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
    count = mapped_items.aggregateByKey((0, 0, 0), seqFunc, combFunc)
    return count


def generate_distinct_top5(items: RDD) -> RDD:
    """
    :param items: [(col,value),...]
    :return: [(col,(distinct_num, [top5...])),(col,(distinct_num, [top5...])),...]
    """
    freq_items = items.map(lambda x: ((x[0], x[1]), 1)) \
        .aggregateByKey((0, 0),
                        (lambda x, y: (0, x[1] + 1)),
                        (lambda x, y: (x[1] + y[1]))) \
        .map(lambda x: ((x[0][0]), (x[0][1], x[1][1])))
    sorted_grouped_freq_items = freq_items.sortBy(lambda x: x[1][1], ascending=False).groupByKey()
    res = sorted_grouped_freq_items.mapValues(lambda x: (len(x), list(itertools.islice(x, 5))))
    return res
