from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from collections import Counter
from pyspark import SparkContext, RDD
from csv import reader
import itertools
import math


def is_date(s: str, col: str):
    col_name = col.lower()
    if 'date' in col_name or 'year' in col_name \
            or 'time' in col_name or 'month' in col_name \
            or 'day' in col_name:
        return True
    else:
        return False


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
    mapped_items = items.map(mapd).cache()
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


def generate_num_statistic(col_num_type_items: RDD) -> RDD:
    """
    Generate statistic for num of type columns and real of type columns.
    :param col_num_type_items: [(('Wave_Number', 'int'), 3),(('Week_Number', 'int'), 40)...]
    :return: ['Wave_Number', 'int'], [max_value, min_value, sum, count, mean, std])
    """

    def seqFunc(local, x):
        max_value = x if x > local[0] else local[0]
        min_value = x if x < local[1] else local[1]
        return (max_value, min_value, local[2] + x, local[3] + 1)

    combFunc = (lambda x, y: (max(x[0], y[0]), min(x[1], y[1]), x[2] + y[2], x[3] + y[3]))
    num_statistic = col_num_type_items.aggregateByKey((0, 0, 0, 0), seqFunc, combFunc)
    num_statistic = num_statistic.map(lambda x: (x[0], [*x[1], x[1][2] / x[1][3]]))
    # [(('col_name', 'num_type'),(value, mean))...]
    col_num_mean_items = col_num_type_items.join(num_statistic.map(lambda x: (x[0], x[1][4])))
    result_dev = col_num_mean_items.aggregateByKey((0,), lambda local, x: (
        local[0] + (x[0] - x[1]) ** 2,), (lambda x, y: (x[0] + y[0])))
    result_std = result_dev.map(lambda x: (x[0], math.sqrt(x[1][0])))
    return num_statistic.join(result_std).map(lambda x: [x[0], [*x[1][0], x[1][1]]])


def generate_text_statistic(col_text_type_items: RDD) -> RDD:
    """
    :param col_text_type_items: columns of text type
        (('Wave_Number', 'text'),'ACTIVE EXPRESS CAR & LIMO 2')...)

    :return: ((col_name,'text'),(shortest,longest,avg_len))
    """
    def seqFunc(local, x):
        #     Not includes empty text
        if local[0] == '#':
            shortest = x
        else:
            shortest = x if len(x) < len(local[0]) else local[0]
        longest = x if len(x) > len(local[1]) else local[1]
        total_len = local[2] + len(x)
        count = local[3] + 1
        return (shortest, longest, total_len, count)

    combFunc = (
        lambda x, y: (
        x[0] if len(x[0]) < len(y[0]) else y[0], x[1] if len(x[1]) > len(y[1]) else y[1],
        x[2] + y[2], x[3] + y[3]))
    shortest_and_longest = col_text_type_items.aggregateByKey(('#', '', 0, 0), seqFunc, combFunc)
    statistic = shortest_and_longest.map(lambda x: (x[0], (x[1][0], x[1][1], x[1][2] / x[1][3])))
    return statistic
