import csv
from datetime import datetime
import json
import sys
import time
import utils
import rdd_util

from pyspark import SparkContext
import pandas as pd

# file_path = sys.argv[1]
from pyspark.sql import SparkSession
import logging

OUTPUT = './output/meta.json'
OUTPUT_DIR = './result/'
LOG = './log/process.log'
logging.basicConfig(filename='./log/spark_log.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    spark = SparkSession \
        .builder \
        .appName("project") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    file_list = "files_1.csv"
    df = pd.read_csv(file_list, sep=' ', names=['path', 'size'])
    res = []
    log_file = open(LOG, 'a+')
    sc = SparkContext.getOrCreate()
    for idx, row in df.head(3).iterrows():
        try:
            print(row['path'])
            start = time.time()
            file_name = row['path'].split('/')[-1]
            meta = rdd_util.generate_meta(sc, row['path'])
            result_fd = open(OUTPUT_DIR + file_name + '.json', 'w')
            json.dump(meta, result_fd, indent=2)
            end = time.time()
            s1 = file_name + " Time elapsed:" + str(end - start) + " seconds"
            log_file.writelines(
                datetime.now().strftime('%m-%d,%H:%M:%S') + ' ' + s1 + '\n')
        # Catch all exceptions
        except:
            logging.error("Exception occurred on " + file_name)
            continue

main()
