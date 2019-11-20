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

OUTPUT = './output/meta.json'
LOG = './output/log'

spark = SparkSession \
    .builder \
    .appName("project") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

file_list = "file_list.csv"
file_directory = '/user/hm74/NYCOpenData/'
df = pd.read_csv(file_list, sep=' ', names=['path', 'size'])
res = []

log_file = open(LOG, 'a+')

sc = SparkContext.getOrCreate()

for idx, row in df.head(3).iterrows():
    print(row['path'])
    start = time.time()
    file_name = row['path'].split('/')[-1]
    meta = rdd_util.generate_meta(sc, row['path'])
    end = time.time()
    s1 = file_name + " Time elapsed:" + str(end - start) + " seconds"
    log_file.writelines(
        datetime.now().strftime('%m-%d,%H:%M:%S') + ' ' + s1+'\n')
    res.append(meta)

with open(OUTPUT, 'w') as outfile:
    json.dump(res, outfile, indent=2)

end = time.time()



