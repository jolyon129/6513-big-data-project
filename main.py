import csv
from datetime import datetime
import json
import sys
import time
import utils

from pyspark import SparkContext
import pandas as pd

# file_path = sys.argv[1]
from pyspark.sql import SparkSession

OUTPUT = './output/meta.json'
LOG = './output/log'

spark = SparkSession \
    .builder \
    .appName("hw2sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

file_list = "file_list.csv"
file_directory = '/user/hm74/NYCOpenData/'
df = pd.read_csv(file_list, sep=' ', names=['path', 'size'])
res = []

start = time.time()

for idx, row in df.head(1).iterrows():
    print(row['path'])
    meta = utils.generate_meta(spark, row['path'])
    res.append(meta)

with open(OUTPUT, 'w') as outfile:
    json.dump(res, outfile, indent=2)

end = time.time()
with open(LOG, 'a+') as log:
    s1 = "Time elapsed: " + str(end - start) + " seconds"
    print(s1)
    log.write(
        datetime.now().strftime('%m-%d %H:%M:%S') + '\t' + s1)
