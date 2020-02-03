import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))

counts = lines.map(lambda x: (('NY' if x[16] == 'NY' else 'Other'), 1)) \
    .reduceByKey(lambda x, y: x + y)
res = counts.map(lambda x: x[0] + '\t' + str(x[1]))
res.saveAsTextFile("task4.out")
