import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))

counts = lines.map(lambda x: ((x[14], x[16]), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .takeOrdered(20, key=lambda x: (-x[1], x[0][0]))

ans = sc.parallelize(counts).map(lambda x: (x[0][0], x[0][1], x[1]))

output = ans.map(lambda x: x[0] + ', ' + x[1] + '\t' + str(x[2]))
output.saveAsTextFile("task6.out")
