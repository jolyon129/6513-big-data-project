import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()

park_lines = sc.textFile(sys.argv[1], 1)
park_lines = park_lines.mapPartitions(lambda x: reader(x))

open_lines = sc.textFile(sys.argv[2], 1)
open_lines = open_lines.mapPartitions(lambda x: reader(x))

park = park_lines.map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
open_violation = open_lines.map(lambda x: (x[0], ('', '', '', '')))

result = park.subtractByKey(open_violation)
output = result.map(
    lambda x: x[0] + '\t' + x[1][0] + ', ' + x[1][1] + ', ' + x[1][2] + ', ' + x[1][3])
output.saveAsTextFile("task1.out")
