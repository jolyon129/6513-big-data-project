#!/bin/sh

FILE=main.py
FILE1=rdd_util.py
OUTPUT=project_output

export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'


#hadoop fs -rm -r ${OUTPUT}.out


spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON --py-files ${FILE1} ${FILE}

#hfs -get ${OUTPUT}.out
#
#hfs -getmerge ${OUTPUT}.out ${OUTPUT}.txt