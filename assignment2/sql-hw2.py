#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]
    conf = sys.argv[3]
    prot = sys.argv[4]

    print ("Executing HW2SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    # create dataframe
    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    # register the dataframe as sql view
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")
    # spark.sql("select count(*) from Pro_Publica").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
               from Pro_Publica \
              group by attr, val \
             having count(*) >= "  + str(supp);
    F1 = spark.sql(query);
    F1.createOrReplaceTempView("F1")

    # YOUR SparkSQL CODE GOES HERE
    # You may use any valid SQL query, and store the output in intermediate temporary views
    # Output must go into R2, R3 and PD_R3 as stated below.  Do not change the format of the output
    # on the last three lines.
    query = "select p1.attr as attr1, p1.val as val1, p2.attr as attr2, p2.val as val2, count(*) as supp \
            from Pro_Publica as p1 join Pro_Publica as p2 on p1.uid = p2.uid \
            where p2.attr='vdecile' and p2.val in (1,2) \
            group by p1.attr,p1.val,p2.attr,p2.val \
            having count(*) >= 500"
    temp = spark.sql(query)
    temp.createOrReplaceTempView("temp")
    query = "select t.attr1,t.val1,t.attr2,t.val2,t.supp as supp,t.supp/f.supp as conf \
                from temp as t join F1 as f on t.attr1 = f.attr and t.val1 = f.val and t.val2 in (1,2) \
                where t.attr1 != 'vdecile' and t.supp/f.supp >= 0.55"
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")

    # MORE OF YOUR SparkSQL CODE GOES HERE
    query = "select p1.attr as attr1, p1.val as val1, p2.attr as attr2, p2.val as val2, count(*) as supp \
            from Pro_Publica as p1 join Pro_Publica as p2 on p1.uid = p2.uid \
            where p2.attr != 'vdecile' and p1.attr < p2.attr \
            group by p1.attr,p1.val,p2.attr,p2.val \
            having count(*) >= 500"
    temp2 = spark.sql(query)
    temp2.createOrReplaceTempView("temp2")
    query = "select p1.attr as attr1, p1.val as val1, p2.attr as attr2, p2.val as val2, p3.attr as attr3, p3.val as val3, count(*) as supp \
            from Pro_Publica as p1 join Pro_Publica as p2 on p1.uid = p2.uid join Pro_Publica as p3 on p1.uid = p3.uid \
            where p2.attr != 'vdecile' and p1.attr < p2.attr and p3.attr='vdecile' and p3.val in (1,2)\
            group by p1.attr,p1.val,p2.attr,p2.val,p3.attr,p3.val \
            having count(*) >= 500"
    temp3 = spark.sql(query)
    temp3.createOrReplaceTempView("temp3")
    query = "select t3.attr1,t3.val1,t3.attr2,t3.val2,t3.attr3,t3.val3,t3.supp as supp,t3.supp/t2.supp as conf \
                from temp3 as t3 join temp2 as t2 on t3.attr1=t2.attr1 and t3.val1=t2.val1 and t3.attr2 = t2.attr2 and t3.val2 = t2.val2 and t3.val3 in (1,2) \
                where t3.supp/t2.supp >= 0.55"
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")

    # MORE OF YOUR SparkSQL CODE GOES HERE
    query = "select R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf,R3.conf/R2.conf as prot \
                from R3 join R2 on R3.attr1 = R2.attr1 and R3.val1 = R2.val1 and R3.val3 in (1,2) \
                where R3.attr2 = 'race' and R3.conf/R2.conf >= 1"
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")

    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")

    sc.stop()

