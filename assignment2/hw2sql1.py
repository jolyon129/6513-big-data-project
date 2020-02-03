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
from pyspark.sql.functions import format_string, format_number

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

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
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
    # F1.show(n=50)

    # YOUR SparkSQL CODE GOES HERE
    # You may use any valid SQL query, and store the output in intermediate temporary views
    # Output must go into R2, R3 and PD_R3 as stated below.  Do not change the format of the output
    # on the last three lines.

    # Compute R2, as described in the homework specification
    # R2(attr1, val1, attr2, val2, supp, conf)
    query = "select P1.attr as attr1, P1.val as val1, P2.attr as attr2, P2.val as val2, count(*) as supp, ROUND(count(*)/F1.supp,2) as conf \
                from Pro_Publica as P1 join Pro_Publica as P2 on P1.uid = P2.uid and P1.attr != P2.attr, F1 \
                where P1.attr = F1.attr \
                and P1.val = F1.val \
                and P2.attr = 'vdecile' \
                group by P1.attr, P1.val, P2.attr, P2.val, F1.supp\
                having supp >=" + str(supp) + " \
                and conf >=" + str(conf) + " \
                order by attr1, val1"
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")
    R2.show()
    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute R3, as described in the homework specification
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)
    # compute frequent itemsets of size 2, store in F2(attr1, val1, attr2, val2)
    query = """select P1.attr as attr1, P1.val as val1, P2.attr as attr2, P2.val as val2, count(*) as supp
                from Pro_Publica as P1 join Pro_Publica as P2 on P1.uid = P2.uid and P1.attr < P2.attr
                where P1.attr != 'vdecile'
                and P2.attr != 'vdecile'
                group by attr1, val1, attr2, val2
                having count(*) >=""" + str(supp) + " \
                order by attr1, val1, attr2"
    F2 = spark.sql(query);
    F2.createOrReplaceTempView("F2")
    # F2.show()
    query = """ select attr1, val1, attr2, val2, P3.attr as attr3, P3.val as val3, count(*) as supp, ROUND(count(*)/F2.supp, 2) as conf
                from F2, Pro_Publica P1 
                join Pro_Publica P2 on P1.uid = P2.uid and P1.attr < P2.attr
                join Pro_Publica P3 on P2.uid = P3.uid
                where P1.attr = F2.attr1 and P1.val = F2.val1
                and P2.attr = F2.attr2 and P2.val = F2.val2
                and P3.attr = 'vdecile'
                group by attr1, val1, attr2, val2, attr3, val3, F2.supp
                having count(*) >= """ + str(supp) + " \
                and conf >= " + str(conf) + " \
                order by attr1, val1, attr2"
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")
    R3.show()
    # MORE OF YOUR SparkSQL CODE GOES HERE

    # Compute PD_R3, as described in the homework specification
    # PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot)
    query = "select DISTINCT R3.attr1, R3.val1, R3.attr2, R3.val2, R3.attr3, R3.val3, R3.supp, R3.conf, Round(R3.conf/R2.conf, 2) as prot \
            from R2, R3 \
            where R3.attr1 = R2.attr1 \
            and R3.attr2 = 'race' \
            and R3.val3 = R2.val2" + " \
            having prot >= " + str(prot) + " \
            order by R2.attr1, R2.val1"
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")
    PD_R3.show()
    # R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    # R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    # PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")

    # sc.stop()

