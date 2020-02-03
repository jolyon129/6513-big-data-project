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

    #inFile = "/user/jds405/NormProPublica.csv"
    #supp = "2000"
    #conf = "0.5" 
    #prot = "1.0"

    print ("Executing HW2SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")
    spark.sql("select count(*) from Pro_Publica").show()

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
               from Pro_Publica \
              group by attr, val \
             having count(*) >= "  + str(supp);
    F1 = spark.sql(query);
    F1.createOrReplaceTempView("F1")

    # diagnostic output
    #spark.sql("select count(*) from F1").show()
    #F1.show()

    # compute candidate itemsets of size 2, store in C2(attr1, val1, attr2, val2)
    query = "select A.attr as attr1, A.val as val1, B.attr as attr2, B.val as val2 \
               from F1 as A, F1 as B \
              where A.attr < B.attr"
    C2 = spark.sql(query)
    C2.createOrReplaceTempView("C2")

    # diagnostic output
    #spark.sql("select count(*) from C2").show()
    #C2.show()

    # compute frequent itemsets of size 2, store in F2(attr1, val1, attr2, val2)
    query = "select C2.attr1, C2.val1, C2.attr2, C2.val2, count(*) as supp \
               from C2, Pro_Publica as T1, Pro_Publica as T2 \
              where C2.attr1 = T1.attr and C2.val1 = T1.val \
                and C2.attr2 = T2.attr and C2.val2 = T2.val \
                and T1.uid = T2.uid \
              group by C2.attr1, C2.val1, C2.attr2, C2.val2 \
             having count(*) >= " + str(supp)
    F2 = spark.sql(query)
    F2.createOrReplaceTempView("F2")

    # diagnostic output
    #spark.sql("select count(*) from F2").show()
    #F2.show()

    # compute association rules of size 2 (1 item on the LHS, vdecile on the RHS)
    # R2(attr1, val1, attr2, val2, supp, conf)
    query = "select F2.attr1 as attr1, F2.val1 as val1, F2.attr2 as attr2, F2.val2 as val2, F2.supp, (F2.supp/F1.supp) as conf \
              from F1, F2 \
             where F2.attr1 = F1.attr and F2.val1 = F1.val \
               and F2.attr2 = 'vdecile' \
               and F2.supp / F1.supp >= " + str(conf)
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")

    # diagnostic output
    #spark.sql("select count(*) from R2").show()
    #R2.show()

    # compute candidate itemsets of size 3, store in C3(attr1, val1, attr2, val2, attr3, val3)
    query = "select A.attr1 as attr1, A.val1 as val1, A.attr2 as attr2, A.val2 as val2, B.attr2 as attr3, B.val2 as val3 \
           from F2 A, F2 B \
          where A.attr1 = B.attr1 and A.val1 = B.val1 \
            and A.attr2 != B.attr2 \
            and B.attr2 = 'vdecile'"
    C3 = spark.sql(query)
    C3.createOrReplaceTempView("C3")
    
    # diagnostic output
    #spark.sql("select count(*) from C3").show()
    #C3.show()

    # compute frequent itemsets of size 3, store in F3 (attr1, val1, attr2, val2, attr3, val3, supp)
    query = "select C3.attr1 as attr1, C3.val1 as val1, C3.attr2 as attr2, C3.val2 as val2, C3.attr3 as attr3, C3.val3 as val3, count(*) as supp \
               from C3, Pro_Publica T1, Pro_Publica T2, Pro_Publica T3 \
              where C3.attr1 = T1.attr and C3.val1 = T1.val \
                and C3.attr2 = T2.attr and C3.val2 = T2.val \
                and C3.attr3 = T3.attr and C3.val3 = T3.val \
                and T1.uid = T2.uid and T1.uid = T3.uid \
           group by C3.attr1, C3.val1, C3.attr2, C3.val2, C3.attr3, C3.val3 \
             having count(*) >= " + str(supp)

    F3 = spark.sql(query)
    F3.createOrReplaceTempView("F3")

    # diagnostic output
    #spark.sql("select count(*) from F3").show()
    #F3.show()


    # compute association rules of size 3 (2 items on the LHS, vdecile on the RHS)
    # R3(attr1, val1, attr2, val2, attr3, val3, supp, conf)
    query = "select F3.attr1, F3.val1, F3.attr2, F3.val2, F3.attr3, F3.val3,  \
                    F3.supp, (F3.supp/F2.supp) as conf \
               from F3, F2   \
              where F3.attr1 = F2.attr1 and F3.val1 = F2.val1 \
                and F3.attr2 = F2.attr2 and F3.val2 = F2.val2 \
                and F3.attr3 = 'vdecile' \
                and F3.supp / F2.supp >= "  +  str(conf)
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")

    # diagnostic output
    #spark.sql("select count(*) from R3").show()
    #R3.show()

    # compute PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot)
    query = " select R3.attr1, R3.val1, R3.attr2, R3.val2, R3.attr3, R3.val3,  \
                     R3.supp, R3.conf, (R3.conf / R2.conf) as prot \
                from R3, R2 \
               where R3.attr3 = R2.attr2 and R3.val3 = R2.val2 \
                 and R3.attr1 = R2.attr1 and R3.val1 = R2.val1 \
                 and R3.attr2 = 'race' \
                 and R3.conf / R2.conf >= " + str(prot)
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")

    # diagnostic output
    #spark.sql("select count(*) from PD_R3").show()
    #PD_R3.show()

    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")

    sc.stop()

