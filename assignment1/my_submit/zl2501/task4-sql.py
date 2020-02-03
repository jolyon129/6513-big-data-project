import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4-sql").config("spark.some.config.option", "some-value").getOrCreate()
park = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

park.createOrReplaceTempView("park")

result = spark.sql("""
    SELECT registration_in_NY_or_not, count(registration_in_NY_or_not) AS num 
    FROM 
        (SELECT 
        CASE 
            WHEN park.registration_state LIKE 'NY' THEN 'NY' 
            ELSE 'Other' 
        END AS registration_in_NY_or_not 
        FROM park) 
    GROUP BY registration_in_NY_or_not
""")

result.select(format_string('%s\t%d', result.registration_in_NY_or_not, result.num)).write.save("task4-sql.out", format = "text")
