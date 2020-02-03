module load python/gnu/3.6.5
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

# call as: ./test.sh inFile supp conf prot outDir
# for example: ./test.sh /user/jds405/HW2data/PP_small.csv 400 0.4 1.2 $HOME/hw2/res1
# for example: ./test.sh /user/jds405/HW2data/PP_small.csv 500 0.55 1.1 $HOME/hw2/res2

/usr/bin/hadoop fs -rm -r "r2.out"
/usr/bin/hadoop fs -rm -r "r3.out"
/usr/bin/hadoop fs -rm -r "pd-r3.out"

rm -rf $5
mkdir $5

spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON hw2sql.py $1 $2 $3 $4

/usr/bin/hadoop fs -getmerge r2.out $5/out.tmp
cat $5/out.tmp | sort -n > $5/r2.out
rm $5/out.tmp

/usr/bin/hadoop fs -getmerge r3.out $5/out.tmp
cat $5/out.tmp | sort -n > $5/r3.out
rm $5/out.tmp

/usr/bin/hadoop fs -getmerge pd-r3.out $5/out.tmp
cat $5/out.tmp | sort -n > $5/pd-r3.out
rm $5/out.tmp

