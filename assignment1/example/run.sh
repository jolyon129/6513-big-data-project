#!/usr/bin/env

#hfs -rm -r example.out

hjs \
-D mapreduce.job.reduces=2 \
-file ~/bigdata/hw1/example/src \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/zl2501/book.txt \
-output /user/zl2501/example.out