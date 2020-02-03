#!/usr/bin/env python
import sys
import os
from csv import reader

file_path = os.environ.get('mapreduce_map_input_file')
file_name = os.path.split(file_path)[-1]

for line in reader(sys.stdin):
    if file_name == 'open-violations.csv':
        print('%s\t<IGNORE>' % (line[0]))

    if file_name == 'parking-violations.csv':
        print('%s\t%s, %s, %s, %s' % (line[0], line[14], line[6], line[2], line[1]))
