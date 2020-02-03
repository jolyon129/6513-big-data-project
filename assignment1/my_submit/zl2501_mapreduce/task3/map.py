#!/usr/bin/env python

import sys
from csv import reader

for line in reader(sys.stdin):
    if len(line[2]) == 3:
        print('%s\t%s %s' % ((line[2]), float(line[12]), 1.00))
