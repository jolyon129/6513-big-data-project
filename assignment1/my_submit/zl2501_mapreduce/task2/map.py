#!/usr/bin/env python

import sys
from csv import reader

for line in reader(sys.stdin):
    print("%s\t%s" % (line[2], 1))
