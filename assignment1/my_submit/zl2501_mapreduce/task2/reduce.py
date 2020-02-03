#!/usr/bin/env python

import sys

current_key = None
current_value = 0
key = None

for line in sys.stdin:
    line.strip()
    key, value = line.split('\t')
    try:
        value = int(value)
    except ValueError:
        continue
    if current_key == key:
        current_value += value
    else:
        if current_key:
            print('%s\t%s' % (current_key, current_value))
        current_key = key
        current_value = value

if current_key == key:
    print('%s\t%s' % (current_key, current_value))

