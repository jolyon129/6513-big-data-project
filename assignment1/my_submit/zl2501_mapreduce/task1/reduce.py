#!/usr/bin/env python

import sys

current_key = current_value = None
key = None
for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    if key == current_key:
        current_value += value
    else:
        if current_key:
            if '<IGNORE>' not in current_value:
                print('%s\t%s' % (current_key, current_value))

        current_key = key
        current_value = value

if key == current_key:
    if '<IGNORE>' not in current_value:
        print('%s\t%s' % (current_key, current_value))

