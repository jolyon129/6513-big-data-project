#!/usr/bin/env python


import sys

current_word = None
current_count = 0
current_total_due = 0

for line in sys.stdin:
    line.strip()
    word, value = line.split('\t', 1)
    due, count = value.split(' ')
    try:
        count = float(count)
        due = float(due)
    except ValueError:
        continue
    if current_word == word:
        current_total_due += due
        current_count += count
    else:
        if current_word:
            average_due = current_total_due/current_count
            print('%s\t%.2f, %.2f' %
                  (current_word, current_total_due, average_due))
        current_word = word
        current_count = count
        current_total_due = due
if current_word == word:
    average_due = current_total_due/current_count
    print('%s\t%.2f, %.2f' % (current_word, current_total_due, average_due))

