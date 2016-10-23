#!/usr/bin/env python3

import csv
import sys


def print_header(rs):
    columns = []
    first_2_columns = []
    for r in rs:
        line = next(r)
        first_2_columns.append(line[:2])
        columns += line[2:]
    assert all_equal(first_2_columns), first_2_columns
    w = csv.writer(sys.stdout)
    w.writerow(first_2_columns[0] + columns)


def all_equal(lst):
    return not lst or lst.count(lst[0]) == len(lst)


def main():
    fs = [open(f) for f in sys.argv[1:]]
    rs = [csv.reader(f) for f in fs]
    w = csv.writer(sys.stdout)

    for i, lines in enumerate(zip(*rs), 1):
        first2 = [line[:2] for line in lines]
        assert all_equal(first2), ("line: %s not matched" % i)
        rest = []
        for line in lines:
            rest += line[2:]
        w.writerow(rest)

    for f in fs:
        f.close()


main()
