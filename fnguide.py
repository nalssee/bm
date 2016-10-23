#!/usr/bin/env python3

# fnguide format to normal csv
import os
import tempfile
import sys
import csv
import locale

from itertools import zip_longest


locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def fn2normal(filename, cols):
    w = csv.writer(sys.stdout)
    n = len(cols)
    with open(filename) as f:
        for _ in range(8):
            f.readline()
        reader = csv.reader(f)
        fcodes = extract_fcodes(next(reader), n)

        for _ in range(5):
            next(reader)

        # write header
        w.writerow(['date', 'fcode', *cols])

        for line in csv.reader(f):
            # date = line[0].replace('-', '')
            date = parse_date(line[0])
            for s, vs in zip(fcodes, grouper(line[1:], n)):
                if s.strip() != '':
                    vs1 = (convert_string(v) for v in vs)
                    w.writerow([date, s, *vs1])


def parse_date(x):
    if '-' in x:
        # 1987-12-31
        return x.replace('-', '')
    else:
        # 31/12/87
        d, m, y = x.split('/')
        if y[0] >= '8':
            return '19' + y + m + d
        else:
            return '20' + y + m + d



def extract_fcodes(xs, n):
    result = []
    for ss in grouper(xs[1:], n):
        assert all_equal(ss), 'Number of columns incorrect'
        result.append(ss[0])
    return result


def all_equal(lst):
    return not lst or lst.count(lst[0]) == len(lst)


def convert_string(x):
    try:
        return locale.atoi(x)
    except ValueError:
        try:
            return locale.atof(x)
        except:
            return x


def iconv(infile, outfile):
    os.system('iconv -c -f euc-kr -t utf-8 ' + infile + ' > ' + outfile)


def main():
    filename = sys.argv[1]
    cols = sys.argv[2:]
    if len(cols) == 0:
        print("You need to specify the column name(s)")
        return

    try:
        fn2normal(filename, cols)
    except UnicodeDecodeError:
        with tempfile.NamedTemporaryFile(mode='w') as f:
            iconv(filename, f.name)
            f.seek(0)
            fn2normal(f.name, cols)


main()
