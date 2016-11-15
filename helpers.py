"""
"""
from pydwork.sqlplus import *
from pydwork.util import yyyymm, isnum
from itertools import takewhile

# There mustn't be any duplicates in rs
def fillin(rs, tdays):
    # empty value: ''

    # make a dictionary
    cols = rs[0].columns

    rs_dict = {}
    for r in rs:
        rs_dict[r.yyyymm] = r
    result = []
    for tday in tdays:
        found = rs_dict.get(tday, False)
        if found:
            result.append(found)
        else:
            # if there's no data for the date, make a dummy for it
            r = Row()
            for c in cols:
                # fill with empty string
                r[c] = ''
            result.append(r)
    # now you have full set of rows
    return result


def gen_yyyymm(start, end):
    tdays = []
    while start <= end:
        tdays.append(start)
        start = yyyymm(start, 1)
    return tdays


def bhr(rs):
    result = 1
    for r in rs:
        result *= 1 + r.ret / 100
    return result - 1


def valid(rs, col):
    return Rows(takewhile(lambda r: isnum(r[col]), rs))


def star(val, pval):
    "put stars according to p-value"
    if pval < 0.001:
        return str(round(val, 3)) + '***'
    elif pval < 0.01:
        return str(round(val, 3)) + '**'
    elif pval < 0.05:
        return str(round(val, 3)) + '*'
    else:
        return str(round(val, 3))
