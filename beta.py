"""
beta for 36 months maximum
"""

from pydwork.sqlplus import *
from pydwork.util import yyyymm, isnum
from itertools import takewhile

set_workspace('workspace')


def gen_yyyymm(start, end):
    tdays = []
    while start <= end:
        tdays.append(start)
        start = yyyymm(start, 1)
    return tdays


def compute_beta(rs):
    for i, r in enumerate(rs):
        if str(r.yyyymm)[4:6] == '04':
            rs0 = Rows(takewhile(lambda r: isnum(r.ret), rs[i:i + 36]))
            if len(rs0) >= 12:
                r0 = Row()
                r0.yyyy = str(r.yyyymm)[0:4]
                r0.fcode = r.fcode
                r0.n = len(rs0)
                reg = rs0.ols('ret ~ vwret')
                r0.beta = reg.params[1]
                yield r0


# There mustn't be any duplicates in rs
def fillin(rs, tdays):
    # empty value: ''

    for r in rs:
        r.yyyymm = int(str(r.date)[0:6])

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


with dbopen('space.db') as c:
    # c.drop('mret0')
    c.run("""
    create table if not exists mret0 as
    select a.date, a.fcode, a.ret, b.size
    from mret as a
    left join msize as b
    on a.fcode = b.fcode and a.date = b.date
    where isnum(a.ret) and isnum(b.size)
    and b.size > 0
    """)


    def mret1():
        def mkt(rs):
            total = sum(r.size for r in rs)
            n = len(rs)
            ewret = sum(r.ret for r in rs) / n
            vwret = sum(r.ret * r.size / total for r in rs)
            for r in rs:
                r.ewret = ewret
                r.vwret = vwret
            yield from rs

        for rs in c.reel("""
        select * from mret0
        order by date
        """, group='date'):
            yield from mkt(rs)

    # c.drop('mret1')
    c.save(mret1)
    # c.describe('mret1')

    def beta():

        tdays = gen_yyyymm(198001, 201512)

        for rs in c.reel("""
        select * from mret1
        order by fcode, date
        """, group='fcode'):
            rs = fillin(rs, tdays)
            yield from compute_beta(rs)

    # c.drop('beta')
    c.save(beta)
    # c.show('beta')
    # c.describe('beta')
    # c.show('beta')

# computing realized return from yields
# def mrf():
#     rs = c.rows('select * from mrfree where isnum(rf) order by date')
#     for r1, r2 in zip(rs, rs[1:]):
#         a = r1.rf / 100
#         b = r2.rf / 100
#         pt = 1 / ((1 + b) ** (11 / 12))
#         pt_1 = 1 / (1 + a)
#
#         r = Row()
#         r.date = r2.date
#         r.rf = (pt - pt_1) / pt_1
#         yield r
