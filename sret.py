"""
size-adjusted returns

Too many extreme values
"""

from itertools import takewhile

from pydwork.sqlplus import *
from pydwork.util import nchunks, yyyymm, isnum

set_workspace('workspace')


def mark_pn(rs):
    for i, rs1 in enumerate(nchunks(rs, 10), 1):
        for r in rs1:
            r.pn = i
            r.sbench = sum(r.ret for r in rs1) / len(rs1)



def compute_yret(rs):
    for i, r in enumerate(rs):
        if str(r.yyyymm)[4:6] == '04':
            r.ret12 = bhr(valid(rs[i:i+12], 'ret'))
            r.ret24 = bhr(valid(rs[i:i+24], 'ret'))
            r.ret36 = bhr(valid(rs[i:i+36], 'ret'))
            r.yyyy = str(r.yyyymm)[0:4]
            r.pn = rs[i - 1].pn
            yield r


def compute_sret(rs):
    n = len(rs)
    for r in rs:
        r.sret12 = r.ret12 - (sum(r.ret12 for r in rs) / n)
        r.sret24 = r.ret24 - (sum(r.ret24 for r in rs) / n)
        r.sret36 = r.ret36 - (sum(r.ret36 for r in rs) / n)
        yield r



with dbopen('space.db') as c:
    def sizeport():
        for rs in c.reel("""
        select * from mdata
        order by date, size
        """, group='date'):
            if str(rs[0].date)[4:6] == '03':
                mark_pn(rs)
            else:
                for r in rs:
                    r.pn = ''
            yield from rs

    # c.drop('sizeport')
    c.save(sizeport)

    def yret():
        tdays = gen_yyyymm(198001, 201512)
        for rs in c.reel("""
        select * from sizeport
        order by fcode, yyyymm
        """, group='fcode'):
            rs = fillin(rs, tdays)
            yield from compute_yret(rs)

    # c.drop('yret')
    c.save(yret)
    # c.show('yret')
    # c.describe('yret')

    def sret():
        for rs in c.reel("""
        select * from yret
        order by yyyymm, pn
        """, group='yyyy, pn'):
            yield from compute_sret(rs)

    # c.drop('sret')
    c.save(sret)
    # c.show('sret')
    # c.show("""
    # select * from mdata where ret > 200
    # """, n=10000)
    # c.describe('mdata')
    # c.describe('sret')
    # c.show('select * from sret where yyyymm=200304 and pn =1 ', n=1000)
    # c.describe('select * from sret where ret > 50')
    # c.describe('sret')
    # c.show("""
    # select * from sret where ret12 <= -0.999
    #
    # """, n=10000)
    c.show('select * from mdata where fcode="A001780"', n=10000)
