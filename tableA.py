"""
"""

from itertools import takewhile

from pydwork.sqlplus import *
from pydwork.util import isnum, nchunks
from pydwork.fi import PRows

from helpers import *
import statistics as st

from statsmodels.stats.weightstats import ttest_ind



set_workspace('workspace')


def create_mdata(c):
    """ Monthly frequency data size(unit: 1,000,000) and ret(%)
    """
    def fn_indcode(r):
        if r.mkt == '유가증권시장':
            r.mkt = 'KSE'
        elif r.mkt == '코스닥':
            r.mkt = 'KOSDAQ'
        else:
            r.mkt = ''
        r.fname = r.fname.strip()
        return r

    c.drop('zz_indcode, zz_mdata')

    c.save("indcode.csv", 'zz_indcode', fn=fn_indcode)
    c.save("mdata.csv", 'zz_mdata')

    c.drop('mdata')

    # tvol at least 40
    c.run("""
    create table if not exists mdata as
    select a.date, cast(substr(a.date, 1, 6) as NUMERIC) as yyyymm,
    cast(substr(a.date, 1, 4) as NUMERIC) as yyyy,

    a.fcode, a.mkt, a.fname, a.icode,
    b.ret, b.size, b.tvol, b.equity - b.pref as book

    from zz_indcode as a
    left join zz_mdata as b
    on a.fcode = b.fcode and a.date = b.date

    where
    a.mkt = 'KSE'
    and isnum(b.ret) and isnum(b.size) and isnum(b.tvol)
    and isnum(b.equity) and isnum(b.pref)
    and b.tvol > 0 and b.size > 0 and b.equity - b.pref > 0

    """)


def create_sret(c):
    def mark_pn(rs):
        for i, rs1 in enumerate(nchunks(rs, 10), 1):
            for r in rs1:
                r.pn = i

    # exclude financial firms
    def zz_sizeport():
        for rs in c.reel("""
        mdata where icode != "K" order by date, size
        """, group='date'):
            if str(rs[0].date)[4:6] == '03':
                mark_pn(rs)
            else:
                for r in rs:
                    r.pn = ''
            yield from rs

    def compute_yret(rs):
        for i, r in enumerate(rs):
            if str(r.yyyymm)[4:6] == '04' \
                and len(valid(rs[i:i+36], 'ret')) == 36:

                r.yyyy = str(r.yyyymm)[0:4]
                # avoid survival bias
                r.ret12 = bhr(rs[i:i+12])
                r.ret24 = bhr(rs[i:i+24])
                r.ret36 = bhr(rs[i:i+36])
                r.pn = rs[i - 1].pn
                yield r

    def zz_yret():
        tdays = gen_yyyymm(198001, 201512)
        for rs in c.reel("""
        zz_sizeport order by fcode, yyyymm
        """, group='fcode'):
            rs = fillin(rs, tdays)
            yield from compute_yret(rs)

    def compute_sret(rs):
        n = len(rs)

        sbench12 = sum(r.ret12 for r in rs) / n
        sbench24 = sum(r.ret24 for r in rs) / n
        sbench36 = sum(r.ret36 for r in rs) / n

        for r in rs:
            r.sbench12 = sbench12
            r.sbench24 = sbench24
            r.sbench36 = sbench36

            r.sret12 = r.ret12 - sbench12
            r.sret24 = r.ret24 - sbench24
            r.sret36 = r.ret36 - sbench36
            yield r

    def sret():
        for rs in c.reel("""
        zz_yret where isnum(ret12) and isnum(pn)
        order by yyyy, pn
        """, group='yyyy, pn'):
            yield from compute_sret(rs)

    c.save(zz_sizeport)
    c.save(zz_yret)
    c.save(sret)


def create_mkt(c):
    """
    market portfolio (eqret, vwret)
    """

    c.drop('zz_mdata01')
    c.run("""
    create table if not exists zz_mdata01 as
    select a.*, yyyymm(yyyymm, 1) as yyyymm_n1
    from mdata as a
    """)

    c.drop('zz_mdata02')
    c.run("""
    create table if not exists zz_mdata02 as
    select a.*, b.size as size_p1
    from zz_mdata01 as a
    left join zz_mdata01 as b
    on a.fcode = b.fcode and a.yyyymm = b.yyyymm_n1
    """)

    def compute_mktret(rs):
        rs = rs.where(lambda r: isnum(r.size_p1))

        n = len(rs)

        if n > 0:
            total = sum(r.size_p1 for r in rs)
            r = Row()
            r.yyyymm = rs[0].yyyymm
            r.n = n
            r.vwret = sum(r.ret * r.size_p1 / total for r in rs)

            yield r

    def mkt():
        for rs in c.reel("""
        zz_mdata02 order by yyyymm
        """, group="yyyymm"):
            yield from compute_mktret(rs)

    c.save(mkt)


# <--
def create_beta(c):
    c.drop('zz_mdata01')

    c.run("""
    create table if not exists zz_mdata01 as
    select a.*, b.vwret
    from mdata as a
    left join mkt as b
    on a.yyyymm = b.yyyymm
    """)

    def compute_beta(rs):
        for i, r in enumerate(rs):
            # from april
            if str(r.yyyymm)[4:6] == '04':
                rs0 = Rows(takewhile(lambda r: isnum(r.ret), rs[i:i + 36]))
                # <----
                if len(rs0) >= 36:
                    r0 = Row()
                    r0.yyyy = str(r.yyyymm)[0:4]
                    r0.fcode = r.fcode
                    r0.fname = r.fname
                    r0.mkt = r.mkt
                    r0.icode = r.icode

                    r0.n = len(rs0)
                    reg = rs0.ols('ret ~ vwret')
                    r0.beta = reg.params[1]

                    yield r0

    def beta():
        tdays = gen_yyyymm(198001, 201512)
        for rs in c.reel("""
        zz_mdata01
        where isnum(ret) and isnum(vwret)
        order by fcode, yyyymm
        """, group='fcode'):
            rs = fillin(rs, tdays)
            yield from compute_beta(rs)

    c.save(beta)


def create_bm(c):
    def zz_book():
        for rs in c.reel("""
        mdata order by fcode, yyyymm
        """, group='fcode, book'):
            r0 = rs[-1]
            yield r0

    def zz_book01():
        for rs in Rows(zz_book()).group('fcode, yyyy'):
            yield rs[-1]

    c.save(zz_book01)

    c.drop('bm')
    c.run("""
    create table if not exists bm as
    select a.*, b.book / (a.size * 1000.0) as bm
    from mdata as a
    left join zz_book01 as b
    on a.fcode = b.fcode and a.yyyy = b.yyyy + 1
    where substr(a.yyyymm, 5, 2) = '03'
    and a.icode != 'K'
    """)


def create_datasetA(c):
    c.drop('datasetA')
    c.run("""
    create table if not exists datasetA as
    select
    a.yyyy, a.fcode, a.fname, a.icode, a.mkt,  a.bm, a.size,
    b.beta,
    c.ret12, c.ret24, c.ret36, c.sret12, c.sret24, c.sret36

    from bm as a
    left join beta as b
    on a.fcode = b.fcode and a.yyyy = b.yyyy

    left join sret as c
    on a.fcode = c.fcode and a.yyyy = c.yyyy
    """)

def print_tableA(c, beg=1981, end=2015):
    rs =  c.rows("""
        datasetA
        where yyyy >= %s and  yyyy <= %s
        and isnum(bm) and isnum(size)
        and isnum(beta)
        order by yyyy, bm
    """ % (beg, end))

    prs = PRows(rs, 'yyyy')
    prs.pn('bm', 5)

    for var in ['bm', 'size', 'beta', 'ret12', 'ret24', 'ret36', 'sret12', 'sret24', 'sret36']:
        prs.pavg(var).pat().csv()


# def print_tableA(c, beg=1981, end=2015):
#     rows =  c.rows("""
#         datasetA
#         where yyyy >= %s and  yyyy <= %s
#         and isnum(bm) and isnum(size)
#         and isnum(beta)
#         order by yyyy, bm
#     """ % (beg, end))
#
#     for rs in rows.group('yyyy'):
#         for i, rs1 in enumerate(nchunks(rs, 5), 1):
#             for r in rs1:
#                 r.pn = i
#
#     chunks = []
#     for i in range(1, 6):
#         chunks.append(rows.where(lambda r: r.pn == i))
#
#     print('var,q1,q2,q3,q4,q5, all, q5 - q1')
#     for var in ['bm', 'size', 'beta', 'ret12', 'ret24', 'ret36', 'sret12', 'sret24', 'sret36']:
#
#         print(var, end=',')
#
#         for chunk in chunks:
#             print(round(st.mean(chunk[var]), 3), end=',')
#         print(round(st.mean(rows[var]), 3), end=',')
#         q5 = chunks[-1][var]
#         q1 = chunks[0][var]
#         print(star(st.mean(q5) - st.mean(q1), ttest_ind(q5, q1)[1]), end=',')
#
#         print()
#     print('n obs', end=',')
#     print(len(rows))
#


with dbopen('space.db') as c:
    # create_mdata(c)
    # create_mkt(c)
    # create_beta(c)

    # create_sret(c)
    # create_bm(c)
    # c.save('mdata.csv')

    # create_datasetA(c)
    # c.desc('datasetA')
    # c.desc('bm')
    print_tableA(c)
    # c.show('datasetA')


    # c.desc('beta where mkt="KSE" and icode!="K"')
    # c.desc('datasetA where mkt="KSE"')

    # pass

    # c.desc('beta  ')
    # c.desc('datasetA where mkt="KSE"')
    # c.show('mdata where fcode="A005930"')
