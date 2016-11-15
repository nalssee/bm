from pydwork.sqlplus import *
from pydwork.util import nchunks
import statistics as st
from helpers import *
from statsmodels.stats.weightstats import ttest_ind


set_workspace('workspace')

def create_datasetC(c):

    def assign_pn(rs, cname, n=5):
        for r in rs:
            r['pn_' + cname] = ''
        for i, rs1 in enumerate(nchunks(rs.num(cname).order(cname), n), 1):
            for r in rs1:
                r['pn_' + cname] = i

    def datasetC():
        for rs in c.reel("""
        select * from datasetB
        where mkt='KSE' and icode != 'K' and isnum(bm)
        and isnum(sret36)
        order by yyyy
        """, group='yyyy'):

            assign_pn(rs, 'bm', 5)
            assign_pn(rs, 'ivolatility')
            assign_pn(rs, 'volume')
            assign_pn(rs, 'price')
            assign_pn(rs, 'size')
            assign_pn(rs, 'analysts')
            assign_pn(rs, 'zerofreq')
            yield from rs

    c.save(datasetC)


def print_tableC(c):
    rs = c.rows("""
    datasetC
    where yyyy >= 1986 and yyyy <= 2015
    and isnum(bm) and mkt = "KSE" and icode != "K"
    """)

    cols = ['ivolatility', 'price', 'zerofreq', 'analysts', 'size']
    print(',' + ','.join(['p' + str(x) for x in range(1, 6)]), 'p5-p1')
    for col in cols:
        print(col, end=',')
        diffs = []
        for pn in range(1, 6):
            rs1 = rs.num('pn_' + col).where(lambda r: r['pn_' + col] == pn)
            diffs1 = []
            for rs2 in rs1.order('yyyy').group('yyyy'):
                q5 = rs2.num('pn_bm').where(lambda r: r['pn_bm'] == 5)['sret36']
                q1 = rs2.num('pn_bm').where(lambda r: r['pn_bm'] == 1)['sret36']
                diffs1.append(st.mean(q5) - st.mean(q1))
            diffs.append(diffs1)
            print(round(st.mean(diffs1), 3), end=',')
        ret = st.mean(diffs[-1]) - st.mean(diffs[0])
        xs = [a - b for a, b in zip(diffs[-1], diffs[0])]

        print(star(ret, ttest_ind(xs, [0] * len(xs))[1]), end='')

        print()


with dbopen('space.db') as c:
    create_datasetC(c)
    # c.desc('datasetC')
    print_tableC(c)
    # c.desc('select sret36 from datasetC')
