from pydwork.sqlplus import *

set_workspace('workspace')


#def yret():
#    """
#
#    """


with dbopen('space.db') as c:
    # c.drop('msize, equity')
    c.save('msize.csv')
    c.save('equity.csv')
    # c.show('select * from equity where fcode="A000810" and    equity != ""', n=1000)
    for rs in c.reel("""select * from equity where isnum(equity) and equity > 0 order by fcode, date
                     """, group='fcode, equity'):
        if len(rs) > 12:
            rs.show()
    c.show('select * from equity where fcode="A001070"')

    # now you can do many things

    # c.drop('datasetA')
    # c.run(
    #     """
    #     create table if not exists datasetA as
    #     select a.date, a.fcode,
    #     a.equity - a.pref as book, b.size, (a.equity - a.pref) / b.size as bm
    #     from equity as a
    #     left join msize as b
    #     on a.fcode = b.fcode
    #     and yyyymm(substr(a.date, 1, 6), 3) = substr(b.date, 1, 6) + 0
    #     where isnum(a.equity) and isnum(a.pref) and isnum(b.size)
    #     order by a.fcode, a.date
    #     """)
    #
    # c.describe('datasetA')
    # c.show('select * from datasetA where book < 0', n=1000)
