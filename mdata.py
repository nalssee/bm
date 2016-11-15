"""
monthly-frequency data
"""

from pydwork.sqlplus import *

set_workspace('workspace')

with dbopen('space.db') as c:
    c.drop('mret, msize')
    c.save('mret.csv')
    c.save('msize.csv')

    mdata = """
    create table if not exists mdata as
    select a.date, substr(a.date, 1, 6) as yyyymm, a.fcode, a.ret, b.size
    from mret as a
    left join msize as b
    on a.fcode = b.fcode and a.date = b.date
    where isnum(a.ret) and isnum(b.size)
    and b.size > 0
    """

    c.drop('mdata')
    c.run(mdata)
    c.show('mdata')
    c.describe('mdata')
