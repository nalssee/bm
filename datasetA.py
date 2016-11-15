from pydwork.sqlplus import *

set_workspace('workspace')

# percentage ret to original ret
def bhr(rs):
    result = 1
    for r in rs:
        result *= 1 + (r.ret / 100)
    return result - 1


with dbopen('space.db') as c:

    # c.drop('msize, equity')
    c.save('msize.csv')
    c.save('equity.csv')

    def equity1():
        for rs in c.reel("""
            select * from equity
            where isnum(equity) and isnum(pref) and (equity - pref) >= 0
            order by fcode, date
        """, group='fcode, equity'):
            yield rs[-1]

    def bval():
        "some firms change fiscal year end, so the latest one survives"
        for rs in Rows(equity1()).group(lambda r: [r.fcode, str(r.date)[:4]]):
            r = Row()
            r0 = rs[-1]
            r.date = r0.date
            r.fcode = r0.fcode
            r.bval = r0.equity - r0.pref
            yield r

    # c.drop('bval')
    c.save(bval)
    # c.describe('bval')

    datasetA0 = """
        create table if not exists datasetA0 as
        select a.date, substr(a.date, 1, 4) as yyyyy, a.fcode, a.bval, b.size,
        (a.bval + 0.0) / (b.size * 1000) as bm
        from bval as a
        left join msize as b
        on a.fcode = b.fcode
        and substr(a.date, 1, 4) + 1 = substr(b.date, 1, 4) + 0

        where substr(b.date, 5, 2) = '03'
        and isnum(a.bval) and isnum(b.size) and a.bval >= 0 and b.size > 0
        order by a.fcode, a.date
    """
    # c.drop('datasetA0')
    c.run(datasetA0)

    # c.drop('datasetA')
    # c.save(c.rows('datasetA0').truncate('bm', 0.005), 'datasetA')

    # c.show('datasetA0')


    # make (s)ret12, (s)ret24, (s)ret36
    # c.drop('mret, msize')
    c.save('mret.csv')
    c.save('msize.csv')

    # c.drop('mret0')
    c.run("""
    create table if not exists mret0 as
    select date, fcode, ret, date,
    case
    when substr(date, 5, 2) <= '03' then substr(date, 1, 4) - 1
    else substr(date, 1, 4) + 0
    end as yyyy
    from mret
    where isnum(ret)
    order by fcode, date
    """)

    c.show('mret0')

    def yret0():
        for rs in c.reel('mret0', group='fcode, yyyy'):
            r = Row()
            r.fcode = rs[0].fcode
            r.yyyy = rs[0].yyyy
            r.ret12 = bhr(rs)
            r.n = len(rs)
            yield r

    # c.drop('yret0')
    c.save(yret0)

    yret1 = """
    create table if not exists yret1 as
    select a.yyyy, a.fcode, a.ret12, b.ret12 as ret12_n1, c.ret12 as ret12_n2,
    a.n, b.n as n1, c.n as n2
    from yret0 as a
    left join yret0 as b
    on a.fcode = b.fcode and a.yyyy + 1 = b.yyyy + 0
    left join yret0 as c
    on a.fcode = c.fcode and a.yyyy + 2 = c.yyyy + 0
    """
    # c.drop('yret1')
    c.run(yret1)
    #c.show('yret1')
    # c.describe('yret1')


    # ret0 = """
    #     create table if not exists ret0 as
    #     select a.date, a.fcode, a.ret, b.size,
    #     case
    #         when substr(a.date, 5, 2) <= '03' then substr(a.date, 1, 4) - 1
    #         else substr(a.date, 1, 4) + 0
    #     end as yyyy
    #     from mret as a
    #     left join msize as b
    #     on a.fcode = b.fcode and a.date = b.date
    #     where isnum(a.ret) and isnum(b.size)
    #     order by a.fcode and a.date
    # """
    #
    # # c.drop('ret0')
    # # c.run(ret0)
    # c.show('select * from ret0 where date = 20000331', n=100)
    # # c.describe('ret0')
    #
    # def yret0():
    #     for rs in c.reel("""
    #         select * from ret0
    #         order by fcode, date
    #     """, group='fcode'):
    #         pass
