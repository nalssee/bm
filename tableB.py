from pydwork.sqlplus import *
from pydwork.util import isnum
import statistics as st

set_workspace('workspace')


def create_ddata(c):
    # c.save('dret.csv')
    # c.save('dsize.csv')
    # c.save('dtvol.csv')

    # c.drop('ddata')
    c.run("""
        create table if not exists ddata as
        select a.date,
        cast(substr(a.date, 1, 6) as NUMERIC) as yyyymm,
        cast(substr(a.date, 1, 4) as NUMERIC) as yyyy,
        a.fcode,
        a.ret, b.size, c.tvol

        from dret as a
        left join dsize as b
        on a.fcode = b.fcode and a.date = b.date

        left join dtvol as c
        on a.fcode = c.fcode and a.date = c.date

        where isnum(a.ret) and isnum(b.size) and isnum(c.tvol)
    """)


# def create_drfee(c):
#     c.save('drfree.csv')
#     c.desc('drfree.csv')


# daily market ret
def create_dmkt(c):
    # tdays =  {}
    # for i, r in enumerate(c.rows('ddata where fcode="A005930" order by date')):
    #     tdays[r.date] = i
    #
    def zz_ddata01():
        for r in c.reel("ddata"):
            r.datei = tdays[r.date]
            yield r

    def zz_ddata02():
        for r in c.reel("ddata"):
            r.datei = tdays[r.date] + 1
            yield r

    # c.save(zz_ddata01)
    # c.save(zz_ddata02)


    # c.drop('ddata01')
    c.run("""
    create table if not exists ddata01 as
    select a.date, a.yyyymm, a.yyyy, a.fcode, a.size, b.size as size_p1,
    a.ret, a.tvol

    from zz_ddata01 as a
    left join zz_ddata02 as b
    on a.fcode = b.fcode and a.datei  = b.datei

    """)

    # c.drop('ddata02')
    c.run("""
    create table if not exists ddata02 as
    select a.date, a.yyyymm, a.yyyy, a.fcode, a.size, a.size_p1, a.ret, a.tvol,
    b.mkt, b.icode
    from ddata01 as a
    left join mdata as b
    on a.fcode = b.fcode and a.yyyymm = b.yyyymm
    """)


    def compute_mktret(rs):
        rs_all = rs.where(lambda r: isnum(r.size_p1))
        rs_kse = rs.where(lambda r: isnum(r.size_p1)  and r.mkt == 'KSE')

        n_all = len(rs_all)
        n_kse = len(rs_kse)

        if n_all > 0 and n_kse > 0:
            total_all = sum(r.size_p1 for r in rs_all)
            total_kse = sum(r.size_p1 for r in rs_kse)

            r = Row()
            r.date = rs[0].date

            r.n_all = n_all
            r.n_kse = n_kse

            r.eqret_all = sum(r.ret for r in rs_all) / n_all
            r.vwret_all = sum(r.ret * r.size_p1 / total_all for r in rs_all)

            r.eqret_kse = sum(r.ret for r in rs_kse) / n_kse
            r.vwret_kse = sum(r.ret * r.size_p1 / total_kse for r in rs_kse)

            yield r

    def dmkt():
        for rs in c.reel("""
        ddata02 order by date
        """, group="date"):
            yield from compute_mktret(rs)

    c.save(dmkt)



def create_ivol(c):
    def zz_ddata01():
        for r in c.reel("ddata02"):
            if r.tvol == 0:
                r.ret = 0
            if str(r.yyyymm)[4:6] <= '03':
                r.yyyy = int(r.yyyy) - 1
            yield r

    # c.save(zz_ddata01)

    # c.drop('zz_ddata02')
    c.run("""
    create table if not exists zz_ddata02 as
    select a.*, b.vwret_kse as vwret
    from ddata02 as a
    left join dmkt as b
    on a.date = b.date
    """)

    # tdays = [r.date for r in c.rows('ddata where fcode="A005930"')]

    def compute_ivol(rs):
        result = rs.ols('ret ~ vwret')
        r = Row()
        r.yyyy = rs[0].yyyy
        r.fcode = rs[0].fcode
        r.ivol = st.variance(result.resid)
        r.zerofreq = len(rs.where(lambda r: r.ret == 0))
        r.mkt = rs[0].mkt
        return r


    def ivol():
        for rs in c.reel("""
        select * from zz_ddata02
        where
        isnum(ret) and isnum(vwret)
        order by fcode, date
        """, group='fcode, yyyy'):
            if len(rs) >= 125:
                yield compute_ivol(rs)

    c.save(ivol)

def create_mdata02(c):
    # c.save('mprc.csv')
    # c.save('manal.csv')

    # c.drop('mdata02')
    c.run("""
    create table if not exists mdata02 as
    select a.*, b.aprc, c.nfollowers

    from mdata as a
    left join mprc as b
    on a.fcode = b.fcode and a.date = b.date

    left join manal as c
    on a.fcode = c.fcode and a.date = c.date
    """)


def create_datasetB(c):
    def zz_mdata03():
        for r in c.reel('mdata02'):
            if str(r.yyyymm)[4:6] <= '03':
                r.yyyy = r.yyyy - 1
            yield r

    # c.save(zz_mdata03)

    def zz_datasetB():
        for rs in c.reel("""
        zz_mdata03
        order by fcode, date
        """, group='fcode, yyyy'):
            # <----
            if len(rs) == 12:
                # the last month must be '03'
                assert(str(rs[-1].yyyymm)[4:6] == '03')
                r = Row()
                r0 = rs[0]
                r.fcode = r0.fcode
                r.yyyy = r0.yyyy
                r.volume = sum(r.tvol for r in rs)
                r.price = st.mean(r.aprc for r in rs)
                r.analysts = rs[-1].nfollowers
                yield r

    # c.save(zz_datasetB)

    c.drop('datasetB')

    c.run("""
    create table if not exists datasetB as
    select a.*, b.volume, b.price, b.analysts,
    c.ivol as ivolatility, c.zerofreq

    from datasetA as a

    left join zz_datasetB as b
    on a.fcode = b.fcode and a.yyyy = b.yyyy + 1

    left join ivol as c
    on a.fcode = c.fcode and a.yyyy = c.yyyy + 1

    """)

def print_tableB(c):

    rows  = c.df("""
    select ivolatility, price, volume, zerofreq, analysts
    from datasetB
    where yyyy >= 1981 and yyyy <= 2015
    and mkt="KSE" and icode != "K"
    """)
    rows.corr().to_csv('tableB.csv')




with dbopen('space.db') as c:
    # create_ddata(c)
    # create_dmkt(c)
    # create_ivol(c)
    # create_mdata02(c)

    # create_datasetB(c)
    print_tableB(c)
    # c.desc('datasetB')
