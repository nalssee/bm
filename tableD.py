from pydwork.sqlplus import *
import math

set_workspace('workspace')


def create_datasetD(c):
    def zz_datasetD():
        for rs in c.reel("""
        """):
            r.ivolatility_inv = 1 / r.ivolatility
            r.zerofreq_inv = 1 / r.zerofreq
            r.ln_volumne = math.log(r.volumne)
            r.ln_me = math.log(r.size)

            r.bm_ivolatility_inv = r.bm * r.ivolatility_inv
            r.bm_price = r.bm * r.price
            r.bm_ln_volumne = r.bm * r.ln_volumne
            r.bm_zerofreq_inv = r.bm * r.zerofreq_inv
            r.bm_analysts = r.bm * r.analysts
            r.bm_ln_me = r.bm * r.ln_me

            yield r

    c.save(zz_datasetD)

    cols1 = ['beta', 'bm']
    cols2 = ['beta', 'bm', 'bm_ivolatility_inv', 'ivolatility_inv']
    cols3 = ['beta', 'bm', 'bm_ivolatility_inv',
             'bm_price', 'bm_ln_volume', 'bm_zerofreq_inv', 'bm_analysts',
             'bm_ln_me', 'ivolatility_inv', 'price', 'ln_volumne',
             'zerofreq_inv', 'analysts', 'ln_me']

    def reg3():
        for rs in r.reel("""
        zz_datasetD
        """, group='yyyy'):
            result3 = rs.ols("""sret36 =             """)

def create_reg(c, cols):
    results = []
    for rs in c.reel("""
    datasetD
    """, group='yyyy'):
        res = rs.ols('sret = ' + ', '.join(cols))
        results.append(res)

    st.mean(for r in results)





with dbopen('space.db') as c:
    c.show('datasetD')
