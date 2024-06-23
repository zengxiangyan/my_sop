#coding=utf-8
import sys
import math
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import models.entity_manager as entity_manager
import models.kadis.api_adjust_coef as api_adjust_coef
import application as app

def history(bid, tip=''):
    mkt = entity_manager.get_market(bid)
    dba = mkt.get_db('chsop')

    sql = '''
        SELECT tip, etbl, tbl, smonth, emonth, cols, data
        FROM artificial.new_sales_log WHERE bid = {} {} ORDER BY created DESC LIMIT 1 BY tip
    '''.format(bid, 'AND tip = %(t)s' if tip != '' else '')
    ret = dba.query_all(sql, {'t':tip})

    return ret


def record(bid, tip, etbl, tbl, smonth, emonth, cols, data):
    mkt = entity_manager.get_market(bid)
    dba = mkt.get_db('chsop')

    sql = 'INSERT INTO artificial.new_sales_log (bid,tip,etbl,tbl,smonth,emonth,cols,data) VALUES'
    dba.execute(sql, [[bid,tip,etbl,tbl,smonth,emonth,cols,data]])


def clone_tbl(bid, etbl='', tbl='', smonth='', emonth=''):
    mkt = entity_manager.get_market(bid)
    dbname, e = mkt.get_plugin().get_e_tbl()
    dba = mkt.get_db(dbname)

    etbl = etbl or e
    tbl = tbl or etbl+'_newsales'

    if tbl == etbl:
        raise Exception('tbl error')

    mkt.clone_etbl(dba, etbl, tbl, where='WHERE pkey >= \'{}\' AND pkey < \'{}\''.format(smonth, emonth) if smonth != '' else '')
    sql = '''
        ALTER TABLE {}
        ADD COLUMN `old_price` Int32 CODEC(ZSTD(1)),
        ADD COLUMN `old_sales` Int64 CODEC(ZSTD(1)),
        ADD COLUMN `old_num` Int32 CODEC(ZSTD(1))
    '''.format(tbl)
    dba.execute(sql)

    sql = 'ALTER TABLE {} UPDATE `old_price` = `price`, `old_sales` = `sales`, `old_num` = `num` WHERE 1'.format(tbl)
    dba.execute(sql)

    while not mkt.check_mutations_end(dba, tbl):
        time.sleep(2)

    return tbl


def run(bid, tbl, cols, data):
    mkt = entity_manager.get_market(bid)
    dba = mkt.get_db('chsop')

    for v in data:
        s1 = cols.index('预期销额')
        n1 = cols.index('预期销量')
        p1 = cols.index('预期单价')
        s2 = cols.index('预期销额系数')
        n2 = cols.index('预期销量系数')
        p2 = cols.index('预期单价系数')

        c1, c2 = [], {}
        for i, c in enumerate(cols):
            if i in [s1, s2, n1, n2, p1, p2]:
                continue
            if v[i] == '':
                continue

            v[i] = int(v[i]) if c in ['sid', 'cid', 'brand'] else str(v[i])

            if c == '平台':
                if v[i] == '淘宝':
                    c1.append('(shop_type < 20 and shop_type > 10 )')
                c, v[i] = 'source', {'淘宝': 1,'天猫': 1,'京东': 2,'国美': 3,'聚美': 4,'考拉': 5,'苏宁': 6,'唯品会': 7,'拼多多': 8,'酒仙': 9,'途虎': 10}[v[i]]

            if c == 'month':
                c, v[i] = 'toYYYYMM(pkey)', int(v[i])

            # test
            if c == 'brand':
                continue

            c1.append('{c}=%({c})s'.format(c=c))
            c2[c] = v[i]

        c1 = ' AND '.join(c1) if len(c1) > 0 else '1'
        sql = '''
            SELECT toString(date), uuid2, old_sales/old_num, org_price, old_num, old_sales FROM {} WHERE {}
        '''.format(tbl, c1)
        rrr = dba.query_all(sql, c2)

        if len(rrr) == 0:
            continue

        param = [int(v[s1] or 0),int(v[n1] or 0),int(v[p1] or 0),int(v[s2] or 0),int(v[n2] or 0),int(v[p2] or 0),'price',rrr]
        rrr = api_adjust_coef.adjust_coef(param)

        uuids = [str(v[1]) for v in rrr]
        sales = [str(math.ceil(v[8])) for v in rrr]
        num   = [str(math.ceil(v[7])) for v in rrr]

        sql = '''
            ALTER TABLE {}
            UPDATE sales = transform(uuid2, [{u}], [{}], old_sales),
                   num = transform(uuid2, [{u}], [{}], old_num)
            WHERE {}
        '''.format(tbl, ','.join(sales), ','.join(num), c1, u=', '.join(uuids))
        dba.execute(sql, c2)

        while not mkt.check_mutations_end(dba, tbl):
            time.sleep(2)

    sql = 'ALTER TABLE {} UPDATE price = sales / num WHERE 1'.format(tbl)
    dba.execute(sql)

    while not mkt.check_mutations_end(dba, tbl):
        time.sleep(2)


def stat(bid, tbl, cols, data, rawdata=False):
    mkt = entity_manager.get_market(bid)
    dba = mkt.get_db('chsop')

    cc1, cc2, cc3 = [], [], {}
    for ii,v in enumerate(data):
        s1 = cols.index('预期销额')
        n1 = cols.index('预期销量')
        p1 = cols.index('预期单价')
        s2 = cols.index('预期销额系数')
        n2 = cols.index('预期销量系数')
        p2 = cols.index('预期单价系数')

        c1 = []
        for i, c in enumerate(cols):
            if i in [s1, s2, n1, n2, p1, p2]:
                continue
            if v[i] == '':
                continue

            v[i] = int(v[i]) if c in ['sid', 'cid', 'brand'] else str(v[i])

            if c == '平台':
                if v[i] == '淘宝':
                    c1.append('(shop_type < 20 and shop_type > 10 )')
                c, v[i] = 'source', {'淘宝': 1,'天猫': 1,'京东': 2,'国美': 3,'聚美': 4,'考拉': 5,'苏宁': 6,'唯品会': 7,'拼多多': 8,'酒仙': 9,'途虎': 10}[v[i]]

            if c == 'month':
                c, v[i] = 'toYYYYMM(pkey)', int(v[i])

            # test
            if c == 'brand':
                continue

            c1.append('{c}=%({c}{})s'.format(ii, c=c))
            cc1.append(c)
            cc3[c+str(ii)] = v[i]
        cc2.append('('+' AND '.join(c1)+')')

    cc1 = list(set(cc1))

    if rawdata:
        cols = mkt.get_cols(tbl, dba, ['sales','num','price','old_sales','old_num','old_price']).keys()
        sql = '''
            SELECT toString({}),
                sales, num, ROUND(sales/num, 3) p,
                old_sales, old_num, ROUND(old_sales/old_num, 3) op,
                ROUND(sales/old_sales, 3), ROUND(num/old_num, 3), ROUND(p/op, 3)
            FROM {} WHERE {}
        '''.format('),toString('.join(cols), tbl, ' OR '.join(cc2))
        ret = dba.query_all(sql, cc3)
        cols = list(cols) + ['sales', 'num', 'price', 'old_sales', 'old_num', 'old_price', 'sales_rate', 'num_rate', 'price_rate']
    else:
        sql = '''
            SELECT {p},
                sum(sales) s, sum(num) n, ROUND(s/n, 3) p,
                sum(old_sales) AS `os`, sum(old_num) AS `on`, ROUND(`os`/`on`, 3) AS `op`,
                ROUND(s/`os`, 3), ROUND(n/`on`, 3), ROUND(p/`op`, 3)
            FROM {} WHERE {} GROUP BY {p}
        '''.format(tbl, ' OR '.join(cc2), p=','.join(cc1))
        ret = dba.query_all(sql, cc3)
        cols = cc1 + ['sales', 'num', 'price', 'old_sales', 'old_num', 'old_price', 'sales_rate', 'num_rate', 'price_rate']

    return [{'title':c, 'width':100} for c in cols], ret