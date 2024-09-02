#coding=utf-8
import sys
import json
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import models.entity_manager as entity_manager
import application as app


def get_result(batch_id, tbl, data):
    mkt = entity_manager.get_market(batch_id)
    dba = mkt.get_db('26_apollo')
    dbb = mkt.get_db('chsop')

    sql = 'SELECT max(id) FROM cleaner.modify_e_sales WHERE eid = {}'.format(mkt.eid)
    ver = dba.query_all(sql)[0][0] or 0

    ver += 1

    test_tbl = tbl.replace('_E', '_T')

    mkt.clone_etbl(dbb, tbl, test_tbl)

    data = json.loads(data)
    for i, v in enumerate(data):
        v = list(v)
        if v[0] == 'item_id':
            v[1] = '\''+v[1].replace(',', '\',\'')+'\''
        if v[2] == 'sales':
            v[2] = '{s} = {s} * {r}, price = {s} * {r} / num'.format(s=v[2], r=v[3])
        if v[2] == 'price':
            v[2] = '{s} = {s} * {r}, sales = {s} * {r} * num'.format(s=v[2], r=v[3])
        if v[2] == 'num':
            v[2] = '{s} = {s} * {r}, price = sales / IF({s} * {r} = 0, 1, {s} * {r})'.format(s=v[2], r=v[3])
        sql = '''
            ALTER TABLE {} UPDATE {} WHERE {} IN ({}) AND date >= '{}' AND date < '{}'
        '''.format(test_tbl, v[2], v[0], v[1], v[4], v[5])
        dbb.execute(sql)

        while not mkt.check_mutations_end(dbb, test_tbl):
            time.sleep(5)

        sql = '''
            INSERT INTO cleaner.modify_e_sales (eid, id, step, col, col_val, `set`, set_rate, smonth, emonth, tbl, test_tbl, createTime)
            VALUES ({}, {}, {}, %s, %s, %s, %s, %s, %s, '{}', '{}', now())
        '''.format(mkt.eid, ver, i+1, tbl, test_tbl)
        dba.execute(sql, (v[0],v[1],v[2],v[3],v[4],v[5],))

    return test_tbl
