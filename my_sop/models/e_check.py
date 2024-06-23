#coding=utf-8
import re
import sys
import json
import math
import time
import random
import requests
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import models.entity_manager as entity_manager
import application as app

def get_other_cate(self):
    return ['其它']


def stat_sp1cid(self, ver, sp, tbl=''):
    dba, etbl = self.get_plugin().get_e_tbl()
    dba = self.get_db(dba)

    tbl = tbl or etbl

    if self.get_entity()['use_all_table']:
        _, atbl = self.get_plugin().get_all_tbl()
        sql = 'SELECT toYYYYMM(min(date)), toYYYYMM(max(date)) FROM {}ver{}'.format(atbl, ver)
    else:
        _, atbl = self.get_plugin().get_d_tbl()
        sql = 'SELECT toYYYYMM(min(date)), toYYYYMM(max(date)) FROM {} WHERE c_out_ver = {}'.format(atbl, ver)
    ret = dba.query_all(sql)

    data = []
    for sps, rate1, cid1, rate2, cid2, in sp:
        sql = '''
            WITH toYYYYMM(date) AS mm, `clean_props.value`[indexOf(`clean_props.name`, '子品类')] AS sp
            SELECT source, cid, sum(sales), sum(num), count(*) FROM {} WHERE mm >= {} AND mm <= {} AND cid IN ({}) GROUP BY source, cid
        '''.format(tbl, ret[0][0], ret[0][1], cid1)
        rr1 = dba.query_all(sql)
        rr1 = {(v[0],v[1]):[v[2],v[3],v[4]] for v in rr1}

        sql = '''
            WITH toYYYYMM(date) AS mm, `clean_props.value`[indexOf(`clean_props.name`, '子品类')] AS sp
            SELECT source, cid, sum(sales), sum(num), count(*) FROM {} WHERE mm >= {} AND mm <= {} AND sp IN ('{}') AND cid IN ({}) GROUP BY source, cid
        '''.format(tbl, ret[0][0], ret[0][1], sps.replace(',','\',\''), cid1)
        rr2 = dba.query_all(sql)

        for source, c, sales, num, cnt, in rr2:
            category = self.get_plugin().get_category_info(source, c)
            # category = {'name':''}
            data.append([c, category['name'], '强相关', sps, round(sales/rr1[(source,c)][0]*100,2), round(num/rr1[(source,c)][1]*100,2), round(cnt/rr1[(source,c)][2]*100,2), rate1])

        sql = '''
            WITH toYYYYMM(date) AS mm, `clean_props.value`[indexOf(`clean_props.name`, '子品类')] AS sp
            SELECT source, cid, sum(sales), sum(num), count(*) FROM {} WHERE mm >= {} AND mm <= {} AND cid IN ({}) GROUP BY source, cid
        '''.format(tbl, ret[0][0], ret[0][1], cid2)
        rr1 = dba.query_all(sql)
        rr1 = {(v[0],v[1]):[v[2],v[3],v[4]] for v in rr1}

        sql = '''
            WITH toYYYYMM(date) AS mm, `clean_props.value`[indexOf(`clean_props.name`, '子品类')] AS sp
            SELECT source, cid, sum(sales), sum(num), count(*) FROM {} WHERE mm >= {} AND mm <= {} AND sp IN ('{}') AND cid IN ({}) GROUP BY source, cid
        '''.format(tbl, ret[0][0], ret[0][1], sps.replace(',','\',\''), cid2)
        rr2 = dba.query_all(sql)

        for source, c, sales, num, cnt, in rr2:
            category = self.get_plugin().get_category_info(source, c)
            # category = {'name':''}
            data.append([c, category['name'], '弱相关', sps, round(sales/rr1[(source,c)][0]*100,2), round(num/rr1[(source,c)][1]*100,2), round(cnt/rr1[(source,c)][2]*100,2), rate2])

    return data


def stat_cache(self, ver, sp, tbl='', other=0, force=False):
    db26 = self.get_db('26_apollo')
    if not force:
        sql = 'SELECT data FROM cleaner.`etbl_spstat` WHERE ver=%s AND sp=%s AND tbl=%s AND other=%s'
        ret = db26.query_all(sql, (ver,sp,tbl,other,)) or [['']]
        ret = self.json_decode(ret[0][0])
        if ret:
            return ret
    ret = stat(self, ver, sp, tbl, other)
    sql = 'INSERT INTO cleaner.`etbl_spstat` (ver, sp, tbl, other, data, created) VALUES (%s, %s, %s, %s, %s, NOW())'
    db26.execute(sql, (ver, sp, tbl, other, json.dumps(ret, ensure_ascii=False)))
    return ret

def stat(self, ver, sp, tbl='', other=0, were=''):

    if sp == '重复链接等':
        return stat_repeat_item(self, ver, sp, tbl='', other=0, were=were)

    dba, etbl = self.get_plugin().get_e_tbl()
    dba = self.get_db(dba)

    tbl = tbl or etbl

    cat = get_other_cate(self)
    a, b = ['%(p{})s'.format(i) for i,c in enumerate(cat)], {'p{}'.format(i):c for i,c in enumerate(cat)}

    where = self.get_kadis(tbl)['where']

    # 最多显示15个 超出页面会崩

    if self.get_entity()['use_all_table']:
        _, atbl = self.get_plugin().get_all_tbl()
        sql = 'SELECT toYYYYMM(addMonths(min(date),-6)), toYYYYMM(addMonths(max(date),6)) FROM {}ver{}'.format(atbl, ver)
    else:
        _, atbl = self.get_plugin().get_d_tbl()
        sql = 'SELECT toYYYYMM(addMonths(min(date),-6)), toYYYYMM(addMonths(max(date),6)) FROM {} WHERE c_out_ver = {}'.format(atbl, ver)
    ret = dba.query_all(sql)

    bidsql, bidtbl = self.get_aliasbid_sql('name')
    pidsql, pidtbl = self.get_aliaspid_sql('name')

    if sp == 'alias_all_bid':
        spp = bidsql.format(v='alias_all_bid')
    elif sp == 'alias_pid':
        spp = pidsql.format(v='alias_pid')
    elif sp == '平台':
        spp = 'source_cn'
    elif self.get_entity()['use_all_table']:
        spp = '`sp{}`'.format(sp)
    else:
        spp = '`clean_props.value`[indexOf(`clean_props.name`, \'{}\')]'.format(sp)

    if self.get_entity()['use_all_table']:
        sp1 = '`sp子品类`'
        pattern = re.compile(r'`clean_props.value`\[indexOf\(`clean_props.name`, \'([^\']+)\'\)\]')
        where = re.sub(pattern, r'`sp\1`', where)
    else:
        sp1 = ''' `clean_props.value`[indexOf(`clean_props.name`, '子品类')] '''

    sql = '''
        WITH toYYYYMM(date) AS mm, IF({sp1} IN [{}], '其它', {sp1}) AS k,
             transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn
        SELECT {} AS kk FROM {} WHERE mm >= {} AND mm <= {} AND {} AND ({}) AND ({}) GROUP BY kk ORDER BY sum(sales) DESC LIMIT 15
    '''.format(
        ','.join(a), 'k' if sp=='子品类' else spp, tbl, ret[0][0], ret[0][1], '1' if other or sp=='子品类' else 'k != \'其它\'', where, were or 1, sp1=sp1
    )
    rrr = dba.query_all(sql, b)

    b.update({'e{}'.format(i+1):v for i,v in enumerate(rrr)})
    c = ['%(e{})s'.format(i+1) for i,v in enumerate(rrr)]

    sql = '''
        WITH IF({sp1} IN [{}], '其它', {sp1}) AS k,
             transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
             {} AS kk, quantiles(0.25,0.5,0.75)(price) AS q
        SELECT toYYYYMM(date) mm, IF(kk IN [{}], kk, '省略') kkk, countDistinct(item_id),
            sum(sales/100), max(sales/100), min(sales/100), sum(num), max(num), min(num),
            max(price/100), min(price/100), ROUND(avg(price/100),2),
            [ROUND(min(price/100),2),ROUND(q[1]/100,2),ROUND(q[2]/100,2),ROUND(q[3]/100,2),ROUND(max(price/100),2)]
        FROM {} WHERE mm >= {} AND mm <= {} AND {} AND ({}) AND ({}) GROUP BY mm, kkk
    '''.format(
        ','.join(a), 'k' if sp=='子品类' else spp, ','.join(c), tbl, ret[0][0], ret[0][1], '1' if other or sp=='子品类' else 'k != \'其它\'', where, were or 1, sp1=sp1
    )
    rrr = dba.query_all(sql, b)
    rrr = list(rrr)
    mmm = [v[0] for v in rrr]
    kkk = {(v[0],v[1]):True for v in rrr}

    sql = '''
        WITH IF({sp1} IN [{}], '其它', {sp1}) AS k,
             transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn
        SELECT {} AS kk FROM {} WHERE {} AND ({}) AND ({}) GROUP BY kk
    '''.format(
        ','.join(a), 'k' if sp=='子品类' else spp, tbl, '1' if other or sp=='子品类' else 'k != \'其它\'', where, were or 1, sp1=sp1
    )
    rr1 = dba.query_all(sql, b)

    sql = '''
        WITH IF({sp1} IN [{}], '其它', {sp1}) AS k,
             transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','test20',''], '') AS source_cn
        SELECT toYYYYMM(date) mm, {} AS kk
        FROM {} WHERE mm >= {} AND mm <= {} AND {} AND ({}) AND ({}) GROUP BY mm, kk
    '''.format(
        ','.join(a), 'k' if sp=='子品类' else spp, tbl, ret[0][0], ret[0][1], '1' if other or sp=='子品类' else 'k != \'其它\'', where, were or 1, sp1=sp1
    )
    rr2 = dba.query_all(sql, b)
    kk2 = {(v[0],v[1]):True for v in rr2}

    for m in list(set(mmm)):
        tmp = []
        for k, in rr1:
            if (m,k) not in kk2:
                tmp.append([m, k, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [0, 0, 0, 0, 0]])
        if len(tmp) > 5:
            tmp = tmp[0:5]
            # tmp.append([m, '...', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [0, 0, 0, 0, 0]])
        rrr += tmp

    return rrr


def stat_repeat_item(self, ver, sp, tbl='', other=0, were=''):

    dba, etbl = self.get_plugin().get_e_tbl()
    dba = self.get_db(dba)

    tbl = tbl or etbl

    cat = get_other_cate(self)
    a, b = ['%(p{})s'.format(i) for i,c in enumerate(cat)], {'p{}'.format(i):c for i,c in enumerate(cat)}

    # 最多显示15个 超出页面会崩

    if self.get_entity()['use_all_table']:
        _, atbl = self.get_plugin().get_all_tbl()
        sql = 'SELECT toYYYYMM(addMonths(min(date),-6)), toYYYYMM(addMonths(max(date),6)) FROM {}ver{}'.format(atbl, ver)
    else:
        _, atbl = self.get_plugin().get_d_tbl()
        sql = 'SELECT toYYYYMM(addMonths(min(date),-6)), toYYYYMM(addMonths(max(date),6)) FROM {} WHERE c_out_ver = {}'.format(atbl, ver)
    ret = dba.query_all(sql)

    if self.get_entity()['use_all_table']:
        sp1 = '`sp子品类`'
    else:
        sp1 = ''' `clean_props.value`[indexOf(`clean_props.name`, '子品类')] '''

    sql = '''
        SELECT toYYYYMM(date) m, `s`, SUM(c) FROM (
            WITH IF(`source`=1 AND (shop_type < 20 AND shop_type > 10), 0, `source`) AS s,
                 IF(`source`=5, [toString(sku_id)], [toString(trade_props.name), toString(trade_props.value)]) AS kk, toYYYYMM(date) AS mm,
                 IF({sp1} IN [{}], '其它', {sp1}) AS k
            SELECT `s`, `date`, 1, countDistinct(uuid2) c
            FROM {} WHERE mm >= {} AND mm <= {} AND {} AND ({})
            GROUP BY `s`, `date`, item_id, kk HAVING c > 1
        ) GROUP BY `s`, m
    '''.format(
        ','.join(a), tbl, ret[0][0], ret[0][1], '1' if other or sp=='子品类' else 'k != \'其它\'', were or 1, sp1=sp1
    )
    rr1 = dba.query_all(sql, b)
    rr1 = {(v[0],v[1]):v[2] for v in rr1}

    sql = '''
        WITH IF(`source`=1 AND (shop_type < 20 AND shop_type > 10), 0, `source`) AS s,
             IF(`source`=5, [toString(sku_id)], [toString(trade_props.name), toString(trade_props.value)]) AS kk,
             IF({sp1} IN [{}], '其它', {sp1}) AS k
        SELECT toYYYYMM(date) AS mm, `s`, count(*)
        FROM {} WHERE mm >= {} AND mm <= {} AND {} AND ({})
        GROUP BY `s`, mm
    '''.format(
        ','.join(a), tbl, ret[0][0], ret[0][1], '1' if other else 'k != \'其它\'', were or 1, sp1=sp1
    )
    rr2 = dba.query_all(sql, b)

    ret = []
    for v in rr2:
        k = (v[0], v[1])
        v = list(v)
        v[1] = self.get_plugin().get_source_en(v[1])
        if k in rr1:
            v.append(rr1[k])
            ret.append(v)
    ret.sort(key=lambda v: v[3]/v[2], reverse=True)

    return ret


# def step_stat(self, m, sp, tbl='', other=0):
#     sql = '''
#         WITH IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')] IN [{}], '其它', `clean_props.value`[indexOf(`clean_props.name`, '子品类')]) AS k
#         SELECT max(price) s FROM {} WHERE toYYYYMM(date) = {} AND {}
#     '''.format(
#         ','.join(a), tbl, m, '1' if other else 'k != \'其它\''
#     )

#     rr1 = dba.query_all(sql)

#     sql = '''
#         WITH IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')] IN [{}], '其它', `clean_props.value`[indexOf(`clean_props.name`, '子品类')]) AS k
#         SELECT toInt32(price/{}) s, count(*) FROM {} WHERE toYYYYMM(date) = {} AND {} GROUP BY s
#     '''.format(
#         ','.join(a), rr1[0][0]/10, tbl, m, '1' if other else 'k != \'其它\''
#     )
#     rr2 = dba.query_all(sql)
#     rr2 = [for v in rr2]

#     sql = '''
#         WITH IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')] IN [{}], '其它', `clean_props.value`[indexOf(`clean_props.name`, '子品类')]) AS k
#         SELECT toInt32(price/{}) s, count(*) FROM {} WHERE toYYYYMM(date) = {} AND {} GROUP BY s
#     '''.format(
#         ','.join(a), rr1[0][0]/10, tbl, m, '1' if other else 'k != \'其它\''
#     )
#     re2 = dba.query_all(sql)
#     re2 = [for v in re2]

# SELECT toInt32(price/10000) s, count(*) FROM sop_e.entity_prod_90473_E WHERE price > 0 AND price < 100000 GROUP BY s;



def statxx(self, month, sp, tbl='', other=0):
    dba, etbl = self.get_plugin().get_e_tbl()
    dba = self.get_db(dba)

    tbl = tbl or etbl

    pcount = 15-1
    cat = get_other_cate(self)
    a, b = ['%(p{})s'.format(i) for i,c in enumerate(cat)], {'p{}'.format(i):c for i,c in enumerate(cat)}

    sql = '''
        WITH IF(`sp子品类` IN [{}], '其它', `sp子品类`) AS k
        SELECT max(clean_sales), min(clean_sales), max(clean_num), min(clean_num), max(clean_price), min(clean_price)
        FROM {} WHERE toYYYYMM(date) = {} AND {}
    '''.format(','.join(a), tbl, month, '1' if other else 'k != \'其它\'')
    rr1 = dba.query_all(sql, b)

    step1, step2, step3 = int((rr1[0][0]-rr1[0][1])/pcount), int((rr1[0][2]-rr1[0][3])/pcount), int((rr1[0][4]-rr1[0][5])/pcount)

    sql = '''
        WITH IF(`sp子品类` IN [{}], '其它', `sp子品类`) AS k
        SELECT `{}` AS kk, count(*),
        sum(clean_sales), max(clean_sales), min(clean_sales), sum(clean_num) sn, max(clean_num), min(clean_num),
        max(clean_price), min(clean_price), avg(clean_price), median(clean_price),
        countResample({}, {}, {})(clean_sales), countResample({}, {}, {})(clean_num), countResample({}, {}, {})(clean_price)
        FROM {} WHERE toYYYYMM(date) = {} AND {} GROUP BY kk ORDER BY sn DESC LIMIT 11
    '''.format(
        ','.join(a), 'k' if sp=='子品类' else 'sp{}'.format(sp),
        rr1[0][1]-1, rr1[0][0]+1, step1, rr1[0][3]-1, rr1[0][2]+1, step2, rr1[0][5]-1, rr1[0][4]+1, step3,
        tbl, month, '1' if other else 'k != \'其它\''
    )
    rr2 = dba.query_all(sql, b)

    if len(rr2) > 15:
        # 超过15个时只显示前15个
        rr2 = list(rr2)[0:15]
        b.update({'e{}'.format(i+1):v[0] for i,v in enumerate(rr2)})
        sql = '''
            WITH IF(`sp子品类` IN [{}], '其它', `sp子品类`) AS k, `{}` AS kk
            SELECT '省略其它', count(*),
            sum(clean_sales), max(clean_sales), min(clean_sales), sum(clean_num), max(clean_num), min(clean_num),
            max(clean_price), min(clean_price), avg(clean_price), median(clean_price),
            countResample({}, {}, {})(clean_sales), countResample({}, {}, {})(clean_num), countResample({}, {}, {})(clean_price)
            FROM {} WHERE toYYYYMM(date) = {} AND {} AND kk NOT IN [%(e1)s,%(e2)s,%(e3)s,%(e4)s,%(e5)s]
        '''.format(
            ','.join(a), 'k' if sp=='子品类' else 'sp{}'.format(sp),
            rr1[0][1]-1, rr1[0][0]+1, step1, rr1[0][3]-1, rr1[0][2]+1, step2, rr1[0][5]-1, rr1[0][4]+1, step3,
            tbl, month, '1' if other else 'k != \'其它\''
        )
        rr3 = dba.query_all(sql, b)
        rr2.append(rr3[0])

    return rr2, [step1, step2, step3]


def repeat_item(self, month, tbl=''):
    dba, etbl = self.get_plugin().get_e_tbl()
    dba = self.get_db(dba)

    tbl = tbl or etbl

    sql = '''
        SELECT `source`, SUM(c) FROM (
            SELECT `source`, `date`, 1, countDistinct(uuid2) c FROM {} WHERE toYYYYMM(date) = {}
            GROUP BY `source`, `date`, item_id, trade_props.name, trade_props.value HAVING c > 1
        ) GROUP BY `source`
    '''.format(tbl, month)
    rr1 = dba.query_all(sql)

    sql = '''
        SELECT `source`, countDistinct(uuid2) FROM {} WHERE toYYYYMM(date) = {} GROUP BY `source`
    '''.format(tbl, month)
    rr2 = dba.query_all(sql)
    rr2 = {v[0]:v[1] for v in rr2}

    ret = []
    for s, c, in rr1:
        ret.append([s, c, rr2[s], c/rr2[s]*100])

    return ret


def check_brush(self, cols, smonth, emonth, brandtotal):
    dba, atbl = self.get_plugin().get_all_tbl()
    dba = self.get_db(dba)
    db26 = self.get_db('26_apollo')

    sql = 'SELECT id, source, snum, p1, tb_item_id, month, flag FROM {}'.format(self.get_tbl())
    bru_items = db26.query_all(sql)

    brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

    tbl = atbl + '_brushcheck'

    dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

    sql = '''
        CREATE TABLE {} (
            `brush_id` UInt32, `sname` String, `source` UInt8, p1 Array(String), p2 String, `item_id` String, `date` Date, `flag` UInt8
        ) ENGINE = Join(ANY, LEFT, source, item_id, p1)
    '''.format(tbl)
    dba.execute(sql)

    data = []
    for id, source, snum, p1, item_id, month, flag, in bru_items:
        p1 = brush_p1(snum, p1, [])
        data.append([id, source, snum, p1, '', item_id, month, flag])

    if len(data) > 0:
        sql = 'INSERT INTO {} VALUES'.format(tbl)
        dba.execute(sql, data)

    bidsql, bidtbl = self.get_aliasbid_sql()
    bidsql = bidsql.format(v='alias_all_bid')
    bnamesql, _ = self.get_aliasbid_sql(col='name', created=False)
    bnamesql = bnamesql.format(v='alias_all_bid')

    tmp = []
    for col in cols.split(','):
        if col == '分月':
            tmp.append('toString(toYYYYMM(date))')
        elif col == '平台':
            tmp.append('source_cn')
        elif col == '品牌':
            tmp.append('CONCAT({}, \'(\', toString({}), \')\')'.format(bnamesql, bidsql))
        else:
            tmp.append('toString({})'.format(col))
    ttt = []
    for v in tmp:
        if v not in ttt:
            ttt.append(v)
    cols = ','.join(ttt)

    if smonth and emonth:
        where = 'date >= \'{}\' AND date < \'{}\''.format(smonth, emonth)
    else:
        where = '1'

    sql = '''
        SELECT {} AS bid, sum(sales) ss FROM {} WHERE {} GROUP BY bid ORDER BY ss DESC LIMIT {}
    '''.format(bidsql, atbl, where, brandtotal or 100)
    ret = dba.query_all(sql)
    bids = [str(v[0]) for v in ret]

    # 总占比
    sql = '''
        WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn
        SELECT [{c}] k, sum(sales), count(*) FROM {} WHERE {} GROUP BY k
    '''.format(atbl, where, c=cols)
    ret = dba.query_all(sql)
    rr0 = {tuple(v[0]):v for v in ret}

    # 出题数占比
    sql = '''
        WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
             {} AS p1
        SELECT [{c}] k, sum(sales), count(*) FROM {}
        WHERE not isNull(joinGet('{}', 'sname', `source`, item_id, p1)) AND {}
        GROUP BY k
    '''.format(filter_p1, atbl, tbl, where, c=cols)
    ret = dba.query_all(sql)
    rr1 = {tuple(v[0]):v for v in ret}

    # 出题top品牌占比
    sql = '''
        WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
             {} AS p1
        SELECT [{c}] k, sum(sales), count(*), countDistinct({bid}) FROM {}
        WHERE not isNull(joinGet('{}', 'sname', `source`, item_id, p1)) AND {bid} IN ({}) AND {}
        GROUP BY k
    '''.format(filter_p1, atbl, tbl, ','.join(bids), where, c=cols, bid=bidsql)
    ret = dba.query_all(sql)
    rr2 = {tuple(v[0]):v for v in ret}

    dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

    if len(rr1.keys()) == 0:
        return

    data1, data2 = {}, {}
    for k in rr1:
        tree1, tree2 = data1, data2
        for kk in k:
            if kk not in tree1:
                tree1[kk] = {'name': str(kk), 'value': 0, 'children':{}}
                tree2[kk] = {'name': str(kk), 'value': 0, 'children':{}}
            tree1[kk]['value'] += rr1[k][1]
            tree2[kk]['value'] += rr1[k][2]
            tree1 = tree1[kk]['children']
            tree2 = tree2[kk]['children']

        tmp = rr2[k] if k in rr2 else [k, 0, 0, 0]
        tree1['top品牌'] = {'name': 'top品牌{}个'.format(tmp[3]), 'value': tmp[1]}
        tree2['top品牌'] = {'name': 'top品牌{}个'.format(tmp[3]), 'value': tmp[2]}

    def fmat(d, total, kkk=[]):
        dd = []
        if 'children' not in d:
            return
        for ddd in d['children']:
            aaa = kkk+[ddd]
            ddd = d['children'][ddd]
            ccc = 0
            for k in total:
                if aaa == list(k)[0:len(aaa)]:
                    ccc += total[k]
            if ccc > 0:
                ddd['name'] += ' {:.2f}%'.format(round(ddd['value']/ccc*100,2))
            fmat(ddd, total, aaa)
            dd.append(ddd)
        d['children'] = dd

    data1 = {'children':data1}
    fmat(data1, {k:rr0[k][1] for k in rr0})
    data1 = data1['children']
    data2 = {'children':data2}
    fmat(data2, {k:rr0[k][2] for k in rr0})
    data2 = data2['children']

    return [data1, data2]


def check_brush2(self, cols, smonth, emonth, brandtotal):
    dba, atbl = self.get_plugin().get_all_tbl()
    dba = self.get_db(dba)
    db26 = self.get_db('26_apollo')

    sql = 'SELECT id, source, snum, p1, tb_item_id, month, flag FROM {}'.format(self.get_tbl())
    bru_items = db26.query_all(sql)

    brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

    tbl = atbl + '_brushcheck'

    dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

    sql = '''
        CREATE TABLE {} (
            `brush_id` UInt32, `sname` String, `source` UInt8, p1 Array(String), p2 String, `item_id` String, `date` Date, `flag` UInt8
        ) ENGINE = Join(ANY, LEFT, source, item_id, p1)
    '''.format(tbl)
    dba.execute(sql)

    data = []
    for id, source, snum, p1, item_id, month, flag, in bru_items:
        p1 = brush_p1(snum, p1, [])
        data.append([id, source, snum, p1, '', item_id, month, flag])

    if len(data) > 0:
        sql = 'INSERT INTO {} VALUES'.format(tbl)
        dba.execute(sql, data)

    bidsql, bidtbl = self.get_aliasbid_sql()
    bidsql = bidsql.format(v='alias_all_bid')
    bnamesql, _ = self.get_aliasbid_sql(col='name', created=False)
    bnamesql = bnamesql.format(v='alias_all_bid')

    tmp = []
    for col in cols.split(','):
        if col == '分月':
            tmp.append('toString(toYYYYMM(date))')
        elif col == '平台':
            tmp.append('source_cn')
        elif col == '品牌':
            tmp.append('CONCAT({}, \'(\', toString({}), \')\')'.format(bnamesql, bidsql))
        else:
            tmp.append('toString({})'.format(col))
    tmp = list(set(tmp))
    cols = ','.join(tmp)

    if smonth and emonth:
        where = 'date >= \'{}\' AND date < \'{}\''.format(smonth, emonth)
    else:
        where = '1'

    # 出题数占比
    sql = '''
        WITH transform(IF(source=1 AND (shop_type<20 and shop_type>10), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') AS source_cn,
             {} AS p1
        SELECT {c} k, toYYYYMM(date) m, countDistinct(joinGet('{t}', 'brush_id', `source`, item_id, p1)) FROM {}
        WHERE not isNull(joinGet('{t}', 'sname', `source`, item_id, p1))
          AND toYYYYMM(joinGet('{t}', 'date', `source`, item_id, p1))=m AND {}
        GROUP BY k, m
    '''.format(filter_p1, atbl, where, t=tbl, c=cols)
    ret = dba.query_all(sql)

    dba.execute('DROP TABLE IF EXISTS {}'.format(tbl))

    data, mmm, sss = {}, [], []
    for sname, month, count, in ret:
        mmm.append(month)
        sss.append(sname)
        data[sname+str(month)] = count

    sss = list(set(sss))
    mmm = list(set(mmm))

    sss.sort()
    mmm.sort()

    tmp = [['product']+mmm]
    for s in sss:
        tmp.append([s])
        for m in mmm:
            if s+str(m) in data:
                tmp[-1].append(data[s+str(m)])
            else:
                tmp[-1].append(0)
    data = tmp

    return data