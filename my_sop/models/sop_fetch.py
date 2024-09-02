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

def aver_info(ent, vers):
    res = {}

    v = [str(v[1]) for v in vers if v[0] == 0]
    if len(v) > 0:
        db39 = ent.get_db('47_apollo')
        sql = 'SELECT version, description FROM apollo.trade_fix_task WHERE version IN ({})'.format(','.join(v))
        rrr = db39.query_all(sql)
        res.update({(0, v[0]): v[1] for v in rrr})

        sql = 'SELECT add_version, description FROM apollo.trade_fix_task WHERE add_version IN ({})'.format(','.join(v))
        rrr = db39.query_all(sql)
        res.update({(0, v[0]): v[1] for v in rrr})

    v = [str(v[1]) for v in vers if v[0] == 1]
    if len(v) > 0:
        db39 = ent.get_db('47_apollo')
        sql = 'SELECT version, description FROM apollo.trade_fix_task WHERE version IN ({})'.format(','.join(v))
        rrr = db39.query_all(sql)
        res.update({(1, v[0]): v[1] for v in rrr})

        sql = 'SELECT add_version, description FROM apollo.trade_fix_task WHERE add_version IN ({})'.format(','.join(v))
        rrr = db39.query_all(sql)
        res.update({(1, v[0]): v[1] for v in rrr})

    v = [str(v[2]) for v in vers if v[0] == 2]
    if len(v) > 0:
        db14 = ent.get_db('68_apollo')
        sql = 'SELECT version, description FROM jdnew.trade_fix_task WHERE version IN ({})'.format(','.join(v))
        rrr = db14.query_all(sql)
        res.update({(2, v[0]): v[1] for v in rrr})

    return res


def update_props_info(id):
    ent = entity_manager.get_entity(id)
    mkt = entity_manager.get_market(id)

    dba = ent.get_db('chsop')

    sql = '''
        CREATE TABLE IF NOT EXISTS artificial.props_{} (
            `type` String CODEC(ZSTD(1)),
            `key` String CODEC(ZSTD(1)),
            `count` UInt32 CODEC(ZSTD(1)),
            `usedCount` UInt32 CODEC(ZSTD(1)),
            `created` DateTime DEFAULT now() CODEC(ZSTD(1))
        ) ENGINE = Log
    '''.format(ent.eid)
    dba.execute(sql)

    _, atbl = ent.get_plugin().get_a_tbl()
    _, etbl = mkt.get_plugin().get_e_tbl()

    a, b = atbl.split('.')
    sql = 'SELECT modification_time FROM `system`.parts WHERE database = \'{}\' AND `table` = \'{}\' ORDER BY modification_time DESC LIMIT 1'.format(a, b)
    t1 = dba.query_all(sql) or None

    sql = 'SELECT created FROM artificial.props_{} WHERE `type` = \'trade_props\' ORDER BY created DESC LIMIT 1'.format(ent.eid)
    t2 = dba.query_all(sql) or None

    a, b = etbl.split('.')
    sql = 'SELECT modification_time FROM `system`.parts WHERE database = \'{}\' AND `table` = \'{}\' ORDER BY modification_time DESC LIMIT 1'.format(a, b)
    t3 = dba.query_all(sql) or [[datetime.datetime(1970,1,1,0,0,0)]]

    sql = 'SELECT created FROM artificial.props_{} WHERE `type` = \'clean_props\' ORDER BY created DESC LIMIT 1'.format(ent.eid)
    t4 = dba.query_all(sql) or [[datetime.datetime(1970,1,1,0,0,0)]]

    if (t1 and t2 is None) or (t3[0][0] > t4[0][0]):
        sql = 'DROP TABLE IF EXISTS artificial.props_{}_tmp'.format(ent.eid)
        dba.execute(sql)

        sql = 'CREATE TABLE artificial.props_{eid}_tmp AS artificial.props_{eid}'.format(eid = ent.eid)
        dba.execute(sql)

        fsql = '''
            INSERT INTO artificial.props_{eid}_tmp
            SELECT '{type}' t, k, count(*) c, 0, now() FROM {} ARRAY JOIN `{type}.name` AS k GROUP BY k
        '''

        sqla = fsql.format(atbl, eid = ent.eid, type = 'trade_props')
        sqlb = fsql.format(atbl, eid = ent.eid, type = 'props')
        sqlc = fsql.format(etbl, eid = ent.eid, type = 'clean_props')
        dba.execute(sqla)
        dba.execute(sqlb)
        dba.execute(sqlc)

        sql = 'DROP TABLE artificial.props_{}'.format(ent.eid)
        dba.execute(sql)

        sql = 'RENAME TABLE artificial.props_{eid}_tmp TO artificial.props_{eid}'.format(eid=ent.eid)
        dba.execute(sql)


def get_props(dba, eid, type):
    try:
        sql = 'SELECT key FROM artificial.props_{} WHERE type = \'{}\' ORDER BY count DESC'.format(eid, type)
        ret = dba.query_all(sql)
    except:
        ret = []
    return [v[0] for v in ret]


def get_cfg(id):
    ent = entity_manager.get_market(id)
    ent = entity_manager.get_brush(id)

    db26 = ent.get_db('26_apollo')
    dbch = ent.get_db('chsop')
    dbcm = ent.get_db('chmaster')
    poslist = ent.get_poslist()

    emonth = datetime.datetime.now().replace(day=1)
    smonth = (emonth - datetime.timedelta(days=1)).replace(day=1)

    mod1 = {'name':'单价1元以下(上月)', 'limit':'1 BY item_id', 'params':[
        ['pkey',' >= ',smonth.strftime('%Y-%m-%d')],['pkey',' < ',emonth.strftime('%Y-%m-%d')],['price',' <= ','100'],['promo_price',' <= ','100']
    ]}
    mod2 = {'name':'淘宝上月', 'params':[ ['month',' = ',smonth.strftime('%Y%m')],['source',' = ','0'] ]}
    mod3 = {'name':'天猫上月', 'params':[ ['month',' = ',smonth.strftime('%Y%m')],['source',' = ','1'] ]}
    mod4 = {'name':'销售额top1000', 'orderby':'sales', 'orderby_type':'DESC', 'limit':'1000', 'params':[ ['uuid2',' = ','0'] ]}
    mod5 = {'name':'上月', 'params':[['pkey',' >= ',smonth.strftime('%Y-%m-%d')],['pkey',' < ',emonth.strftime('%Y-%m-%d')]]}

    cfg = {}

    _, tbl = ent.get_plugin().get_a_tbl()
    cols = ent.get_cols(tbl, dbch, ['trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','props.name','props.value'])
    props = ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')] + ['宝贝-'+p for p in get_props(dbch, ent.eid, 'props')]
    cfg['A'] = {
        'search_cols':list(cols.keys())+['价格段（元）']+props[0:50],
        'search_full_cols': list(cols.keys())+['价格段（元）']+props,
        'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['日数据', '月数据', '月数据-分交易属性', '排重-分宝贝id', '排重-分交易属性'],
        'stat':True,
        'stat_cols':list(cols.keys())+props,
        'mods':[mod1,mod4,mod5]
    }

    cfg['A表版本差异'] = {
        'search_cols':['pkey', 'ver', 'created'], 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['brand', 'cid', 'sid', 'item_id'],
        'mods':[mod5]
    }

    cfg['A表日导差异'] = {
        'search_cols':['source', 'month'], 'search_opr':[' = '],
        'search_exp':['不更新', '更新，44预估时间10分钟'], 'mods':[mod2, mod3],
        'mods':[{'name':'上月', 'params':[ ['month',' = ',smonth.strftime('%Y%m')] ]}]
    }

    _, tbl = ent.get_plugin().get_b_tbl()
    cols = ent.get_cols(tbl, dbch)
    cfg['B'] = {
        'search_cols':list(cols.keys())+['价格段（元）'], 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())
    }

    _, tbl = ent.get_plugin().get_c_tbl()
    cols = ent.get_cols(tbl, dbch)
    cfg['C'] = {
        'search_cols':list(cols.keys())+['价格段（元）'], 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())
    }

    _, tbl = ent.get_plugin().get_d_tbl()
    cols = ent.get_cols(tbl, dbch, ['trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','c_props.name','c_props.value'])
    props = ['清洗-'+p for p in get_props(dbch, ent.eid, 'clean_props')] + ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')]
    cfg['D'] = {
        'search_cols':list(cols.keys())+props[0:100], 'search_full_cols': props , 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())+props
    }

    _, tbl = ent.get_plugin().get_e_tbl()
    cols = ent.get_cols(tbl, dbch, ['trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','clean_props.name','clean_props.value'])
    props = ['清洗-'+p for p in get_props(dbch, ent.eid, 'clean_props')] + ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')]
    cfg['E'] = {
        'search_cols':list(cols.keys())+['价格段（元）']+props[0:100],
        'search_full_cols': list(cols.keys())+['价格段（元）']+props,
        'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['日数据', '月数据', '月数据-分交易属性', '排重-分宝贝id', '排重-分交易属性'],
        'stat':True,
        'stat_cols':list(cols.keys())+props,
        'mods':[mod1,mod4,mod5]
    }

    _, tbl = ent.get_plugin().get_all_tbl()
    cols = ent.get_cols(tbl, dbch, ['pkey'])
    cols['month'] = 'UInt32'
    props = ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')]
    cfg['ALL'] = {
        'search_cols':list(cols.keys())+props[0:100], 'search_full_cols': props , 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())+props
    }

    cfg['店铺状态-仅项目店铺'] = {
        'search_cols':['sid'], 'search_opr':[' = ', ' in '],'stat':False,
    }

    cfg['店铺状态-跟项目无关'] = {
        'search_cols':['sid'], 'search_opr':[' = ', ' in '],'stat':False,
    }

    _, tbl = ent.get_plugin().get_e_tbl()
    cols = ent.get_cols(tbl, dbch, ['trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','clean_props.name','clean_props.value'])
    props = ['清洗-'+p for p in get_props(dbch, ent.eid, 'clean_props')] + ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')]
    cfg['E表品牌合并差异'] = {
        'search_cols':list(cols.keys())+['价格段（元）']+props[0:100],
        'search_full_cols': list(cols.keys())+['价格段（元）']+props,
        'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
    }

    _, tbl = ent.get_plugin().get_e_tbl()
    cols = ent.get_cols(tbl, dbch, ['trade_props.name','trade_props.value','trade_props_full.name','trade_props_full.value','clean_props.name','clean_props.value'])
    props = ['清洗-'+p for p in get_props(dbch, ent.eid, 'clean_props')] + ['交易-'+p for p in get_props(dbch, ent.eid, 'trade_props')]
    cfg['E表对比'] = {
        'search_cols':list(cols.keys())+['价格段（元）']+props[0:100],
        'search_full_cols': list(cols.keys())+['价格段（元）']+props,
        'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['日数据'],
        'forceDownload': True,
        'stat':True,
        'stat_cols':list(cols.keys())+props,
        'mods':[mod5]
    }

    # cols = ent.get_cols('ali.trade_all', dbcm)
    cfg['底层无法自助查询'] = {
        'search_cols':list(cols.keys()),
        'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['日数据', '月数据', '月数据-分交易属性', '排重-分宝贝id', '排重-分交易属性'],
        'stat':True,
        'stat_cols':list(cols.keys()),
        'mods':[mod5]
    }

    tbl = ent.get_tbl()
    cols = ent.get_cols(tbl, db26)
    cfg['答题表'] = {
        'search_cols':list(cols.keys()), 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())
    }
    cfg['答题表级联检查'] = {
        'search_cols':['id'], 'search_opr':[' = '],
        'search_exp':['检查当前项目', '检查全部项目']
    }
    _, tbl = ent.get_plugin().get_b_tbl()
    cols = ent.get_cols(tbl, dbch)
    cfg['答题机洗sp对不上'] = {
        'search_cols':list(cols.keys()), 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'search_exp':['看全部']+['sp{}'.format(pos) for pos in poslist]
    }

    tbl = ent.get_product_tbl()
    cols = ent.get_cols(tbl, db26)
    cfg['答题sku库'] = {
        'search_cols':list(cols.keys()), 'search_opr':[' = ', ' != ', ' in ', ' > ',' < ',' >= ',' <= ', ' like '],
        'stat':True, 'stat_cols':list(cols.keys())
    }

    cfg['答题覆盖日期修正'] = {
        'search_cols':['item_id'], 'search_opr':[' = ', ' in '],
    }

    cfg['E表链接对比'] = {
        'search_cols':['batch_ids'], 'search_opr':[' in ']
    }
    cfg['盒马brand'] = { }
    cfg['出数刷sp'] = { }
    cfg['E表brand'] = { }
    cfg['E表category'] = { }
    cfg['E表product'] = {'search_cols':['pid','sku_id'], 'search_opr':[' in '],'search_exp':['全部', '只看不同的'] }
    cfg['答题表配置'] = { }
    cfg['答题表属性配置'] = { }
    cfg['答题表级联'] = { }
    cfg['属性差异查询'] = { 'search_cols':['source', 'preRange', 'searchRange', 'searchKey'] , 'search_opr':[' in ', ' between ', ' = '] }

    for k in cfg:
        for kk in ['search_cols','search_full_cols','search_opr','search_exp','stat_cols','mods']:
            if kk not in cfg[k]:
                cfg[k][kk] = []
            if kk == 'stat_cols':
                cfg[k][kk] = [v for v in cfg[k][kk] if v not in ['2','sign','ver','tip','created']]
        if 'stat' not in cfg[k]:
            cfg[k]['stat'] = False
        if 'forceDownload' not in cfg[k]:
            cfg[k]['forceDownload'] = False

    return cfg

def fetch(id, params, type, chgtbl='', tbls=[]):
    ent = entity_manager.get_brush(id)
    dbcm = ent.get_db('chmaster')
    dbch = ent.get_db('chsop')
    db26 = ent.get_db('26_apollo')
    params = json.loads(params)

    if type == 'A':
        _, tbl = ent.get_plugin().get_a_tbl()
        return fetch_abcde_data(ent, dbch, chgtbl or tbl, params, type, tbls)
    elif type == 'A表版本差异':
        _, tbl = ent.get_plugin().get_a_tbl()
        return fetch_aver(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'A表日导差异':
        _, tbl = ent.get_plugin().get_a_tbl()
        return fetch_a_growth(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'B':
        _, tbl = ent.get_plugin().get_b_tbl()
        return fetch_abcde_data(ent, dbch, chgtbl or tbl, params, type, tbls)
    elif type == 'C':
        _, tbl = ent.get_plugin().get_c_tbl()
        return fetch_abcde_data(ent, dbch, chgtbl or tbl, params, type, tbls)
    elif type == 'D':
        _, tbl = ent.get_plugin().get_d_tbl()
        return fetch_abcde_data(ent, dbch, chgtbl or tbl, params, type, tbls)
    elif type == 'E':
        _, tbl = ent.get_plugin().get_e_tbl()
        return fetch_abcde_data(ent, dbch, chgtbl or tbl, params, type, tbls)
    elif type == 'ALL':
        _, tbl = ent.get_plugin().get_all_tbl()
        return fetch_data(ent, dbch, tbl, params, type)
    elif type == '店铺状态-仅项目店铺':
        tbl = 'artificial.shop_check'
        return fetch_shop_data(ent, dbch, tbl, params, type)
    elif type == '店铺状态-跟项目无关':
        return fetch_shop_bysid(params, type)
    elif type == 'E表品牌合并差异':
        _, tbl = ent.get_plugin().get_e_tbl()
        return fetch_e_diff_aliasbid(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'E表对比':
        return fetch_e_diff(ent, dbch, chgtbl, params, type)
    elif type == '底层自助查询':
        return fetch_data(ent, dbcm, chgtbl, params, type)
    elif type == 'E表链接对比':
        _, tbl = ent.get_plugin().get_e_tbl()
        return fetch_ecompare(ent, dbch, tbl, params, type)
    elif type == '答题表':
        tbl = ent.get_tbl()
        return fetch_data(ent, db26, chgtbl or tbl, params, type)
    elif type == '答题sku库':
        tbl = ent.get_product_tbl()
        return fetch_data(ent, db26, chgtbl or tbl, params, type)
    elif type == '答题表级联检查':
        tbl = ent.get_tbl()
        return fetch_brush_relation(ent, db26, chgtbl or tbl, params, type)
    elif type == '出数刷sp':
        tbl = 'artificial.alter_update'
        return fetch_etbl_chgprops(ent, dbch, tbl, params, type)
    elif type == '盒马brand':
        tbl = 'cleaner.hema_brand_alias'
        return fetch_data(ent, db26, tbl, {'limit': 100000}, type, {'pkey': 0})
    elif type == 'E表brand':
        tbl = 'artificial.brand_{}'.format(ent.eid)
        return fetch_data(ent, dbch, tbl, {'limit': 100000}, type, {'pkey': 0})
    elif type == 'E表category':
        tbl = 'artificial.category_{}'.format(ent.eid)
        cfg = {'pkey': 0, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['lv1name', 'lv2name', 'lv3name', 'lv4name', 'lv5name']}
        return fetch_data(ent, dbch, tbl, params, type, cfg)
    elif type == 'E表product':
        ent = entity_manager.get_market(id)
        tbl = 'artificial.product_{}'.format(ent.eid)
        return fetch_eproduct(ent, dbch, chgtbl or tbl, params, type)
    elif type == '答题机洗sp对不上':
        tbl = ent.get_tbl()
        return fetch_bspcheck(ent, db26, chgtbl or tbl, params, type)
    elif type == '答题表属性配置':
        tbl = 'dataway.clean_props'
        return fetch_brush_props(ent, db26, tbl, params, type)
    elif type == '答题表配置':
        tbl = 'brush.project'
        return fetch_brush(ent, db26, tbl, params, type)
    elif type == '答题表级联':
        tbl = 'dataway.clean_props_relation'
        return fetch_brush_props_relation(ent, db26, tbl, params, type)
    elif type == '属性差异查询':
        _, tbl = ent.get_plugin().get_e_tbl()
        return fetch_props_diff(ent, dbch, chgtbl or tbl, params, type)
    elif type == '答题覆盖日期修正':
        _, tbl = ent.get_plugin().get_all_tbl()
        return fetch_chg_brush_date(ent, dbch, tbl, params, type)


def format_props(cols, data):
    c = list(cols.keys())
    t1, t2, p1, p2, a1, a2 = -1, -1, -1, -1, -1, -1
    if 'trade_props.name' in c:
        t1, t2 = c.index('trade_props.name'), c.index('trade_props.value')

    if 'props.name' in c:
        p1, p2 = c.index('props.name'), c.index('props.value')

    if 'clean_props.name' in c:
        a1, a2 = c.index('clean_props.name'), c.index('clean_props.value')

    if 'c_props.name' in c:
        a1, a2 = c.index('c_props.name'), c.index('c_props.value')

    pcols, ret = [], []
    for v in data:
        v = list(v)
        v, ev = v[0:len(c)], v[len(c):]
        if t1 != -1:
            c[t1] = 'trade_props'
            v[t1] = json.dumps({k:v[t2][i] for i, k in enumerate(v[t1]) if v[t2][i] != ''}, ensure_ascii=False)
        if p1 != -1:
            for p in v[p1]:
                if p not in pcols:
                    pcols.append(p)
            pv = ['']*len(pcols)
            for i, vv in enumerate(v[p2]):
                if pv[pcols.index(v[p1][i])] != '':
                    pv[pcols.index(v[p1][i])]+= ', '
                pv[pcols.index(v[p1][i])] += vv
            v += pv
        if a1 != -1:
            for p in v[a1]:
                if p not in pcols:
                    pcols.append(p)
            pv = ['']*len(pcols)
            for i, vv in enumerate(v[a2]):
                if pv[pcols.index(v[a1][i])] != '':
                    pv[pcols.index(v[a1][i])]+= ', '
                pv[pcols.index(v[a1][i])] += vv
            v += pv
        v = [vv for i,vv in enumerate(v) if i not in [t2,p1,p2,a1,a2]] + ev
        ret.append(v)
    return [col for i,col in enumerate(c) if i not in [t2,p1,p2,a1,a2]]+pcols, ret


def fetch_data_totmptbl(ent, dba, csql, fsql, p, cols):
    tbl = 'artificial.fetch_data_tmp_' + str(random.random()).replace('0.','')

    sql = 'CREATE TABLE {} ENGINE = Log AS '.format(tbl)+csql
    dba.execute(sql)

    sql = 'INSERT INTO {} '.format(tbl)+fsql
    dba.execute(sql, p)

    sql = 'SELECT * FROM {}'.format(tbl)
    ret = dba.query_all(sql)
    cols, ret = format_props(cols, ret)

    for v in ret:
        for ii, vv in enumerate(v):
            if len(cols) > ii and cols[ii] in ('clean_pid', 'alias_pid'):
                v[ii] = format_data(ent, cols[ii], vv)

    ret = {(i, str(v[-1])): list(v[:-1]) for i, v in enumerate(ret)}
    return cols, ret, tbl


def fetch_data_by_tmptbl(ent, dba, tbl, type, ttbl):
    cols = ent.get_cols(tbl, dba, ['trade_props_full.name','trade_props_full.value'])
    sql = '''
        SELECT `{}`, `uuid2` FROM {} WHERE (source, pkey, `uuid2`) IN (SELECT source, pkey, real_uuid FROM {}) {}
    '''.format('`,`'.join(cols.keys()), tbl, ttbl, 'AND sign = 1' if type == 'A' else '')
    ret = dba.query_all(sql)
    cols, ret = format_props(cols, ret)

    for v in ret:
        for ii, vv in enumerate(v):
            if len(cols) > ii and cols[ii] in ('clean_pid', 'alias_pid'):
                v[ii] = format_data(ent, cols[ii], vv)

    ret = {str(v[-1]): list(v[:-1]) for v in ret}
    cols = [{'title': c, 'width': 100} for c in cols]
    return cols, ret


def fetch_abcde_data(ent, dba, tbl, params, type, tbls=[]):
    cols = ent.get_cols(tbl, dba, ['trade_props_full.name','trade_props_full.value'])
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, ext1, _, _, _ = format_params(ent, type, params, cols, posname)

    if ext1.find('月数据') == 0 or ext1.find('排重') == 0:
        cc = []
        for col in cols.keys():
            if ext1.find('排重') == 0 and col == 'date':
                cc.append('toStartOfMonth(min(date))')
            elif col == 'date':
                cc.append('toStartOfMonth(date)')
            elif col == 'sign':
                cc.append('sum(sign)')
            elif col == 'sales':
                cc.append('sum(sales*sign)')
            elif col == 'num':
                cc.append('sum(num*sign)')
            elif col == 'price':
                cc.append('sum(sales*sign)/sum(num*sign)')
            else:
                cc.append('argMin(`{}`, date)'.format(col))
        if ext1.find('排重') == 0 and orderby.find('date') == 0:
            orderby = orderby.replace('date', 'toStartOfMonth(min(date))')
        elif orderby.find('date') == 0:
            orderby = orderby.replace('date', 'toStartOfMonth(date)')
        elif orderby.find('sales') == 0:
            orderby = orderby.replace('sales', 'sum(sales*sign)')
        elif orderby.find('num') == 0:
            orderby = orderby.replace('num', 'sum(num*sign)')
        elif orderby.find('price') == 0:
            orderby = orderby.replace('price', 'sum(sales*sign)/sum(num*sign)')
        else:
            orderby = ''
        orderby = '' if orderby == '' else 'ORDER BY {}'.format(orderby)
        if ext1 == '排重-分交易属性':
            groupby = 'source, item_id, trade_props.name, trade_props.value'
        elif ext1 == '排重-分宝贝id':
            groupby = 'source, item_id'
        elif ext1 == '月数据-分交易属性':
            groupby = 'source, item_id, trade_props.name, trade_props.value, toStartOfMonth(date)'
        elif ext1 == '月数据':
            groupby = 'source, item_id, toStartOfMonth(date)'
        else:
            groupby = ''
        sql = '''
            SELECT {}, argMin(`uuid2`, date) AS real_uuid FROM {} {} GROUP BY {} {} LIMIT {}
        '''.format(','.join(cc), tbl, where, groupby, orderby, limit)
    else:
        if type == 'A':
            where2 = 'AND uuid2 NOT IN (SELECT uuid2 FROM {} {} AND sign = -1)'.format(tbl, where)
        else:
            where2 = ''
        orderby = '' if orderby == '' else 'ORDER BY {}'.format(orderby)
        sql = '''
            SELECT `{}`, `uuid2` AS real_uuid FROM {} {} {} {} LIMIT {}
        '''.format('`,`'.join(cols.keys()), tbl, where, where2, orderby, limit)

    # 建表用
    csql = 'SELECT `{}`, `uuid2` AS real_uuid FROM {} WHERE 0'.format('`,`'.join(cols.keys()), tbl)

    cols, ret, ttbl = fetch_data_totmptbl(ent, dba, csql, sql, p, cols)
    cols = [{'title': c, 'width': 100} for c in cols]

    for tt in tbls:
        if tt == type:
            continue
        elif tt == 'A':
            dbname, t = ent.get_plugin().get_a_tbl()
        elif tt == 'B':
            dbname, t = ent.get_plugin().get_b_tbl()
        elif tt == 'C':
            dbname, t = ent.get_plugin().get_c_tbl()
        elif tt == 'D':
            dbname, t = ent.get_plugin().get_d_tbl()
        elif tt == 'E':
            dbname, t = ent.get_plugin().get_e_tbl()
        else:
            continue

        c, r = fetch_data_by_tmptbl(ent, dba, t, tt, ttbl)
        cols = cols + [{'title': tt+'表数据', 'width': 100}] + c
        for (i, k) in ret:
            if k not in r:
                ret[(i, k)] = ret[(i, k)] + [''] + [''] * len(c)
            else:
                ret[(i, k)] = ret[(i, k)] + [''] + r[k]

    sql = 'DROP TABLE {}'.format(ttbl)
    dba.execute(sql)

    cfg = {}

    return [[type, cols, list(ret.values()), cfg]]


def fetch_data(ent, dba, tbl, params, type, cfg=None):
    poslist = ent.get_poslist()
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = 'SELECT `{}` FROM {} {} LIMIT {}'.format('`,`'.join(cols.keys()), tbl, where, limit)
    sql = sql.replace('(%)', '(%%)')
    ret = dba.query_all(sql, p)

    data = []
    for v in ret:
        tmp = []
        for vv in v:
            tmp.append(str(vv))
        data.append(tmp)

    cols = [{'title': c, 'width': 200 if len(cols) < 5 else 100} for c in cols]
    cfg = cfg or {}

    return [[type, cols, data, cfg]]


def fetch_shop_data(ent, dba, tbl, params, type):
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = '''
        SELECT `source`, sid, url, closed, createTime FROM {}
        {} AND eid = {} ORDER BY createTime DESC LIMIT 1 BY `source`, sid
    '''.format(tbl, where, ent.eid)
    ret = dba.query_all(sql, p)

    cols = ['source', 'sid', 'url', 'closed', 'createTime']
    cols = [{'title': c, 'width': 200 if len(cols) < 5 else 100} for c in cols]
    cfg = {}

    return [[type, cols, ret, cfg]]


def get_stat(url):
    r = requests.get(url=url,headers=header)
    for his in r.history:
        if his.headers['location'].find('noshop') != -1:
            return 1

    if r.status_code == 200 and len(r.history) == 0:
        return 0

    return 2


def fetch_shop_bysid(params, type):
    sids = []
    if 'sid = ' in params:
        sids.append(params['sid = '])
    else:
        sids = params['sid in ']

    header = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'cookie': 'enc=KFit3SeeZ5%2BTiEmorDyQIs9cV4CJYZAZlF8icl5%2FcSsnx7q15yN1ZRLhTRTTn5oPeAVaYLK6IJJxEGVeZ1wbCQ%3D%3D; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; cna=UDCqGKs4tnsCAXxPTDc/Edf0; tracknick=juno3960; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0; cookie2=15743e767d6bd859797fbfbddf0556c2; t=76c63656d6bc3934be3197c3089f33f4; _samesite_flag_=true; lgc=juno3960; cancelledSubSites=empty; dnk=juno3960; v=0; _tb_token_=ee83b56d301aa; xlly_s=1; _m_h5_tk=c14dc9700429a4834aaa6663a87ca0c7_1632886584091; _m_h5_tk_enc=ef4c07cb632596f1de890ccd55927ab7; unb=1795122777; cookie17=UoYXKa8H1D4EsA%3D%3D; _l_g_=Ug%3D%3D; sg=075; _nk_=juno3960; cookie1=Vvbb4I5vFaEMNARJhnY75%2F3va5gvb%2B7HjfZgHEtU2%2Bk%3D; mt=ci=68_1; sgcookie=E100YEoCAAkjXPSrm%2BiC65dAXOfCibWefiZC2VD5GMt18eiEe8mw1c%2FIpc3dvb3y7fq%2FmOywBwHIlDZRmHz0y8p6Aex8P7uNQaENaqIVyZkSL0s%3D; uc3=id2=UoYXKa8H1D4EsA%3D%3D&lg2=VFC%2FuZ9ayeYq2g%3D%3D&nk2=CcYsWs3hDYE%3D&vt3=F8dCujaPmmj8LBhz65k%3D; csg=b70f716d; skt=9ffa90c585db65d0; existShop=MTYzMjg4MzA2NA%3D%3D; uc4=nk4=0%40C%2FnHNFoCejbdSzjvbBWbPIRBzg%3D%3D&id4=0%40UO6eyJeJteQBziBHwbN9Iteeo9%2B5; _cc_=UtASsssmfA%3D%3D; uc1=cookie16=WqG3DMC9UpAPBHGz5QBErFxlCA%3D%3D&cookie15=W5iHLLyFOGW7aA%3D%3D&existShop=false&pas=0&cookie21=UIHiLt3xThH8t7YQoFNq&cart_m=0&cookie14=Uoe3dYlRhw4psg%3D%3D; l=eBS_-6r4gi0ls8tSBOfZourza77T7IRAguPzaNbMiOCPO95k52COW6ecgXLDCnGVhs_6R3kQee-kBeYBqoXYB53Ee5DDwIMmn; tfstk=cREFB7OLaMIUtci5kDiPVX4WwLbdZLw3Ehkj-y4YDNAcNYuhiOO-Iz-iQYGAj2f..; isg=BG1tMic38J9KL5Tz2D96rLbtfAnnyqGcS7fYpK9yt4RzJo3YdxlQbAB0FPrAprlU',
        'referer': 'https://www.taobao.com/',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    }
    db39 = app.get_db('47_apollo')
    db39.connect()

    ret = []
    for sid in sids:
        sql = 'SELECT tb_sid FROM apollo.shop WHERE sid = {}'.format(sid)
        rrr = db39.query_all(sql)
        url = 'https://shop{}.taobao.com/'.format(rrr[0][0])
        closed = get_stat(url)
        ret.append([sid, url, closed])
        time.sleep(2)

    cols = ['sid', 'url', 'closed']
    cols = [{'title': c, 'width': 200 if len(cols) < 5 else 100} for c in cols]
    cfg = {}

    return [[type, cols, ret, cfg]]



def fetch_aver(ent, dba, tbl, params, type):
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    sql = 'SELECT source FROM {} GROUP BY source'.format(tbl)
    rrr = dba.query_all(sql)

    db26 = ent.get_db('26_apollo')
    where, p, limit, orderby, ext1, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = 'SELECT DATE_ADD(start_month ,INTERVAL 1 month) , end_month , is_mixed, is_tb FROM dataway.entity WHERE id = {}'.format(ent.eid)
    ret = db26.query_all(sql)
    smonth, emonth, wtf, hastb = ret[0][0], ret[0][1], '2' if ret[0][2] else '', ret[0][3]

    sql = 'SELECT only_brand, only_shop FROM dataway.entity_tip{wtf} WHERE eid = {}'.format(ent.eid, wtf=wtf)
    ret = db26.query_all(sql)
    only_brand, only_shop = ret[0][0], ret[0][1]

    res = []
    for source, in rrr:
        params['source = '] = source

        where2, p, _, _, _, _, _, _ = format_params(ent, type, params, cols, posname)

        s_name = ['','ali','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy'][int(source)]

        sql = 'SELECT cid, key_name, `not` FROM dataway.entity_key_words{wtf} WHERE eid = {} AND source = \'{}\' AND tip = 1'.format(ent.eid, s_name, wtf=wtf)
        ret = db26.query_all(sql)

        namein, namenotin = {}, {}
        for cid, key_name, n, in ret:
            if cid not in namein:
                namein[cid] = []
                namenotin[cid] = []
            if n == 1:
                namenotin[cid].append(key_name)
            else:
                namein[cid].append(key_name)

        sql = 'SELECT cid FROM dataway.entity_tip_category{wtf} WHERE eid = {} AND source = \'{}\' AND tip = 1'.format(ent.eid, s_name, wtf=wtf)
        ret = db26.query_all(sql)
        cids = [v[0] for v in ret]

        sql = 'SELECT sid FROM dataway.entity_sid{wtf} WHERE eid = {} AND source = \'{}\' AND tip = 1'.format(ent.eid, s_name, wtf=wtf)
        ret = db26.query_all(sql)
        sids = [str(v[0]) for v in ret]

        sql = '''
            SELECT IF(alias_bid=0,bid,alias_bid) FROM brush.all_brand WHERE bid IN (
                SELECT alias_all_bid FROM dataway.entity_brand_limit{wtf} WHERE eid = {} AND tip = 1
            )
        '''.format(ent.eid, wtf=wtf)
        ret = db26.query_all(sql)
        bids = [str(v[0]) for v in ret]

        fsql = []
        for cid in cids:
            s = '(cid = {}'.format(cid)
            if cid in namein and len(namein[cid]) > 0:
                s += ' AND multiSearchFirstPosition(lower(name), [\'{}\'])'.format('\',\''.join(namein[cid]))
            if cid in namein and len(namenotin[cid]) > 0:
                s += ' AND NOT multiSearchFirstPosition(lower(name), [\'{}\'])'.format('\',\''.join(namenotin[cid]))
            s += ')'
            fsql.append(s)
        fsql = ['('+' OR '.join(fsql)+')']

        source_brand_type = 'toUInt32'
        if source == 6:
            source_brand_type = 'toString'

        if only_brand and len(bids) > 0:
            s = '''dictGetUInt32('{}_brand_has_alias_bid_month', 'alias_bid', tuple({}(brand))) IN ({})'''.format(s_name, source_brand_type, ','.join(bids))
            fsql.append(s)

        if only_shop and len(sids) > 0:
            s = '''sid IN ({})'''.format(','.join(sids))
            fsql.append(s)

        if ent.bid == 131:
            s = '''sid = 10566463'''
            fsql.append(s)

        fsql = (' OR ' if wtf else ' AND ').join(fsql) or 1

        if ext1 == 'brand':
            fcola = '''dictGetUInt32('{}_brand_has_alias_bid_month', 'alias_bid', tuple({}(brand)))'''.format(s_name, source_brand_type)
            fcola = '''IF(dictGetUInt32('all_brand', 'alias_bid', tuple({f}))=0,{f},dictGetUInt32('all_brand', 'alias_bid', tuple({f})))'''.format(f=fcola)
            fcolb = '''dictGetUInt32('all_brand', 'alias_bid', tuple({}(all_bid)))'''.format('toUInt32')
        else:
            fcola = fcolb = ext1

        chsql = app.connect_clickhouse_http('chsql')
        a, b = tbl.split('.')
        sql = '''
            SELECT toYYYYMM(pkey) m, toString({}) a,
                ROUND(sum(sales*sign)/100, 2) b, sum(num*sign) c,
                groupUniqArray(ver) v
            FROM {}.trade {} AND ({}) AND w_start_date('{}') AND w_end_date_exclude('{}') AND platformsIn('all')
            GROUP BY m, a ORDER BY m, a
        '''.format(fcola, s_name, where, fsql, smonth, emonth)
        # where.replace('WHERE', 'AND').replace('ver', '\'0\'', 1), ' AND shop_type >= 20' if source==1 and not hastb else '', addfsql, w=where2, p=fcol)
        rr1 = chsql.query_all(sql % {k:'\'{}\''.format(p[k]) for k in p})
        rr1 = {(v['m'],v['a']):[v['m'],v['a'],v['b'],v['c'],eval(v['v'])] for v in rr1}

        sql = '''
            SELECT toYYYYMM(pkey) m, toString({}) a,
                ROUND(sum(sales*sign)/100, 2) b, sum(num*sign) c,
                groupUniqArray(ver) v
            FROM {} {}
            GROUP BY m, a ORDER BY m, a
        '''.format(fcolb, tbl, where2)
        rr2 = dba.query_all(sql, p)
        rr2 = {(v[0],v[1]):v for v in rr2}

        key = list(set(list(rr1.keys())+list(rr2.keys())))
        key.sort(key=lambda x: (x[0], x[1]))

        ret = []
        for k in key:
            if k not in rr1:
                ret.append([
                    rr2[k][0], rr2[k][1], 'NONE', rr2[k][2], '∞', 'NONE', rr2[k][3], '∞', rr2[k][-1]
                ])
                continue
            if k not in rr2:
                ret.append([
                    rr1[k][0], rr1[k][1], rr1[k][2], 'NONE', '∞', rr1[k][3], 'NONE', '∞', rr1[k][-1]
                ])
                continue

            d = list(set(rr1[k][-1]) - set(rr2[k][-1])) + list(set(rr2[k][-1]) - set(rr1[k][-1]))
            if len(d) > 0:
                ret.append([
                    rr1[k][0], rr1[k][1],
                    rr1[k][2], rr2[k][2], round((rr1[k][2]-rr2[k][2])/max(rr1[k][2],1)*100, 5),
                    rr1[k][3], rr2[k][3], round((rr1[k][3]-rr2[k][3])/max(rr1[k][3],1)*100, 5),
                    d
                ])

        vers = []
        for v in ret:
            vers += v[-1]

        if str(source) == '1' and len(vers) > 0:
            db39 = ent.get_db('47_apollo')
            sql = 'SELECT version, description FROM apollo.trade_fix_task WHERE version IN ({})'.format(','.join([str(v) for v in vers]))
            rrr = db39.query_all(sql)
            vvv = {str(v[0]): v[1] for v in rrr}

            sql = 'SELECT add_version, description FROM apollo.trade_fix_task WHERE add_version IN ({})'.format(','.join([str(v) for v in vers]))
            rrr = db39.query_all(sql)
            vvv.update({str(v[0]): v[1] for v in rrr})

            vers = vvv
        elif str(source) == '2' and len(vers) > 0:
            db14 = ent.get_db('68_apollo')
            sql = 'SELECT version, description FROM jdnew.trade_fix_task WHERE version IN ({})'.format(','.join([str(v) for v in vers]))
            rrr = db14.query_all(sql)
            vers = {str(v[0]): v[1] for v in rrr}
        else:
            vers = {}

        for v in ret:
            v = list(v)

            if ext1 == 'brand':
                col = 'alias_all_bid'
            elif ext1 == 'cid':
                col = 'cid'
                v[1] = '[{},{}]'.format(source, v[1])
            elif ext1 == 'sid':
                col = 'sid'
                v[1] = '[{},{}]'.format(source, v[1])
            else:
                col = ext1
            v[1] = format_data(ent, col, str(v[1]))

            vv = []
            for ver in v[-1]:
                vs = vers[str(ver)] if str(ver) in vers else ''
                vv.append('v:{} {}'.format(ver, vs or '没找到版本记录'))
            v[-1] = '<br />'.join(vv)

            res.append([s_name] + v)

    cols = [
        {'title': 'source', 'type': 'text', 'width': '200px'},
        {'title': 'month', 'type': 'text', 'width': '200px'},
        {'title': ext1, 'type': 'text', 'width': '200px'},
        {'title': '底层表 sales', 'type': 'text', 'width': '200px'},
        {'title': 'A表 sales', 'type': 'text', 'width': '200px'},
        {'title': 'sales rate%', 'type': 'text', 'width': '200px'},
        {'title': '底层表 num', 'type': 'text', 'width': '200px'},
        {'title': 'A表 num', 'type': 'text', 'width': '200px'},
        {'title': 'num rate%', 'type': 'text', 'width': '200px'},
        {'title': '新增版本', 'type': 'text', 'width': '500px'}
    ]
    cfg = {'search': False, 'filters': False, 'columnSorting': False, 'defaultColWidth':100}

    return [[type, cols, res, cfg]]


def fetch_a_growth(ent, dba, tbl, params, type):
    if 'source = ' in params:
        source = params['source = ']
    else:
        source = None
    month = params['month = ']
    ext1 = params['ext1']

    if ext1 != '不更新':
        poslist = ent.get_poslist()
        sp = ['sp{}'.format(pos) for pos in poslist if poslist[pos]['type'] == 900]
        update_a_growth(ent, sp[0], month)

    _, ctbl = ent.get_plugin().get_c_tbl()

    dd = datetime.datetime.strptime(month, "%Y%m")
    d  = dd + datetime.timedelta(days=40)
    day_range = 11

    cols = ['月份'+dd.strftime('%Y-%m'), '最后更新时间', '当前的total 单位分'] + [d.strftime('%Y-%m-{:0>2d}'.format(day_range-i)) for i in range(day_range)]

    sql = '''
        SELECT sp, (source, ver), sales, num, last_created FROM artificial.a_growth WHERE eid = {} AND month = {} {}
    '''.format(ent.eid, month, '' if source is None else 'AND source = {}'.format(source))
    rrr = dba.query_all(sql)

    vers = [v[1] for v in rrr]
    vers = aver_info(ent, vers)

    tmp = {}
    for sp, ver, ss, sn, last_created, in rrr:
        if sp not in tmp:
            tmp[sp] = {'sumsales':0, 'sumnum':0}
        if last_created.strftime('%Y-%m-%d') not in tmp[sp]:
            tmp[sp][last_created.strftime('%Y-%m-%d')] = []
        tmp[sp][last_created.strftime('%Y-%m-%d')].append([ver, ss, sn, last_created])
        tmp[sp]['sumsales'] += ss
        tmp[sp]['sumnum'] += sn

    res = []
    for sp in tmp:
        data_a, data_b, data_c, chg_sales, chg_num, last_created = [], [], [], 0, 0, datetime.datetime(1970,1,1,0,0,0)
        for i in range(day_range):
            dd = d.strftime('%Y-%m-{:0>2d}'.format(day_range-i))
            st1, st2, st3 = 0, 0, ''

            if dd in tmp[sp]:
                st1 = sum([v[1] for v in tmp[sp][dd]])
                st2 = sum([v[2] for v in tmp[sp][dd]])
                st3 = '\n'.join(['{} sales:{} num:{}'.format(
                    '没找到版本记录{}'.format(v[0][1]) if v[0] not in vers else vers[v[0]], v[1], v[2]
                ) for v in tmp[sp][dd]])
                last_created = max(last_created, max([v[3] for v in tmp[sp][dd]]))

            chg_sales -= st1
            chg_num -= st2

            data_a.append(str(round(chg_sales/max(1, tmp[sp]['sumsales'])*100, 3))+'%')
            data_b.append(str(round(chg_num/max(1, tmp[sp]['sumnum'])*100, 3))+'%')
            data_c.append(st3)

        res.append([sp+' sales', str(last_created), tmp[sp]['sumsales']] + data_a)
        res.append([sp+' num', '', tmp[sp]['sumnum']] + data_b)
        res.append([sp+' ver', '', ''] + data_c)

    cols = [{'title': c, 'type': 'text', 'width': '200px'} for c in cols]
    cfg = {'search': False, 'filters': False, 'columnSorting': False, 'defaultColWidth':100}

    return [[type, cols, res, cfg]]


def update_a_growth(ent, sp, month):
    d = datetime.datetime.strptime(month, "%Y%m")

    dname, atbl = ent.get_plugin().get_a_tbl()
    dname, ctbl = ent.get_plugin().get_c_tbl()
    dba = ent.get_db(dname)

    sql = 'ALTER TABLE artificial.a_growth DROP PARTITION ({}, {})'.format(ent.eid, month)
    dba.execute(sql)

    sql = '''
        INSERT INTO artificial.a_growth (eid, month, sp, source, ver, sales, num, last_created)
        WITH IF(source=1 AND (shop_type < 20 AND shop_type > 10), 0, source) AS nsource
        SELECT {}, {}, ifNull(sp, ''), nsource, ver, sum(ss), sum(sn), max(created)
        FROM (
            SELECT `source`, `date`, `item_id`, `trade_props.name`, `trade_props.value`, sales*sign ss, num*sign sn, shop_type, ver, created
            FROM {t} WHERE pkey >= '{smonth}' AND pkey < '{emonth}'
        ) c
        LEFT JOIN (
            SELECT `source`, `date`, `item_id`, `trade_props.name`, `trade_props.value`, any({c}) AS sp
            FROM (
                SELECT uuid2, {c} FROM {} WHERE pkey >= '{smonth}' AND pkey < '{emonth}'
            ) a
            JOIN (
                SELECT `source`, `date`, `item_id`, `trade_props.name`, `trade_props.value`, `uuid2`
                FROM {t} WHERE pkey >= '{smonth}' AND pkey < '{emonth}'
            ) b
            USING (uuid2)
            GROUP BY `source`, `date`, `item_id`, `trade_props.name`, `trade_props.value`
        ) d
        USING (`source`, `date`, `item_id`, `trade_props.name`, `trade_props.value`)
        GROUP BY sp, nsource, ver
    '''.format(
        ent.eid, month, ctbl, t=atbl, c=sp, smonth=d.strftime('%Y-%m-01'), emonth=d.strftime('%Y-%m-31')
    )
    dba.execute(sql)


# 答题项目
def fetch_brush(ent, dba, tbl, params, type):
    poslist = ent.get_poslist()
    cols = ent.get_cols(tbl, dba, ['eid', 'spid'])
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = '''
        SELECT `eid`,`name`,`waibu_flag`,`type`,`split`, `price_correct`,`ab_check`,`created` FROM {} WHERE eid = {}
    '''.format(tbl, ent.eid)
    ret = dba.query_all(sql)

    cols = [
        { 'title': 'eid', 'width': 100 },
        { 'title': 'name', 'width': 200 },
        { 'title': 'waibu_flag', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0','name':'内部答题'},{'id':'1','name':'外部答题'}] },
        { 'title': 'type', 'width': 100, 'type': 'dropdown', 'source': [{'id':'4','name':'属性答题'},{'id':'5','name':'sku答题'}] },
        { 'title': 'split', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0','name':'不拆套包'},{'id':'1','name':'拆套包'}] },
        { 'title': 'price_correct', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0','name':'关闭建议价'},{'id':'1','name':'开启建议价'}] },
        { 'title': 'ab_check', 'width': 100, 'type': 'dropdown', 'source': ['0','1'] },
        { 'title': 'created', 'width': 200 }
    ]

    cfg = {'pkey': 0}

    return [[type, cols, ret, cfg]]


# 答题属性
def fetch_brush_props(ent, dba, tbl, params, type):
    poslist = ent.get_poslist()
    cols = ent.get_cols(tbl, dba, ['eid', 'spid'])
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = '''
        SELECT `spid`,`tip`,`name`, `choice_value`,`choice_content`, `define`, `order`,`show_in_brush`,`created`,`group_number`,`copy_used`,`copy_check`, `create_copy` FROM {} WHERE eid = {}
    '''.format(tbl, ent.eid)
    ret = dba.query_all(sql)

    cols = [
        {'title': 'spid', 'width': 100},
        {'title': 'tip', 'width': 100},
        {'title': 'name', 'width': 100},
        {'title': 'choice_value', 'width': 300, 'group': 'a', 'grouptype': 'str'},
        {'title': 'choice_content', 'width': 300, 'group': 'a', 'grouptype': 'str'},
        {'title': 'define', 'width': 100, 'type': 'dropdown', 'source': [{'id':'str','name':'填空'},{'id':'enum','name':'单选'},{'id':'multi_enum','name':'多选'}]},
        {'title': 'order', 'width': 100},
        {'title': 'show_in_brush', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0','name':'都显示'},{'id':'1','name':'仅属性'},{'id':'2','name':'仅sku'}] },
        {'title': 'created', 'width': 100},
        {'title': 'group_number', 'width': 100},
        {'title': 'copy_used', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0', 'name':'不复制'},{'id':'1', 'name':'复制'}]},
        {'title': 'copy_check', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0', 'name':'不高亮'},{'id':'1', 'name':'高亮'}]},
        {'title': 'create_copy', 'width': 100, 'type': 'dropdown', 'source': [{'id':'0','name':'创建时不赋值'},{'id':'1','name':'创建时赋值'}] },
    ]

    cfg = {'pkey': 0}

    return [[type, cols, ret, cfg]]


# 级联检查
def fetch_brush_relation(ent, dba, tbl, params, type):
    ext1 = params['ext1']

    if ext1 == '检查当前项目':
        ret = [[ent.bid, ent.eid]]
    else:
        sql = 'SELECT batch_id, eid FROM cleaner.clean_batch WHERE eid > 0 AND eid NOT IN (90474, 1)'
        ret = dba.query_all(sql)

    res = []
    for bid, eid, in ret:
        sql = 'SELECT pos_norelay, define_norelay, pos, define FROM dataway.clean_props_relation WHERE eid = {}'.format(eid)
        rrr = dba.query_all(sql)

        for apos, arr, bpos, brr, in rrr:
            arr = json.loads(arr)[0]
            brr = json.loads(brr)
            sql = 'SELECT id, name, spid{}, spid{} FROM product_lib.entity_{}_item WHERE flag > 0'.format(apos, bpos, eid)
            rrrr = dba.query_all(sql)
            for id, n, av, bv, in rrrr:
                if av != arr:
                    continue
                if bv not in brr:
                    k = json.dumps({'eid':eid, 'id':id, 'spa':apos, 'spb':bpos}, ensure_ascii=False)
                    res.append([k, bid, eid, id, n, apos, av, bpos, bv, ','.join(brr)])

    cols = [
        {'title': 'id', 'width': 100},
        {'title': 'batch_id', 'width': 100},
        {'title': 'eid', 'width': 100},
        {'title': 'brush_id', 'width': 100},
        {'title': 'name', 'width': 300},
        {'title': 'Asp', 'width': 100},
        {'title': 'A答了', 'width': 200},
        {'title': 'Bsp', 'width': 100},
        {'title': 'B答了', 'width': 200},
        {'title': 'B只能选', 'width': 300, 'group': 'a', 'grouptype': 'str'}
    ]

    cfg = {'pkey': 0, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['A答了', 'B答了']}

    return [[type, cols, res, cfg]]


# 答题属性级联
def fetch_brush_props_relation(ent, dba, tbl, params, type):
    poslist = ent.get_poslist()
    cols = ent.get_cols(tbl, dba, ['eid', 'id'])
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = '''
        SELECT id,pos_norelay,define_norelay,pos,define,created FROM {} WHERE eid = {}
    '''.format(tbl, ent.eid)
    ret = dba.query_all(sql)

    cols = [
        {'title': 'id', 'width': 100},
        {'title': 'spA', 'name': 'pos_norelay', 'width': 100},
        {'title': '选了[1,2,3]', 'name': 'define_norelay', 'width': 400, 'group': 'a', 'grouptype': 'json'},
        {'title': 'spB', 'name': 'pos', 'width': 100},
        {'title': '只能选[4,5,6]', 'name': 'define', 'width': 400, 'group': 'b', 'grouptype': 'json'},
        {'title': 'created', 'width': 150}
    ]

    cfg = {'pkey': 0}

    return [[type, cols, ret, cfg]]


# e表对比
def fetch_ecompare(ent, dba, etbl, params, type):
    limit = params['limit'] or 100
    limit = min(int(limit), 10000)
    batch_ids = params['batch_ids in '].split(',')

    tbl, eids = 'sop_e.entity_E_compare', []
    for bid in batch_ids:
        ent = entity_manager.get_brush(bid)
        _, etbl = ent.get_plugin().get_e_tbl()
        eids.append(str(ent.eid))

        sql = 'ALTER TABLE {} DROP PARTITION ({})'.format(tbl, ent.eid)
        dba.execute(sql)

        sql = '''
            INSERT INTO {}
            SELECT {}, '{sp}', item_id a,
            toString(arraySort(arrayFilter(x -> x != '', arrayMap((k, v) -> IF(v='', '', CONCAT(k, ':', v)), trade_props.name, trade_props.value)))) b,
            `clean_props.value`[indexOf(`clean_props.name`, '{sp}')] c
            FROM {} WHERE c != '其它' GROUP BY a, b, c
        '''.format(tbl, ent.eid, etbl, sp='子品类')
        dba.execute(sql)

        sql = 'SELECT toString(groupArray(pid)), toString(groupArray(name)) FROM artificial.product_{}'.format(ent.eid)
        ret = dba.query_all(sql)

        if ret[0][0] == '[]' or ret[0][1] == '[]':
            continue

        sql = '''
            INSERT INTO {}
            SELECT {}, '{sp}', item_id a,
            toString(arraySort(arrayFilter(x -> x != '', arrayMap((k, v) -> IF(v='', '', CONCAT(k, ':', v)), trade_props.name, trade_props.value)))) b,
            transform(clean_pid, {}, {}, '') c
            FROM {} GROUP BY a, b, c
        '''.format(tbl, ent.eid, ret[0][0], ret[0][1], etbl, sp='sku')
        dba.execute(sql)

    data = []

    sql = '''
        SELECT sp, item_id, trade_props, toString(arraySort(groupArray(v))) FROM (
            SELECT sp, item_id, trade_props, CONCAT(toString(any(eid)),':',val) v
            FROM {t} WHERE eid IN ({e}) AND sp = '子品类'
            AND (item_id, trade_props) IN (
                SELECT item_id, trade_props FROM {t} WHERE eid IN ({e})
                GROUP BY item_id, trade_props HAVING countDistinct(eid) > 1
            )
            GROUP BY sp, item_id, trade_props, val HAVING countDistinct(eid) = 1
        ) GROUP BY sp, item_id, trade_props LIMIT {}
    '''.format(limit, t=tbl, e=','.join(eids))
    ret = dba.query_all(sql)

    data += list(ret)

    data.append([])

    sql = '''
        SELECT sp, item_id, trade_props, toString(arraySort(groupArray(v))) FROM (
            SELECT sp, item_id, trade_props, CONCAT(toString(any(eid)),':',val) v
            FROM {t} WHERE eid IN ({e}) AND sp = 'sku'
            AND (item_id, trade_props) IN (
                SELECT item_id, trade_props FROM {t} WHERE eid IN ({e})
                GROUP BY item_id, trade_props HAVING countDistinct(eid) > 1
            )
            GROUP BY sp, item_id, trade_props, val HAVING countDistinct(eid) = 1
        ) GROUP BY sp, item_id, trade_props LIMIT {}
    '''.format(limit, t=tbl, e=','.join(eids))
    ret = dba.query_all(sql)

    data += list(ret)

    cols = [
        {'title': 'type', 'width': 100},
        {'title': 'item_id', 'width': 100},
        {'title': 'trade_props', 'width': 300},
        {'title': 'sps', 'width': 300}
    ]
    cfg = {}

    return [[type, cols, data, cfg]]


def fetch_etbl_chgprops(ent, dba, tbl, params, type):
    sql = '''
        WITH argMax(deleteFlag, ver) AS d
        SELECT uuid,
               argMax(`col`, ver),
               argMax(`value.from`, ver),
               argMax(`col_change`, ver),
               argMax(`value.to`, ver),
               argMax(`where`, ver),
               argMax(`order`, ver) o,
               toString(argMax(`created`, ver))
        FROM {} WHERE eid = {} GROUP BY uuid HAVING d = 0 ORDER BY o
    '''.format(tbl, ent.eid)
    ret = dba.query_all(sql)

    res = []
    for v in ret:
        v = list(v)
        v[2] = json.dumps(v[2], ensure_ascii=False, separators=(',',':'))
        v[4] = json.dumps(v[4], ensure_ascii=False, separators=(',',':'))
        res.append(v)

    cols = [
        { 'title': 'uuid'},
        { 'title': 'col'},
        { 'title': 'value.from', 'group': 'a', 'grouptype': 'json' },
        { 'title': 'col_change'},
        { 'title': 'value.to', 'group': 'a', 'grouptype': 'json' },
        { 'title': 'where' },
        { 'title': 'order', 'width': 50 },
        { 'title': 'created' }
    ]
    cfg = {'pkey': 0}

    return [[type, cols, res, cfg]]


def fetch_eproduct(ent, dba, tbl, params, type):
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, limit, orderby, ext1, _, _, _ = format_params(ent, type, params, cols, posname)

    if ext1 != '只看不同的':
        return fetch_data(ent, dba, tbl, params, type, {'pkey': 0, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['name_final','custom_pid']})

    ret = ent.diff_product()
    ret1 = [['<input type="checkbox" class="saveid_'+str(v[0][0])+'"/>', v[0][0],v[0][1] if v[0][1] != v[1][1] else '',v[0][1],v[0][2] if v[0][2] != v[1][2] else '',v[0][2]] for v in ret]
    ret2 = [['<input type="checkbox" class="saveid_'+str(v[0][0])+'"/>', v[0][0],v[0][1] if v[0][1] != v[1][1] else '',v[1][1],v[0][2] if v[0][2] != v[1][2] else '',v[1][2]] for v in ret]

    cols = [
        { 'type':'html', 'width':'50px' },
        { 'title': 'pid', 'width':'100px'},
        { 'title': 'old_all_bid', 'width':'100px'},
        { 'title': 'all_bid', 'width':'100px'},
        { 'title': 'old_name'},
        { 'title': 'name'}
    ]

    cfg = {'pkey': 1, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['all_bid','name']}

    return [[type, cols, ret1, cfg, ret2]]


# 拉sp对不上
def fetch_bspcheck(ent, dba, tbl, params, type):
    dbname, btbl = ent.get_plugin().get_b_tbl()
    dbname, ctbl = ent.get_plugin().get_c_tbl()

    dbch = ent.get_db(dbname)

    cols = ent.get_cols(btbl, dbch)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, limit, orderby, ext1, _, _, _ = format_params(ent, type, params, cols, posname)

    if ext1 == '看全部':
        ext1 = '1'

    if ent.get_entity()['use_all_table']:
        _, ctbl = ent.get_plugin().get_all_tbl()
        plista = ['b_sp{}'.format(pos) for pos in poslist]
        plistb = ['c_sp{}'.format(pos) for pos in poslist]
        sql = '''
            SELECT b_id, {}, {} FROM {} WHERE {}
            AND b_split_rate = 1 AND b_id > 0 AND num > 0 AND b_similarity = 0 LIMIT 1 BY b_id
        '''.format(','.join(plista), ','.join(plistb), ctbl, 'b_{x}!=c_{x}'.format(x=ext1) if ext1!='1' else '1')
        rr1 = dbch.query_all(sql, p)
        rrr = {str(v[0]):[v[1:len(poslist)+1], v[-len(poslist):]] for v in rr1}
    else:
        plist = ['argMax(sp{}, similarity)'.format(pos) for pos in poslist]
        sql = '''
            SELECT brush_id, argMax(uuid2, similarity), argMax(`source`, similarity), argMax(pkey, similarity), argMax({}, similarity), {}
            FROM {} {} AND split_rate = 1 AND similarity >= 1 AND num > 0 GROUP BY brush_id
        '''.format(ext1, ','.join(plist), btbl, where or 'WHERE 1')
        rr1 = dbch.query_all(sql, p)

        if len(rr1) > 0:
            w = ['({},\'{}\',\'{}\')'.format(v[2],v[3],v[1]) for v in rr1]

            plist = ['sp{}'.format(p) for p in poslist]
            sql = '''
                SELECT uuid2, {}, {} FROM {} WHERE (source, pkey, uuid2) IN ({})
            '''.format(ext1, ','.join(plist), ctbl, ','.join(w))
            rr2 = dbch.query_all(sql)
            rr2 = {v[0]: v for v in rr2}

        rrr = {}
        for v in rr1:
            v = list(v)
            if v[1] not in rr2:
                rrr[str(v[0])] = [v[-len(poslist):], ['None'] * len(poslist)]
            elif rr2[v[1]][-len(poslist)-1] == 1 or v[-len(poslist)-1] == 1:
                rrr[str(v[0])] = [v[-len(poslist):], rr2[v[1]][-len(poslist):]]
            elif rr2[v[1]][-len(poslist)-1] != v[-len(poslist)-1]:
                rrr[str(v[0])] = [v[-len(poslist):], rr2[v[1]][-len(poslist):]]

    cols = ['id','pkey','snum','ver','uuid2','tb_item_id','tip','source','month','name','sid','shop_name','shop_type','cid','real_cid','region_str','brand','all_bid','alias_all_bid','super_bid','sub_brand','sub_brand_name','product','avg_price','price','org_price','promo_price','trade','num','sales','visible','visible_check','clean_flag','prop_check','p1','trade_prop_all','prop_all','pid','alias_pid','number','uid','check_uid','b_check_uid','batch_id','flag','split','img','is_set','count','created','modified']
    if rrr:
        sql = 'SELECT {} FROM {} WHERE id IN ({})'.format(','.join(cols+['modify_sp{}'.format(p) for p in poslist]), tbl, ','.join(rrr.keys()))
        ret = dba.query_all(sql)
    else:
        ret = []

    for p in poslist:
        cols += ['sp{}'.format(p), 'spid{}'.format(p), 'modify_sp{}'.format(p)]

    res = []
    for v in ret:
        v1, v2 = list(v[0:-len(poslist)]), v[-len(poslist):]

        for i, p in enumerate(poslist.keys()):
            v1 += [rrr[str(v1[0])][1][i], rrr[str(v1[0])][0][i], v2[i]]
        res.append([str(v) for v in v1])

    cols = [{'title': c, 'width': 100} for c in cols]
    cfg = {'pkey': 0, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['modify_sp{}'.format(p) for p in poslist]}

    return [[type, cols, res, cfg]]

#属性差异查询
def fetch_props_diff(ent, dba, tbl, params, type):
    dbname, etbl = ent.get_plugin().get_e_tbl()

    dbch = ent.get_db(dbname)

    cols = ent.get_cols(tbl, dba)
    # cols = [{'title': 'item_id', 'width': 100}]

    # poslist = ent.get_poslist()
    # posname = [poslist[p]['name'] for p in poslist]
    # where, p, limit, orderby, statCol, statType, calcCol, _ = format_params(ent, type, params, cols, posname)

    source = "source in ({})".format(params['source in '])
    limit = params['limit']

    preRange = params['preRange between '].split(',')
    tmp = []
    for time in preRange:
        tmp.append("'{}'".format(time))
    timeRange1 = "pkey between " + " and ".join(tmp)

    searchRange = params['searchRange between '].split(',')
    tmp = []
    for time in searchRange:
        tmp.append("'{}'".format(time))
    timeRange2 = "pkey between " + " and ".join(tmp)

    fetchCols = ['item_id', 'source', 'cid', 'name', 'pkey', 'sales', 'uuid2', 'clean_props.name', 'clean_props.value'] #
    retCols = fetchCols
    searchKey = params['searchKey = ']
    if searchKey in cols:
        compareCondition = "a.{searchKey} != b.{searchKey}".format(searchKey=searchKey)
    else:
        compareCondition = "a.search_prop != b.search_prop"
        fetchCols.append("`clean_props.value`[indexOf(`clean_props.name`, '{}')] as search_prop".format(searchKey))
        retCols.append("search_prop")

    tmp = retCols
    count = len(retCols)

    for col in tmp[3:(count - 1)]:
        retCols.append(col)

    #b.tb_item_id, a.name as new_name, b.name as old_name, b.source
    sql = '''
        SELECT *
        FROM (SELECT {fetchCols} FROM {table} WHERE {timeRange2}) a left join (SELECT {fetchCols} FROM {table} WHERE {timeRange1}) b using (source, cid, item_id)
        WHERE {compareCondition} LIMIT {limit}
    '''.format(fetchCols=", ".join(fetchCols), table=etbl, timeRange1=timeRange1, timeRange2=timeRange2, compareCondition=compareCondition, limit=limit)
    ret = dba.query_all(sql)

    cols = [{'title':c, 'width': 100} for c in retCols]
    cfg = {}

    return [[type, cols, ret, cfg]]


def fetch_e_diff_aliasbid(ent, dba, tbl, params, type):
    _, etbl = ent.get_plugin().get_e_tbl()
    tbl = tbl or etbl

    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, _, _, _, _, _, _ = format_params(ent, type, params, cols, posname)

    sql = '''
        WITH dictGet('all_brand', 'alias_bid', tuple(all_bid)) AS alias_bid
        SELECT all_bid, alias_all_bid, alias_bid, min(`date`), max(`date`), sum(sales)
        FROM {} {} AND alias_bid > 0 AND alias_all_bid != alias_bid
        GROUP BY all_bid, alias_all_bid, alias_bid ORDER BY alias_all_bid
    '''.format(tbl, where)
    ret = dba.query_all(sql, p)

    bids = []
    for a, b, c, _, _, _, in ret:
        bids += [str(a), str(b), str(c)]

    if len(bids) == 0:
        return [[type, [], [], {}]]

    db26 = ent.get_db('26_apollo')
    sql = '''
        SELECT bid, name, name_cn, name_en, name_cn_front, name_en_front, source, modified, created
        FROM brush.all_brand WHERE bid IN ({})
    '''.format(','.join(bids))
    tmp = db26.query_all(sql)
    bids = {v[0]: list(v) for v in tmp}

    res = []
    for b1, b2, b3, sm, em, sales, in ret:
        res.append([b1] + bids[b1][1:] + [b2] + bids[b2][1:4] + [b3] + bids[b3][1:4] + [sm, em, sales])

    cols = ['bid', 'name', 'name_cn', 'name_en', 'name_cn_front', 'name_en_front', 'source', 'modified', 'created', 'E_alias_bid', 'name', 'name_cn', 'name_en', 'last_alias_bid', 'name', 'name_cn', 'name_en', 'start_month', 'end_month', 'sales']
    cols = [{'title':c, 'width': 100} for c in cols]
    cfg = {}

    return [[type, cols, res, cfg]]


def fetch_e_diff(ent, dba, tbl, params, type):
    _, etbl = ent.get_plugin().get_e_tbl()
    tbl = tbl or etbl

    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, limit, orderby, ext1, _, _, cmptbl = format_params(ent, type, params, cols, posname)

    sql = 'SELECT `clean_props.name` FROM {} LIMIT 1'.format(tbl)
    ret = dba.query_all(sql)
    props = ret[0][0]

    cols = ['source', 'date', 'cid', 'item_id', 'name', 'sid', 'shop_type', 'alias_all_bid', 'price', 'org_price', 'promo_price', 'img', 'sales', 'num', 'clean_pid', 'created']

    sql = "SELECT DISTINCT (`source`, toString(`pkey`)) FROM {} UNION DISTINCT SELECT DISTINCT (`source`, toString(`pkey`)) FROM {};".format(tbl, cmptbl)
    data = dba.query_all(sql)
    ret = []
    for row in data:
        source, pkey = row[0]
        sql = '''
            SELECT * FROM (
                WITH [{props}] AS colnames, arrayMap(i->CONCAT(colnames[i], '：', a.props[i], ' -> ', b.props[i]), arrayFilter((i) -> a.props[i] != b.props[i], arrayEnumerate(colnames))) AS r
                SELECT uuid2, alias_pid, IF(isNull(a.uid), 'NULL', ''), IF(isNull(b.uid), 'NULL', ''), {}, arrayStringConcat(r, '\n') AS rr
                FROM (
                    SELECT uuid2, alias_pid, {cols}, arrayMap(x -> `clean_props.value`[indexOf(`clean_props.name`, x)], [{props}]) AS props
                    FROM {} {where} AND source={source} AND pkey='{pkey}'
                ) a
                FULL OUTER JOIN (
                    SELECT uuid2, alias_pid, {cols}, arrayMap(x -> `clean_props.value`[indexOf(`clean_props.name`, x)], [{props}]) AS props
                    FROM {} {where} AND source={source} AND pkey='{pkey}'
                ) b
                USING (uuid2, alias_pid)
                WHERE LENGTH (r) > 0
            ) ORDER BY rr LIMIT 1 BY rr
        '''.format(','.join(['ifNull(a.{c}, b.{c})'.format(c=c) for c in cols]), tbl, cmptbl, cols=','.join(cols), props=','.join(['\'{}\''.format(p) for p in props]), where=where, pkey=pkey, source=source)
        ret += dba.query_all(sql, p)

    cols = [{'title':c, 'width': 100} for c in ['uuid2', 'pid', 'etbl', 'tbl']+cols+['diff col']]
    cfg = {}

    return [[type, cols, ret, cfg]]


def fetch_chg_brush_date(ent, dba, tbl, params, type):
    ptbl = ent.get_product_tbl()
    btbl = ent.get_tbl()
    db26 = ent.get_db('26_apollo')

    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, limit, orderby, ext1, _, _, cmptbl = format_params(ent, type, params, cols, posname)

    sql = '''
        SELECT `source`, item_id, trade_props_arr, b_id, date, name FROM {t}
        WHERE (`source` , item_id , trade_props_arr) IN (
            SELECT `source` , item_id , trade_props_arr FROM {t}
            WHERE b_id > 0 GROUP BY `source` , item_id , trade_props_arr
            HAVING countDistinct(b_id) > 1 AND countDistinct(name) > 1
        ) ORDER BY `source`, item_id, trade_props_arr, `date`
    '''.format(t=tbl)
    ret = dba.query_all(sql)

    res, a, b, c, d, e = [], None, None, None, None, []
    for source, item_id, trade, bid, date, name, in ret:
        if (a, b, c) != (source,item_id,trade):
            a, b, c, d, e = source, item_id, trade, bid, [[name, date]]
            continue

        if d != bid and d > 0 and bid > 0:
            if len(e) == 1 or name != e[-1][0]:
                res.append([bid, a, b, str(c), str(date), str(d), str(bid), '', name, '', ''])
            else:
                res.append([bid, a, b, str(c), str(date), str(d), str(bid), str(e[-1][1]), e[-2][0], name, str(e[-1][1])])

            d = bid

        if e[-1][0] != name:
            e.append([name, date])

    ret1 = [['<input type="checkbox" class="saveid_'+str(v[0])+'"/>' if v[-1]!='' else ''] + v[:-1] + [''] for v in res]
    ret2 = [['<input type="checkbox" class="saveid_'+str(v[0])+'"/>' if v[-1]!='' else ''] + v for v in res]

    cols = [
        { 'type':'html', 'width':'50px' },
        { 'title': '答题id', 'width': 100 },
        { 'title': 'source', 'width': 60 },
        { 'title': 'item_id', 'width': 100 },
        { 'title': '交易属性', 'width': 100 },
        { 'title': '答题改变日', 'width': 100 },
        { 'title': '答题旧', 'width': 100 },
        { 'title': '答题新', 'width': 100 },
        { 'title': '宝贝名改变日', 'width': 100 },
        { 'title': '宝贝名旧', 'width': 200 },
        { 'title': '宝贝名新', 'width': 200 },
        { 'title': '出题日期修改成', 'width': 100 },
    ]

    cfg = {'pkey': 1, 'allowInsertRow': False, 'allowDeleteRow': False, 'allowModifyCol': ['出题日期修改成']}

    return [[type, cols, ret1, cfg, ret2]]


def format_params(ent, type, params, cols, posname, stat=False):
    cols = {k:cols[k] for k in cols}
    params = json.loads(json.dumps(params))
    where = []

    if 'cmptbl' in params:
        cmptbl = params['cmptbl']
        del params['cmptbl']
    else:
        cmptbl = ''

    if 'limit' in params:
        limit = params['limit'] or 100
        del params['limit']
    else:
        limit = 0

    if 'orderby' in params:
        orderby = params['orderby']
        del params['orderby']
    else:
        orderby = ''

    if 'ext1' in params:
        ext1 = params['ext1']
        del params['ext1']
    else:
        ext1 = ''

    if 'ext2' in params:
        ext2 = params['ext2']
        del params['ext2']
    else:
        ext2 = ''

    if 'ext3' in params:
        ext3 = params['ext3']
        del params['ext3']
    else:
        ext3 = ''

    ext1, tmp = ext1.split(','), []
    for col in ext1:
        if col.replace('交易-', '') != col:
            col = '`trade_props.value`[indexOf(`trade_props.name`, \'{}\')]'.format(col.replace('交易-', ''))
        if col.replace('宝贝-', '') != col:
            # col = '`props.value`[indexOf(`props.name`, \'{}\')]'.format(col.replace('宝贝-', ''))
            col = 'arrayStringConcat(arrayFilter((k, x) -> x = \'{}\', `props.value`, `props.name`), \', \')'.format(col.replace('宝贝-', ''))
        if col.replace('清洗-', '') != col and type[0] == 'D':
            # col = '`c_props.value`[indexOf(`c_props.name`, \'{}\')]'.format(col.replace('清洗-', ''))
            col = 'arrayStringConcat(arrayFilter((k, x) -> x = \'{}\', `c_props.value`, `c_props.name`), \', \')'.format(col.replace('清洗-', ''))
        if col.replace('清洗-', '') != col and type[0] == 'E':
            # col = '`clean_props.value`[indexOf(`clean_props.name`, \'{}\')]'.format(col.replace('清洗-', ''))
            col = 'arrayStringConcat(arrayFilter((k, x) -> x = \'{}\', `clean_props.value`, `clean_props.name`), \', \')'.format(col.replace('清洗-', ''))
        tmp.append(col)
    ext1 = tmp if stat else tmp[0]

    if 'sales' in cols:
        cols['sales/num/100'] = cols['sales']
    cols['toYYYYMM(date)'] = 'UInt32'

    p = {}
    for c in params:
        cname, opr, v = c.split(' ')[0], c.split(' ')[1], params[c]
        if cname == '':
            continue
        if cname in ['uuid','uuid2'] and str(v) in ['0','']:
            continue
        if cname == '价格段（元）':
            cname = 'sales/num/100'
        if cname == 'month':
            cname = 'toYYYYMM(date)'
        c = cname + ' ' + opr
        if cname not in cols:
            propscol = ''
            if cname.replace('交易-', '') != cname:
                cname = cname.replace('交易-', '')
                propscol = 'trade_props'
            if cname.replace('宝贝-', '') != cname:
                cname = cname.replace('宝贝-', '')
                propscol = 'props'
            if cname.replace('清洗-', '') != cname and type[0] == 'D':
                cname = cname.replace('清洗-', '')
                propscol = 'c_props'
            if cname.replace('清洗-', '') != cname and type[0] == 'E':
                cname = cname.replace('清洗-', '')
                propscol = 'clean_props'
            if not isinstance(v, list):
                where.append('`{pname}.value`[indexOf(`{pname}.name`, \'{}\')] {} %({})s'.format(cname, opr, c, pname=propscol))
                p[c] = v
            else:
                cc = []
                for i, v in enumerate(v):
                    cc.append('%({}xx{})s'.format(c, i))
                    p['{}xx{}'.format(c, i)] = v
                where.append('`{pname}.value`[indexOf(`{pname}.name`, \'{}\')] IN ({})'.format(cname, ','.join(cc), pname=propscol))
        elif v == '' and c.find('id') >= 0:
            continue
        elif v == '':
            where.append('{} = \'\''.format(c))
        elif v in cols:
            where.append('{} `{}`'.format(c, v))
        elif isinstance(v, list):
            cc = []
            for i, v in enumerate(v):
                cc.append('%({}xx{})s'.format(c, i))
                p['{}xx{}'.format(c, i)] = ent.safe_insert(cols[cname], v) if cols[cname].lower() not in ('date') else v
            where.append('{} ({})'.format(c, ','.join(cc)))
        else:
            where.append('{c} %({c})s'.format(c=c))
            p[c] = ent.safe_insert(cols[cname], v) if cols[cname].lower() not in ('date') else v

    where = 'WHERE 1' if len(where) == 0 else 'WHERE {}'.format(' AND '.join(where))
    return where, p, limit, orderby, ext1, ext2, ext3, cmptbl


def stat(id, params, type, chgtbl=''):
    ent = entity_manager.get_brush(id)
    dbch = ent.get_db('chsop')
    dbcm = ent.get_db('chmaster')
    db26 = ent.get_db('26_apollo')
    params = json.loads(params)

    if type == 'A':
        _, tbl = ent.get_plugin().get_a_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'B':
        _, tbl = ent.get_plugin().get_b_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'C':
        _, tbl = ent.get_plugin().get_c_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'D':
        _, tbl = ent.get_plugin().get_d_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'E':
        _, tbl = ent.get_plugin().get_e_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == 'ALL':
        _, tbl = ent.get_plugin().get_all_tbl()
        return stat_data(ent, dbch, chgtbl or tbl, params, type)
    elif type == '底层自助查询':
        sss = 'source'
        dba = dbch
        for ii, ss in enumerate(['', 'ali', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy']):
            if ii > 0 and chgtbl.find(ss) != -1:
                sss = ii
                dba = dbcm
                break
        return stat_data(ent, dba, chgtbl, params, type, source=sss)
    elif type == '答题表':
        tbl = ent.get_tbl()
        return stat_brush(ent, db26, chgtbl or tbl, params, type)
    elif type == 'E表对比':
        return stat_e_diff(ent, dbch, chgtbl, params, type)


def stat_data(ent, dba, tbl, params, type, source='source'):
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, statCol, statType, calcCol, _ = format_params(ent, type, params, cols, posname, stat=True)

    sign = 'sign' if type == 'A' else '1'

    if calcCol == '统计sales':
        fcols = 'ROUND(sum(sales*{s})/100, 2)'.format(s=sign)
    elif calcCol == '统计price':
        fcols = 'IF(sum(num*{s})=0,0,ROUND(sum(sales*{s})/sum(num*{s})/100, 2))'.format(s=sign)
    elif calcCol == '统计num':
        fcols = 'sum(num*{s})'.format(s=sign)
    else:
        fcols = 'sum({s})'.format(s=sign)

    if orderby != '':
        orderby = 'ORDER BY ss ' + 'DESC' if orderby.find('DESC') > -1 else 'ASC'

    pieces = []
    if statType == '分价格段统计':
        sql = 'SELECT min(sales/num/100), max(sales/num/100) FROM {} {}'.format(tbl, where)
        mmm = dba.query_all(sql, p)
        sprice, eprice = mmm[0][0], mmm[0][1]
        pre = (eprice - sprice)/ 10
        for i in range(10):
            eprice -= pre
            pieces.append('>= {}'.format(round(eprice,2)))
    elif statType == '分年统计':
        sql = 'SELECT min(toYear(pkey)), max(toYear(pkey)) FROM {} {}'.format(tbl, where)
        mmm = dba.query_all(sql, p)
        syear, eyear = mmm[0][0], mmm[0][1]

        while syear <= eyear:
            pieces.append(syear)
            syear += 1
    elif statType == '分月统计':
        sql = 'SELECT min(toYYYYMM(pkey)), max(toYYYYMM(pkey)) FROM {} {}'.format(tbl, where)
        mmm = dba.query_all(sql, p)
        smonth, emonth = mmm[0][0], mmm[0][1]

        while smonth <= emonth:
            pieces.append(smonth)
            smonth += 1
            if smonth %100 == 13:
                smonth = int(smonth/100 + 1)*100 + 1
    else:
        pieces = ['']

    scol = []
    for col in statCol:
        if col in ('brand', 'cid', 'sid'):
            col = 'toString([{},{}])'.format(source, col)
        else:
            col = 'toString({})'.format(col)
        scol.append(col)
    data = [['','']]

    if statType == '分价格段统计':
        sql = '''
            SELECT multiIf({} '>=0') y, '' m, [{}] p, {} AS ss
            FROM {} {} GROUP BY y, m, p {} LIMIT {} BY y, m
        '''.format(''.join(['sales/num/100 {p}, \'{p}\','.format(p=piece) for piece in pieces]), ','.join(scol), fcols, tbl, where, orderby, limit)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[2]) for v in rr1]))
        rr1 = {'{}{}-{}'.format(v[0], v[1], v[2]):v for v in rr1}
    elif statType == '分年统计':
        sql = '''
            SELECT toYear(pkey) y, [{}] p, {} AS ss
            FROM {} {} GROUP BY y, p {} LIMIT {} BY y
        '''.format(','.join(scol), fcols, tbl, where, orderby, limit)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[1]) for v in rr1]))
        rr1 = {'{}-{}'.format(v[0], v[1]):v for v in rr1}
    elif statType == '分月统计':
        sql = '''
            SELECT toYear(pkey) y, toMonth(pkey) m, [{}] p, {} AS ss
            FROM {} {} GROUP BY y, m, p {} LIMIT {} BY y, m
        '''.format(','.join(scol), fcols, tbl, where, orderby, limit)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[2]) for v in rr1]))
        rr1 = {'{}{:0>2d}-{}'.format(v[0], v[1], v[2]):v for v in rr1}
    else:
        sql = '''
            SELECT [{}] p, {} AS ss
            FROM {} {} GROUP BY p {} LIMIT {}
        '''.format(','.join(scol), fcols, tbl, where, orderby, limit)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[0]) for v in rr1]))
        rr1 = {'-{}'.format(v[0]):v for v in rr1}

    x = format_col('C', len(pieces))
    for i, s in enumerate(props):
        sss, s = [], eval(s)
        for ii, ss in enumerate(statCol):
            sss.append(format_data(ent, ss, str(s[ii])))
        data.append([','.join(sss), '=SPARKLINE({{"data":C{i}:{}{i}}}, "bar")'.format(x, i=i+2)])

    for ii, m in enumerate(pieces):
        for i, s in enumerate(props):
            k = '{}-{}'.format(m, s)
            data[i+1].append(0 if k not in rr1 else rr1[k][-1])

        c = format_col('C', ii)
        data[0].append('=SPARKLINE({c}{}:{c}{}, "pie")'.format(2, len(props)+1, c=c))

    for i, s in enumerate(props):
        data[i+1].append(0)
    pieces.append('')

    cols = [
        {'type': 'text', 'title': '', 'width': '100px'}, {'type': 'text', 'width': '200px'}
    ] + [
        {'type': 'text', 'title': str(m), 'width': '100px'} for m in pieces
    ]
    cfg = {'search': False, 'filters': False, 'columnSorting': False, 'defaultColWidth':100}

    return [[type, cols, data, cfg]]


def stat_brush(ent, dba, tbl, params, type):
    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]

    where, p, limit, orderby, statCol, statType, calcCol, _ = format_params(ent, type, params, cols, posname)

    arrr, maxc = [], 0
    cols, prop = [], [statCol]
    for pp in ['source', 'cid', 'month', 'alias_all_bid']:
        if pp not in prop:
            prop.append(pp)

    for pp in prop:
        cols += [
            {'title': pp, 'width': 100},
            {'title': 'count', 'width': 100},
            {'title': '|', 'width': 50}
        ]
        sql = '''
            SELECT {p}, count(*) FROM {} {} GROUP BY {p} LIMIT {}
        '''.format(tbl, where, limit, p='CONCAT(\'[\', snum, \',\', {}, \']\')'.format(pp) if pp in ('brand', 'cid', 'sid') else pp)
        ret = dba.query_all(sql, p)
        rrr = []
        for a, b, in ret:
            a = format_data(ent, pp, str(a))
            rrr.append([a, b])
        arrr.append(rrr)
        maxc = max(len(rrr), maxc)

    data = []
    for i in range(maxc):
        data.append([''] * len(cols))

    for col, arr in enumerate(arrr):
        for i in range(maxc):
            if i < len(arr):
                data[i][col*3] = arr[i][0]
                data[i][col*3+1] = arr[i][1]
        col += 1

    cfg = {'search': False, 'filters': False, 'columnSorting': False, 'defaultColWidth':100}

    return [[type, cols, data, cfg]]


def stat_e_diff(ent, dba, tbl, params, type):
    _, etbl = ent.get_plugin().get_e_tbl()
    tbl = tbl or etbl

    cols = ent.get_cols(tbl, dba)
    poslist = ent.get_poslist()
    posname = [poslist[p]['name'] for p in poslist]
    where, p, limit, orderby, statCol, statType, calcCol, cmptbl = format_params(ent, type, params, cols, posname, stat=True)

    sign = 'sign' if type == 'A' else '1'
    # print(params, where, p, limit, orderby, statCol, statType, calcCol)
    # exit()

    if calcCol == '统计sales':
        fcols = 'ROUND(sum(sales*{s})/100, 2)'.format(s=sign)
    elif calcCol == '统计price':
        fcols = 'ROUND(IF(sum(num*{s})=0,0,ROUND(sum(sales*{s})/sum(num*{s}), 2))/100, 2)'.format(s=sign)
    elif calcCol == '统计num':
        fcols = 'sum(num*{s})'.format(s=sign)
    else:
        fcols = 'sum({s})'.format(s=sign)

    pieces = []
    if statType == '分年统计':
        sql = 'SELECT min(toYear(pkey)), max(toYear(pkey)) FROM {} {}'.format(tbl, where)
        mmm = dba.query_all(sql, p)
        syear, eyear = mmm[0][0], mmm[0][1]

        while syear <= eyear:
            pieces.append(syear)
            syear += 1
    elif statType == '分月统计':
        sql = 'SELECT min(toYYYYMM(pkey)), max(toYYYYMM(pkey)) FROM {} {}'.format(tbl, where)
        mmm = dba.query_all(sql, p)
        smonth, emonth = mmm[0][0], mmm[0][1]

        while smonth <= emonth:
            pieces.append(smonth)
            smonth += 1
            if smonth %100 == 13:
                smonth = int(smonth/100 + 1)*100 + 1
    else:
        pieces = ['']

    scol = []
    for col in statCol:
        if col in ('brand', 'cid', 'sid'):
            col = 'toString([source,{}])'.format(col)
        else:
            col = 'toString({})'.format(col)
        scol.append(col)

    if statType == '分年统计':
        sql = '''
            SELECT y, p, a.s, b.s
            FROM (SELECT toYear(pkey) y, [{p}] p, {s} s FROM {} {w} GROUP BY y, p) a
            FULL OUTER JOIN (SELECT toYear(pkey) y, [{p}] p, {s} s FROM {} {w} GROUP BY y, p) b
            USING (y, p)
            WHERE a.s != b.s OR isNull(a.s) OR isNull(b.s)
        '''.format(tbl, cmptbl, p=','.join(scol), s=fcols, w=where)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[1]) for v in rr1]))
        rr1 = {'{}-{}'.format(v[0], v[1]):v for v in rr1}
    elif statType == '分月统计':
        sql = '''
            SELECT y, m, p, a.s, b.s
            FROM (SELECT toYear(pkey) y, toMonth(pkey) m, [{p}] p, {s} s FROM {} {w} GROUP BY y, m, p) a
            FULL OUTER JOIN (SELECT toYear(pkey) y, toMonth(pkey) m, [{p}] p, {s} s FROM {} {w} GROUP BY y, m, p) b
            USING (y, m, p)
            WHERE a.s != b.s OR isNull(a.s) OR isNull(b.s)
        '''.format(tbl, cmptbl, p=','.join(scol), s=fcols, w=where)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[2]) for v in rr1]))
        rr1 = {'{}{:0>2d}-{}'.format(v[0], v[1], v[2]):v for v in rr1}
    else:
        sql = '''
            SELECT p, a.s, b.s
            FROM (SELECT [{p}] p, {s} s FROM {} {w} GROUP BY p) a
            FULL OUTER JOIN (SELECT [{p}] p, {s} s FROM {} {w} GROUP BY p) b
            USING (p)
            WHERE a.s != b.s OR isNull(a.s) OR isNull(b.s)
        '''.format(tbl, cmptbl, p=','.join(scol), s=fcols, w=where)
        rr1 = dba.query_all(sql, p)
        props = list(set([str(v[0]) for v in rr1]))
        rr1 = {'-{}'.format(v[0]):v for v in rr1}

    data = []
    for i, s in enumerate(props):
        sss, s = [], eval(s)
        for ii, ss in enumerate(statCol):
            sss.append(format_data(ent, ss, str(s[ii])))
        data.append(sss)

    if len(pieces) > 1:
        for ii, m in enumerate(pieces):
            for i, s in enumerate(props):
                k = '{}-{}'.format(m, s)
                if k in rr1:
                    a, b = rr1[k][-2] or 0, rr1[k][-1] or 0
                    v = '{} <- {}\n差异：{} 增长：{}%'.format(a, b, round(a-b, 3), round((a-b)/b*100, 3) if a > 0 and b > 0 else 'NaN')
                else:
                    v = ''
                data[i].append(v)
    else:
        for i, s in enumerate(props):
            m = pieces[0]
            k = '{}-{}'.format(m, s)
            if k in rr1:
                a, b = rr1[k][-2] or 0, rr1[k][-1] or 0
                v1, v2, v3, v4 = a, b, round(a-b, 3), round((a-b)/b*100, 3) if a > 0 and b > 0 else 'NaN'
            else:
                v1 = ''
                v2 = ''
                v3 = ''
                v4 = ''
            data[i].append(v1)
            data[i].append(v2)
            data[i].append(v3)
            data[i].append(v4)

    cols = []
    for c in statCol:
        cc = re.search( r'\'([^\']+)\'', c, re.M|re.I)
        c = cc.group(1) if cc else c
        cols.append({'type': 'text', 'title': c, 'width': '100px'})
    if len(pieces) > 1:
        cols += [{'type': 'text', 'title': str(m), 'width': '100px'} for m in pieces]
    else:
        cols += [
            {'type': 'text', 'title': '原销售额', 'width': '100px'},
            {'type': 'text', 'title': '新销售额', 'width': '100px'},
            {'type': 'text', 'title': '差异', 'width': '100px'},
            {'type': 'text', 'title': '增长%', 'width': '100px'}
        ]

    cfg = {}

    return [[type, cols, data, cfg]]


def format_col(s, offset):
    c = ord(s)+offset-ord('A')
    c = (chr(int(c/26-1) + ord('A')) if c >= 26 else '') + chr(c%26 + ord('A'))
    return c


def format_data(self, t, v):
    source_cn = {'1':'阿里','2':'京东','3':'国美','4':'聚美','5':'考拉','6':'苏宁','7':'唯品会','8':'拼多多','9':'酒仙','10':'途虎','11':'抖音','12':'cdf','13':'旅购日上','14':'得物','15':'盒马','16':'新旅购','17':'test17','18':'test18','19':'test19','24':'ks','999':''}
    if t == 'source' and str(v) in source_cn:
        return str(v) + ' (' + source_cn[str(v)] + ')'
    if t in ('all_bid', 'alias_all_bid', 'c_all_bid', 'c_alias_all_bid'):
        r = self.get_plugin().get_allbrand_info(int(v)) or {'name': ''}
        return str(v) + ' (' + r['name'] + ')'
    if t in ('cid'):
        v = eval(v)
        r = self.get_plugin().get_category_info(v[0], v[1]) or {'full_name': ''}
        return str(v[1]) + ' (' + r['full_name'] + ')'
    if t in ('sid'):
        v = eval(v)
        r = self.get_plugin().get_shop_info(v[0], v[1]) or {'name': ''}
        return str(v[1]) + ' (' + r['name'] + ')'
    if t in ('clean_pid', 'alias_pid'):
        r = get_product_info(self, v) or {'name': ''}
        return str(v) + ' (' + r['name'] + ')'
    return v


def get_product_info(self, pid):
    cache = self.get_plugin().cache
    if 'product' not in cache:
        dbname, tbl = self.get_plugin().get_product_tbl()
        dba = self.get_db(dbname)
        sql = 'SELECT pid, name FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        cache['product'] = {str(pid): {'pid':pid, 'name':name} for pid,name, in ret}

    if str(pid) in cache['product']:
        return cache['product'][str(pid)]
    else:
        return {'pid':pid, 'name':''}


def save(id, ids, data, ext1, type, chgtbl=''):
    ent = entity_manager.get_brush(id)
    dbch = ent.get_db('chsop')
    db26 = ent.get_db('26_apollo')
    data = json.loads(data)

    if type == '盒马brand':
        tbl = 'cleaner.hema_brand_alias'
        for v in data:
            v['is_new'] = 0
        return save_mysql(ent, db26, tbl, data, type, 'hema_brand')
    elif type == '出数刷sp':
        tbl = 'artificial.alter_update'
        return save_etbl_chgprops(ent, dbch, tbl, data, type)
    elif type == '答题机洗sp对不上':
        tbl = ent.get_tbl()
        return save_bspcheck(ent, db26, chgtbl or tbl, data, type)
    elif type == '答题表属性配置':
        tbl = 'dataway.clean_props'
        return save_brush_props(ent, db26, tbl, data, type)
    elif type == '答题表配置':
        tbl = 'brush.project'
        return save_brush(ent, db26, tbl, data, type)
    elif type == '答题表级联检查':
        tbl = ent.get_tbl()
        return save_brush_relation(ent, db26, tbl, data, type)
    elif type == '答题表级联':
        tbl = 'dataway.clean_props_relation'
        return save_brush_props_relation(ent, db26, tbl, data, type)
    elif type == 'E表brand':
        tbl = 'artificial.brand_{}'.format(ent.eid)
        return save_clickhouse(ent, dbch, tbl, data, type, 'bid')
    elif type == 'E表product':
        ent = entity_manager.get_market(id)
        tbl = 'artificial.product_{}'.format(ent.eid)
        return save_eproduct(ent, dbch, tbl, ids, data, ext1, type)
    elif type == 'E表category':
        tbl = 'artificial.category_{}'.format(ent.eid)
        return save_clickhouse(ent, dbch, tbl, data, type, 'cid')


def save_etbl_chgprops(ent, dba, tbl, data, type):
    cols = ent.get_cols(tbl, dba, ['uuid', 'eid', 'ver', 'deleteFlag', 'created'])
    for v in data:
        if v['__opr'] == 'add':
            r = []
            for c in cols:
                r.append(ent.safe_insert(cols[c], v[c]))
            r += [1, ent.eid, 0]
            sql = 'INSERT INTO {} (`{}`, `ver`, `eid`, `deleteFlag`) VALUES'.format(tbl, '`,`'.join(cols))
            dba.execute(sql, [r])
        else:
            sql = '''
                SELECT {}, ver FROM {} WHERE eid = {} AND uuid = '{}' ORDER BY ver DESC LIMIT 1
            '''.format(','.join(cols.keys()), tbl, ent.eid, v['__pkey'])
            r = dba.query_all(sql)
            if len(r) == 0:
                continue
            r = list(r[0])
            for i, c in enumerate(cols.keys()):
                if c in v:
                    r[i] = ent.safe_insert(cols[c], v[c])
            r[-1] += 1
            r += [v['__pkey'], ent.eid, 1 if v['__opr'] == 'del' else 0]
            sql = 'INSERT INTO {} (`{}`, `ver`, `uuid`, `eid`, `deleteFlag`) VALUES'.format(tbl, '`,`'.join(cols.keys()))
            dba.execute(sql, [r])

    _, dtbl = ent.get_plugin().get_d_tbl()
    sql = 'DROP TABLE IF EXISTS {}_atest'.format(dtbl)
    dba.execute(sql)
    sql = 'CREATE TABLE {t}_atest AS {t}'.format(t=dtbl)
    dba.execute(sql)
    ent.get_plugin().transform(dtbl+'_atest', dba)
    sql = 'DROP TABLE IF EXISTS {}_atest'.format(dtbl)
    dba.execute(sql)

    return 'succ'


def save_bspcheck(ent, dba, tbl, data, type):
    for v in data:
        if v['__opr'] == 'modify':
            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey', 'uuid2']:
                    continue
                a.append('{}=%s'.format(k))
                b.append(v[k])
            sql = 'UPDATE {} SET {} WHERE id = {}'.format(tbl, ','.join(a), v['__pkey'])
            dba.execute(sql, tuple(b))
            dba.commit()

    return 'succ'


def save_brush(ent, dba, tbl, data, type):
    for v in data:
        if v['__opr'] == 'del':
            sql = 'DELETE FROM {} WHERE eid = {}'.format(tbl, ent.eid)
            dba.execute(sql)
            dba.commit()

        if v['__opr'] == 'add':
            a, b, c = [], [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'created']:
                    continue
                a.append('`{}`'.format(k))
                b.append('%s')
                c.append(v[k])
            sql = '''
                INSERT INTO {} (eid, {}, cid, status, sop_project, show_on_ali, show_on_hw) VALUES ({}, {}, 16, 'created', 1, 0, 1)
            '''.format(tbl, ','.join(a), ent.eid, ','.join(b))
            dba.execute(sql, tuple(c))
            dba.commit()

        if v['__opr'] == 'modify':
            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'created']:
                    continue
                a.append('{}=%s'.format(k))
                b.append(v[k])
            sql = 'UPDATE {} SET {} WHERE eid = {}'.format(tbl, ','.join(a), ent.eid)
            dba.execute(sql, tuple(b))
            dba.commit()

    return 'succ'


def save_brush_props(ent, dba, tbl, data, type):
    for v in data:
        print(v)

        if v['__opr'] == 'del':
            sql = 'DELETE FROM {} WHERE eid = {} AND spid = {}'.format(tbl, ent.eid, v['__pkey'])
            dba.execute(sql)
            dba.commit()
            continue

        if v['__opr'] == 'add':
            # if v['choice_content'] != '':
            #     v['type'] = 0
            #     v['define'] = 'enum'
            # else:
            #     v['type'] = 1
            #     v['define'] = 'str'
            if v['define'] == 'str':
                v['type'] = 1
            else:
                v['type'] = 0

            a, b, c = [], [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'spid', 'created']:
                    continue
                a.append('`{}`'.format(k))
                b.append('%s')
                c.append(v[k])

            sql = 'INSERT INTO {} (eid, spid, {}) VALUES ({}, {}, {})'.format(tbl, ','.join(a), ent.eid, v['spid'], ','.join(b))
            dba.execute(sql, tuple(c))
            dba.commit()

        if v['__opr'] == 'modify':
            if 'define' in v:
                # if v['choice_content'] != '':
                #     v['type'] = 0
                #     v['define'] = 'enum'
                # else:
                #     v['type'] = 1
                #     v['define'] = 'str'
                if v['define'] == 'str':
                    v['type'] = 1
                else:
                    v['type'] = 0

            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'spid', 'created']:
                    continue
                a.append('`{}`=%s'.format(k))
                b.append(v[k])
            sql = 'UPDATE {} SET {} WHERE eid = {} AND spid = {}'.format(tbl, ','.join(a), ent.eid, v['__pkey'])
            dba.execute(sql, tuple(b))
            dba.commit()

    return 'succ'


def save_brush_relation(ent, dba, tbl, data, type):
    for v in data:
        pkey = json.loads(v['__pkey'])
        if v['__opr'] == 'modify':
            a, b = [], []
            for k in v:
                if k == 'A答了':
                    a.append('spid{}=%s'.format(pkey['spa']))
                    b.append(v[k])
                if k == 'B答了':
                    a.append('spid{}=%s'.format(pkey['spb']))
                    b.append(v[k])
            sql = 'UPDATE product_lib.entity_{}_item SET {} WHERE id = {}'.format(pkey['eid'], ','.join(a), pkey['id'])
            dba.execute(sql, tuple(b))
            dba.commit()

    return 'succ'


def save_brush_props_relation(ent, dba, tbl, data, type):
    for v in data:
        if v['__opr'] == 'del':
            sql = 'DELETE FROM {} WHERE eid = {} AND id = {}'.format(tbl, ent.eid, v['__pkey'])
            dba.execute(sql)
            dba.commit()
            continue

        if v['__opr'] == 'add':
            a, b, c = [], [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'id', 'created']:
                    continue
                a.append('`{}`'.format(k))
                b.append('%s')
                c.append(v[k])

            sql = 'INSERT INTO {} (eid, {}) VALUES ({}, {})'.format(tbl, ','.join(a), ent.eid, ','.join(b))
            dba.execute(sql, tuple(c))
            dba.commit()

        if v['__opr'] == 'modify':
            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey', 'eid', 'id', 'created']:
                    continue
                a.append('{}=%s'.format(k))
                b.append(v[k])
            sql = 'UPDATE {} SET {} WHERE eid = {} AND id = {}'.format(tbl, ','.join(a), ent.eid, v['__pkey'])
            dba.execute(sql, tuple(b))
            dba.commit()

    return 'succ'


def save_eproduct(ent, dba, tbl, ids, data, ext1, type):
    if ext1 == '只看不同的':
        ent.update_product(ids)
    else:
        for v in data:
            pkey = json.loads(v['__pkey'])
            if v['__opr'] == 'modify':
                ent.update_product_bypid(pkey, v['custom_pid'])

    return 'succ'

    # for v in data:
    #     if v['__opr'] == 'modify':
    #         a, b = [], {}
    #         for k in v:
    #             if k in ['__opr', '__pkey', 'pid']:
    #                 continue
    #             a.append('{k}=%({k})s'.format(k=k))
    #             b[k] = v[k]
    #         sql = 'ALTER TABLE {} UPDATE {} WHERE pid = \'{}\''.format(tbl, ','.join(a), v['__pkey'])
    #         dba.execute(sql, b)
    #         while not ent.check_mutations_end(dba, tbl):
    #             time.sleep(1)

    # return 'succ'


def save_clickhouse(ent, dba, tbl, data, type, pkey='uuid2'):
    for v in data:
        if v['__opr'] == 'del':
            sql = 'ALTER TABLE {} DELETE WHERE {} = \'{}\''.format(tbl, pkey, v['__pkey'])
            dba.execute(sql)
            while not ent.check_mutations_end(dba, tbl):
                time.sleep(1)

        if v['__opr'] == 'add':
            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey']:
                    continue
                if k == pkey and not v[k]:
                    continue
                a.append('`{}`'.format(k))
                b.append(v[k])
            sql = 'INSERT INTO {} ({}) VALUES'.format(tbl, ','.join(a))
            dba.execute(sql, [b])

        if v['__opr'] == 'modify':
            a, b = [], {}
            for k in v:
                if k in ['__opr', '__pkey', pkey]:
                    continue
                a.append('{k}=%({k})s'.format(k=k))
                b[k] = v[k]
            sql = 'ALTER TABLE {} UPDATE {} WHERE {} = \'{}\''.format(tbl, ','.join(a), pkey, v['__pkey'])
            dba.execute(sql, b)
            while not ent.check_mutations_end(dba, tbl):
                time.sleep(1)

    return 'succ'

def save_mysql(ent, dba, tbl, data, type, key='id'):
    for v in data:
        if v['__opr'] == 'del':
            sql = 'DELETE FROM {} WHERE {} = {}'.format(tbl, key, v['__pkey'])
            dba.execute(sql)

        if v['__opr'] == 'add':
            a, b, c = [], [], []
            for k in v:
                if k in ['__opr', '__pkey', 'uuid2']:
                    continue
                a.append('`{}`'.format(k))
                b.append('%s')
                c.append(v[k])
            sql = 'INSERT INTO {} ({}) VALUES ({})'.format(tbl, ','.join(a), ','.join(b))
            dba.execute(sql, tuple(c))

        if v['__opr'] == 'modify':
            a, b = [], []
            for k in v:
                if k in ['__opr', '__pkey', 'uuid2']:
                    continue
                a.append('{}=%s'.format(k))
                b.append(v[k])
            sql = 'UPDATE {} SET {} WHERE {} = {}'.format(tbl, ','.join(a), key, v['__pkey'])
            dba.execute(sql, tuple(b))

    return 'succ'


header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'cookie': 'enc=KFit3SeeZ5%2BTiEmorDyQIs9cV4CJYZAZlF8icl5%2FcSsnx7q15yN1ZRLhTRTTn5oPeAVaYLK6IJJxEGVeZ1wbCQ%3D%3D; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; cna=UDCqGKs4tnsCAXxPTDc/Edf0; tracknick=juno3960; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0; cookie2=15743e767d6bd859797fbfbddf0556c2; t=76c63656d6bc3934be3197c3089f33f4; _samesite_flag_=true; lgc=juno3960; cancelledSubSites=empty; dnk=juno3960; v=0; _tb_token_=ee83b56d301aa; xlly_s=1; _m_h5_tk=c14dc9700429a4834aaa6663a87ca0c7_1632886584091; _m_h5_tk_enc=ef4c07cb632596f1de890ccd55927ab7; unb=1795122777; cookie17=UoYXKa8H1D4EsA%3D%3D; _l_g_=Ug%3D%3D; sg=075; _nk_=juno3960; cookie1=Vvbb4I5vFaEMNARJhnY75%2F3va5gvb%2B7HjfZgHEtU2%2Bk%3D; mt=ci=68_1; sgcookie=E100YEoCAAkjXPSrm%2BiC65dAXOfCibWefiZC2VD5GMt18eiEe8mw1c%2FIpc3dvb3y7fq%2FmOywBwHIlDZRmHz0y8p6Aex8P7uNQaENaqIVyZkSL0s%3D; uc3=id2=UoYXKa8H1D4EsA%3D%3D&lg2=VFC%2FuZ9ayeYq2g%3D%3D&nk2=CcYsWs3hDYE%3D&vt3=F8dCujaPmmj8LBhz65k%3D; csg=b70f716d; skt=9ffa90c585db65d0; existShop=MTYzMjg4MzA2NA%3D%3D; uc4=nk4=0%40C%2FnHNFoCejbdSzjvbBWbPIRBzg%3D%3D&id4=0%40UO6eyJeJteQBziBHwbN9Iteeo9%2B5; _cc_=UtASsssmfA%3D%3D; uc1=cookie16=WqG3DMC9UpAPBHGz5QBErFxlCA%3D%3D&cookie15=W5iHLLyFOGW7aA%3D%3D&existShop=false&pas=0&cookie21=UIHiLt3xThH8t7YQoFNq&cart_m=0&cookie14=Uoe3dYlRhw4psg%3D%3D; l=eBS_-6r4gi0ls8tSBOfZourza77T7IRAguPzaNbMiOCPO95k52COW6ecgXLDCnGVhs_6R3kQee-kBeYBqoXYB53Ee5DDwIMmn; tfstk=cREFB7OLaMIUtci5kDiPVX4WwLbdZLw3Ehkj-y4YDNAcNYuhiOO-Iz-iQYGAj2f..; isg=BG1tMic38J9KL5Tz2D96rLbtfAnnyqGcS7fYpK9yt4RzJo3YdxlQbAB0FPrAprlU',
    'referer': 'https://www.taobao.com/',
    'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
}

def get_shop_stat(url):
    r = requests.get(url=url,headers=header)
    for his in r.history:
        if his.headers['location'].find('noshop') != -1:
            return 1

    if r.status_code == 200 and len(r.history) == 0:
        return 0

    return 2
