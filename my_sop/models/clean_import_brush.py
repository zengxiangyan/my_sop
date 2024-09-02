#coding=utf-8
import re
import csv
import time
import sys
import json
import math
import getopt
import random
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
from models.clean_batch import CleanBatch
from models.plugin_manager import PluginManager
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')


def eid_by_bid(dba, bid):
    # check bid exsits
    sql = 'SELECT eid, status FROM cleaner.clean_batch where batch_id = {}'.format(bid)
    ret = dba.query_all(sql)
    if len(ret) == 0:
        raise Exception("bid not exists")

    return ret[0][0], ret[0][1]  # eid,status =

def set_status(dba, bid, status):
    sql = 'UPDATE cleaner.clean_batch set status = "{}", error_msg = "" where batch_id = {}'.format(status, bid)
    dba.execute(sql)
    dba.commit()


def pre_count(dba, eid, rate=0.02):
    sql ='SELECT count(*) FROM `entity_{}_model`'.format(eid)
    ret = dba.query_all(sql)
    total = ret[0][0]

    return int(total * rate), total

def topN_withp(dba, eid, p, limit):
    sql ='SELECT pid FROM `entity_{}_model` where alias_all_bid > 0 and name != "" order by {} desc limit {}'.format(eid, p, limit)
    pids = dba.query_all(sql)
    pids = [pid[0] for pid in pids]

    return pids

def create_tables(dba, eid, spN):
    sp = []
    sps = []
    for i in range(spN):
        sp.append('`sp{}` varchar(100) DEFAULT NULL,'.format(i+1))
        sps.append('`sp{}select` text,'.format(i+1))

    sql = '''
    CREATE TABLE IF NOT EXISTS `product_lib`.`entity_{}_product` (
        `pid` int(11) NOT NULL,
        `source` varchar(10) DEFAULT NULL,
        `name` varchar(50) NOT NULL,
        `sku_id` int(11) DEFAULT NULL,
        `all_bid` int(11) DEFAULT NULL,
        `alias_all_bid` int(11) DEFAULT NULL,
        `is_set` int(11) DEFAULT NULL,
        `sp0` varchar(50) DEFAULT '是',
        {}
        {}
        `clean_flag` int(2) DEFAULT '0',
        `flag` int(2) DEFAULT '0',
        `confirmed` int(2) DEFAULT '0',
        `order` int(11) DEFAULT NULL,
        `uid` int(11) DEFAULT NULL,
        `check_uid` INT(10) NULL DEFAULT NULL,
        `b_check_uid` INT(10) NULL DEFAULT NULL,
        `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY `pid` (`pid`,`clean_flag`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8'''.format(eid, '\n        '.join(sp), '\n        '.join(sps))
    dba.execute(sql)

    sql = '''
    CREATE TABLE IF NOT EXISTS `product_lib`.`entity_{}_item` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `tb_item_id` varchar(40) NOT NULL,
        `source` varchar(10) NOT NULL,
        `month` date NOT NULL,
        `name` varchar(60) NOT NULL,
        `sku_name` varchar(60) DEFAULT NULL,
        `sku_url` varchar(255) DEFAULT NULL,
        `sid` int(11) NOT NULL,
        `shop_name` varchar(50) NOT NULL,
        `shop_type` varchar(10) NOT NULL,
        `cid` int(11) NOT NULL,
        `real_cid` int(11) NOT NULL,
        `region_str` varchar(20) DEFAULT NULL,
        `brand` varchar(20) NOT NULL,
        `all_bid` int(11) NOT NULL,
        `alias_all_bid` int(11) NOT NULL,
        `super_bid` int(11) NOT NULL,
        `sub_brand` int(11) NOT NULL,
        `sub_brand_name` varchar(50) DEFAULT NULL,
        `product` text,
        `prop_all` text,
        `avg_price` int(11) NOT NULL,
        `trade` int(11) NOT NULL,
        `num` int(11) NOT NULL,
        `sales` bigint(20) NOT NULL,
        `visible` tinyint(3) NOT NULL,
        `p1` varchar(255) NOT NULL,
        `clean_type` tinyint(3) DEFAULT '0',
        `clean_flag` tinyint(3) DEFAULT '0',
        `visible_check` tinyint(3) DEFAULT '0',
        {}
        {}
        `pid` int(11) NOT NULL,
        `batch_id` int(11) NOT NULL,
        `flag` int(11) NOT NULL,
        `uid` int(10) DEFAULT NULL,
        `check_uid` INT(10) NULL DEFAULT NULL,
        `b_check_uid` INT(10) NULL DEFAULT NULL,
        `tip` int(11) DEFAULT '0',
        `img` varchar(255) DEFAULT NULL,
        `is_set` tinyint(3) NOT NULL,
        `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        KEY `x_month_source` (`month`,`source`),
        KEY `x_pid` (`pid`),
        KEY `x_alias_all_bid` (`alias_all_bid`),
        KEY `x_cid` (`cid`),
        KEY `x_visible` (`visible`),
        KEY `index8` (`clean_flag`,`tb_item_id`,`source`,`month`,`p1`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8'''.format(eid, '\n        '.join(sp).replace('sp','mp').replace('(50)','(100)'), '\n        '.join(sp))
    dba.execute(sql)

    dba.commit()

def add_model_problem(dba, eid):
    # sql = 'DELETE FROM `product_lib`.`entity_props` where eid = %s'
    # dba.execute(sql, eid)

    sql = '''INSERT IGNORE INTO `product_lib`.`entity_props` (`eid`,`pos`,`prop_name`,`type`,`define`,`valid`,`tips`,`clean_flag`,`readonly`,`created`,`order`)
             VALUES (%s, 'sp0', '是否仅包含一个型号', 'enum', '["是","否"]', NULL, NULL, 0, 0, CURRENT_TIMESTAMP, 0)'''
    dba.execute(sql, (eid,))
    sql = '''INSERT IGNORE INTO `product_lib`.`entity_props` (`eid`,`pos`,`prop_name`,`type`,`define`,`valid`,`tips`,`clean_flag`,`readonly`,`created`,`order`)
             VALUES (%s, 'sp0', '是否是型号', 'enum', '["是","否"]', NULL, NULL, 1, 0, CURRENT_TIMESTAMP, 0)'''
    dba.execute(sql, (eid,))

    dba.commit()

def get_model_props(dba, eid, bid, mod):
    ret = []
    try:
        sql = 'SELECT sp_no, sp_value, prop_ratio FROM `entity_{}_model_all_props` where alias_all_bid=%s and model=%s and sp_value!="" order by sp_no, prop_ratio desc'.format(eid)
        ret = dba.query_all(sql, (bid, mod))
    except Exception as e:
        print(e.args)

    props = {}
    for v in ret:
        k = 'sp{}select'.format(v[0])
        if k not in props:
            props[k] = []

        if len(props[k]) > 10:
            continue

        props[k].append('%s %.2f%%' % (v[1], v[2]*100))

    return props

def get_models(dba, eid, pids, mcols, total):
    #sql = 'SELECT {} FROM `entity_{}_model` where pid in ({})'.format(','.join(mcols), eid, ','.join('%s' % id for id in pids))
    sql = 'SELECT {}, if (pid in ({}), {}, 0)+model_count FROM `entity_{}_model`'.format(
        ','.join(mcols), ','.join('%s' % id for id in pids), total, eid)
    ret = dba.query_all(sql)

    mcols.append('`order`')
    for c in mcols:
        if re.match('^sp\d+$',c) is not None:
            mcols.append(c+'select')

    res = []
    for v in ret:
        v = list(v) + [''] * (len(mcols)-len(v))

        bid = v[mcols.index('alias_all_bid')]
        mod = v[mcols.index('name')]
        props = get_model_props(dba, eid, bid, mod)

        for i,c in enumerate(mcols):
            if c in props:
                v[i] = '\n'.join(props[c])

        res.append(v)

    return mcols, res

def insert_models(dba, eid, mcols, data):
    cols = ['%s' for v in range(len(mcols))]
    sql1 = 'INSERT IGNORE INTO `product_lib`.`entity_{}_product` (sp0, {}, clean_flag) VALUE ("是", {}, 0)'.format(eid, ','.join(mcols), ','.join(cols))
    sql2 = 'INSERT IGNORE INTO `product_lib`.`entity_{}_product` (sp0, {}, clean_flag) VALUE ("否", {}, 1)'.format(eid, ','.join(mcols), ','.join(cols))
    for v in data:
        dba.execute(sql1, tuple(v))
        dba.execute(sql2, tuple(v))

    dba.commit()

def get_model_items(dba, eid, pid, icols, limit=5):
    sql = 'SELECT {}, sum(sales) o1 FROM `entity_{}_item` where pid = {} group by tb_item_id order by o1 desc limit {}'.format(','.join(icols), eid, pid, limit)
    ret = dba.query_all(sql)

    return ret

def insert_items(dba, eid, icols, data, clean_flag):
    if len(data) == 0:
        return

    icols = icols + ['clean_flag','flag']
    cols = ['%s' for v in range(len(icols))]
    sql = 'INSERT IGNORE INTO `product_lib`.`entity_{}_item` ({}) VALUE ({})'.format(eid, ','.join(icols), ','.join(cols))
    for v in data:
        v = list(v)
        v.append(clean_flag)
        v.append(0)
        dba.execute(sql, tuple(v))

    dba.commit()

def main(bid):
    db_26 = app.get_db('26_apollo')
    db_26.connect()
    artificial = app.get_db('artificial_new')
    artificial.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    # if status != '开始型号出题':
    #     raise Exception("wrong status {}".format(status))

    # update status
    set_status(db_26, bid, '正在型号出题')

    r = 0.02
    c, total = pre_count(artificial, eid, r)
    pids = topN_withp(artificial, eid, 'model_count', c+50)
    pids+= topN_withp(artificial, eid, 'sum_sales', c+50)
    pids+= topN_withp(artificial, eid, 'recent_sales', c+50)
    pids = list(set(pids))

    # 抽出来的占所有型号30%以下则r+0.5%再抽一次
    if len(pids) < total*0.3:
        c, total = pre_count(artificial, eid, r+0.005)
        pids = topN_withp(artificial, eid, 'model_count', c+50)
        pids+= topN_withp(artificial, eid, 'sum_sales', c+50)
        pids+= topN_withp(artificial, eid, 'recent_sales', c+50)
        pids = list(set(pids))

    sql = 'DESC `entity_{}_item`'.format(eid)
    ret = artificial.query_all(sql)
    first_column = [v[0] for v in ret]
    mcols = ['pid', 'name', 'all_bid', 'alias_all_bid', 'modified', 'confirmed']
    icols = ['tb_item_id', 'source', 'month', 'name', 'sid', 'shop_name', 'shop_type', 'cid', 'real_cid', 'region_str', 'brand', 'all_bid', 'alias_all_bid', 'super_bid', 'sub_brand', 'sub_brand_name', 'product', 'prop_all', 'avg_price', 'trade', 'num', 'sales', 'visible', 'p1', 'clean_type', 'visible_check', 'pid', 'is_set', 'created', 'modified']
    for v in first_column:
        if re.match('^sp\d+$',v) is not None:
            mcols.append(v)
            icols.append(v.replace('sp', 'mp'))
            icols.append(v)

    create_tables(db_26, eid, 75)

    add_model_problem(db_26, eid)

    sql = 'DELETE FROM `product_lib`.`entity_{}_product`'.format(eid)
    db_26.execute(sql)

    mcols, models = get_models(artificial, eid, pids, mcols, total)
    insert_models(db_26, eid, mcols, models)

    sql = 'UPDATE `product_lib`.`entity_{}_product` set sp0 = "是", flag = 1 where confirmed > 0'.format(eid)
    db_26.execute(sql)

    sql = 'DELETE FROM `product_lib`.`entity_{}_item` where clean_flag < 2'.format(eid)
    db_26.execute(sql)
    db_26.commit()

    for pid in pids:
        items = get_model_items(artificial, eid, pid, icols)
        insert_items(db_26, eid, icols, items, 0)
        insert_items(db_26, eid, icols, items, 1)

    set_status(db_26, bid, '完成型号出题')
    db_26.close()
    artificial.close()

    # all done

def format_csv(file):
    input_file = open(file, 'r', encoding = 'gbk', errors = 'ignore')
    reader = csv.reader(input_file)
    data = [row for row in reader]
    first_row = data[1]
    spn = data[0]
    data = data[2:]

    rel_line = []
    for v in data:
        is_rel = True
        for vv in v:
            if vv != '':
                is_rel = False
                break
        if is_rel:
            rel_line.append([])
            continue

        if len(rel_line) > 0:
            for ii,vv in enumerate(v):
                if vv == '':
                    continue
                rel_line[-1].append(ii)
                rel_line[-1].append(vv)

    result = {}
    for i,k in enumerate(first_row):
        if k == '':
            continue

        d = []
        for rr in data:
            if rr[i] != '':
                vv = '"{}"'.format(rr[i])
                if vv not in d:
                    d.append(vv)
        if len(d) > 0:
            d = '[{}]'.format(','.join(d))
        else:
            d = 'str'
        result[i] = {'name': k, 'spn': spn[i], 'data': d, 'order': i}

    rel = []
    for v in rel_line:
        sn, spval, snon, spnoval = None, [], None, []
        while len(v) > 1:
            if sn == None:
                sn, vv = tuple(v[0:2])
                spval.append(vv)
                v = v[2:]
            else:
                snon, vv = tuple(v[0:2])
                spnoval.append(vv)
                v = v[2:]
        if snon is not None:
            no = json.loads(result[snon]['data'])
            define = list(set(no).difference(set(spnoval)))
            # print('----', sn, spval, snon, spnoval, define)
            rel.append(['sp'+str(spn[sn]), spval, 'sp'+str(spn[snon]), define])
    return result, rel

def loadcsv(bid, file):
    data, rel = format_csv(file)

    db_26 = app.get_db('26_apollo')
    db_26.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    sql = 'SELECT name, pos_id FROM cleaner.clean_target where batch_id = %s and deleteFlag = 0'
    pos_target, tmp = {}, db_26.query_all(sql, (bid,))
    for name, pid,  in tmp:
        pid = str(pid)
        if pid not in pos_target:
            pos_target[pid] = []
        if name not in pos_target[pid]:
            pos_target[pid].append(name)

    sql = 'SELECT pos_id FROM cleaner.clean_pos where batch_id = %s and in_question = 0'
    ignore_pos = db_26.query_all(sql, (bid,))
    ignore_pos = [str(v[0]) for v in ignore_pos]
    err_msg = []
    for k in data:
        r = data[k]
        if str(r['spn']) in ignore_pos:
            continue

        # check
        if r['spn'] in pos_target:
            d = [] if r['data'] == 'str' else json.loads(r['data'])
            # if len(pos_target[r['spn']]) != len(d):
            #     err_msg.append('failed sp: {} {} ====> {}'.format(r['spn'], pos_target[r['spn']], d))

        if len(err_msg) > 0:
            continue

        type, define = 'str', 'NULL'

        if r['data'] != 'str':
            type = 'enum'
            define = "'{}'".format(r['data'].replace('其他', '其它'))

        sql = '''INSERT INTO `product_lib`.`entity_props` (`eid`,`pos`,`prop_name`,`type`,`define`,`valid`,`tips`,`clean_flag`,`readonly`,`created`,`order`)
                 VALUES (%s, %s, %s, %s, {}, NULL, NULL, 2, 0, CURRENT_TIMESTAMP, %s)'''.format(define)
        print(sql, eid, 'sp'+r['spn'], r['name'], type, r['order'])
        db_26.execute(sql, (eid, 'sp'+r['spn'], r['name'], type, r['order']))

    if len(err_msg) > 0:
        return err_msg

    for v in rel:
        spn, spval, spnon, define = tuple(v)

        sql = '''
            INSERT INTO `product_lib`.`entity_props_relation`
                (`eid`,`ep_id_norelay`,`ep_id`,`pos_norelay`,`pos`,`prop_name_norelay`,`prop_name`,`define_norelay`,`define`,`clean_flag`,`created`)
                VALUES (%s,0,0,%s,%s,"","",%s,%s,2,CURRENT_TIMESTAMP())
        '''
        db_26.execute(sql, (eid, spn, spnon, json.dumps(spval, ensure_ascii=False), json.dumps(define, ensure_ascii=False)))

    db_26.commit()
    db_26.close()

    return ''

def model(bid):
    db_26 = app.get_db('26_apollo')
    db_26.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    sql = 'SELECT pid from `product_lib`.`entity_{}_product` where clean_flag = 0 and flag = 1 and sp0 = "是"'.format(eid)
    pids = db_26.query_all(sql)
    if len(pids) == 0:
        return

    pids = [str(p[0]) for p in pids]

    sql = 'UPDATE `product_lib`.`entity_{}_product` set `order`=`order`%2000000000+2000000000, sp0 = "是" where pid in ({}) and clean_flag = 1'.format(eid, ','.join(pids))
    db_26.execute(sql)

    db_26.commit()

    db_26.close()

def item(bid):
    db_26 = app.get_db('26_apollo')
    db_26.connect()
    artificial = app.get_db('artificial_new')
    artificial.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    # if status != '开始宝贝出题':
    #     raise Exception("wrong status {}".format(status))

    # update status
    set_status(db_26, bid, '正在宝贝出题')

    create_tables(db_26, eid, 75)

    sql = 'SELECT max(clean_flag), max(month) from product_lib.entity_{}_item where clean_flag > 1'.format(eid)
    ret = db_26.query_all(sql)

    clean_flag, max_month = tuple(ret[0]) if len(ret) > 0 and ret[0][0] is not None else (1, '1970-01-01')
    max_month = '1970-01-01'
    clean_flag = clean_flag + 1
    print(clean_flag, max_month)

    PluginManager.loadAllPlugin()
    plugin = PluginManager.getPluginObject('randQuestion')
    topids, randids = plugin.start(eid, max_month, 2000, 1000)

    if clean_flag > 2:
        sql = 'INSERT ignore into `product_lib`.`entity_props` (eid, pos, prop_name, type, define, valid, tips, clean_flag, readonly, created, `order`) select eid, pos, prop_name, type, define, valid, tips, {}, readonly, created, `order` from `product_lib`.`entity_props` where eid = {} and clean_flag = {}'.format(clean_flag, eid, clean_flag-1)
        db_26.execute(sql)
        sql = 'INSERT ignore into `product_lib`.`entity_props_relation` (eid, ep_id, ep_id_norelay, pos, pos_norelay, prop_name, prop_name_norelay, define, define_norelay, tips, clean_flag, created) select eid, ep_id, ep_id_norelay, pos, pos_norelay, prop_name, prop_name_norelay, define, define_norelay, tips, {}, created from `product_lib`.`entity_props_relation` where eid = {} and clean_flag = {}'.format(clean_flag, eid, clean_flag-1)
        db_26.execute(sql)

    sql = 'UPDATE `product_lib`.`entity_props_relation` a join `product_lib`.`entity_props` b on (a.eid=b.eid and a.pos=b.pos and a.clean_flag=b.clean_flag) set a.ep_id=b.id,a.prop_name=b.prop_name where a.eid = {}'.format(eid)
    db_26.execute(sql)
    sql = 'UPDATE `product_lib`.`entity_props_relation` a join `product_lib`.`entity_props` b on (a.eid=b.eid and a.pos_norelay=b.pos and a.clean_flag=b.clean_flag) set a.ep_id_norelay=b.id,a.prop_name_norelay=b.prop_name where a.eid = {}'.format(eid)
    db_26.execute(sql)
    db_26.commit()

    sql = 'DESC `entity_{}_item`'.format(eid)
    ret = artificial.query_all(sql)

    first_column = [v[0] for v in ret]
    mcols = ['pid', 'name', 'alias_all_bid', 'modified', 'confirmed']
    icols = ['tb_item_id','source','month','name','sid','shop_name','shop_type','cid','real_cid','region_str','brand','all_bid','alias_all_bid','super_bid','sub_brand','sub_brand_name','product','prop_all','avg_price','trade','num','sales','visible','p1','clean_type','img','pid','is_set','created','modified']
    for v in first_column:
        if v == 'sp17' or v == 'spid17' or v == 'mp17':
            continue
        if re.match('^sp\d+$',v) is not None:
            mcols.append(v)
            icols.append(v)

    itema, itemb = [], []
    if len(topids) > 0:
        sql = 'SELECT {},1 FROM `entity_{}_item` where id in ({})'.format(','.join(icols), eid, ','.join([str(v) for v in topids]))
        itema = list(artificial.query_all(sql))
    if len(randids) > 0:
        sql = 'SELECT {},2 FROM `entity_{}_item` where id in ({})'.format(','.join(icols), eid, ','.join(randids))
        itemb = list(artificial.query_all(sql))
    insert_items(db_26, eid, icols+['visible_check'], itema+itemb, clean_flag)

    print('Item count:', len(itema+itemb))

    # update status
    set_status(db_26, bid, '完成宝贝出题')

    db_26.close()
    artificial.close()


def load_model(bid):
    db_26 = app.get_db('26_apollo')
    db_26.connect()
    artificial = app.get_db('artificial_new')
    artificial.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    sql = 'DESC `entity_{}_model`'.format(eid)
    ret = artificial.query_all(sql)
    first_column = [v[0] for v in ret]
    mcols = []
    for v in first_column:
        if re.match('^sp\d+$',v) is not None and v != 'sp0':
            mcols.append(v)
    mcols.append('pid')

    sql = 'SELECT {} from `product_lib`.`entity_{}_product` where clean_flag=1 and flag=1 and sp0 = "是"'.format(','.join(mcols), eid)
    ret = db_26.query_all(sql)

    db = app.get_db('default')
    db.connect()

    mbatch = CleanBatch(bid)
    pos = mbatch.pos

    sql = 'SELECT max(confirmed) from `entity_{}_model`'.format(eid)
    max_confirmed = artificial.query_all(sql)
    max_confirmed = max_confirmed[0][0]+1

    for v in ret:
        new_v = []
        for ii, vv in enumerate(v):
            val = vv
            if re.match('^sp\d+$', mcols[ii]) is not None:
                pos_id = int(mcols[ii].replace('sp', ''))

                # 型号需要特殊处理
                if pos[pos_id]['type'] == 100:
                    print(pos[pos_id])
                    val = val.replace('（', '(').replace('）', ')').replace('【', '[').replace('】', ']').upper().strip()
            new_v.append(val)

        cols = ['{}=%s'.format(c) for c in mcols]
        sql = 'UPDATE `entity_{}_model` set confirmed=if(confirmed=0,{},confirmed), {} where pid={}'.format(eid, max_confirmed, ','.join(cols[:-1]), new_v[-1])
        artificial.execute(sql, tuple([vv.strip() for vv in new_v[0:-1]]))

    artificial.commit()

    db_26.close()
    artificial.close()

rule = None
def check_sp(bid, eid, no, val):
    return True
    global rule;
    if rule is None:
        rule = {}
        db_26 = app.get_db('26_apollo')
        db_26.connect()
        sql = 'SELECT pos, if(define is NULL,"[]",define) FROM `product_lib`.`entity_props` where eid = %s'
        ret = db_26.query_all(sql, (eid,))
        for pos_id, selector in ret:
            rule[pos_id] = {'selector': json.loads(selector), 'unit': [], 'range': []}

        sql = 'SELECT concat("sp",pos_id), if(only_quantifier is NULL,"",only_quantifier), if(unit_conversion is NULL,"{}",unit_conversion), if(num_range is NULL,"",num_range) FROM cleaner.clean_pos where batch_id = %s'
        ret = db_26.query_all(sql, (bid,))
        for pos_id, only_quantifier, unit_conversion, num_range in ret:
            unit = [str(u) for u in only_quantifier.split(',') if u != ''] + [str(u) for u in eval(unit_conversion)]
            num_range = [float(i) for i in num_range.split(',') if i != '']
            rule[pos_id]['unit'] += unit
            rule[pos_id]['range'] += num_range

        sql = 'SELECT concat("sp",pos_id), base_unit, change_unit FROM cleaner.clean_multi_units where batch_id = %s'
        ret = db_26.query_all(sql, (bid,))
        for pos_id, unita, unitb in ret:
            rule[pos_id]['unit'] += [unita, unitb]

    if no not in rule:
        return True

    if len(rule[no]['selector']) > 0 and val.strip() not in rule[no]['selector']:
        return False

    only_num = 0
    if len(rule[no]['unit']) > 0:
        pattern = r'([0-9\.]+)({})'.format('|'.join(rule[no]['unit']))
        matches = re.findall(pattern, str(val))
        if len(matches) == 0:
            return False

        only_num = float(matches[0][0])

    if len(rule[no]['range']) > 0:
        mi, ma = rule[no]['range'][0], rule[no]['range'][1]
        if only_num < mi or only_num > ma:
            return False

    return True

def load_item(bid, newest=True):
    db_26 = app.get_db('26_apollo')
    db_26.connect()
    artificial = app.get_db('artificial_new')
    artificial.connect()

    # check bid exsits
    eid,status = eid_by_bid(db_26, bid)

    set_status(db_26, bid, '正在回填宝贝题')

    sql = 'SELECT pos_id, from_multi_spid FROM cleaner.clean_pos where batch_id = %s and in_question = 0 and from_multi_spid is not null and from_multi_spid !=""'
    imp = db_26.query_all(sql, (bid,))
    imp = {v[0]: v[1].split(',') for v in imp}

    sql = 'SELECT spid from `dataway`.`clean_props` where eid = {}'.format(eid)
    ret = db_26.query_all(sql)
    pos_list = ['spid{}'.format(v[0]) for v in ret]

    sql = 'SELECT clean_flag FROM `product_lib`.`entity_{}_item` group by clean_flag order by clean_flag'.format(eid)
    ret = db_26.query_all(sql)
    if newest:
        ret = [ret[-1]]

    db = app.get_db('default')
    db.connect()

    mbatch = CleanBatch(bid)
    pos = mbatch.pos

    mbatch.check_lack_fields()

    modify = {}
    sql = 'SELECT base_pos_id,base_sp_value,change_pos_id,change_sp_value FROM cleaner.clean_unify_result where batch_id = %s and deleteFlag = 0'
    rules = db_26.query_all(sql, (bid,))

    for v in ret:
        clean_flag = v[0]
        columns = pos_list + ['visible_check', 'clean_flag', 'all_bid', 'tb_item_id', 'source', 'real_p1']
        sql = 'SELECT {}, DATE_FORMAT(month, \'%%Y-%%m-01\'), name from `product_lib`.`entity_{}_item` where clean_flag=%s and flag=1 order by clean_flag, month'.format(','.join(columns), eid)
        columns+= ['month', 'name']
        items = db_26.query_all(sql, (clean_flag,))

        sql = 'UPDATE `entity_{}_item` set clean_flag=0, visible_check=0 where clean_flag=%s'.format(eid)
        artificial.execute(sql, (clean_flag,))

        for item in items:
            item = list(item)

            new_v = []
            for i, val in enumerate(item):
                if re.match('^spid\d+$', columns[i]) is not None:
                    pos_id = int(columns[i].replace('spid', ''))

                    # 26 之后不允许修改brand 改为改allbid
                    if pos[pos_id]['type'] == 1000:
                        # 小五要求品牌取人工答案对应的aliasbid
                        sql = "SELECT if(alias_bid=0,bid,alias_bid) from brush.all_brand where name = %s"
                        all_bid = artificial.query_all(sql, (val.strip(),))

                        if len(all_bid) > 0:
                            sql = "SELECT name from brush.all_brand where bid = %s"
                            val = artificial.query_all(sql, (all_bid[0][0],))
                            val = val[0][0]
                        else:
                            val = ''

                    # 型号需要特殊处理
                    if pos[pos_id]['type'] == 100:
                        if bid == 48:
                            val = val.replace('（','(').replace('）',')').replace('【','[').replace('】',']').strip()
                        else:
                            val = val.replace('（','(').replace('）',')').replace('【','[').replace('】',']').upper().strip()

                    if pos[pos_id]['type']>=3 and pos[pos_id]['type']<100 and not (bid == 76 and pos_id == 3):
                        val = '' if val==0 or val=='0' else val
                        _, val, _ = mbatch.quantify_num(pos[pos_id], [val, val, val])

                    key = '{}{}{}{}spid{}'.format(item[-5],item[-4],item[-3],item[-2],pos_id)
                    if key in modify:
                        val = modify[key]

                new_v.append(val)

            cols = ['{}=%s'.format(c.replace('spid', 'spid')) for c in columns]
            cols[cols.index('all_bid=%s')] = 'all_bid_spid=%s'

            # if bid == 48:
            # sql = 'UPDATE `entity_{}_item` set {},visible_check=3 where tb_item_id=%s and source=%s and p1=%s and clean_flag=0'.format(
            #     eid, ','.join(cols[:-5]))
            # artificial.execute(sql, tuple(new_v[:-2]))
            # else:
            #     sql = 'UPDATE `entity_{}_item` set {},visible_check=3 where tb_item_id=%s and source=%s and p1=%s and clean_flag=0 and name=%s'.format(
            #         eid, ','.join(cols[:-5]))
            #     artificial.execute(sql, tuple(new_v[:-2]+[new_v[-1]]))

            sql = 'UPDATE `entity_{}_item` set {} where tb_item_id=%s and source=%s and p1=%s and month=%s'.format(
                eid, ','.join(cols[:-5]))
            artificial.execute(sql, tuple(new_v[:-1]))


        # 答案多选 a > b > c
        for pos_id in imp:
            impids = imp[pos_id]
            if len(impids) == 0:
                break
            sps = None
            for i in reversed(impids):
                i = "spid{}".format(i)
                if sps is None:
                    sps = i
                    continue
                sps = "IF({a} is not NULL and {a} != '', {a}, {b})".format(a=i, b=sps)
            sql = 'UPDATE `entity_{}_item` set spid{}={} where clean_flag=%s'.format(eid, pos_id, sps)
            artificial.execute(sql, (clean_flag,))

        for rule in rules:
            try:
                sql = "UPDATE `entity_{}_item` set spid{}=%s where spid{} in ('{}') and clean_flag=%s".format(eid, rule[2], rule[0], rule[1].replace(',', "','"))
                artificial.execute(sql, (rule[3], clean_flag,))
            except Exception as e:
                pass

        artificial.commit()

    # 热水器 spid10 特殊处理
    if bid == 13:
        sql = '''
            update `entity_{eid}_item` set
                spid10 = if(spid7='太阳能', spid7, if((if(spid5='电热',1,0)+if(spid6='燃热',1,0)+if(spid9='其它',1,0))>1,'复合',if(spid5='电热',spid5,if(spid6='燃热',spid6,if(spid8='复合',spid8,'其它')))))
            where clean_flag > 0
        '''.format(eid=eid)
        artificial.execute(sql)

    artificial.commit()

    set_status(db_26, bid, '完成宝贝题回填')

    db_26.close()
    artificial.close()



def load_sku(cols, item):
    # all_bid, name = item[cols.index('all_bid')], item[cols.index('name')]
    # print(cols, all_bid, name)
    # sql = 'INSERT INTO `entity_90383_model` (`alias_all_bid`,`name`,{spn},`confirmed`)'
    # print(sql)
    # # 答题中判断是sku则回填到sku表
    pass


def usage():
    print('''Run clean_import_item.py following by below command-line arguments:
    -b "batchId(positive integer)"
    -h(help)''')
    exit()
