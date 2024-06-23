# coding: utf-8

"""
This script is to calculate correct rate of a clean batch and insert into db.
"""

import time
import datetime
import sys
import getopt

from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app


def eid_by_bid(dba, bid):
    # check bid exsits
    sql = 'SELECT eid, status FROM cleaner.clean_batch where batch_id = {}'.format(bid)
    ret = dba.query_all(sql)

    if len(ret) == 0:
        raise Exception("bid not exists")

    # eid,status =
    return ret[0][0], ret[0][1]


def set_status(dba, bid, status):
    sql = 'UPDATE cleaner.clean_batch set status = %s, error_msg = "" where batch_id = %s'
    dba.execute(sql, (status, bid))
    dba.commit()


def compare(sp, spid):
    if (sp == 'NA' or sp == '不适用') and spid == '':
        return True

    return sp.strip().lower() == spid.strip().lower()

cache_bname, cache_bid = {}, {}


def brandCompare(dba, sp, spid, all_bid):
    # return sp.find(spid) > -1 or spid.find(sp) > -1
    if all_bid == 0 and spid == '':
        return True

    if all_bid == 0 or spid == '':
        return False

    global cache_bid
    if all_bid not in cache_bid:
        sql = "SELECT if(alias_bid=0,bid,alias_bid) from brush.all_brand where bid = %s"
        cache_bid[all_bid] = dba.query_all(sql, (all_bid,))
    b1 = cache_bid[all_bid]

    global cache_bname
    if spid.strip() not in cache_bname:
        sql = "SELECT if(alias_bid=0,bid,alias_bid) from brush.all_brand where name = %s"
        cache_bname[spid.strip()] = dba.query_all(sql, (spid.strip(),))
    b2 = cache_bname[spid.strip()]

    if len(b1) == 0 or len(b2) == 0:
        return False

    return b1[0][0] == b2[0][0]


def catCompare(sp, spid, cid):
    if sp == 'NA' and spid == '':
        return True

    return sp.strip().lower() == spid.strip().lower()


def rate(dba, eid, props, rmi={}, modify=None, visible_check=0, clean_flag=2):

    cols = [v for v in props] + [v.replace('sp','spid') for v in props] + ['tb_item_id', 'source', 'month', 'p1', 'clean_type', 'all_bid_spid', 'all_bid_sp']
    s = ['!= 3', '= 1', '= 2', '= 3'][visible_check]
    sql = 'SELECT {} from `entity_{}_item` where visible_check {} and clean_flag = %s'.format(','.join(cols), eid, s)
    ret = dba.query_all(sql, (clean_flag,))

    props_succ = {}
    all_succ = 0
    for r in ret:
        r = list(r)
        r[-5] = r[-5].strftime('%Y-%m-%d') if isinstance(r[-5], datetime.date) else r[-5]

        asucc = True
        colc = int((len(cols)-7) / 2)
        for i, c in enumerate(cols[0:colc]):
            si = i + colc
            succ = False
            if props[c].find('品牌') == 0:
                succ = brandCompare(dba, r[i], r[si], max(r[cols.index('all_bid_sp')] or 0, r[cols.index('all_bid_spid')] or 0))
            elif props[c].find('子品类') == 0:
                succ = catCompare(r[i], r[si], 0)
            elif eid == 90473 and c == 'sp6':
                if r[i].isnumeric():
                    r[i] = '{:.2f}'.format(float(r[i]))
                if r[si].isnumeric():
                    r[si] = '{:.2f}'.format(float(r[si]))
                succ = r[i] == r[si]
            else:
                succ = compare(r[i], r[si])

            if c not in rmi or rmi[c][1] == 1:
                asucc = asucc and (succ if r[si]!='' else True)

            if c not in props_succ:
                props_succ[c] = {'succ':0, 'count':0}

            # clean_type -1 错误类目
            if r[cols.index('clean_type')] == -1 and props[c].find('品牌') == -1 and props[c].find('子品类') == -1:  # '是否套包'
                continue

            props_succ[c]['succ'] = props_succ[c]['succ'] + (1 if succ else 0)
            props_succ[c]['count'] = props_succ[c]['count'] + 1
        all_succ = all_succ + (1 if asucc else 0)

    return props_succ, all_succ, len(ret)


def ratec(bid, dba, props, rmi_column, clean_flag, visible_check, rates, asucc, count):
    kind = ['all', 'top', 'random', 'same'][visible_check]

    sql = 'DELETE FROM `cleaner`.`clean_correct_rate` where batch_id=%s and clean_flag=%s and kind=%s'
    dba.execute(sql, (bid, clean_flag, kind))

    if rates:
        rate = '%.3f' % (asucc / count)
        record(dba, bid, clean_flag, 'sp-1', '完全正确率', rate, kind, count)

        rate = '%.3f' % (
            sum([rates[x]['succ']  for x in rates if x not in rmi_column or rmi_column[x][1]==1])
          / sum([rates[x]['count'] for x in rates if x not in rmi_column or rmi_column[x][1]==1])
        )
        c = sum([rates[x]['count'] for x in rates if x not in rmi_column or rmi_column[x][1]==1])
        record(dba, bid, clean_flag, 'sp-2', '总正确率', rate, kind, c)

    for k in rates:
        if k in rmi_column and rmi_column[k][0] == 0:
            continue
        v = rates[k]
        name = props[k]
        rate = '%.3f' % (v['succ'] / v['count'])
        record(dba, bid, clean_flag, k, name, rate, kind, v['count'])

    dba.commit()


def other(dba, eid, props, otherk, modify=None, clean_flag=2):
    cols = [v for v in props] + [v.replace('sp','spid') for v in props] + ['tb_item_id', 'source', 'month', 'p1', 'clean_type', 'alias_all_bid', 'real_cid']

    sql = 'SELECT {} from `entity_{}_item` where clean_flag = %s'.format(','.join(cols), eid)
    ret = dba.query_all(sql, (clean_flag,))

    props_succ = {}
    for r in ret:
        r = list(r)
        r[-5] = r[-5].strftime('%Y-%m-%d') if isinstance(r[-5], datetime.date) else r[-5]

        colc = int((len(cols)-7) / 2)
        for i, c in enumerate(cols[0:colc]):
            si = i + colc
            oks = otherk[c.replace('sp', '')] if c.replace('sp', '') in otherk else ['','其它']
            succ = r[i] in oks or r[si] in oks

            if c not in props_succ:
                props_succ[c] = {'succ':0, 'count':0}

            props_succ[c]['succ'] = props_succ[c]['succ'] + (1 if succ else 0)
            props_succ[c]['count'] = props_succ[c]['count'] + 1

    return props_succ


def otherc(bid, dba, props, clean_flag, rates):
    kind = 'other'

    sql = 'DELETE FROM `cleaner`.`clean_correct_rate` where batch_id=%s and clean_flag=%s and kind=%s'
    dba.execute(sql, (bid, clean_flag, kind))

    for k in rates:
        v = rates[k]
        name = props[k]
        rate = '%.3f' % (v['succ'] / v['count'])
        record(dba, bid, clean_flag, k, name, rate, kind, v['count'])

    dba.commit()


def record(dba, bid, clean_flag, sp_no, sp_name, rate, kind, count):
    sql = '''INSERT INTO `cleaner`.`clean_correct_rate` (`clean_flag`,`batch_id`,`sp_no`,`sp_name`,`correct_rate`,`kind`,`modified`,`count`)
             VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
            '''
    dba.execute(sql, (clean_flag, bid, sp_no.replace('sp', ''), sp_name, rate, kind, count))

    if sp_no == 'sp-2':
        sql = '''INSERT IGNORE INTO `cleaner`.`clean_correct_rate_flag_comment` (`batch_id`,`clean_flag`,`comment`, `create_time`,`update_time`,`delete_flag`)
                VALUES (%s, %s, '', UNIX_TIMESTAMP(), CURRENT_TIMESTAMP(), 0)
            '''.format(kind=kind)
        dba.execute(sql, (bid, clean_flag,))

def main_calculate(bid):
    artificial = app.get_db('artificial_new')
    artificial.connect()
    db_26 = app.get_db('26_apollo')
    db_26.connect()

    eid, status = eid_by_bid(db_26, bid)

    set_status(db_26, bid, '正在计算正确率')

    # get modified
    modify = {}
    try:
        sql = 'select tb_item_id, source, month, p1, spidN, spidV from entity_{}_item_modify'.format(eid)
        ret = artificial.query_all(sql)
        for v in ret:
            modify['-'.join(v[0:-1])] = v[-1]
    except Exception as e:
        print(e.args)

    sql = 'SELECT concat("sp",pos_id), single_rate, all_rate from cleaner.clean_pos where deleteFlag = 0 and batch_id = %s and (single_rate = 0 or all_rate = 0)'
    rmi = db_26.query_all(sql, (bid,))
    rmi = {v[0]: list(v[1:]) for v in rmi}

    sql = 'SELECT pos_id, name FROM cleaner.clean_target where mark = -1 and batch_id = %s'
    ret, otherk = db_26.query_all(sql, (bid,)), {}
    for v in ret:
        pos_id, name = str(v[0]), v[1]
        if pos_id not in otherk:
            otherk[pos_id] = []
        otherk[pos_id].append(name)

    # list clean_flag, visible_check
    sql = 'SELECT clean_flag, visible_check from `entity_{}_item` where clean_flag > 0 group by clean_flag, visible_check'.format(eid)
    ret = artificial.query_all(sql)

    props = {}
    for v in ret:
        clean_flag, visible_check = tuple(v)
        if clean_flag not in props:
            # get props by clean_flag
            sql = 'SELECT concat("sp",pos_id), name from cleaner.clean_pos where deleteFlag = 0 and batch_id = %s and in_question = 0'
            rr1 = db_26.query_all(sql, (bid,))
            rr1 = {r[0]: r[1] for r in rr1}
            sql = 'SELECT a.pos, b.name from `product_lib`.`entity_props` a join `cleaner`.`clean_pos` b on (a.pos=concat("sp",b.pos_id)) where a.eid = %s and b.batch_id = %s and a.clean_flag = %s and deleteFlag = 0'
            rr2 = db_26.query_all(sql, tuple([eid, bid, clean_flag]))
            rr2 = {r[0]: r[1] for r in rr2}
            props[clean_flag] = {**rr1, **rr2}

            rates, asucc, count = rate(artificial, eid, props[clean_flag], rmi=rmi, modify=modify, clean_flag=clean_flag) # all
            ratec(bid, db_26, props[clean_flag], rmi, clean_flag, 0, rates, asucc, count)

        rates, asucc, count = rate(artificial, eid, props[clean_flag], rmi=rmi, modify=modify, visible_check=visible_check, clean_flag=clean_flag)
        ratec(bid, db_26, props[clean_flag], rmi, clean_flag, visible_check, rates, asucc, count)

        rates = other(artificial, eid, props[clean_flag], modify=modify, clean_flag=clean_flag, otherk=otherk)
        otherc(bid, db_26, props[clean_flag], clean_flag, rates)

    set_status(db_26, bid, '完成计算正确率')

    artificial.close()
    db_26.close()

    return 'Batch{}的正确率已更新完毕。'.format(bid)

def usage():
    print('''Run clean_correct_rate.py following by below command-line arguments:
    -b="bid"
    -h(help)''')