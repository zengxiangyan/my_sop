import copy
import csv
import json
import numpy as np
import re
import time
import ujson
import logging
import application as app
import arrow
logger = logging.getLogger('report')

from extensions import utils
from models.report import qp
from models.report import common
from models.report.ATable import ATable
from models.report.BTable import BTable
from models.report.CTable import CTable
from models.report.AllTable import AllTable
from models.report.DTable import DTable
from models.report.ETable import ETable
from models.report.ENewTable import ENewTable
from models.report.ITable import ITable
from models.report.PTable import PTable
from models.report.PAllTable import PAllTable
from models.report.STable import STable
from models.report.SplitTable import SplitTable
from models.time_log import TimeLog

table_list_ref = {
    'A': ATable,
    'B': BTable,
    'C': CTable,
    'All': AllTable,
    'D': DTable,
    'E': ETable,
    'E_new': ENewTable,
    'I': ITable,
    'P': PTable,
    'PAll': PAllTable,
    'S': STable,
    'Split': SplitTable
}
table_list_get_item_direct = ['A', 'D', 'E', 'I', 'P']
MODE_E_CONSTANT = 1
MODE_A_CONSTANT = 2
MODE_I_CONSTANT = 3
MODE_P_CONSTANT = 4
MODE_P_ALL_CONSTANT = 5 #取人工答题表所有数据
MODE_E_NEW_CONSTANT = 6 #新E表作为入口
MODE_C_CONSTANT = 7
BASIS_TONGBI = 1
BASIS_HUANBI = 2
table_by_mode = ['', 'E', 'A', 'I', 'P', 'P', 'E_new', 'C']

def get_top_item(h_db, p):
    db = h_db['chmaster']
    mixdb = h_db['mixdb']
    type = qp.get_with_checker(p, 'type', 'uint', 5)
    eid = qp.get_with_checker(p, 'eid', 'uint')
    limit = qp.get_with_checker(p, 'limit', 'uint', 100)
    top = limit
    start_date = qp.get_with_checker(p, 'start_date', 'date', '2020-01-01')
    end_date = qp.get_with_checker(p, 'end_date', 'date', '2020-02-01')
    table_list = p.get('table_list', ['E'])
    mode = MODE_E_NEW_CONSTANT if 'E_new' in table_list else MODE_E_CONSTANT
    where = p['where'] if 'where' in p else {}
    if mode == MODE_E_NEW_CONSTANT:
        dbname = 'sop_e'
        table_model = ENewTable(h_db, eid)
        date_key = 'pkey'
        if 'shop_name' in where:
            del where['shop_name']
    else:
        dbname = 'sop'
        table_model = ETable(h_db, eid)
        date_key = 'month'
    #month 平台 品类 品牌
    group_by = qp.get_with_checker(p, 'group_by', 'json_str_list', [])
    is_get_cid_top = 'data_type' in p and p['data_type'] == 'top'
    if is_get_cid_top:
        group_by = ['cid']
        limit = 20000
        top = p.get('cid_top', 100)
    group_by.append('trade_prop_all.value')
    group_by = pre_process_group_by(group_by)
    fields, group_by_new = get_fields_by_group(group_by, mode=mode)
    group_by = table_model.transform_list_origin(group_by)
    group_by_new = table_model.transform_list_origin(group_by_new)
    fields = table_model.transform_list_origin(fields, for_select=True)
    #当所有group项都选择时，不做group
    if 'date' in group_by and 'source' in group_by and 'cid' in group_by and 'brand' in group_by and 'trade_prop_all.value' in group_by:
            limit_by_sql = ''
    else:
        limit_by_sql = ' limit 1 by {}'.format(','.join(group_by_new))
    if is_get_cid_top:
        limit_by_sql = ''
    if eid <= 0:
        return []

    if p.get('pid',[]) != []:
        p = sku_convert(p, h_db)

    table_model.transform_keys(where)
    table_model.transform_values(where)
    where = common.check_where(where)
    # where.append("prop_all.value[indexOf(prop_all.name, '子品类')]!='其它'")
    where.append("{date_key}>='{start_date}'".format(start_date=start_date, date_key=date_key))
    where.append("{date_key}<'{end_date}'".format(end_date=end_date, date_key=date_key))
    p2 = {
        'where': {'format': where},
        'fields': fields + ['sum(sales) as sales_total'],
        'group_by': group_by_new,
        'top': top,
        'order_by': ['sales_total desc'],
    }
    if is_get_cid_top:
        p2['limit_by'] = ['cid']
    for k in ['prop_name', 'pid']:
        if k in p:
            p2[k] = p[k]
    sql1 = table_model.query_item(p2, run=False)

    month_add_sql = ",concat(toString(toYear({month})),'-',if(toMonth({month})<10,'0',''),toString(toMonth({month}))) as month_str".format(month=date_key) if 'month_str' in group_by else ''
    sql = "select *{month_add_sql} from {dbname}.entity_prod_{eid}_E where {where_sql} and ({group_by}) in (select {group_by} from ({sql1})) {limit_by_sql}".format(dbname=dbname, month_add_sql=month_add_sql, eid=eid, sql1=sql1, group_by=(','.join(group_by_new)), limit_by_sql=limit_by_sql, where_sql=' and '.join(where))
    if limit is not None:
        sql += ' limit {limit}'.format(limit=limit)

    logger.info(sql)
    data = db.query_all(sql)

    #拼接品牌名
    h_db = {'chmaster': db}
    etable = ETable(h_db, eid)
    cols = table_model.get_columns()
    idx_alias_all_bid = cols.index('alias_all_bid')
    idx_pid = cols.index('clean_pid' if mode == MODE_E_NEW_CONSTANT else 'pid')
    h_bid = {}
    h_pid = {}
    for row in data:
        bid = row[idx_alias_all_bid]
        if bid != 0:
            h_bid[bid] = 1
        pid = row[idx_pid]
        if pid != 0:
            h_pid[pid] = 1
    h_brand = {}
    if len(h_bid) > 0:
        sql = 'select bid,name from brush.all_brand where bid in ({})'.format(','.join(str(bid) for bid in h_bid.keys()))
        d = mixdb.query_all(sql)
        for row in d:
            h_brand[row[0]] = row[1]
    h_product = {}
    if len(h_pid) > 0:
        sql = 'select pid,name from artificial.product_{eid} where pid in ({ids})'.format(eid=eid, ids=','.join(str(id) for id in h_pid.keys()))
        d = db.query_all(sql)
        for row in d:
            h_product[row[0]] = row[1]

    r = []
    for row in data:
        bid = row[idx_alias_all_bid]
        if bid == 0 or bid not in h_brand:
            name = ''
        else:
            name = h_brand[bid]
        row = list(row)
        row.append(name)

        pid = row[idx_pid]
        if pid == 0 or pid not in h_product:
            product_name = ''
        else:
            product_name = h_product[pid]
        row.append(product_name)
        r.append(row)

    logger.debug(r)
    return r

def get_fields_by_group(group_by, mode=MODE_E_CONSTANT):
    if 'date' in group_by:
        del group_by[group_by.index('date')]
        if 'month_str' in group_by:
            del group_by[group_by.index('month_str')]
        group_by.insert(0, 'month')
    l = []
    group_by_new = []
    date_key = 'pkey' if mode == MODE_E_NEW_CONSTANT else 'month'
    for k in group_by:
        if k == 'month_str':
            l.append("concat(toString(toYear({month})),'-',if(toMonth({month})<10,'0',''),toString(toMonth({month}))) as month_str".format(month=date_key))
        else:
            l.append(k)
        group_by_new.append(k)
    l.append('tb_item_id')
    group_by_new.append('tb_item_id')
    return l,group_by_new

def get_item_detail(h_db, p, as_dict=False):
    eid = qp.get_with_checker(p, 'eid', 'uint')
    h = {}
    table_list = p.get('table_list', ['E', 'I', 'P'])
    for k in table_list:
        # print(k)
        v = table_list_ref[k]
        fields_key = '{k}_fields'.format(k=k)
        if fields_key in p:
            new_p = copy.deepcopy(p)
            new_p['fields'] = p[fields_key]
        else:
            new_p = p
        h[k] = v(h_db, eid).query_item(new_p, as_dict=as_dict)
        add_time_log('get_item_detail {k}'.format(k=k))
    return h

def add_time_log(k):
    global time_log
    if 'time_log' not in globals():
        time_log = TimeLog()
    logger.debug('step:' + k)
    if isinstance(time_log, TimeLog):
        time_log.add(k)

def get_mode(mode, l):
    if mode is not None:
        return mode
    if 'E' in l:
        return MODE_E_CONSTANT
    elif 'A' in l:
        return MODE_A_CONSTANT
    elif 'C' in l:
        return MODE_C_CONSTANT
    elif 'I' in l:
        return MODE_I_CONSTANT
    elif 'P' in l:
        return MODE_P_CONSTANT
    elif 'P_all' in l:
        return MODE_P_ALL_CONSTANT
    elif 'E_new' in l:
        return MODE_E_NEW_CONSTANT
    else:
        return None

#预处理参数
def pre_process_params(mode, p):
    if mode != MODE_E_CONSTANT and mode != MODE_E_NEW_CONSTANT:
        for k in ['prop_name_limit', 'pid_limit']:
            if k in p:
                del p[k]
    if mode == MODE_E_CONSTANT:
        type = 5
        tb_item_id_field = 'tb_item_id'
        month_field = 'month'
        trade_prop_field = 'trade_prop_all.value'
    elif mode == MODE_E_NEW_CONSTANT or mode == MODE_C_CONSTANT:
        type = 5
        tb_item_id_field = 'item_id'
        month_field = 'date'
        trade_prop_field = 'trade_props.value'
    elif mode == MODE_A_CONSTANT:
        type = 1
        tb_item_id_field = 'item_id'
        month_field = 'date'
        trade_prop_field = 'trade_props.value'
    else:
        type = -1
        tb_item_id_field = 'tb_item_id'
        month_field = 'month'
        trade_prop_field = 'trade_prop_all'

    group_by = qp.get_with_checker(p, 'group_by', 'json_str_list', [])
    group_by = pre_process_group_by(group_by, month_field=month_field)
    #当所有group项都选择时，不做group
    if 'date' in group_by and 'source' in group_by and 'cid' in group_by and 'alias_all_bid' in group_by:
        group_by = []
    return [type, tb_item_id_field, month_field, trade_prop_field, group_by]

def pre_process_group_by(group_by, month_field='month'):
    if 'month_str' in group_by and 'date' in group_by:
        del group_by[group_by.index('month_str')]
    if 'brand' in group_by:
        group_by[group_by.index('brand')] = 'alias_all_bid'
    if 'date' in group_by:
        group_by[group_by.index('date')] = month_field
    return group_by

def get_sample_e_table(mode, table_model, fields, p, where, limit, group_by):
    eid = p['eid']
    where = copy.deepcopy(where)
    date_key = 'pkey' if mode == MODE_E_NEW_CONSTANT else 'month' #todo
    if len(group_by) == 0:
        limit_by = ''
    else:
        limit_by = 'limit 1 by {group_by},{trade_prop},name,{pid}'.format(group_by=','.join([('toStartOfMonth({x})'.format(x=date_key) if x == 'month_str' else x) for x in group_by]), trade_prop=('tb_item_id,trade_prop_all.name,trade_prop_all.value' if mode == MODE_E_CONSTANT else 'item_id,trade_props.name,trade_props.value'), pid=('pid' if mode == MODE_E_CONSTANT else 'clean_pid'))
    sql = '''
      select * from (select {fields} from {dbname}.entity_prod_{eid}_E where %s {limit_by}) limit %s
    '''.format(eid=eid, fields=(','.join(fields)), limit_by=limit_by, dbname=('sop' if mode==MODE_E_CONSTANT else 'sop_e'))
    data = []
    where_add = []
    where_add.append("{date_key}>='{start_date}'".format(start_date=p['start_date'], date_key=date_key))
    where_add.append("{date_key}<'{end_date}'".format(end_date=p['end_date'], date_key=date_key))
    where['format'] = where_add
    if mode == MODE_E_NEW_CONSTANT:
        pid_key = 'clean_pid'
        prop_name_key = 'clean_props'
        if 'shop_name' in where:
            del where['shop_name']
    else:
        pid_key = 'pid'
        prop_name_key = 'prop_name'
    table_model.transform_keys(where)
    table_model.transform_values(where)
    if 'where' in p and 'prop_name' in p['where']:
        where_str = common.check_where(where)
        where_str = ' and '.join(where_str)
        data += table_model.query_all(sql %(where_str, limit))
    elif 'pid_limit' in p and 'pid' in p:
        where[pid_key] = p['pid']
        where_str = common.check_where(where)
        where_str = ' and '.join(where_str)
        data += table_model.query_all(sql %(where_str, str(p['pid_limit']) + ' by {pid_key} limit '.format(pid_key=pid_key) +str(limit)))
    else:
        where_str = common.check_where(where)
        where_str = ' and '.join(where_str)
        data += table_model.query_all(sql %(where_str, limit))
    return data

def get_table_list_by_mode(mode):
    if mode == MODE_E_CONSTANT:
        return ["E", "A", "D", "I", "P", "S", "Split"]
    elif mode == MODE_A_CONSTANT:
        return [ "A"]
    elif mode == MODE_I_CONSTANT:
        return ["I", "P", "S", "Split"]
    elif mode == MODE_P_ALL_CONSTANT:
        return ["E", "A", "D", "I", "P", "S", "Split"]
    elif mode == MODE_E_NEW_CONSTANT:
        return ["E_new", "A", "I", "P", "S", "Split"]
    else:
        return ["I", "P", "S", "Split"]

def get_from_hash_by_keys(h, key_list, trade_prop_name, trade_prop_value, default_value=0):
    trade_props = common.format_key_value_to_string(trade_prop_name, trade_prop_value)
    h_sub = utils.get_or_new(h, key_list + [trade_props], [default_value])
    if h_sub[0] == default_value:
        trade_props = common.format_key_value_to_string(trade_prop_name, trade_prop_value, separators=(',', ':'))
        h_sub = utils.get_or_new(h, key_list + [trade_props], [default_value])
    return h_sub[0]

#取抽样数据
#1.根据选择条件抽取E表中的tb_item_id数据，再补上每个tb_item_id 按照同一tb_item_id+交易属性取3条
#2.根据tb_item_id+交易属性取查I表 P表数据，根据tb_item_id+交易属性+时间去查A表数据
def get_sample_item(h_db, p):
    global time_log
    eid = qp.get_with_checker(p, 'eid', 'uint')
    limit = qp.get_with_checker(p, 'limit', 'uint', None)
    limit = min(limit, 5000)
    mode = qp.get_with_checker(p, 'mode', 'uint', None)
    start_date = qp.get_with_checker(p, 'start_date', 'date', '2020-01-01')
    end_date = qp.get_with_checker(p, 'end_date', 'date', '2020-02-01')
    #month 平台 品类 品牌
    where = p['where'] if 'where' in p else {}
    origin_where = where
    table_list = p.get('table_list', ['E', 'I', 'P'])
    mode = get_mode(mode, table_list)
    table_in = table_by_mode[mode]
    table_list = get_table_list_by_mode(mode)
    type, tb_item_id_field, month_field, trade_prop_field, group_by = pre_process_params(mode, p)
    r = {}
    if eid <= 0:
        return r
    for k in table_list:
        l = []
        r[k] = l
        l.append(table_list_ref[k](h_db, eid).get_columns())

    #第一步取入口表数据
    atable = ATable(h_db, eid)
    etable = ETable(h_db, eid)
    etable_new = ENewTable(h_db, eid)
    itable = ITable(h_db, eid)
    ptable = PTable(h_db, eid)

    if (mode == MODE_A_CONSTANT or mode == MODE_I_CONSTANT) and 'shop_type' in where:
        where['shop_type'] = common.get_shop_type_by_name_list(where['shop_type'])
    if (mode == MODE_E_NEW_CONSTANT or mode == MODE_A_CONSTANT) and 'source' in where:
        where['source_str'] = where.pop('source')
    if mode == MODE_E_CONSTANT:
        fields = ['toString({k})'.format(k=k) if k in ['month', 'prop_all.name', 'prop_all.value', 'created'] else k for k in r['E'][0]]
        data = get_sample_e_table(mode, etable, fields, p, origin_where, limit, group_by)
    elif mode == MODE_E_NEW_CONSTANT:
        fields = ['toString({k})'.format(k=k) if k in ['uuid', 'uuid2', 'pkey', 'date', 'clean_props.name', 'clean_props.value'] else ("multiIf( source=1 and (shop_type<20 and shop_type>10), 'tb', source=1 and shop_type>20, 'tmall', source=2, 'jd', source=3, 'gome', source=4, 'jumei', source=5, 'kaola', source=6, 'suning', source=7, 'vip', source=8, 'pdd', source=9, 'jx', source=10, 'tuhu', NULL ) as source_str" if k == 'source' else k) for k in r['E_new'][0]]
        data = get_sample_e_table(mode, etable_new, fields, p, origin_where, limit, group_by)
    elif mode == MODE_A_CONSTANT:
        if 'shop_name' in where:
            del where['shop_name']
        p = {
            'fields': ['toString({k})'.format(k=k) if k in ['uuid', 'pkey', 'date', 'trade_props_full.name', 'trade_props_full.value', 'props.name', 'props.value', 'created'] else ("multiIf( source=1 and (shop_type<20 and shop_type>10), 'tb', source=1 and shop_type>20, 'tmall', source=2, 'jd', source=3, 'gome', source=4, 'jumei', source=5, 'kaola', source=6, 'suning', source=7, 'vip', source=8, 'pdd', source=9, 'jx', source=10, 'tuhu', NULL ) as source_str" if k == 'source' else k) for k in r['A'][0]],
            'where': utils.merge(where, { 'pkey': [start_date, end_date]}),
            'limit': limit,
        }
        if len(group_by) > 0:
            p['top'] = 1
            p['limit_by'] = group_by + ['item_id','trade_props.name','trade_props.value']
        data = atable.query_item(p)
    else:
        table_model = itable if mode == MODE_I_CONSTANT else ptable
        date_key = 'month'
        where_add = []
        where_add.append("{date_key}>='{start_date}'".format(start_date=p['start_date'], date_key=date_key))
        where_add.append("{date_key}<'{end_date}'".format(end_date=p['end_date'], date_key=date_key))
        origin_where['format'] = where_add
        if 'prop_name' in origin_where:
            del origin_where['prop_name']
        if mode == MODE_I_CONSTANT and 'pid' in origin_where:
            del origin_where['pid']
        p = {
            'where': origin_where,
            'top': limit
        }
        data = table_model.query_item((p if mode != MODE_P_ALL_CONSTANT else ({'top': 10} if logger.level == logging.INFO else {})))

    if len(data) == 0:
        return r
    logger.info('table_in item count:' + str(len(data)))
    add_time_log('sample get etable')

    #第二步取各表item数据
    cols = r[table_in][0]
    idx_cid = cols.index('cid')
    idx_sid = cols.index('sid')
    idx_tb_item_id = cols.index(tb_item_id_field)
    idx_name = cols.index('name')
    idx_source = cols.index('source')
    idx_pkey = cols.index(month_field)
    idx_trade_prop_value = cols.index(trade_prop_field)
    idx_E_month = cols.index(month_field)

    get_pid = 'S' in table_list
    idx_pid = r['P'][0].index('pid') if 'P' in table_list else -1
    h_pid = {}
    get_split = 'Split' in table_list
    idx_split = r['P'][0].index('split') if 'P' in table_list else -1
    idx_split_pid = r['Split'][0].index('pid') if 'Split' in table_list else -1
    h_entity_id = {}
    if 'A' in table_list:
        idx_uuid = r['A'][0].index('uuid')

    if 'D' in table_list:
        r['D'][0].remove('pkey')
        idx_uuid_D = r['D'][0].index('uuid')
    table_list_for_item = get_table_list_for_item(table_list, table_in)
    #同时取了I和P表数据时，从uniq_k来取P表数据
    if (mode != MODE_A_CONSTANT and mode != MODE_P_CONSTANT and mode != MODE_P_ALL_CONSTANT) and 'P' in table_list_for_item:
        table_list_for_item.remove('P')
    l = []
    h_unique_check = {}
    h_item_id_pre = {}
    for row in data:
        source = row[idx_source]
        if mode == MODE_E_CONSTANT or mode == MODE_A_CONSTANT or mode == MODE_E_NEW_CONSTANT:
            prop_name = row[idx_trade_prop_value-1]
            prop_value = row[idx_trade_prop_value]
        else:
            props = common.pre_format(row[idx_trade_prop_value])
            if props is None:
                prop_name = []
                prop_value = []
            else:
                prop_name = list(props.keys())
                prop_value = list(props.values())
        h_item_id_pre[row[idx_tb_item_id]] = 1
        v = [row[idx_cid], row[idx_sid], row[idx_tb_item_id], row[idx_name], source, str(row[idx_pkey]), prop_name, prop_value]
        key = common.get_join_key(v)
        if key in h_unique_check:
            continue
        h_unique_check[key] = 1
        logger.debug(json.dumps(v, ensure_ascii=False, cls=utils.MyEncoder))
        l.append(v)
    add_time_log('pre process')

    # 对A表item数量不做limit
    p = {
        'eid': eid,
        'prewhere': {'tb_item_id': list(h_item_id_pre.keys())},
        'where': {
            ('cid', 'sid', 'tb_item_id', 'name', 'source', 'date', 'trade_prop_all.name', 'trade_prop_all.value'): l
        },
        'table_list': table_list_for_item,
        'top': 1,
        'limit_by': group_by + ['tb_item_id', 'name', 'trade_prop_all.value']
    }
    if 'A' in table_list:
        p['A_fields'] = ['toString({k})'.format(k=k) if k in ['pkey', 'date',  'trade_props_full.name', 'trade_props_full.value', 'props.name', 'props.value'] else k for k in r['A'][0]]
    if 'D' in table_list:
        p['D_fields'] = ['toString({k})'.format(k=k) if k in ['uuid', 'pkey',  'date', 'trade_props_full.name', 'trade_props_full.value', 'props.name', 'props.value', 'created', 'clean_props.name', 'clean_props.value', 'clean_uuid', 'trade_props.name', 'trade_props.value'] else k for k in r['D'][0]]
    if 'E' in table_list:
        p['E_fields'] = ['toString({k})'.format(k=k) if k in ['month', 'prop_all.name', 'prop_all.value', 'created'] else k for k in r['E'][0]]
    d = get_item_detail(h_db, p)
    if table_in == 'E_new':
        table_in = 'E'
        r['E'] = r.pop('E_new')
    d[table_in] = data
    add_time_log('sample get all data')

    h_item_id = {}
    l_uniq_k = []
    if 'I' in d:
        cols = r['I'][0]
        idx_I_tb_item_id = cols.index('tb_item_id')
        idx_I_name = cols.index('name')
        idx_I_trade_prop_value = cols.index('trade_prop_all')
        idx_I_snum = cols.index('snum')
        idx_I_pkey = cols.index('pkey')
        idx_I_uniq_k = cols.index('uniq_k')
        for row in d['I']:
            item_id = row[0]
            snum = row[idx_I_snum]
            pkey = str(row[idx_I_pkey])
            uniq_k = row[idx_I_uniq_k]
            l_uniq_k.append([snum, pkey, uniq_k])
            h = utils.get_or_new(h_item_id, [row[idx_I_tb_item_id], row[idx_I_name], row[idx_I_trade_prop_value]], [0])
            h[0] = item_id
            r['I'].append(row)
        add_time_log('sample process I table')
    h_uuid = {}
    if 'D' in d:
        cols = r['D'][0]
        idx_D_tb_item_id = cols.index('item_id')
        idx_D_name = cols.index('name')
        idx_D_trade_prop_value = cols.index('trade_props.value')
        idx_D_month = cols.index('date')
        for v in d['D']:
            v = list(v)
            v[idx_uuid_D] = str(v[idx_uuid_D])
            prop_name = common.pre_format(v[idx_D_trade_prop_value-1])
            prop_value = common.pre_format(v[idx_D_trade_prop_value])
            trade_prop_value = common.format_key_value_to_string(prop_name, prop_value)
            h = utils.get_or_new(h_uuid, [v[idx_D_month], v[idx_D_tb_item_id], v[idx_D_name], trade_prop_value], [''])
            h[0] = v[idx_uuid_D]
            item_id = get_from_hash_by_keys(h_item_id, [v[idx_D_tb_item_id], v[idx_D_name]], prop_name, prop_value)
            v.insert(0, item_id)
            r['D'].append(v)
        add_time_log('sample process D table')

    if 'E' in d:
        if mode == MODE_P_ALL_CONSTANT:
            cols = r['E'][0]
            idx_tb_item_id = cols.index('tb_item_id')
            idx_name = cols.index('name')
            idx_E_month = cols.index('month')
            idx_trade_prop_value = cols.index('trade_prop_all.value')

        for v in d['E']:
            v = list(v)
            #将uuid放到最后
            if mode != MODE_E_NEW_CONSTANT:
                uuid = get_from_hash_by_keys(h_uuid, [v[idx_E_month], v[idx_tb_item_id], v[idx_name]], v[idx_trade_prop_value-1], v[idx_trade_prop_value], default_value='')
                v.append(uuid)
            item_id = get_from_hash_by_keys(h_item_id, [v[idx_tb_item_id], v[idx_name]], v[idx_trade_prop_value - 1], v[idx_trade_prop_value], default_value=0)
            v[idx_trade_prop_value - 1] = json.dumps(v[idx_trade_prop_value-1], ensure_ascii=False)
            v[idx_trade_prop_value] = json.dumps(v[idx_trade_prop_value], ensure_ascii=False)
            v.insert(0, item_id)
            r['E'].append(v)
        add_time_log('sample process E table')

    h_uuid_all = {}
    if 'A' in d:
        cols = r['A'][0]
        idx_A_tb_item_id = cols.index('item_id')
        idx_A_name = cols.index('name')
        idx_A_trade_prop_value = cols.index('trade_props.value')
        for v in d['A']:
            v = list(v)
            v[idx_uuid] = str(v[idx_uuid])
            # h_uuid_all[v[idx_uuid]] = 1
            item_id = get_from_hash_by_keys(h_item_id, [v[idx_A_tb_item_id], v[idx_A_name]], v[idx_A_trade_prop_value-1], v[idx_A_trade_prop_value])
            v[idx_A_trade_prop_value - 1] = json.dumps(v[idx_A_trade_prop_value-1],ensure_ascii=False)
            v[idx_A_trade_prop_value] = json.dumps(v[idx_A_trade_prop_value],ensure_ascii=False)
            v.insert(0, item_id)
            r['A'].append(v)
        add_time_log('sample process A table')

    #取人工答题表数据
    h_brush_id = {}
    if len(l_uniq_k) > 0:
        sql = 'select uuid from artificial_local.entity_{eid}_origin_parts where (snum,pkey,uniq_k) in ({where})'.format(eid=eid, where=','.join("({snum},'{pkey}',{uniq_k})".format(snum=row[0], pkey=row[1],uniq_k=row[2]) for row in l_uniq_k))
        data = h_db['chslave'].query_all(sql)
        for row in data:
            h_uuid_all[row[0]] = 1

    if len(h_uuid_all) > 0:
        sql = "select distinct brush_id from sop_b.entity_prod_90513_B where uuid in ({uuid_str})".format(uuid_str=','.join(["'{x}'".format(x=x) for x in h_uuid_all.keys()]))
        data = h_db['chmaster'].query_all(sql)
        for row in data:
            h_brush_id[row[0]] = 1

    if len(h_brush_id) > 0:
        p = {
            'eid': eid,
            'where': {'id': list(h_brush_id.keys())},
            'table_list': ['P']
        }
        r2 = get_item_detail(h_db, p)
        for k in r2:
            d[k] = r2[k]
    add_time_log('sample get P table data')
    if 'P' in d:
        for v in d['P']:
            if get_split and int(v[idx_split]) > 0: #在下面的insert语句之前
                h_entity_id[v[0]] = 1
            if get_pid:
                h_pid[v[idx_pid]] = 1
            v = list(v)
            item_id = h_brush_id.get(str(v[0]), [0,0])[1]   #此处特殊，因为key里面已经加入了item_id
            v.insert(0, item_id)
            r['P'].append(v)
        add_time_log('sample process P table')

    #第三步处理套包
    if get_split and len(h_entity_id) > 0:
        split_table = table_list_ref['Split'](h_db, eid)
        p = {'where': {'entity_id': list(h_entity_id.keys())}}
        d = split_table.query_item(p)
        for v in d:
            r['Split'].append(v)
            h_pid[v[idx_split_pid]] = 1
        add_time_log('sample get Split table')

    #第四步取sku信息
    if get_pid and len(h_pid) > 0:
        stable = table_list_ref['S'](h_db, eid)
        p = {
            'eid': eid,
            'where': {
                'pid': list(h_pid.keys())
            },
        }
        d = stable.query_item(p)
        r['S'].extend(d)
        add_time_log('sample get S table')

    if 'E' in r:
        r['E'][0].insert(0, 'item_id')
        if mode != MODE_E_NEW_CONSTANT:
            r['E'][0].append('uuid')
    if 'A' in r:
        r['A'][0].insert(0, 'item_id')
    if 'D' in r:
        r['D'][0].insert(0, 'item_id')
    if 'P' in r:
        r['P'][0].insert(0, 'item_id')
    return r


def get_fields(batch_id, fields, table, h_db, a=0):
    pos_name = {}
    if table == 'I':
        sql = 'select pos_id,name from cleaner.clean_pos where batch_id = {} and deleteFlag=0'.format(batch_id)
        for i in h_db['mixdb'].query_all(sql):
            pos_name[int(list(i)[0])] = list(i)[1]

    fields_ret = []

    p1 = re.compile(r'^mp(\d+)$')
    p2 = re.compile(r'^sp(\d+)$')
    p3 = re.compile(r'^spid(\d+)$')
    for i in fields:
        f = i
        r1 = p1.findall(f)
        r2 = p2.findall(f)
        r3 = p3.findall(f)
        if table == 'I':
            if len(r1) > 0 or len(r2) > 0 or len(r3) > 0:
                try:
                    name = pos_name.get(int(r1[0]), '')
                except:
                    try:
                        name = pos_name.get(int(r2[0]), '')
                    except:
                        name = pos_name.get(int(r3[0]), '')
                fields_ret.append(f + '{}(清洗表I)'.format(name))
                print(f + '{}(清洗表I)'.format(name))
            else:
                fields_ret.append(f)
        else:
            if len(r1) > 0 or len(r2) > 0 or len(r3):
                fields_ret.append(f + '({})'.format(table))
            else:
                fields_ret.append(f)
    return fields_ret


def sku_convert(p, h_db):
    # sql_sku_id = ','.join(['\''+str(t)+'\'' for t in p['pid']])
    sql_sku_id = p['pid']
    sku_id_list = sql_sku_id
    for each_sql_sku_id in sql_sku_id:
        sql = 'select distinct(pid) from product_lib.entity_{eid}_item_split where entity_id in (select id from product_lib.entity_{eid}_item where pid = {})'.format(each_sql_sku_id, eid=p['eid'])
        # select * from product_lib.product_90513 where pid in (select pid from product_lib.entity_90513_item_split where entity_id = 168);
        tmp = h_db['mixdb'].query_all(sql)
        sub_pid = []
        if tmp != None or (len(tmp) > 0):
            sub_pid = [int(t[0]) for t in tmp]
        sku_id_list = sku_id_list + sub_pid

    sql = 'select pid from artificial.product_{} where sku_id in ({})'.format(p['eid'], ','.join([str(t) for t in sku_id_list]))
    pid_list = [t[0] for t in h_db['chslave'].query_all(sql)]
    p['pid'] = pid_list
    return p


def source_ref(source, shop_type):
    map = {2: 'jd', 3: 'gome', 4: 'jumei', 5: 'kaola', 6: 'suning', 7: 'vip', 8: 'pdd', 9: 'jx', 10: 'tuhu',
           '2': 'jd', '3': 'gome', '4': 'jumei', '5': 'kaola', '6': 'suning', '7': 'vip', '8': 'pdd', '9': 'jx', '10': 'tuhu'}
    if source == 1 and (shop_type < 20 and shop_type > 10 ):
        return 'tb',
    elif source == 1 and (shop_type > 20 or shop_type < 10 ):
        return 'tmall',
    else:
        return map[source]


def read_sheet(r, h_db,p):
    table_list = p.get('table_list', ['I'])
    mode = get_mode(None, table_list)
    batch_id = p['batch_id']
    entity_id_by_pid = {}
    brush_by_item_id = {}

    e_by_item_id = {}
    pid_by_item_id = {}

    # 'E'
    tb_item_id_by_pid = {}
    E_fields = []
    sku_name_by_ch_pid = {}
    sku_name_by_sql_pid = {}
    sql_pid_by_ch_pid = {}
    h_pid = {}
    h_ch_pid_by_item_id = {}
    pid_by_tb_item_id = {}
    a_by_tb_item_id = {}
    e_by_incre_id = {}
    mpp_e = {}
    e_by_uuid = {}
    brand_by_bid = {}
    d_by_uuid = {}
    if 'D' in r and len(r['D']) > 1:
        table = r['D']
        j = 0
        idx_D_uuid = -1
        for row in table:
            j += 1
            if j == 1:
                idx_D_uuid = row.index('uuid')
                idx_cid = row.index('cid')
                idx_sid = row.index('sid')
                idx_tb_item_id = row.index('item_id')
                idx_name = row.index('name')
                idx_pkey = row.index('date')
                idx_trade_prop_value = row.index('trade_props.value')
                idx_prop_value = row.index('props.value')

                continue
            uuid = row[idx_D_uuid]
            prop_name = common.pre_format(row[idx_trade_prop_value-1])
            prop_value = common.pre_format(row[idx_trade_prop_value])
            l = [row[idx_cid], row[idx_sid], row[idx_tb_item_id], row[idx_name], row[idx_pkey], prop_value]
            key = common.get_join_key(l)
            d_by_uuid[key] = uuid
            prop_name = common.pre_format(row[idx_prop_value-1])
            prop_value = common.pre_format(row[idx_prop_value])
            prop = ','.join([i[1] + ':' + prop_value[i[0]] for i in enumerate(prop_name)])
            e_by_uuid[uuid] = prop

    if 'E' in r:
        table = r['E']
        E_fields = table[0]
        E_fields = get_fields(batch_id, E_fields, 'E', h_db)
        h_bid_list = {}
        for j in range(len(table)):
            E_incre_id = j
            if j == 0:
                continue
            i = table[j]
            a = {}
            item_id = int(i[0])
            for idx in range(len(E_fields)):
                a[E_fields[idx]] = i[idx]
            if item_id not in e_by_item_id:
                e_by_item_id[item_id] = []
            e_by_item_id[item_id].append(a)
            pid = int((a['clean_pid'] if mode == MODE_E_NEW_CONSTANT else a['pid']))
            tb_item_id = (a['item_id'] if mode == MODE_E_NEW_CONSTANT else a['tb_item_id'])
            source = a['source']
            prop = (a['clean_props.value'] if mode == MODE_E_NEW_CONSTANT else a['prop_all.value'])
            if pid not in h_pid and pid != 0:
                h_pid[pid] = 1
            h_ch_pid_by_item_id[item_id] = pid

            if pid not in tb_item_id_by_pid:
                tb_item_id_by_pid[pid] = []
            if tb_item_id not in tb_item_id_by_pid[pid]:
                tb_item_id_by_pid[pid].append(tb_item_id)

            if tb_item_id not in pid_by_tb_item_id:
                pid_by_tb_item_id[tb_item_id] = []
            pid_by_tb_item_id[tb_item_id].append(pid)
            if tb_item_id not in a_by_tb_item_id:
                a_by_tb_item_id[tb_item_id] = []

            a_by_tb_item_id[tb_item_id].append(a)
            e_by_incre_id[E_incre_id] = a


            key = '{}-{}-{}'.format(tb_item_id, source, prop)
            if key not in mpp_e:
                mpp_e[key] = 0
            if item_id != 0:
                mpp_e[key] = item_id
            # e_by_uuid[a['uuid']] = a  #改成从D表中查询

            # key = '{}-{}-{}'.format(a['name'],tb_item_id, source, prop)
            # if key not in mpp_e:
            #     mpp_e[key] = 0
            # if item_id != 0:
            #     mpp_e[key] = item_id
            # e_by_uuid[a['uuid']] = a
            if a['all_bid'] > 0:
                h_bid_list[str(a['all_bid'])] = 1
            if a['alias_all_bid'] > 0:
                h_bid_list[str(a['alias_all_bid'])] = 1

        l = h_bid_list.keys()
        if len(l) > 0:
            sql = 'select bid, name from artificial.all_brand where bid in ({})'.format(','.join(l))
            brand_by_bid = {int(v[0]): v[1] for v in h_db['chslave'].query_all(sql)}

        if len(h_pid) > 0:
            sql = 'select sku_id,pid,name from product_{} where pid in ({})'.format(p['eid'], ','.join([str(t) for t in h_pid.keys()]))
            data = h_db['chslave'].query_all(sql)
            for row in data:
                sku_name_by_ch_pid[int(row[1])] = row[2]
                sql_pid_by_ch_pid[int(row[1])] = int(row[0] if row[0] != None else 0)
            sql = 'select pid, name from product_lib.product_{} where pid in (%s)'.format(p['eid'])
            ret = utils.easy_query(h_db['mixdb'], sql, list(sql_pid_by_ch_pid.values()))
            sku_name_by_sql_pid = {int(v[0]): v[1] for v in ret}
        for item_id in h_ch_pid_by_item_id:
            pid_by_item_id[item_id] = sql_pid_by_ch_pid.get(h_ch_pid_by_item_id[item_id], '')

    if list(r.keys()) == ['E']:
        # 只传入E表，单独对E表数据做处理时
        return E_fields, mpp_e, e_by_incre_id

    # 'I'
    clean_fields = []
    clean_by_item_id = {}
    if 'I' in r:
        table = r['I']
        clean_fields = table[0]
        clean_fields = get_fields(batch_id, clean_fields, 'I', h_db, 1)

        for j in range(len(table)):
            i = table[j]
            if j == 0:
                continue
            a = {}
            item_id = int(i[0])
            for idx in range(len(clean_fields)):
                a[clean_fields[idx]] = i[idx]
            clean_by_item_id[item_id] = a

    # 'A'
    a_by_item_id = {}
    a_by_uuid = {}
    a_fields = []
    if 'A' in r:
        table = r['A']
        a_fields = table[0]
        a_fields = get_fields(batch_id, a_fields, 'A', h_db, 1)
        for j in range(len(table)):
            i = table[j]
            if j == 0:
                continue
            a = {}
            item_id = int(i[0])

            for idx in range(len(a_fields)):
                a[a_fields[idx]] = i[idx]
            uuid = a['uuid']
            a_by_uuid[uuid] = a
            a_by_item_id[item_id] = a
    # 'P'
    uuid_by_item_id = {}
    brush_fields = []
    pack_pid_by_entity_id = {}
    if 'P' in r:
        table = r['P']
        brush_fields = table[0]
        brush_fields = get_fields(batch_id, brush_fields, 'P', h_db)
        for j in range(len(table)):
            i = table[j]
            if j == 0:
                continue
            a = {}
            item_id = int(i[0])
            for idx in range(len(brush_fields)):
                a[brush_fields[idx]] = i[idx]
            brush_by_item_id[item_id] = a
            pid_by_item_id[item_id] = int(a['pid'])
            entity_id = int(a['id'])
            pid = int(a['pid'])
            if pid not in entity_id_by_pid:
                entity_id_by_pid[pid] = []
            entity_id_by_pid[pid].append(entity_id)
            pack_pid_by_entity_id[entity_id] = pid
            uuid = a['uuid']
            uuid_by_item_id[item_id] = uuid

    # 'S'
    sku_fields = []
    sku_by_pid = {}
    if 'S' in r:
        table = r['S']
        sku_fields = table[0]
        sku_fields = get_fields(batch_id, sku_fields, 'S', h_db)
        for j in range(len(table)):
            i = table[j]
            if j == 0:
                continue
            a = {}
            pid = int(i[0])
            for idx in range(len(sku_fields)):
                a[sku_fields[idx]] = i[idx]
            sku_by_pid[int(pid)] = a

    # 'Split'
    split_fields = []
    entity_id_list = []
    num_by_entity_id_pid = {}
    split_pid_by_entity_id = {}
    if 'Split' in r:
        table = r['Split']
        split_fields = table[0]
        split_fields = get_fields(batch_id, split_fields, 'Split', h_db)
        for j in range(len(table)):
            i = table[j]
            if j == 0:
                continue
            entity_id = int(i[0])
            pid = int(i[1])
            if entity_id not in split_pid_by_entity_id:
                split_pid_by_entity_id[entity_id] = []
            split_pid_by_entity_id[entity_id].append(pid)
            entity_id_list.append(entity_id)
            num_by_entity_id_pid[(entity_id, pid)] = int(i[2])
        entity_id_list = list(set(entity_id_list))

    return brush_fields, clean_fields, E_fields, sku_fields, pid_by_item_id, brush_by_item_id, clean_by_item_id, \
           sku_by_pid, e_by_item_id, sku_name_by_ch_pid, sku_name_by_sql_pid, tb_item_id_by_pid, \
           pid_by_tb_item_id, a_by_tb_item_id, entity_id_list, num_by_entity_id_pid, pack_pid_by_entity_id, e_by_incre_id, mpp_e, \
           uuid_by_item_id, a_by_uuid, a_by_item_id, e_by_uuid, sql_pid_by_ch_pid, brand_by_bid, d_by_uuid


def get_sales(h_db, uuid_list, eid):
    data_by_uuid = {}
    if len(uuid_list) == 0:
        return data_by_uuid

    sql = "select sum(sales*sign) ss,sum(num*sign) sn, if(sum(num*sign)>0, toString(ss/sn), '-'), uuid from sop.entity_prod_{}_A where uuid in ({}) group by uuid ".format(eid, ','.join(["'{}'".format(x) for x in uuid_list]))
    ret = h_db['chmaster'].query_all(sql)
    for v in ret:
        sum_sales = v[0]
        sum_num = v[1]
        avg_price = v[2]
        uuid = str(v[3])
        data_by_uuid[uuid] = [sum_sales, sum_num, avg_price]
    return data_by_uuid


def add_sheet_concatenate(h_db, p):
    global time_log
    time_log = TimeLog()
    if p.get('eid', 0) <= 0:
        return []

    if 'table_list' in p and ('E' in p['table_list'] or 'E_new' in p['table_list']) and p.get('pid',[]) != []:
        p = sku_convert(p, h_db)

    r = get_sample_item(h_db, p)
    if 'A' in r:
        # r['A'][0].insert(1, 'tb_item_id')
        r['A'][0][0] = '清洗表ID'
    if 'E' in r:
        r['E'][0][0] = '清洗表ID'
    if 'D' in r:
        r['D'][0][0] = '清洗表ID'
    if 'P' in r:
        r['P'][0][0] = '清洗表ID'
    mode = get_mode(None, p.get('table_list', []))

    # print(r)
    brush_fields, clean_fields, E_fields, sku_fields, pid_by_item_id, brush_by_item_id, clean_by_item_id, sku_by_pid, e_by_item_id, sku_name_by_ch_pid, \
    sku_name_by_sql_pid, tb_item_id_by_pid, pid_by_tb_item_id, a_by_tb_item_id, entity_id_list, num_by_entity_id_pid, pack_pid_by_entity_id, e_by_incre_id, \
    mpp_e, uuid_by_item_id, a_by_uuid, a_by_item_id, e_by_uuid, sql_pid_by_ch_pid, brand_by_bid, d_by_uuid = read_sheet(r, h_db, p)
    add_time_log('read_sheet')

    pattern = re.compile(r'^sp(\d+)')

    fo = []
    if len(clean_by_item_id) == 0:
        ultimate_sheet_name(r)
        return r
    elif 'E' not in list(r.keys()):
        ultimate_sheet_name(r)
        return r

    count_sp = []
    for key in clean_fields:
        a = re.findall(pattern, key)
        if len(a) > 0:
            count_sp.append(int(a[0]))
    count_sp = max(count_sp)

    h_a_data_by_uuid = get_sales(h_db, list(e_by_uuid.keys()), p['eid'])
    add_time_log('get_sales')

    clean_sp_fields = {}
    for key in clean_by_item_id[list(clean_by_item_id.keys())[0]].keys():
        a = re.findall(pattern, key)
        if len(a) > 0:
            sp_pos = int(a[0])
            clean_sp_fields[sp_pos] = key

    for incre_id in e_by_incre_id:
        data = e_by_incre_id[incre_id]
        # key = '{}-{}-{}'.format(data['name'],data['tb_item_id'], data['source'], data['prop_all.value'])
        # item_id = mpp_e[key]
        item_id = data['清洗表ID']
        id_clean = item_id

        if item_id != 0:
            trade_prop_all = clean_by_item_id[item_id]['trade_prop_all']
            prop_all = ''
            all_bid = data['all_bid']
            brand_all_bid = brand_by_bid.get(int(all_bid), '')
            alias_all_bid = data['alias_all_bid']
            brand_alias_all_bid = brand_by_bid.get(int(alias_all_bid), '')
            tb_item_id = data[('item_id' if mode == MODE_E_NEW_CONSTANT else 'tb_item_id')]
            source = data['source']
            cid = data['cid']
            month = data[('pkey' if mode == MODE_E_NEW_CONSTANT else 'month')]
            name = data['name']
            shop_name = ('' if mode == MODE_E_NEW_CONSTANT else data['shop_name'])
            shop_type = data['shop_type']
            sid = data['sid']
            pid = data[('clean_pid' if mode == MODE_E_NEW_CONSTANT else 'pid')]
            sku_name_ch = sku_name_by_ch_pid.get(int(pid), '')
            sku_id = sql_pid_by_ch_pid.get(int(pid), '')
            sku_name_db = sku_name_by_sql_pid.get(sku_id, '')
            sales = data.get('sales', '')
            e_avg_price = data.get('avg_price', '')
            e_num = data.get('num', '')
            sp = []
            prop_value = eval(data[('clean_props.value' if mode == MODE_E_NEW_CONSTANT else 'prop_all.value')])
            trade_prop_name_e = str(data[('trade_props.name' if mode == MODE_E_NEW_CONSTANT else 'trade_prop_all.name')])
            trade_prop_value_e = str(data[('trade_props.value' if mode == MODE_E_NEW_CONSTANT else 'trade_prop_all.value')])

            for key in clean_by_item_id[item_id].keys():
                a = re.findall(pattern, key)
                if len(a) > 0:
                    sp_pos = int(a[0])
                    # 按 清洗表，答题表，SKU表，出数表的顺序
                    sp.append(clean_by_item_id[item_id][key])
                    sp.append(brush_by_item_id.get(item_id, {}).get('spid{}(P)'.format(sp_pos), ''))
                    sp.append(brush_by_item_id.get(item_id, {}).get('modify_sp{}(P)'.format(sp_pos), ''))
                    sp.append(sku_by_pid.get(pid_by_item_id.get(item_id, ''), {}).get('spid{}(S)'.format(sp_pos), ''))
                    if len(prop_value) >= sp_pos:
                        sp.append(prop_value[sp_pos - 1])
                    else:
                        sp.append('')
                    sp.append('')
            row = data
            # l = [row['cid'], row['sid'], row['tb_item_id'], row['name'], row['month'], row['trade_prop_all.value']]
            # key = common.get_join_key(l)
            # uuid = d_by_uuid.get(key, '')
            uuid = row[('uuid2' if mode == MODE_E_NEW_CONSTANT else 'uuid')]
            a_prop = e_by_uuid.get(uuid, '')
            a_sales, a_num, a_avg_price = h_a_data_by_uuid.get(uuid, ['', '', ''])
            row = [id_clean, source, tb_item_id, month, name, shop_name, shop_type, sid, cid, all_bid, brand_all_bid, alias_all_bid,
                   brand_alias_all_bid, trade_prop_name_e, trade_prop_value_e, a_prop,
                   pid, sku_name_ch, sku_id, sku_name_db, uuid, sales, a_sales, e_num, a_num, e_avg_price, a_avg_price] + sp
            fo.append(row)
    # -------------------------------------------#
    column_names = ['清洗表ID', 'source(出数表E)', 'tb_item_id(出数表E)', 'month(出数表E)', 'name(出数表E)', 'shop_name(出数表E)', 'shop_type(出数表E)', 'sid(出数表E)', 'cid(出数表E)',
                    'all_bid(出数表E)', 'brand_all_bid(出数表E)', 'alias_all_bid(出数表E)', 'brand_alias_all_bid(出数表E)', '交易属性名(出数表E)', '交易属性值(出数表E)','props(底层表A)', 'pid(出数表E)', 'sku_name(出数表E)',
                    'pid(型号库SKU_id)', 'sku_name(型号库SKU_name)','uuid', 'sales(出数表E)', 'A_sales(底层表A)', 'num(出数表E)', 'sum_num(底层表A)', 'avg_price(出数表E)',
                    'avg_price(底层表A)']
    sp_names = []
    for sp_pos in range(1, count_sp + 1):
        sp_names = sp_names + [clean_sp_fields[sp_pos], 'spid{}(宝贝答题表P属性)'.format(sp_pos), 'modify_sp{}(宝贝答题表P属性)'.format(sp_pos), 'spid{}(型号库答题表S属性)'.format(sp_pos), 'sp{}(出数表E属性)'.format(sp_pos), '']
    column_names = column_names + sp_names

    r['Concat'] = [column_names] + fo
    ultimate_sheet_name(r)
    add_time_log('concat data')
    logger.info(str(time_log))
    return r

def ultimate_sheet_name(r):
    h = {
        'A': '底层表A',
        'D': '中间表D',
        'E': '出数表E',
        'I': '机洗表I',
        'P': '宝贝答题表P',
        'S': '型号库答题表S',
        'Split': '拆套包表Split',
    }
    for k in ['I', 'P', 'S', 'Split', 'A', 'D', 'E']:
        v = h[k]
        if k in r:
            r[v] = r.pop(k)

def get_table_list_for_item(l, table_in):
    l2 = table_list_get_item_direct
    l3 = list(set(l) & set(l2))
    if table_in in l3:
        l3.remove(table_in)
    return l3

def query_summary(h_db, p):
    eid = qp.get_with_checker(p, 'eid', 'uint')
    mode = qp.get_with_checker(p, 'mode', 'uint', None)
    #month 平台 品类 品牌
    where = p['where'] if 'where' in p else {}
    table_list = p.get('table_list', ['E', 'I', 'P'])
    mode = get_mode(mode, table_list)
    type, tb_item_id_field, month_field, trade_prop_field, group_by = pre_process_params(mode, p)
    fields = copy.deepcopy(group_by)
    where_add = []
    if mode == MODE_A_CONSTANT:
        fields += ['count(1)', 'sum(num*sign) as num_total', 'sum(sales*sign) as sales_total']
        where_add.append('uuid not in (select uuid from sop.entity_prod_{eid}_A where sign=-1)'.format(eid=eid))
    else:
        fields += ['count(1)', 'sum(num) as num_total', 'sum(sales) as sales_total']
    where_add.append("{date_key}>='{start_date}'".format(start_date=p['start_date'], date_key=month_field))
    where_add.append("{date_key}<'{end_date}'".format(end_date=p['end_date'], date_key=month_field))
    if 'where' not in p:
        p['where'] = {}
    p['where']['format'] = where_add
    p['fields'] = fields
    p['group_by'] = group_by
    v = table_list_ref[table_by_mode[mode]]
    data = list(v(h_db, eid).query_item(p))
    data.insert(0, group_by + ['item_count', 'num_total', 'sales_total'])
    return data

def query_report(h_db, p):
    eid = qp.get_with_checker(p, 'eid', 'uint')
    mode = qp.get_with_checker(p, 'mode', 'uint', MODE_E_NEW_CONSTANT)
    type, tb_item_id_field, month_field, trade_prop_field, group_by = pre_process_params(mode, p)
    where_add = []
    where_add.append("{date_key}>='{start_date}'".format(start_date=p['start_date'], date_key=month_field))
    where_add.append("{date_key}<'{end_date}'".format(end_date=p['end_date'], date_key=month_field))
    where_add.append("pkey>='{start_date}'".format(start_date=utils.get_month_date_str(p['start_date'])))
    where_add.append("pkey<'{end_date}'".format(end_date=utils.get_month_date_str(p['end_date'], True)))
    if 'where' not in p:
        p['where'] = {}
    p['where']['format'] = where_add
    v = table_list_ref[table_by_mode[mode]]
    data = list(v(h_db, eid).query_item(p))
    return data

def query_trend_report(h_db, p):
    basis = qp.get_with_checker(p, 'basis', 'uint', BASIS_TONGBI)
    start_date = p['start_date']
    start_date = utils.get_month_date_str(start_date)
    end_date = p['end_date']
    a = arrow.get(start_date, 'YYYY-MM-DD')
    if basis == BASIS_TONGBI:
        args_p = {'years': -1}
    elif basis == BASIS_HUANBI:
        args_p = {'months': -1}
    search_start_date = a.shift(**args_p).format('YYYY-MM-DD')
    p['start_date'] = search_start_date
    data = query_report(h_db, p)
    h = {}
    for row in data:
        date, key, value = list(row)
        date = str(date)
        if date not in h:
            h[date] = {}
        h[date][key] = value
    data = {}
    for r in arrow.Arrow.span_range('month', arrow.get(start_date), arrow.get(end_date)):
        start_a = r[0]
        start = start_a.format('YYYY-MM-DD')
        if start not in h:
            continue
        check_a = start_a.shift(**args_p)
        check = check_a.format('YYYY-MM-DD')
        data[start] = {}
        for key in h[start]:
            value = h[start][key]
            last = 0
            if check in h and key in h[check]:
                last = h[check][key]
            data[start][key] = [value, last]
    return data

def query_summary_special(h_db, p):
    eid = qp.get_with_checker(p, 'eid', 'uint')
    mode = qp.get_with_checker(p, 'mode', 'uint', None)
    #month 平台 品类 品牌
    where = p['where'] if 'where' in p else {}
    table_list = p.get('table_list', ['C'])
    mode = get_mode(mode, table_list)
    if mode != MODE_C_CONSTANT:
        return {}

    db = h_db['mixdb']
    sql = 'select use_all_table from clean_batch where eid={}'.format(eid)
    use_all_table = db.query_scalar(sql)
    if use_all_table is None:
        return {}

    if int(use_all_table) == 1:
        table_model = AllTable(h_db, eid)
        v = AllTable
        prefix = 'c_'
        alias_all_bid = 'multiIf(b_all_bid>0,b_all_bid,c_all_bid>0,c_all_bid,alias_all_bid) as alias_all_bid'
        if 'where' in p:
            for k in p['where']:
                if not re.match('^sp\d+$', k):
                    continue
                p['where']['{}{}'.format(prefix, k)] = p['where'][k]
                del p['where'][k]
    else:
        table_model = CTable(h_db, eid)
        v = table_list_ref[table_by_mode[mode]]
        prefix = ''
        alias_all_bid = "if(alias_all_bid_sp = 0, alias_all_bid, alias_all_bid_sp) as alias_all_bid"
    cols = table_model.get_columns()
    h_cols = {}
    for k in cols:
        h_cols[k] = 1

    type, tb_item_id_field, month_field, trade_prop_field, group_by = pre_process_params(mode, p)
    fields_num = ['count(1) as amount']
    where_add = []
    where_add.append("{date_key}>='{start_date}'".format(start_date=p['start_date'], date_key=month_field))
    where_add.append("{date_key}<'{end_date}'".format(end_date=p['end_date'], date_key=month_field))
    where_add.append("pkey>='{start_date}'".format(start_date=utils.get_month_date_str(p['start_date'])))
    where_add.append("pkey<'{end_date}'".format(end_date=utils.get_month_date_str(p['end_date'], True)))
    if 'where' not in p:
        p['where'] = {}
    p['where']['format'] = where_add

    h_brand_pos = get_brand_pos(h_db['mixdb'], eid)

    #处理参数中的where
    i = 0
    h_sp = {}

    while True:
        i += 1
        key = 'sp{i}'.format(i=i)
        if key not in h_cols:
            break
        if key not in p['where']:
            continue
        h_sp[key] = p['where'][key]
        del p['where'][key]

    sp1 = '{prefix}sp1'.format(prefix=prefix)
    group_by = ['source', 'cid', sp1]
    p['fields'] = group_by + fields_num
    p['group_by'] = group_by
    p['order_by'] = ['amount desc']
    h = {}
    h['category'] = list(v(h_db, eid).query_item(p))

    i = 0
    while True:
        i += 1
        key = '{prefix}sp{i}'.format(i=i, prefix=prefix)
        if key not in h_cols:
            break
        if i in h_brand_pos:
            p['fields'] = ["if({sp}='' and {sp1}!='其它', 'blank_special', {sp}) as sp".format(sp1=sp1, sp=key), alias_all_bid] + fields_num
            p['group_by'] = ['sp', 'alias_all_bid']
        else:
            p['fields'] = ["if({sp}='' and {sp1}!='其它', 'blank_special', {sp}) as sp".format(sp1=sp1, sp=key)] + fields_num
            p['group_by'] = ['sp']
        h[key] = list(v(h_db, eid).query_item(p))

    if len(h_sp) > 0:
        group_by = []
        for key in h_sp:
            group_by.append(key)
            p['where'][key] = h_sp[key]
        p['fields'] = group_by + fields_num
        p['group_by'] = group_by
        h['sp_list'] = list(v(h_db, eid).query_item(p))
    h['brand_pos'] = h_brand_pos

    return h

def get_brand_pos(db, eid, type=1000):
    sql = 'select distinct pos_id from cleaner.clean_pos where batch_id in (select batch_id from cleaner.clean_batch where eid={eid}) and type={type}'.format(eid=eid, type=type)
    data = db.query_all(sql)
    h = {}
    for row in data:
        h[row[0]] = 1
    return h

def get_project_list(h_db, p):
    eid = p['id'] if 'id' in p else 0
    table_model = PAllTable(h_db, eid)
    if 'id' in p:
        del p['id']

    #整理参数
    l = common.transfer_tb_item_id(p)
    p = {
        'fields': ['eid', 'count(1)'],
        'where': {('source', 'tb_item_id'): l},
        'group_by': ['eid']
    }
    if eid != 0:
        p['where']['eid'] = eid

    data = table_model.query_item(p)
    h_eid = {}
    for row in data:
        eid, c = list(row)
        h_eid[eid] = c
    h = {}
    if len(h_eid) > 0:
        sql = 'select batch_id, eid, name from cleaner.clean_batch where eid in ({})'.format(','.join([str(x) for x in h_eid]))
        data = h_db['mixdb'].query_all(sql)
        for row in data:
            batch_id, eid, name = list(row)
            h[eid] = [batch_id, name]
    l = utils.vsort(h_eid)
    r = []
    for eid, c in l:
        batch_id, name = list(h[eid])
        r.append({'id': eid, 'name': name, 'count': c, 'batch_id': batch_id})
    return {'project_info': r}

def get_item_detail_manual(h_db, p):
    eid = p['id']
    table_model = PTable(h_db, eid)

    tb_item_id = p['tb_item_id'] if 'tb_item_id' in p else {}
    page = p.get('page', 1)
    limit = p.get('limit', 1000)
    limit = limit if limit > 0 else 1000
    start, end = (page-1) * limit, page * limit
    mixdb = h_db['mixdb']
    l = common.transfer_tb_item_id(tb_item_id)
    sql = 'select spid,name from dataway.clean_props where eid={}'.format(eid)
    data = mixdb.query_all(sql)

    fields = ['id', 'source', 'tb_item_id', 'name', 'trade_prop_all', 'img']
    idx_max = len(fields)
    h_spid = {}
    idx = 0
    for row in data:
        spid, name = list(row)
        h_spid[idx] = name
        idx += 1
        fields.append('spid{}'.format(spid))


    p = {
        'fields': fields,
        'where': {('source', 'tb_item_id'): l},
        'order_by': ['id'],
        'limit': "{},{}".format(start, end)
    }
    data2 = table_model.query_item(p)
    l = []
    for row in data2:
        id, source, tb_item_id, name, trade_prop_all, img = row[:idx_max]
        l_spid = row[idx_max:]
        clean_props = {}
        for i in range(idx_max):
            k = h_spid[i]
            v = l_spid[i]
            clean_props[k] = v
        l.append({
            'id': id,
            'source': source,
            'tb_item_id': tb_item_id,
            'name': name,
            'trade_prop_all': trade_prop_all,
            'img': img,
            'clean_props': clean_props
        })

    return {'item_info': l}

def get_project_summary(h_db, p):
    eid = p['id']
    mixdb = h_db['mixdb']
    table_model = PTable(h_db, eid)

    sql = 'select spid,name from dataway.clean_props where eid={}'.format(eid)
    data = mixdb.query_all(sql)
    clean_props = []
    for spid, name in data:
        k = 'spid{}'.format(spid)
        p = {
            'fields': [k, 'any_value(source)', 'any_value(tb_item_id)', 'sum(sales)/100 as sales_total'],
            'order_by': ['sales_total desc', 'id'],
            'group_by': [k],
            'limit': 10,
        }
        data2 = table_model.query_item(p)
        values = []
        for v, source, tb_item_id, sales in data2:
            values.append({
                'value': v,
                'source': source,
                'tb_item_id': tb_item_id,
                'sales': int(sales)
            })
        clean_props.append({
            'id': spid,
            'name': name,
            'values': values
        })

    #查询sku统计信息
    sku = []
    try:
        sql = "select alias_all_bid,count(distinct pid) as c from product_lib.entity_{eid}_item where alias_all_bid not in (0,26) and flag=2 and pid not in (select pid from product_lib.product_{eid} where name like '\_\_%') group by alias_all_bid order by c desc limit 10".format(
            eid=eid
        )
        data = mixdb.query_all(sql)
        l_bid = []
        for row in data:
            l_bid.append(row[0])
        h_bid = {}
        if len(l_bid) > 0:
            sql = 'select bid,name from brush.all_brand where bid in ({})'.format(','.join([str(x) for x in l_bid]))
            data2 = mixdb.query_all(sql)
            for bid, name in data2:
                h_bid[bid] = name
        for row in data:
            bid, c = list(row)
            name = h_bid[bid]
            sku.append({
                'alias_all_bid': bid,
                'name': name,
                'count': c
            })

    except Exception as e:
        print(e)

    return {'clean_props': clean_props, 'sku': sku}












