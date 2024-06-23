
# from collections import Iterator
import csv
import json
import re
import time
from models.report import qp
from models.report import base
from extensions import utils
import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('report')

source_en2cn = {'tb':'淘宝','jd':'京东','tmall':'天猫'}
source_name_ref = ['', 'ali','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu']
type_ref = ['', 'A', 'B', 'C', 'D', 'E']
blank_string = '[_blank_]'
clickhouse_name_array_list = ['prop_all.name', 'prop_all.value', 'trade_prop_all.name', 'trade_prop_all.value', 'trade_props.name', 'trade_props.value', 'trade_props_full.name', 'trade_props_full.value']

shop_type_ref = {
    'tb': {11: '集市', 12: '全球购'},
    'tmall': {21: '天猫', 22: '天猫国际', 23: '天猫超市', 24: '天猫国际', 25: '天猫', 26: '猫享自营', 27: '猫享自营国际'},
    'jd': {11: '京东国内自营', 12: '京东国内POP', 21: '京东海外自营', 22: '京东海外POP'},
    'gome': {11: '国美国内自营', 12: '国美国内POP', 21: '国美海外自营', 22: '国美海外POP'},
    'jumei': {11: '聚美国内自营', 12: '聚美海外自营'},
    'kaola': {11: '考拉国内自营', 12: '考拉国内POP', 21: '考拉海外自营', 22: '考拉海外POP'},
    'pdd': {11: '拼多多国内自营', 12: '拼多多国内POP', 21: '拼多多海外自营', 22: '拼多多海外POP'},
    'suning': {11: '苏宁国内自营', 12: '苏宁国内POP', 21: '苏宁海外自营', 22: '苏宁海外POP'},
    'vip': {11: '唯品会国内自营', 12: '唯品会海外自营'},
    'jx': {11: '酒仙自营', 12: '酒仙非自营'},
    'tuhu': {11: '途虎'},
    'dewu': {11: '得物'},
}

def get_shop_type_by_name_list(l):
    r = {}
    h = {}
    for k in l:
        h[k] = 1
    for source in shop_type_ref:
        for shop_type in shop_type_ref[source]:
            shop_type_name = shop_type_ref[source][shop_type]
            if shop_type_name in h:
                r[shop_type] = 1
    return list(r.keys())


def query_data_report(db, p, run=True):
    p = parse_params(p)
    sql = build_sql(p)
    if run:
        return db.query_all(sql)
    else:
        return sql

def parse_params(p):
    new_p = {}
    source = qp.get_with_checker(p, 'source', 'uint', 1)
    eid = qp.get_with_checker(p, 'eid', 'uint')
    type = qp.get_with_checker(p, 'type', 'uint')
    start_date = qp.get_with_checker(p, 'start_date', 'date', '2020-01-01')
    end_date = qp.get_with_checker(p, 'end_date', 'date', '2020-02-01')
    group_by = qp.get_with_checker(p, 'group_by', 'json_str_list', [])
    group_by = check_group_by(group_by)
    fields = p['fields'] if 'fields' in p else []
    table_list = p.get('table_list', ['E'])
    is_new_e = 'E_new' in table_list
    prop_name_key = 'clean_props' if is_new_e else 'prop_all'
    has_all = False
    for i in range(len(fields)):
        if fields[i] == 'month_str':
            fields[i] = "concat(toString(toYear(month)),'-',if(toMonth(month)<10,'0',''),toString(toMonth(month))) as month_str"
        elif fields[i] == '*':
            has_all = True
    if 'prop_name' in p:
        i = 0
        for name in p['prop_name']:
            i += 1
            sp = 'sp' + str(i)
            key = "{prop_name_key}.value[indexOf({prop_name_key}.name, '{name}')] AS {sp}".format(name=name, sp=sp, prop_name_key=prop_name_key)
            fields.append(key)
            group_by.append(sp)
    if not has_all:
        fields = fields + check_fields(type)
    where = check_where(p['where']) if 'where' in p else []
    date_key = 'pkey' if type == 1 or is_new_e else 'month' #todo
    where.append("{date_key}>='{start_date}'".format(start_date=start_date, date_key=date_key))
    where.append("{date_key}<'{end_date}'".format(end_date=end_date, date_key=date_key))
    new_p['table'] = get_table_name(source, eid, type, is_new_e)
    new_p['fields'] = fields
    new_p['where'] = where
    new_p['group_by'] = group_by
    for k in ['order_by', 'limit_by', 'top']:
        if k in p:
            new_p[k] = p[k]
    return new_p

def build_sql(p):
    logger.debug(p)
    sql = 'select '
    sql += ','.join(p['fields']) if 'fields' in p else '*'
    sql += ' from ' + p['table']
    if 'prewhere' in p and len(p['prewhere']) > 0:
        sql += ' prewhere ' + ' and '.join(p['prewhere'])
    if 'where' in p and len(p['where']) > 0:
        sql += ' where ' + ' and '.join(p['where'])
    if 'group_by' in p and p['group_by'] is not None and len(p['group_by']) > 0:
        sql += ' group by ' + ','.join(p['group_by'])
    if 'order_by' in p:
        sql += ' order by ' + ','.join(p['order_by'])
    if 'top' in p:
        sql += ' limit ' + str(p['top'])
        if 'limit_by' in p:
            sql += ' by ' + ','.join(p['limit_by'])
    if 'limit' in p:
        sql += ' limit ' + str(p['limit'])
    logger.info(sql)
    return sql

def check_fields(type):
    return get_stat_sum_fields(type)

def check_group_by(group_by):
    return group_by

def get_sum_fields():
    return ['num', 'sales']

def get_stat_sum_fields(type):
    l = []
    for k in get_sum_fields():
        if type == 1:
            l.append("sum({field}*sign) as {field}_total".format(field=k))
        else:
            l.append("sum({field}) as {field}_total".format(field=k))
    return l

def get_table_name(source, eid, type, is_new_e):
    dbname = 'sop_e' if is_new_e else 'sop'
    return '{dbname}.entity_prod_{eid}_{type}'.format(dbname=dbname, eid=eid, type=type_ref[type])

def get_link(source=None, item_id=None):
    mp = {
        'ali'  : "http://detail.tmall.com/item.htm?id={}",
        'tb'     : "http://item.taobao.com/item.htm?id={}",
        'tmall'  : "http://detail.tmall.com/item.htm?id={}",
        'jd'     : "http://item.jd.com/{}.html",
        'beibei' : "http://www.beibei.com/detail/00-{}.html",
        'gome'   : "http://item.gome.com.cn/{}.html",
        'jumei'  : "http://item.jumei.com/{}.html",
        'kaola'  : "http://www.kaola.com/product/{}.html",
        'suning' : "http://product.suning.com/{}.html",
        'vip'    : "http://archive-shop.vip.com/detail-0-{}.html",
        'yhd'    : "http://item.yhd.com/item/{}",
        'tuhu'   : "https://item.tuhu.cn/Products/{}.html",
        'jx'     : "http://www.jiuxian.com/goods-{}.html",
        'dy'     : "https://haohuo.jinritemai.com/views/product/item2?id={}",
        'cdf'     : "http://www.jiuxian.com/goods-{}.html",
        'lvgou'     : "https://static-image.tripurx.com/productQrV2/{}.jpg",
        'dewu'  : "",
    }
    if source:
        return mp[source].format(item_id)
    return mp

def get_cn_name(name):
    if name in source_en2cn:
        return source_en2cn[name]
    else:
        return name

def check_where(p, table_type=base.table_type_clickhouse):
    if isinstance(p, list):
        return p
    l = []
    for k in p:
        v = parse_blank(p[k])
        logger.debug('k:{k} v:{v}'.format(k=k, v=json.dumps(v)))
        if k == 'format':
            for vv in v:
                l.append(vv)
        elif k in ['prop_name', 'trade_prop_name', 'props', 'trade_prop_name', 'clean_props', 'trade_props']:
            prop_key = 'prop_all' if k == 'prop_name' else 'trade_prop_all' if k == 'trade_prop_name' else k
            for kk in v:
                vv = parse_blank(v[kk])
                l.append(format_key_value("{prop_key}.value[indexOf({prop_key}.name, '{k}')]".format(prop_key=prop_key, k=kk), vv))
        elif k in clickhouse_name_array_list:
            l.append("{k}={v}".format(k=k, v=str(list(v))))
        elif isinstance(k, tuple):
            if not isinstance(v, list) and len(v) > 0:
                continue
            if len(v) == 0:
                continue
            if not isinstance(v[0], list):
                v = [v]
            ll = []
            for vv in v:
                lll = []
                for i in range(len(k)):
                    if k[i] == 'pkey':
                        if isinstance(vv[i], list):
                            month, next_month = list(vv[i])
                        else:
                            month, next_month = utils.get_month_with_next(vv[i])
                        if table_type == base.table_type_clickhouse:
                            lll.append("toDate('{month}')".format(month=month))
                        else:
                            lll.append("'{month}'".format(month=month))
                    else:
                        lll.append(format_key_value(k[i], vv[i], with_key=False))
                ll.append('(' + ' , '.join(lll) + ')')
            l.append('(' + ','.join([("toStartOfMonth(pkey)" if table_type == base.table_type_clickhouse else "DATE_ADD(pkey,interval -day(pkey)+1 day)") if x == 'pkey' else x for x in k]) + ') in (' + ' , '.join(ll) + ')')
        else:
            r = format_key_value(k, v)
            if r is not None:
                l.append(r)
    return l

def parse_blank(v):
    if isinstance(v, list):
        for i in range(len(v)):
            vv = v[i]
            if vv == blank_string:
                v[i] = ''
        return v
    if v == blank_string:
        return ''
    return v

def format_key_value(k, v, with_key=True):
    if isinstance(v, int):
        return "{k}={v}".format(k=k, v=v) if with_key else str(v)
    elif isinstance(v, list) or isinstance(v, tuple):
        if k in clickhouse_name_array_list:
            return "{k}={v}".format(k=k, v=str(list(v))) if with_key else str(list(v))
        if len(v) == 0:
            return None
        return "{k} in ({v})".format(k=k, v=format_list_value(v))
    elif isinstance(v, dict):
        if 'compare' in v:
            vv = v['compare']
            r = []
            if 'min' in vv:
                r.append('{k}>={v}'.format(k=k, v=vv['min']))
            if 'max' in vv:
                r.append('{k}<={v}'.format(k=k, v=vv['max']))
            if 'min_exclude' in vv:
                r.append('{k}>{v}'.format(k=k, v=vv['min_exclude']))
            if 'max_exclude' in vv:
                r.append('{k}<{v}'.format(k=k, v=vv['max_exclude']))
            if len(r) == 0:
                return None
            elif len(r) == 1:
                return r[0]
            else:
                return '(' + ' and '.join(r) + ')'
    else:
        v = v.replace("'", r"\'")
        return "{k}='{v}'".format(k=k, v=v) if with_key else "'{v}'".format(v=v)

def format_list_value(v):
    return ','.join('{kk}'.format(kk=kk) if isinstance(kk, int) else "'{kk}'".format(kk=kk.replace("'", r"\'")) for kk in v)

def list_to_table(l):
    ret = []
    for i in l:
        ret.append([i])
    return ret

def get_e_fields(l, prefix=''):
    r = []
    for k in l:
        if k in ['month', 'prop_all.name', 'prop_all.value', 'trade_prop_all.name', 'trade_prop_all.value', 'created']:
            k = 'toString({prefix}{k})'.format(prefix=prefix, k=k)
        else:
            k = '{prefix}{k}'.format(prefix=prefix, k=k)
        r.append(k)
    return r

def format_key_value_to_string(k, v, separators=None):
    # k = k.replace("'", '"')
    # k = json_decode(k)
    # v = v.replace("'", '"')
    # v = json.decode(v)
    h = {}
    for i in range(len(k)):
        h[k[i]] = v[i]
    return json.dumps(h, ensure_ascii=False, separators=separators)

def pre_format(v):
    v = v.replace("'", '"')
    v = json_decode(v)
    return v

def json_decode(v):
    try:
        v = json.loads(v)
    except:
        return None
    return v

def get_join_key(l, sep='#'):
    return sep.join(str(x) for x in l)

def format_month(date):
    return date[:8] + '01'

def transfer_tb_item_id(p):
    l = []
    for source in p:
        source_name = source_name_ref[int(source)]
        for tb_item_id in p[source]:
            l.append([source_name, tb_item_id])
    return l
