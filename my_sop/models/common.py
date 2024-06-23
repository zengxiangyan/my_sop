from extensions import utils
import application as app
import unicodedata
import csv
import re

en_word_pattern = re.compile(r"^[a-zA-Z0-9\.\'\&\s\-\+;\\°/’#]+$")

#取淘宝brand信息
def get_brand(db, h_return, data, idx=0, sql = None, auto = True):
    if sql is None:
        sql = 'select bid,name from dw_entity.brand where bid in (%s)'
    h = dict()
    for row in data:
        if idx > len(row) - 1:
            return
        id = row[idx]
        if id not in h_return and id != 0:
            h[id] = 1
    if len(h.keys()) == 0:
        return
    print('bids:', h.keys())
    data2 = utils.easy_query(db, sql, h.keys())
    if not auto:
        return data2
    for row in data2:
        id,name = list(row)
        h_return[id] = name

#取jd # brand信息
def get_brand_jd(db, h_return, data, idx=0):
    h = dict()
    for row in data:
        if idx > len(row) - 1:
            return
        id = row[idx]
        if id not in h_return and id != 0:
            h[id] = 1
    if len(h.keys()) == 0:
        return
    sql = 'select bid,name from jd.brand where bid in (%s)'
    print('bids:', h.keys())
    data2 = utils.easy_query(db, sql, h.keys())
    for row in data2:
        id,name = list(row)
        h_return[id] = name

#取淘宝sub_brand信息
def get_sub_brand(db, h_return, data, idx=0):
    h = dict()
    for row in data:
        if idx > len(row) - 1:
            return
        id = row[idx]
        if id not in h_return and id != 0:
            h[id] = 1
    if len(h.keys()) == 0:
        return
    sql = 'select sbid,full_name from dw_entity.sub_brand where sbid in (%s)'
    print('bids:', h.keys())
    data2 = utils.easy_query(db, sql, h.keys())
    for row in data2:
        id,name = list(row)
        h_return[id] = name

  #0 `bid` int(10) unsigned NOT NULL AUTO_INCREMENT,
  #1 `name` varchar(255) NOT NULL DEFAULT '',
  #2 `name_cn` varchar(255) NOT NULL DEFAULT '',
  #3 `name_en` varchar(255) NOT NULL DEFAULT '',
  #4 `name_cn_front` varchar(255) NOT NULL,
  #5 `name_en_front` varchar(255) NOT NULL,
  #6 `source` enum('tmall','ali','jd','yhd','suning','gome','jumei','kaola','vip','beibei') NOT NULL,
  #7 `is_hot` tinyint(3) unsigned NOT NULL DEFAULT '0',
  #8 `alias_bid` int(10) unsigned NOT NULL DEFAULT '0',
  #9 `sales` bigint(20) unsigned NOT NULL,
def get_all_brand(db, bids):
    sql = 'select * from all_site.all_brand where bid in (%s)'
    return utils.easy_query(db, sql, bids)

#取所有品牌关联关系
def get_relation_brand(db, bids):
    h_bid_maker_id = dict()
    h_maker_info = dict()
    sql = 'select bid,maker_id from all_site.all_brand_maker where bid in (%s)'
    data = utils.easy_query(db, sql, bids)
    if len(data) == 0:
        return h_bid_maker_id, h_maker_info

    h_maker_id = dict()
    for row in data:
        bid, maker_id = list(row)
        h_maker_id[maker_id] = 1
        h_bid_maker_id[bid] = maker_id

    sql = 'select id,name,all_bid,brand_num from all_site.maker where id in (%s)'
    data = utils.easy_query(db, sql, h_maker_id.keys())
    h_maker_info = utils.transfer_list_to_dict(data, True)
    return h_bid_maker_id, h_maker_info

#取淘宝shop信息
def get_shop(db, h_return, data, idx=0):
    h = dict()
    for row in data:
        if idx > len(row) - 1:
            return
        id = row[idx]
        if id not in h_return and id != 0:
            h[id] = 1
    if len(h.keys()) == 0:
        return
    sql = 'select sid,title from dw_entity.shop where sid in (%s)'
    print('bids:', h.keys())
    data2 = utils.easy_query(db, sql, h.keys())
    for row in data2:
        id,name = list(row)
        h_return[id] = name

#取类目信息
def get_category(db, h_return, data, idx=0, return_list = False):
    h = dict()
    for row in data:
        if idx > len(row) - 1:
            return
        id = row[idx]
        if id not in h_return and id != 0:
            h[id] = 1
    if len(h.keys()) == 0:
        return
    keys_list = h.keys()

def get_category_by_ids(db, ids):
    sql = 'select * from item_category_backend_all where cid in (%s)'
    print('cids:', ids)
    data = utils.easy_query(db, sql, ids)
    return data

def get_category_last_name(db, cid_list):
    h_category = dict()
    data = get_category_by_ids(db, cid_list)
    for row in data:
        for idx in [5, 4, 3, 2, 1]:
            if row[idx] != '':
                name = row[idx]
                break
        h_category[row[0]] = name
    return h_category

def get_category_by_ids_with_type(db, ids, type='jdnew'):
    sql = 'select cid, name, full_name from %s.item_category where cid in ' %(type, ) + '(%s)'
    return utils.easy_query(db, sql, ids)

def get_category_for_jd(ids, db=None):
    if db is None:
        db = app.connect_db('14_apollo')
    data = get_category_by_ids_with_type(db, ids, type='jdnew')
    return utils.transfer_list_to_dict(data, False)
    h = dict()
    for row in data:
        h[row[0]] = row[1]
    return h

def get_category_for_kaola(ids, db=None):
    if db is None:
        db = app.connect_db('14_apollo')
    data = get_category_by_ids_with_type(db, ids, type='kaola')
    return utils.transfer_list_to_dict(data, False)
    h = dict()
    for row in data:
        h[row[0]] = row[1]
    return h

def get_all_category_with_type(db = None, type='jd'):
    h_type = {'jd': 'jdnew', 'kaola': 'kaola'}
    db_name = h_type[type]
    if db is None:
        db = app.connect_db('14_apollo')
    sql = 'select * from %s.item_category' %(db_name,)
    return db.query_all(sql)

#db, db_name
def get_db_for_prop(source, h):
    if source == 'tmall':
        return [h['1'], 'apollo']
    elif source == 'jd':
        return [h['18'], 'jd']
    elif source == 'suning':
        return [h['14'], 'new_suning']
    elif source == 'kaola':
        return [h['14'], 'kaola']
    else:
        return None

#取item_yymm表db信息 db, db_name, table_name, tb_item_id_col, table_default
def get_db_for_item(source, h):
    if source == 'tmall':
        return [h['4'], 'apollo', 'mall_item', 'tb_item_id', 'mall_item', 0]
    elif source == 'jd':
        return [h['14'], 'jdnew', 'item', 'item_id', 'item_201707', 1498838400]
    elif source == 'suning':
        return [h['14'], 'new_suning', 'item', 'uniq_item_id', 'item_bak', 1496246400]
    elif source == 'kaola':
        return [h['14'], 'kaola', 'stat_item', 'uniq_item_id', 'item_bak', 1496246400]
    else:
        return None

def get_db_for_brand(source, h):
    if source == 'tmall':
        return [h['18'], 'dw_entity']
    elif source == 'jd':
        return [h['18'], 'jd']
    else:
        return None

def table_backup(db, table_name, suffix = '_delete', keep_origin = False): # 标准化清洗用
    if keep_origin:
        sql = "DROP TABLE IF EXISTS {table}{suffix};".format(table=table_name, suffix=suffix)
        print(sql)
        db.execute(sql)
        sql = 'CREATE TABLE {table}{suffix} LIKE {table};'.format(table=table_name, suffix=suffix)
        print(sql)
        db.execute(sql)
        sql = 'ALTER TABLE {table}{suffix} ENGINE=tokudb;'.format(table=table_name, suffix=suffix)
        print(sql)
        db.execute(sql)
        sql = 'INSERT INTO {table}{suffix} SELECT * FROM {table};'.format(table=table_name, suffix=suffix)
        print(sql)
        db.execute(sql)
        db.commit()
        print('Backup {host} {schema}.{table} to {schema}.{table}{suffix}'.format(host=db.host, schema=db.db, table=table_name, suffix=suffix))
    else:
        sql = "DROP TABLE IF EXISTS {table}{suffix};".format(table=table_name, suffix=suffix)
        print(sql)
        db.execute(sql)
        try:
            sql = "RENAME TABLE {table} TO {table}{suffix};".format(table=table_name, suffix=suffix)
            print(sql)
            db.execute(sql)
        except:
            print('↑ Fail')
            pass
        else:
            print('Backup and delete {host} {schema}.{table}'.format(host=db.host, schema=db.db, table=table_name))

def write_csv(output_path_name, data, first_row = None):
    print('{line}WRITE CSV {file}{line}'.format(line='-'*30, file=output_path_name))
    with open(output_path_name, 'w', encoding = 'utf-8-sig', errors = 'ignore', newline = '') as output_file:
        writer = csv.writer(output_file)
        if first_row:
            writer.writerow(first_row)
        for row in data:
            writer.writerow(row)

def to_sql_string(s): # 标准化清洗用
    return "'" + s.replace("'","''").replace("\\","\\\\") + "'"

def to_halfwidth(s, extra_map = dict(), case_mode = None): # 标准化清洗用
    if type(s) != str:
        return None
    mode = 'NFKC'               # NFKC or NFKD
    s = unicodedata.normalize(mode, s)
    extra_trans = {             # '、'和'·'暂不处理
        '【': '[',  '】': ']',
        '《': '<',  '》': '>',
        '“': '"', '”': '"',
        '‘': "'", '’': "'",
        '—': '-', '─': '-',
        '。': '.'
    }
    for key in extra_trans:
        s = s.replace(key, extra_trans[key])
    for key in extra_map:
        s = s.replace(key, extra_map[key])
    if case_mode == 'upper':
        s = s.upper()
    elif case_mode == 'lower':
        s = s.lower()
    return s

def remove_spilth(s, erase_sign = '', erase_all = set(), erase_duplication = set()): # 标准化清洗用
    for i in sorted(erase_all, key = lambda x:(len(x),x), reverse = True):
        s = s.replace(i, erase_sign)
    for i in sorted(erase_duplication, key = lambda x:(len(x),x), reverse = True):
        while s != s.replace(i+i, i+erase_sign):
            s = s.replace(i+i, i+erase_sign)
    return s

def prev_format_for_json(s): # 标准化清洗用
    s = s.replace('"null"', '""')
    # s = s.replace('\t', '\\t')
    # s = s.replace('\r', '\\r')
    # s = s.replace('\n', '\\n')
    s = re.sub(re.compile(r'[\x00-\x1F\x7F]', re.S), " ", s)
    return s

def get_shortprops_db(source):

    # by sop
    if source == 'tb':
        return ['clickhouse', 'ali.stat_ali_trade', 'tb_item_id', 'apollo.stat_dw_props', '1_apollo']
    elif source == 'tmall':
        return ['clickhouse', 'ali.stat_tmall_trade', 'tb_item_id', 'apollo.stat_dw_props', '1_apollo']
    elif source == 'jd':
        return ['clickhouse2', 'jd.stat_jd_trade_dist', 'item_id', 'jd.stat_dw_props', '18_apollo']
    elif source == 'suning':
        return ['clickhouse', 'suning.stat_suning_trade', 'item_id', 'new_suning.stat_dw_props', '14_apollo']
    elif source == 'kaola':
        return ['clickhouse', 'kaola.stat_kaola_trade_dist', 'item_id', 'kaola.stat_dw_props', '14_apollo']
    elif source == 'gome':
        return ['clickhouse', 'gome.stat_gome_trade', 'item_id', 'gm.stat_dw_props', '14_apollo']
    else:
        return None

    # by import_web
    # if source == 'tb':
    #     return ['18_apollo', 'apollo.category_{cid}', 'tb_item_id', 'apollo.stat_dw_props', '1_apollo']
    # elif source == 'tmall':
    #     return ['18_apollo', 'apollo.category_{cid}_mall', 'tb_item_id', 'apollo.stat_dw_props', '1_apollo']
    # elif source == 'jd':
    #     jd.stat_jd_trade
    #     return ['18_apollo', 'jd.category_{cid}', 'tb_item_id', 'jd.stat_dw_props', '18_apollo']
    # elif source == 'suning':
    #     return ['5_apollo', 'all_site.category_all_suning', 'item_id', 'new_suning.stat_dw_props', '14_apollo']
    # elif source == 'kaola':
    #     return ['5_apollo', 'all_site.category_all_kaola', 'item_id', 'kaola.stat_dw_props', '14_apollo']
    # else:
    #     return None

def get_fullprops_db(source):
    if source == 'tb':
        return ['4_apollo', 'apollo2.cd_item_{m}', 'tb_item_id', 'apollo.props']
    elif source == 'tmall':
        return ['4_apollo', 'apollo2.mall_item_{m}', 'tb_item_id', 'apollo.props']
    elif source == 'jd':
        return ['14_apollo', 'jdnew.item_{m}', 'item_id', 'jdnew.props']
    elif source == 'suning':
        return ['14_apollo', 'new_suning.item_{m}', 'uniq_item_id', 'new_suning.props']
    elif source == 'kaola':
        return ['14_apollo', 'kaola.item', 'item_id', 'kaola.props']
    elif source == 'gome':
        return ['14_apollo', 'gm.item', 'item_id', 'gm.props']
    elif source == 'jd_trade':
        return ['14_apollo', 'jdnew.series_item', 'item_id', 'jdnew.series_props']
    else:
        return None


def sync_item(table_name):
    db = app.get_db('default')
    db.connect()
    db_click = app.get_clickhouse('default')
    db_click.connect()
    sql = 'DROP TABLE IF EXISTS {}_sql'.format(table_name)
    db_click.execute(sql)
    sql = 'CREATE TABLE {}_sql ENGINE = Log AS SELECT * FROM mysql(\'{}\', \'{}\', \'{}\', \'{}\', \'{}\')'.format(
        table_name, db.host, 'artificial', table_name, db.user, db.passwd
    )
    try:
        db_click.execute(sql)
    except Exception as e:
        a = e.__class__.__name__
        b = e
        return str(a) + ' ' + str(b)
    return table_name + '_sql'

def get_brand_db(source):
    if source == 'tb' or source == 'tmall':
        return ['18_apollo', 'dw_entity', 'brand']
    elif source == 'jd':
        return ['14_apollo', 'jdnew', 'brand']
    elif source == 'suning':
        return ['14_apollo', 'new_suning', 'brand']
    elif source == 'kaola':
        return ['14_apollo', 'kaola', 'brand']
    elif source == 'jx':
        return ['14_apollo', 'jx', 'brand']
    else:
        return []

def get_brand_by_source_bids(h_db, h_bid, use_row=False, simple=True, with_all_brand=False):
    h = {}
    for source in h_bid:
        l = get_brand_db(source)
        if len(l) != 3:
            print('no support for source:', source)
            continue
        host_name, db_name, table_name = list(l)
        db = h_db[host_name]
        sql = 'select bid,name,bid_new from {db_name}.{table} where bid in (%s)'.format(db_name=db_name, table=table_name)
        data = utils.easy_query(db, sql, h_bid[source].keys())
        h[source] = utils.transfer_list_to_dict(data, use_row=use_row, simple=simple)
    return h
