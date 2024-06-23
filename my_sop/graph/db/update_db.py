import sys
import time
import csv
from os.path import abspath, join, dirname
from models.cleaner import Cleaner
from scripts.kgraph import fetch_data
from . import extract_log
from io import StringIO
import json
from extensions import utils
import numpy as np
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))

def extract_etable_brandcategory(h_db, entity, propNames, gbid_by_all_bid, gcid_by_name, now, test=True):
    # 品牌-品类关系：从E表抽取
    b_list = ','.join([str(v) for v in gbid_by_all_bid.keys()])
    new_bc = {}
    # propNames 选中作为子品类的sp
    propNames = propNames.split(',')
    sql = '''
    select pos_id from cleaner.clean_pos where deleteFlag=0 and batch_id = {} and name in ({})
    '''.format(entity.bid, ','.join('\'{}\''.format(v) for v in propNames))
    pos_id = [v[0] for v in h_db['mixdb'].query_all(sql)]
    if len(b_list) > 0:
        ret = []
        for each_pos_id in pos_id:
            sql = 'select alias_all_bid, clean_props.value[{}] from sop_e.entity_prod_{}_E where alias_all_bid in ({}) group by alias_all_bid, clean_props.value[{}]'.format(each_pos_id, entity.eid, b_list, each_pos_id)
            ret = ret + h_db['chmaster'].query_all(sql)
        for row in ret:
            all_bid, cat = list(row)
            gbid = gbid_by_all_bid.get(int(all_bid), 0)
            gcid = gcid_by_name.get(cat, 0)
            if gbid != 0 and gcid != 0:
                if gcid not in new_bc:
                    new_bc[gcid] = []
                if gbid not in new_bc[gcid]:
                    new_bc[gcid].append(gbid)
    bid_by_cid = fetch_data.read_graph_category_brand()
    if not test and len(new_bc) > 0:
        fetch_data.update_graph_category_brand(bid_by_cid, new_bc, now, source, test)


def extract_clean_target_as_pv(h_db, entity, now, test=True):
    # 属性值：同名属性值直接合并，不存在的属性值进行添加
    sql = 'select distinct(name) from cleaner.clean_target where batch_id = {} and deleteFlag = 0'.format(entity.bid)
    ret = h_db['mixdb'].query_all(sql)
    new_propvalue = [[v[0]] for v in ret]
    pvid_by_name, name_by_pvid = fetch_data.read_graph_vertex_type(h_db['mixdb'], 'propValue', ['name'])
    if not test and len(new_propvalue) > 0:
        pvid_by_name, name_by_pvid = fetch_data.update_graph_vertex_type(h_db['mixdb'], 'propValue', ['name'], pvid_by_name, name_by_pvid, new_propvalue, now, source, test)
    return pvid_by_name


def extract_etable_brand_propvalue(h_db, entity, gbid_by_all_bid, pvid_by_name, now, test=True):
    # 属性值：同名属性值直接合并，不存在的属性值进行添加
    # 品牌-属性值关系：从E表抽取
    # gbid_by_all_bid 仅出现的品牌
    if len(gbid_by_all_bid) > 0:
        b_list = ','.join([str(v) for v in gbid_by_all_bid.keys()])
        new_b_pv = {}
        sql = 'select alias_all_bid, clean_props.value from sop_e.entity_prod_{}_E where alias_all_bid in ({}) group by alias_all_bid, clean_props.value '.format(entity.eid, b_list)
        ret = h_db['chmaster'].query_all(sql)
        for row in ret:
            all_bid, pv = list(row)
            gbid = gbid_by_all_bid.get(int(all_bid), 0)
            # pv = eval(pv) # Array(String)
            for each_pv in pv:
                pvid = pvid_by_name.get(each_pv, 0)
                if pvid != 0 and gbid != 0:
                    if gbid not in new_b_pv:
                        new_b_pv[gbid] = []
                    if pvid not in new_b_pv[gbid]:
                        new_b_pv[gbid].append(pvid)
        pvid_by_bid = fetch_data.read_graph_edge_type(h_db['mixdb'], 'brandpropvalue', 'bid', 'pvid')
        if not test and len(new_b_pv) > 0:
            fetch_data.update_graph_edge_type(h_db['mixdb'], 'brandpropvalue', ['bid', 'pvid'], pvid_by_bid, new_b_pv, now, source, test)


def extract_etable_sku(h_db, entity, bid_by_all_bid, now, test=True):
    # SKU（model）：自动添加（不要重复添加）
    if len(bid_by_all_bid) > 0:
        bids = ','.join([str(v) for v in bid_by_all_bid.keys()])
        eid = entity.eid
        sql = 'select pid,name,all_bid from artificial.product_{} where all_bid in ({})'.format(eid, bids)
        new_sku_list = []
        ret = h_db['chmaster'].query_all(sql)
        for v in ret:
            pid, name, all_bid = list(v)
            # new_sku_list.append('{}-{}-{}'.format(eid, pid, name))
            new_sku_list.append([eid, pid, name])
        sid_by_sku, sku_by_sid = fetch_data.read_graph_sku()

        if not test and len(new_sku_list) > 0:
            sid_by_sku, sku_by_sid = fetch_data.update_graph_sku(sid_by_sku, sku_by_sid, new_sku_list, now, source, test)
            # 品牌-SKU关系
            new_sid_by_bid = {}
            for v in ret:
                pid, name, all_bid = list(v)
                sku = '{}-{}-{}'.format(eid, pid, name)
                sid = sid_by_sku[sku]
                gbid = bid_by_all_bid.get(int(all_bid), 0)
                if gbid != 0:
                    if gbid not in new_sid_by_bid:
                        new_sid_by_bid[gbid] = []
                    if sid not in new_sid_by_bid[gbid]:
                        new_sid_by_bid[gbid].append(sid)
            sid_by_bid = fetch_data.read_graph_edge_type(h_db['mixdb'], 'skubrand', 'bid', 'sid')
            if not test and len(new_sid_by_bid) > 0:
                fetch_data.update_graph_edge_type(h_db['mixdb'], 'skubrand', ['bid', 'sid'], sid_by_bid, new_sid_by_bid, now, source, test)


def extract_category_propname(h_db, entity, pnid_by_name, cid_by_name, batch_cat, batch_pn, now, test):
    # 品类-品类属性名关系：从清洗配置抽取
    new_pnid_by_cid = {}
    # 从拉取的表读取新品类-品类属性名关联
    for each_cat in batch_cat:
        cid = cid_by_name.get(each_cat, 0)
        if cid != 0:
            if cid not in new_pnid_by_cid:
                new_pnid_by_cid[cid] = []
            for each_pn in batch_pn:
                pnid = pnid_by_name.get(each_pn, 0)
                if pnid != 0 and pnid not in new_pnid_by_cid[cid]:
                    new_pnid_by_cid[cid].append(pnid)
    pnid_by_cid = fetch_data.read_graph_edge_type(h_db['mixdb'], 'categorypropname', 'cid', 'pnid')
    if not test and len(new_pnid_by_cid) > 0:
        pnid_by_cid = fetch_data.update_graph_edge_type(h_db['mixdb'], 'categorypropname', ['cid', 'pnid'], pnid_by_cid, new_pnid_by_cid, now, source, test)
    return pnid_by_cid


def process_brand(h_db, brand_sheet, now, test):
    # 关联gbid 填关联的graph bid，如果没有找到则填-1，表示插入新品牌
    # 是否更新graph中alias 填1则更新graph中信息
    log_bid_by_all_bid = extract_log.read_log(h_db['mixdb'], 'brand')
    new_brand = []
    update_sql = []
    all_bid_list = []
    c = 0
    brand_sheet = json.loads(brand_sheet)
    brand_sheet = StringIO(brand_sheet, newline='\n')
    brand_sheet = csv.reader(brand_sheet, delimiter=',')
    for row in brand_sheet:
        c = c + 1
        if c == 1 or c == 2:
            continue
        if row[1] != '' and row[0] != '★':
            # 仅限没处理过的
            all_bid = int(row[1])
            all_bid_list.append(all_bid)
            if_update_alias_flag = row[13]
            bid_map_flag = row[12]
            name, name_cn, name_en, alias_all_bid = row[2:6]
            if bid_map_flag == '-1':
                new_brand.append([name, name_cn, name_en, all_bid, alias_all_bid])
            else:
                if if_update_alias_flag == '1':
                    sql = 'update graph.brand set bid0={},alias_bid0={},janusFlag=0 where bid={}'.format(all_bid, alias_all_bid, bid_map_flag)
                    update_sql.append(sql)
                else:
                    pass
                    # bid0都对，从db获取即可
    # print(all_bid_list)
    while 0 in all_bid_list:
        all_bid_list.remove(0)
    if not test and len(new_brand) > 0:
        fetch_data.update_graph_vertex_type(h_db['mixdb'], 'brand', ['name', 'name_cn', 'name_en', 'bid0', 'alias_bid0'], {}, {}, new_brand, now, source, test)
    if not test:
        for each_sql in update_sql:
            h_db['mixdb'].execute(each_sql)
            h_db['mixdb'].commit()

    bid_by_all_bid = {}
    if len(all_bid_list) > 0:
        sql = 'select bid, bid0 from graph.brand where deleteFlag=0 and confirmType != 2 and bid0 in ({})'.format(','.join([str(v) for v in all_bid_list]))
        ret = h_db['mixdb'].query_all(sql)
        bid_by_all_bid = {v[1]: v[0] for v in ret}
        extract_log.add_log(h_db['mixdb'], 'brand', bid_by_all_bid, now)
        # 新品牌写入log
        bid_by_all_bid.update(log_bid_by_all_bid)
    return bid_by_all_bid


def process_category(h_db, category_sheet, now, test):
    # 关联现有品类 填入关联的graph cid，没有就新建
    log_cid_by_name = extract_log.read_log(h_db['mixdb'], 'category')
    batch_cat = []
    gcid_by_target = {} #等同cid_by_name，仅限出现在target中的品类
    c = 0
    category_sheet = json.loads(category_sheet)
    category_sheet = StringIO(category_sheet, newline='\n')
    category_sheet = csv.reader(category_sheet, delimiter=',')
    for row in category_sheet:
        c = c + 1
        if c == 1:
            continue
        if row[1] != '' and row[0] != '★':
            # 关联
            # 仅未处理的品类
            # 最终返回的还会加上已处理的
            name = row[3]
            gcid = int(row[1])
            batch_cat.append(name)
            gcid_by_target[name] = gcid
    extract_log.add_log(h_db['mixdb'], 'category', gcid_by_target, now)
    gcid_by_target.update(log_cid_by_name)
    batch_cat = batch_cat + list(log_cid_by_name.keys())
    return gcid_by_target, batch_cat


def process_propname(h_db, propname_sheet, now, test):
    # 是否关联属性 人工判断 填1表示确认关联
    # 是否存在语义关联 程序判断
    # 属性名要求同名，语义单元不要求同名
    log_pnid_by_name = extract_log.read_log(h_db['mixdb'], 'propname')
    c = 0
    batch_pn = []
    gpnid_by_name = {}
    new_pnid_by_pid = {}
    new_pn = [] # 必须是二维数组
    propname_sheet = json.loads(propname_sheet)
    propname_sheet = StringIO(propname_sheet, newline='\n')
    reader = csv.reader(propname_sheet, delimiter=',')
    for row in reader:
        c = c + 1
        if c == 1:
            continue
        map_part = row[7]
        if_new_pn_flag = row[8]
        batch_pn.append(row[2])
        if if_new_pn_flag == '1' and row[0] != '★':
            # 插入新属性
            new_pn.append([row[2]])

    if not test:
        pnid_by_name, name_by_pnid = fetch_data.read_graph_vertex_type(h_db['mixdb'], 'propName', ['name'])
        if len(new_pn) > 0:
            pnid_by_name, name_by_pnid = fetch_data.update_graph_vertex_type(h_db['mixdb'], 'propName', ['name'], pnid_by_name, name_by_pnid, new_pn, now, source, test)
        # gpnid_by_name = {v: pnid_by_name[v] for v in batch_pn}
        for v in batch_pn:
            if v in pnid_by_name:
                gpnid_by_name[v] = pnid_by_name[v]
        extract_log.add_log(h_db['mixdb'], 'propname', gpnid_by_name, now)
        gpnid_by_name.update(log_pnid_by_name)
        batch_pn = batch_pn + list(log_pnid_by_name.keys())
        c = 0
        propname_sheet.seek(0)
        for row in reader:
            c = c + 1
            if c == 1:
                continue
            map_part = row[7]
            if_new_pn_flag = row[8]
            if map_part != '' and row[0] != '★':
                pid = int(map_part)
                if if_new_pn_flag == '':
                    if row[3] == '':
                        # 不是新属性，也没有关联属性id 跳过
                        continue
                    else:
                        pnid = int(row[3])
                    # 从sheet中取，避免从数据库中取出同名propname
                else:
                    pnid = int(gpnid_by_name[row[2]])
                    # 从数据库中取
                if pid not in new_pnid_by_pid:
                    new_pnid_by_pid[pid] = []
                if pnid not in new_pnid_by_pid[pid]:
                    new_pnid_by_pid[pid].append(pnid)

        if len(new_pnid_by_pid) > 0:
            pnid_by_pid = fetch_data.read_graph_edge_type(h_db['mixdb'], 'partpropname', 'pid', 'pnid')
            fetch_data.update_graph_edge_type(h_db['mixdb'], 'partpropname', ['pid', 'pnid'], pnid_by_pid, new_pnid_by_pid, now, source, test)

    return gpnid_by_name, batch_pn


def process(msg, h_db, now, propNames, test):
    batch_id = msg['batch_id']
    entity = Cleaner(batch_id)
    brand_sheet = msg['brand']
    gbid_by_all_bid = process_brand(h_db, brand_sheet, now, test)
    # 仅含该清洗项目中出现的bid
    category_sheet = msg['category']
    gcid_by_name, batch_cat = process_category(h_db, category_sheet, now, test)
    propname_sheet = msg['propname']
    gpnid_by_name, batch_pn = process_propname(h_db, propname_sheet, now, test)
    # # 属性值：同名属性值直接合并，不存在的属性值进行添加 √
    # # 品类属性名-通用属性
    # # 通用属性-属性名概念
    # # sheet处理完成，得到E表中品牌，属性名等字段和graph中vertex的映射关系
    print('S1')
    extract_etable_sku(h_db, entity, gbid_by_all_bid, now, test)
    # SKU（mextract_etable_skuodel）：自动添加（不要重复添加）
    # 品牌-SKU关系
    print('S2')
    extract_etable_brandcategory(h_db, entity,propNames, gbid_by_all_bid, gcid_by_name, now, test)
    # 品牌-品类关系：从E表抽取 √
    print('S3')
    pvid_by_name = extract_clean_target_as_pv(h_db, entity, now, test)
    print('S4')
    extract_etable_brand_propvalue(h_db, entity, gbid_by_all_bid, pvid_by_name, now, test)
    # 品牌-属性值关系：从E表抽取 √
    print('S5')
    extract_category_propname(h_db, entity, gpnid_by_name, gcid_by_name, batch_cat, batch_pn, now, test)
    print('S6')
    # 品类-品类属性名关系：从清洗配置抽取 √


def main(msg, h_db, propNames):
    global source
    source = 6
    now = int(time.time())
    print('now', now)
    test = False
    process(msg, h_db, now, propNames, test)
    return True

