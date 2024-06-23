#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
import time
from gremlin_python import statics
from extensions import gremlin_full_text
statics.load_statics(globals())
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from extensions import utils
from graph.db import category as categoryModel
from nlp import pnlp
import json
import collections

front_type_ref = {
    # -1: -1,
    # 0:  0,
    # 1:  1,
    # 2:  2,
    # 3:  1000,
    # 4:  2000,
    # 5:  1002,
    # 6:  1001,
    # 7:  3,
    # 8:  3000,
    # 9:  3002,
    # 10: 4,
    # 11: 5,
    # 12: 6,
    # 13: 5006,
    # 14: 1006,
    # 15: 4006
}
vetex_type_ref = {
    0: 'keyword',
    1: 'category',
    2: 'brand',
    3: 'maker',
    4: 'propName',
    5: 'propValue',
    6: 'categoryPropName',
    7: 'semantic',
    8: 'part',
    9: 'sku',
    10: 'concept',
    11: 'feature'
}
vetex_table_ref = {}
for k in vetex_type_ref:
    v = vetex_type_ref[k]
    vetex_table_ref[v] = k
edge_type_ref = {
    1000:   'categoryKeyword',
    1001:   'categoryParent',
    1002:   'categoryBrand',
    1006:   'categoryPropNameR',
    1007:   'categorySemantic',
    1008:   'categoryPart',
    2000:   'brandKeyword',
    2002:   'brandReject',
    2005:   'brandPropValue',
    2007:   'brandSemantic',
    2008:   'brandPart',
    1002002:'brandRelation',
    3000:   'makerKeyword',
    3002:   'makerBrand',
    4000:   'categoryPropNameKeyword',
    4006:   'categoryPropNameL',
    5000:   'propValueKeyword',
    5006:   'categoryPropValue',
    6008:   'categoryPropNamePart',
    7000:   'semanticKeyword',
    7008:   'semanticPart',
    8000:   'partKeyword',
    8004:   'partPropName',
    8008:   'partRelation',
    9000:   'skuKeyword',
    9001:   'skuCategory',
    9002:   'skuBrand',
    9009:   'skuParent',
    10000:  'conceptKeyword',
    10010:  'conceptParent',
    1010010:'conceptRelation',
    10011:  'conceptFeature',
    11000:  'featureKeyword',
    11011:  'featureParent'
}
edge_table_ref = {}
for k in edge_type_ref:
    v = edge_type_ref[k]
    edge_table_ref[v] = k
id_key_list = ['kid', 'cid', 'bid', 'mid', 'pnid', 'pvid', 'cpnid', 'xid', 'pid', 'sid', 'yid', 'fid']
#category relation keyword relation

graph_vetex_config = {
    'keyword': ['kid', 'name', ['kid', 'name', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source', 'speech', 'uid'], 'categoryKeyword', ''],
    'category': ['cid', 'cname', ['cid', 'cname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid', 'categoryBrandRelation', 'categoryPropNameRRelation'], 'categoryParent', 'categoryKeyword'],
    'brand': ['bid', 'bname', ['bid', 'bname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'isShop', 'memo', 'alias_bid0', 'confirmType','alias_bid0_status', 'source','uid'], 'categoryBrand', 'brandKeyword'],
    'maker': ['mid', 'mname', ['mid', 'mname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid'], ['categoryBrand', 'makerBrand'], 'makerKeyword'],
    'propName': ['pnid', 'pnname', ['pnid', 'pnname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid'], ['categoryPropNameR', 'categoryPropNameL'], ''],
    'propValue': ['pvid', 'pvname', ['pvid', 'pvname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','origin','target','uid'], ['categoryPropNameR', 'categoryPropValue'], 'propValueKeyword'],
    'categoryPropName': ['cpnid', 'cpnname', ['cpnid', 'cpnname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'weight', 'confirmType','source','uid'], 'categoryPropNameR', 'categoryPropNameKeyword'],
    'sku': ['sid', 'sname', ['sid', 'sname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid'], 'skuCategory', 'skuKeyword'],
    'semantic': ['xid', 'xname', ['xid', 'xname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'mode', 'confirmType','source','uid'], '', 'semanticKeyword'],
    'part': ['pid', 'pname', ['pid', 'pname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'mode', 'confirmType','source','uid'], '', 'partKeyword'],
    'concept': ['yid', 'yname', ['yid', 'yname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid'], '', 'conceptKeyword'],
    'feature': ['fid', 'fname', ['fid', 'fname', 'confirmType'], ['id', 'graphID', 'type', 'name', 'showName', 'confirmType','source','uid'], '', 'featureKeyword'],
}
graph_edge_config = {
    'categoryKeyword': ['ckid'],
    'brandKeyword': ['bkid'],
    'categoryBrand': ['cbid'],
    'brandAlias': ['abid'],
    'categoryParent': ['cpid'],
    'makerKeyword': ['mkid'],
    'makerBrand': ['mbid'],
    'categoryPropValue': ['cpvid'],
    'categoryPropNameR': ['cpnid1'],
    'categoryPropNameL': ['cpnid2'],
    'brandReject': ['brid'],
    'skuKeyword': ['skid'],
    'skuCategory': ['scid'],
    'skuBrand': ['sbid'],
    'brandSemantic': ['bxid'],
    'semanticPart': ['xpid'],
    'partKeyword': ['pkid'],
    'categoryPropNameKeyword': ['cpnkid'],
    'propValueKeyword': ['pvkid'],
    'semanticKeyword': ['xkid'],
    'categorySemantic': ['cxid'],
    'categoryPart': ['cpaid'],
    'brandPart': ['bpaid'],
    'conceptKeyword': ['ykid'],
    'conceptParent': ['ypaid'],
    'conceptRelation': ['ypaid1'],
    'conceptFeature': ['yfid'],
    'featureKeyword': ['fkid'],
    'featureParent': ['fpaid'],
    'brandRelation':['bpaid1'],
    'partRelation':['ppaid'],
    'categoryPropNamePart':['cpnpid'],
    'brandPropValue':['bpvid'],
    'partPropName':['ppnid'],
}
category_mergeout = {
    'categoryBrand': 'bid',
    'categoryKeyword': 'kid',
    'categoryParent': 'pcid',
    'categoryPart': 'pid',
    'categoryPropNameR': 'cpnid',
    'categorySemantic': 'xid'
}
category_mergein = {
    'skuCategory': ['sid','cid'],
    'categoryParent': ['cid','pcid']
}
brand_mergeout = {
    'brandKeyword': 'kid',
    'brandPart': 'pid',
    'brandReject': 'rbid',
    'brandRelation': 'pabid1',
    'brandSemantic': 'xid',
    'brandPropValue': 'pvid'
}
brand_mergein = {
    'brandReject': ['bid','rbid'],
    'brandRelation': ['bid','pabid1'],
    'categoryBrand': ['cid','bid'],
    'makerBrand': ['mid','bid'],
    'skuBrand': ['sid','bid']
}

#其它通用配置
graph_vetext_db = [[['kid', 'name', 'speech'], ['keyword', 'kid', 'name', 'speech']],
          [['cid', 'name', 'level'], ['category', 'cid', 'cname', 'level']],
          [['bid', 'name', 'confirmType', 'name_cn', 'name_en', 'sales', 'alias_bid0'], ['brand', 'bid', 'bname', 'confirmType', 'name_cn', 'name_en', 'sales', 'alias_bid0']],
          [['mid', 'name'], ['maker', 'mid', 'mname']],
          [['sid', 'name'], ['sku', 'sid', 'sname']],
          [['xid', 'name'], ['semantic', 'xid', 'xname']],
          [['pid', 'name', 'mode'], ['part', 'pid', 'pname', 'mode']],
          [['pnid', 'name'], ['propName', 'pnid', 'pnname']],
          [['pvid', 'name', 'origin', 'target'], ['propValue', 'pvid', 'pvname', 'origin', 'target']],
          [['id', 'name', 'weight'], ['categoryPropName', 'cpnid', 'cpnname', 'weight']]]
common_key_list = ['confirmType', 'source', 'uid', 'type']
set_key_list = ['speech']

graph_edge_db = [['categoryKeyword', ['id', 'cid', 'kid'], ['ckid', 'cid', 'kid']],
          ['brandKeyword', ['id', 'bid', 'kid', 'mode'], ['bkid', 'bid', 'kid', 'mode']],
          ['categoryBrand', ['id', 'cid', 'bid', 'num', 'sales'], ['cbid', 'cid', 'bid', 'num', 'sales']],
          ['brandAlias', ['bid', 'bid', 'alias_bid'], ['abid', 'bid', 'bid']],
          ['categoryParent', ['id', 'cid', 'pcid'], ['cpid', 'cid', 'pcid']],
          ['makerKeyword', ['id', 'mid', 'kid'], ['mkid', 'mid', 'kid']],
          ['makerBrand', ['id', 'mid', 'bid'], ['mbid', 'mid', 'bid']],
          ['categoryPropNameR', ['id', 'cid', 'id'], ['cpnid1', 'cid', 'cpnid']],
          ['categoryPropNameL', ['id', 'pnid', 'id'], ['cpnid2', 'pnid', 'cpnid']],
          ['categoryPropValue', ['id', 'cpnid', 'pvid'], ['cpvid', 'cpnid', 'pvid']],
          ['brandReject', ['id', 'bid', 'rbid'], ['brid', 'bid', 'rbid']],
          ['skuKeyword', ['id', 'sid', 'kid'], ['skid', 'sid', 'kid']],
          ['skuBrand', ['id', 'sid', 'bid'], ['sbid', 'sid', 'bid']],
          ['skuCategory', ['id', 'sid', 'cid'], ['scid', 'sid', 'cid']],
          ['brandSemantic', ['id', 'bid', 'xid'], ['bxid', 'bid', 'xid']],
          ['semanticPart', ['id', 'xid', 'pid'], ['xpid', 'xid', 'pid']],
          ['partKeyword', ['id', 'pid', 'kid'], ['pkid', 'pid', 'kid']],
          ['categorySemantic', ['id', 'cid', 'xid'], ['cxid', 'cid', 'xid']],
          ['categoryPart', ['id', 'cid', 'pid'], ['cpaid', 'cid', 'pid']],
          ['brandPart', ['id', 'bid', 'pid'], ['bpaid', 'bid', 'pid']],
          ['propValueKeyword', ['id', 'pvid', 'kid', 'mode'], ['pvkid', 'pvid', 'kid', 'mode']],
          ['brandPropValue', ['id', 'bid', 'pvid'], ['bpvid', 'bid', 'pvid']],
          ['partPropName', ['id', 'pid', 'pnid', 'mode'], ['ppnid', 'pid', 'pnid', 'mode']],
          ['skuParent', ['id', 'sid', 'psid'], ['spid', 'sid', 'psid']]]

def get_value_iter(t, v, default=0):
    return choose(t.has(v), t.values(v), constant(default))
def get_values_iter(t, v, default=0):
    return choose(t.has(v), t.valueMap(v).select(v), constant([]))
def get_category_filter(t, category_relation, cid, table):
    if table != 'category':
        if not isinstance(category_relation, list):
            l = [category_relation]
        else:
            l = category_relation.copy()
            l.reverse()
        for relation in l:
            t = t.both(relation)
    t = t.repeat(out('categoryParent').simplePath()).until(__.not_(out('categoryParent')).or_().has('cid', cid)).has('cid', cid)
    return t

def search(g, p):
    type = int(p['type'])
    id = utils.parse_id(str(p['id'])) if 'id' in p and p['id'] != '' else []
    name = str(p['name']) if 'name' in p else ''
    confirmType = p['confirmType'] if 'confirmType' in p else [-1]
    speech = p['speech'] if 'speech' in p else []
    source = int(p['source']) if 'source' in p else -1
    mode = int(p['mode']) if 'mode' in p else -1
    alias_bid0 = int(p['alias_bid0']) if 'alias_bid0' in p else -1
    alias_bid0_status = int(p['alias_bid0_status']) if 'alias_bid0_status' in p else -1
    cid = utils.parse_category(p)
    isShop = int(p['isShop']) if 'isShop' in p else -1
    type = front_type_ref[type] if type in front_type_ref else type
    if type not in vetex_type_ref:
        return {}
    table = vetex_type_ref[type]
    config = graph_vetex_config[table]
    id_key, name_key, key_list, return_key_list, category_relation, keyword_relation, *add_args = list(config)
    t = g.V()
    if id == [] and name == '':
        if cid > 0 and category_relation != '':
            t = t.has('cid', cid).union(__.identity(),
                                        __.repeat(inE('categoryParent').outV()).until(__.not_(inE('categoryParent'))))
            if table != 'category':
                if not isinstance(category_relation, list):
                    category_relation = [category_relation]
                for relation in category_relation:
                    t = t.both(relation)
        t = t.has('type', type)
        if -1 not in confirmType:
            t = t.has('confirmType',within(confirmType))
    else:
        if id != []:
            t = t.has(id_key, within(id))
        use_filter_mode = False
        if name != '':
            if 'like' in p and p['like']:
                name = '{}'.format(name)
                t = t.has(name_key, textContains(name))
                use_filter_mode = True
            else:
                t = t.has(name_key, name)
        if -1 not in confirmType:
            # if use_filter_mode:
            #     t = t.filter(__.values('confirmType').is_(confirmType))
            # else:
            t = t.has('confirmType', within(confirmType))
        if cid > 0 and category_relation != '':
            t = t.filter(get_category_filter(__, category_relation, cid, table))
        t = t.has('type',type)
    if 'type2' in p:
        type2 = int(p['type2'])
        if type2 in vetex_type_ref:
            table2 = vetex_type_ref[type2]
            if table2 in graph_vetex_config:
                config2 = graph_vetex_config[table2]
                keyword_relation2 = config2[5]
                if keyword_relation2 != '':
                    t = t.inE(keyword_relation2).inV()
    if type == 1 and source != -1:
        t = t.has('source',source)
    if type == 0 and speech != [] and -1 not in speech:
        t = t.has('speech',within(speech))
    if type == 2 and isShop != -1:
        if isShop == 0:
            t = t.union(has('isShop',isShop),not_(has('isShop')))
        else:
            t = t.has('isShop',isShop)
    if type == 2 and alias_bid0_status != -1:
        if alias_bid0_status == 0:
            t = t.union(has('alias_bid0_status',alias_bid0_status),not_(has('alias_bid0_status')))
        else:
            t = t.has('alias_bid0_status',alias_bid0_status)
    if (type == 7 or type == 8) and mode != -1:
        if mode == 0:
            t = t.union(has('mode',mode),not_(has('mode')))
        else:
            t = t.has('mode', mode)
    if type == 2 and alias_bid0 != -1:
        if alias_bid0 == 0:
            # t = t.union(has('alias_bid0', alias_bid0), not_(has('alias_bid0')))
            t = t.union(has('alias_bid0', alias_bid0), where(not_(has('alias_bid0'))))
        else:
            # t = t.has('alias_bid0', neq(0))
            t = t.where(__.values('alias_bid0').is_(neq(0)))
    print(return_key_list)
    t = t.project(*return_key_list)
    for key in return_key_list:
        if key == 'id':
            t = t.by(__.values(id_key))
            # key = id_key_list
            # t = t.by(__.values(*key))
        elif key == 'name':
            # t = t.by(__.values(name_key))
            t = t.by(get_value_iter(__, name_key,''))
        elif key == 'showName':
            # t = t.by(__.values(name_key))
            t = t.by(get_value_iter(__, name_key,''))
        elif key == 'uid':
            t = t.by(get_value_iter(__, key,-1))
        elif key == 'graphID':
            t = t.by(__.id())
        elif key == 'confirmType' or key == 'source' or key == 'weight':
            t = t.by(get_value_iter(__, key))
        elif key == 'origin' or key == 'target' or key == 'mode' or key == 'isShop' or key == 'alias_bid0':
            t = t.by(get_value_iter(__, key))
        elif key == 'memo':
            t = t.by(get_value_iter(__, key, ''))
        elif key == 'alias_bid0_status':
            t = t.by(get_value_iter(__, key, 0))
        elif key == 'speech':
            t = t.by(get_values_iter(__, key))
        elif len(key) > 8 and key[-8:] == 'Relation':
            key = key[0:-8]
            t = t.by(__.choose(__.both(key), constant(1), constant(0)))
        else:
            # t = t.by(__.values(key))
            t = t.by(get_value_iter(__, key,''))
    t = t.dedup()
    if p['limit'] != '':
        limit = int(p['limit'])
        start = int(p['current']) if 'current' in p else 0
        end = start + limit
        t = t.range(start, end)
    print(t)
    data = t.toList()
    print('data:', data)
    h_cid = {}
    for row in data:
        if row['type'] == 1:
            h_cid[row['id']] = 1
    if len(h_cid.keys()) > 0:
        h_cid_name = categoryModel.get_full_name(g, list(h_cid.keys()))
    else:
        h_cid_name = {}
    arr = []
    for row in data:
        item = []
        if row['type'] == 1 and row['id'] in h_cid_name:
            row['showName'] = h_cid_name[row['id']]
        for key in return_key_list:
            if key in row:
                item.append(row[key])
                # item.append((row[key]['@value'] if key == 'graphID' and isinstance(row[key], dict) and '@type' in row[key] else row[key]))
            else:
                item.append('')
        arr.append(item)
    return {
        'columns': return_key_list,
        'data': arr
    }

def confirm(g, p):
    if 'type' not in p:
        return
    type = int(p['type'])
    ids = p['ids'] if 'ids' in p else []
    uid = int(p['uid']) if 'uid' in p else -1
    if len(ids) == 0:
        return
    confirmType = int(p['confirmType']) if 'confirmType' in p else 0
    if confirmType not in [0, 1, 2, 3]:
        return
    
    type = front_type_ref[type] if type in front_type_ref else type
    table = vetex_type_ref[type]
    config = graph_vetex_config[table]
    id_key = config[0]
    if (vetex_type_ref[type] == 'part' or vetex_type_ref[type] == 'semantic') and 'mode' in p:
        mode = int(p['mode'])
        g.V().has(id_key, within(ids)).property('confirmType', confirmType).property('mode', mode).property('uid', uid).toList()
    elif vetex_type_ref[type] == 'keyword' and 'speech' in p:
        speech = p['speech']
        g.V().has(id_key, within(ids)).properties('speech').drop()
        t = g.V().has(id_key, within(ids))
        for speech_i in speech:
            t = t.property('speech',speech_i)
        t.property('uid', uid).toList()
    elif vetex_type_ref[type] == 'brand':
        t = g.V().has(id_key, within(ids)).property('confirmType', confirmType)
        if 'isShop' in p:
            isShop = int(p['isShop'])
            t = t.property('isShop', isShop)
        if 'memo' in p:
            memo = p['memo']
            t = t.property('memo', memo)
        if 'alias_bid0_status' in p:
            alias_bid0_status = p['alias_bid0_status']
            t = t.property('alias_bid0_status', alias_bid0_status)
        t.property('uid', uid).toList()
    elif vetex_type_ref[type] == 'categoryPropName':
        t = g.V().has(id_key, within(ids)).property('confirmType', confirmType)
        if 'weight' in p:
            weight = int(p['weight'])
            t = t.property('weight', weight)
        t.property('uid', uid).toList()
    else:
        g.V().has(id_key, within(ids)).property('confirmType', confirmType).property('uid', uid).toList()

def add(g, p, db=None):
    type = int(p['type'])
    confirmType = int(p['confirmType']) if 'confirmType' in p else 0
    name = p['name'] if 'name' in p else ''
    uid = int(p['uid']) if 'uid' in p else -1
    origin = int(p['origin']) if 'origin' in p else 0
    target = int(p['target']) if 'target' in p else 0
    mode = int(p['mode']) if 'mode' in p else 0
    memo = p['memo'] if 'memo' in p else ''
    isShop = int(p['isShop']) if 'isShop' in p else 0
    weight = int(p['weight']) if 'weight' in p else 0
    speech = p['speech'] if 'speech' in p else []
    h_keyword = dict()
    name_cn, name_en = '', ''
    if type == 2:
        name, l, name_cn, name_en = parse_brand_name(p)
    name = name.strip()
    name = pnlp.unify_character(name)
    if name == '':
        return 1
    type = front_type_ref[type] if type in front_type_ref else type
    table = vetex_type_ref[type]
    config = graph_vetex_config[table]
    id_key, name_key, key_list, return_key_list, category_relation, keyword_relation, *add_args = list(config)
    max_id = utils.get_next_id(db, table)

    g1 = g.V().has(name_key, name)
    print(g1)
    if g1.hasNext():
        data = g.V().has(name_key, name).project('id', 'gid').by(__.values(id_key)).by(__.id()).limit(1).toList()
    else:
        t = g.addV(table).property('type', type).property(id_key, max_id).property(name_key, name).property('confirmType', confirmType).property('uid', uid)
        if type == 0:
            for speech_i in speech:
                t = t.property('speech',speech_i)
        if type == 2:
            t = t.property('name_cn', name_cn).property('name_en', name_en).property("isShop",isShop).property('memo',memo)
        if type == 6:
            t = t.property('weight', weight)
        if type == 5:
            t = t.property('origin', origin).property('target', target)
        if type == 7 or type == 8:
            t = t.property('mode', mode)
        print(t)
        data = t.project('id', 'gid').by(__.values(id_key)).by(__.id()).toList()
    print('data:', data)
    data = data[0]
    id1, gid = data['id'], data['gid']
        # g1 = g.V(data).property(id_key, __.id()).next()
    if type == 0:
        return {"code":0,"id":id1,"graphID":gid,"type":type,"name":name,"name_cn":name_cn,"name_en":name_en,"uid":uid}

    keywords = p['keywords'] if 'keywords' in p else ''
    keywords = keywords.split(',')
    keywords = keywords + list(h_keyword.keys())
    table2 = keyword_relation
    relation_id_key, *add_args = list(graph_edge_config[keyword_relation])
    add_keyword(g, keywords, table2, id_key, id1, relation_id_key, uid, db)
    return {"code":0,"id":id1,"graphID":gid,"type":type,"name":name,"name_cn":name_cn,"name_en":name_en,"uid":uid,"target":target,"origin":origin,"mode":mode,"isShop":isShop,"weight":weight}

def add_keyword(g, keywords, table, id1_key, id1, id_key, uid, db=None, confirmType=0):
    type = edge_table_ref[table]
    h = dict()
    size = len(keywords)
    max_id = utils.get_next_id(db, 'keyword', size) - 1
    max_id2 = utils.get_next_id(db, table, size) - 1
    t = g
    f = False
    for k in keywords:
        max_id += 1
        max_id2 += 1
        k = k.strip()
        if k == '':
            continue
        k = pnlp.unify_character(k)
        if k in h:
            continue
        f = True
        h[k] = 1
        g2 = g.V().has('name', k)
        if g2.hasNext():
            t = t.V().has('name', k)
        else:
            t = t.addV('keyword').property('type', 0).property('kid', max_id).property('name', k).property('confirmType', confirmType).property('uid', uid)
        t = t.as_('g2').V().has(id1_key, id1)
        t = t.as_('g1').choose(__.out(table).where(eq('g2')), __.select('g1'), __.addE(table).to('g2').property('type', type).property(id_key, max_id2).property(id1_key, id1).property('kid', select('g2').values('kid')).property('confirmType', get_value_iter(select('g2'), 'confirmType', 1)).property('uid', uid))
    if f:
        t = t.toList()
    return 0

def add_keyword_by_params(g, p, db):
    if 'type' not in p or 'id' not in p or 'keywords' not in p:
        return 1

    type = int(p['type'])
    id = int(p['id'])
    uid = int(p['uid']) if 'uid' in p else -1
    keywords = p['keywords'] if 'keywords' in p else ''
    keywords = keywords.split(',')
    type = front_type_ref[type] if type in front_type_ref else type
    table = vetex_type_ref[type]
    config = graph_vetex_config[table]
    id_key, name_key, key_list, return_key_list, category_relation, keyword_relation, *add_args = list(config)
    table2 = keyword_relation
    g1 = g.V().has('type',type).has(id_key,id)
    if not g1.hasNext():
        return 2
    relation_id_key, *add_args = list(graph_edge_config[keyword_relation])
    add_keyword(g, keywords, table2, id_key, id, relation_id_key, uid, db)
    return 0

def parse_brand_name(p):
    name_cn = p['name_cn'].strip() if 'name_cn' in p else ''
    name_en = p['name_en'].strip() if 'name_en' in p else ''
    name_cn = pnlp.unify_character(name_cn)
    name_en = pnlp.unify_character(name_en)
    l = []
    if name_cn == '' or name_en == '':
        name = name_en + name_cn
        l.append(name)
    else:
        name = name_en + '/' + name_cn
        l.append(name_en)
        l.append(name_cn)
    return (name, l, name_cn, name_en)

def change_name(g, p, db):
    if 'type' not in p or 'id' not in p or 'name' not in p:
        return 1

    type = int(p['type'])
    id = int(p['id'])
    uid = int(p['uid']) if 'uid' in p else -1
    name = p['name']
    name = pnlp.unify_character(name.strip())
    type = front_type_ref[type] if type in front_type_ref else type
    table = vetex_type_ref[type]
    config = graph_vetex_config[table]
    id_key, name_key, key_list, return_key_list, category_relation, keyword_relation, *add_args = list(config)
    v = g.V().has(id_key, id).values(name_key).toList()
    if v[0] == name:
        return 0
    if g.V().has(name_key, name).hasNext():
        return 2
    g.V().has(id_key, id).property(name_key, name).property('uid', uid).toList()
    return 0

def merge(g, p, db):
    uid = int(p['uid']) if 'uid' in p else -1
    for k in ['type1', 'id1', 'type2', 'id2']:
        if k not in p:
            return 1
    type1, id1, type2, id2 = int(p['type1']), int(p['id1']), int(p['type2']), int(p['id2'])
    if type1 != type2:
        return 2
    if type1 == 1:
        r = merge_category(g, id1, id2, uid)
    elif type1 == 2:
        r = merge_brand(g, id1, id2, uid)
    else:
        return 4
    return r

def merge_out_iter(t, base_key, table, id_key, relation_id_key, uid):
    return t.sideEffect(__.select('g2').outE(table).as_('e1').inV().where(neq('g1')).as_('r2').filter(
        __.not_(__.in_(table).where(eq('g1')))).addE(table).from_('g1').as_('newe').select('e1').properties().unfold().as_('p').select('newe').property(__.select('p').key(),__.select('p').value())
                 #.property('type', __.select('e1').values('type')).property(relation_id_key, __.select('e1').values(relation_id_key))
                 .property(base_key, __.select('g1').values(base_key)).property(id_key, __.select('e1').values(id_key))
                 #.property('confirmType', get_value_iter(__.select('e1'), 'confirmType'))
                 .property('merge', __.select('e1').values(base_key))
                 .property('uid', uid)
                        )

def merge_in_iter(t, base_key, table, id_key, relation_id_key, uid):
    return t.sideEffect(__.select('g2').inE(table).as_('e1').outV().where(neq('g1')).as_('r4').filter(
        __.not_(__.out(table).where(eq('g1')))).addE(table).to('g1').as_('newe').select('e1').properties().unfold().as_('p').select('newe').property(__.select('p').key(),__.select('p').value())
                 #.property('type', __.select('e1').values('type')).property(relation_id_key, __.select('e1').values(relation_id_key))
                 .choose(__.select('g1').has(base_key), __.property(base_key, __.select('g1').values(base_key)), __.property(base_key, __.select('g1').values(id_key)))
                 .property(id_key, __.select('e1').values(id_key))
                 #.property('confirmType', get_value_iter(__.select('e1'), 'confirmType'))
                 .property('merge', __.select('e1').values(base_key))
                 .property('uid', uid)
                        )

#relation: categoryKeyword categoryBrand categoryParent
def merge_category(g, id1, id2, uid):
    t = g.V().has('cid', id1).as_('g1')
    t = t.V().has('cid', id2).as_('g2')
    base_key = 'cid'
    for table in category_mergeout:
        id_key = category_mergeout[table]
        relation_id_key = graph_edge_config[table][0]
        t = merge_out_iter(t, base_key, table, id_key, relation_id_key, uid)
    for table in category_mergein:
        id_key,base_key = category_mergein[table]
        relation_id_key = graph_edge_config[table][0]
        t = merge_in_iter(t, base_key, table, id_key, relation_id_key, uid)
    t = t.sideEffect(__.select('g2').property('confirmType', 2).property('merge', id1).property('uid', uid))
    t = t.sideEffect(__.select('g1').property('confirmType', 1))
    print(t)
    t = t.toList()
    return 0

#relation: brandKeyword categoryBrand
def merge_brand(g, id1, id2, uid):
    t = g.V().has('bid', id1).both('brandReject').has('bid', id2)
    if t.hasNext():
        return 3

    t = g.V().has('bid', id1).as_('g1')
    t = t.V().has('bid', id2).as_('g2')
    base_key = 'bid'
    for table in brand_mergeout:
        id_key = brand_mergeout[table]
        relation_id_key = graph_edge_config[table][0]
        t = merge_out_iter(t, base_key, table, id_key, relation_id_key, uid)
    for table in brand_mergein:
        id_key,base_key = brand_mergein[table]
        relation_id_key = graph_edge_config[table][0]
        t = merge_in_iter(t, base_key, table, id_key, relation_id_key, uid)
    t = t.sideEffect(__.select('g2').property('confirmType', 2).property('merge', id1).property('uid', uid))
    t = t.sideEffect(__.select('g1').property('confirmType', 1))
    print(t)
    t = t.toList()
    return 0

def get_vetex_ids():
    ids = []
    names = []
    for k in graph_vetex_config:
        v = graph_vetex_config[k]
        ids.append(v[0])
        names.append(v[1])

    return ids, names

def merge_category_all(db, g, cid1, cid2, run=False):
    for table, key in [['categoryKeyword', 'kid'], ['categoryBrand', 'bid'], ['skuCategory', 'sid']]:
        sql = 'select count({key}) from {table} where cid={cid2} and {key} not in (select {key} from {table} where cid={cid1})'.format(cid1=cid1, cid2=cid2, table=table, key=key)
        c = db.query_scalar(sql)
        if c > 0:
            print('need update {table}'.format(table=table))
            sql = 'update {table} set cid={cid1},merge={cid2} where cid={cid2} and {key} not in (select {key} from (select {key} from {table} where cid={cid1}) as b)'.format(cid1=cid1, cid2=cid2, table=table, key=key)
            print(sql)
            if run:
                db.execute(sql)
    sql = 'update category set confirmType=2,alias={cid1} where cid={cid2}'.format(cid1=cid1,cid2=cid2)
    print(sql)
    if run:
        db.execute(sql)
    sql = 'update category set confirmType=1 where cid={cid1}'.format(cid1=cid1)
    print(sql)
    if run:
        db.execute(sql)
    p = '{"type1": 1, "id1": %d, "type2": 1, "id2": %d}' % (cid1, cid2)
    p = json.loads(p)
    if run:
        merge(g, p, db)

def search_cor_brand(db, alias_bid0):
    t = db.V().has('alias_bid0', alias_bid0).not_(where(__.values('confirmType').is_(2))).project('bid', 'name', 'alias_bid0_status').by('bid').by('bname').by(choose(__.has('alias_bid0_status'), __.values('alias_bid0_status'), constant(0)))
    print(t)
    t = t.toList()

    return t

def link_brand(db, params):
    uid = int(params['uid']) if 'uid' in params else -1
    t = db.V().has('bid', params['local_brand_id']).as_('g1')
    t = t.sideEffect(__.select('g1').property('alias_bid0', params['alias_bid']))
    t = t.property('uid', uid)
    print(t)
    t = t.toList()
    
    return 0

def checkCategoryKeyword(db, check_dict):
    # check_dict = {word: [cid, cid]}
    word_list = list(check_dict.keys())
    word_link_dict = collections.defaultdict(lambda: False)
    # word_link_dict = dict()
    t = db.V().has('name', within(word_list)).as_('keyword')
    t = t.inE('categoryKeyword').outV().as_('category')
    t = t.project('kname','cid').by(__.select('keyword').values('name')).by(__.select('category').values('cid'))
    data = t.toList()

    for info in data:
        if info['cid'] in check_dict[info['kname']]:
            word_link_dict[(info['kname'], info['cid'])] = True
    
    return word_link_dict


