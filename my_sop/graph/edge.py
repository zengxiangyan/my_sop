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
from graph import vetex as vetexManager
from extensions import utils
from graph.db import category as categoryModel

graph = app.get_graph('default')
front_type_ref = vetexManager.front_type_ref
vetex_type_ref = vetexManager.vetex_type_ref
edge_type_ref = vetexManager.edge_type_ref
vetex_table_ref = vetexManager.vetex_table_ref
edge_table_ref = vetexManager.edge_table_ref
id_key_list = vetexManager.id_key_list
graph_vetex_config = vetexManager.graph_vetex_config
graph_edge_config = vetexManager.graph_edge_config
graph_relation_ref = {
    1: {
        0: ['categoryKeyword'],
        1: ['categoryParent'],
        2: ['categoryBrand'],
        6: ['categoryPropNameR'],
        7: ['categorySemantic'],
        8: ['categoryPart']
    },
    2: {
        0: ['brandKeyword'],
        2: ['brandReject','brandRelation', 'brandAlias'],
        5: ['brandPropValue'],
        7: ['brandSemantic'],
        8: ['brandPart']
    },
    3: {
        0: ['makerKeyword'],
        2: ['makerBrand']
    },
    4: {
        0: ['categoryPropNameKeyword'],
        6: ['categoryPropNameL']
    },
    5: {
        0: ['propValueKeyword']
    },
    6: {
        8: ['categoryPropNamePart'],
        5: ['categoryPropValue']
    },
    7:  {
        0: ['semanticKeyword'],
        8: ['semanticPart']
    },
    8:  {
        0: ['partKeyword'],
        4: ['partPropName'],
        8: ['partRelation']
    },
    9: {
        0: ['skuKeyword'],
        1: ['skuCategory'],
        2: ['skuBrand'],
        9: ['skuParent']
    },
    12: {
        0: ['spuKeyword'],
        8: ['spuPart'],
        12: ['spuRelation']
    }
}
edge_duplicate_vetex = {
    1001:   'pcid',
    2002:   'rbid',
    1002002:'pabid1',
    8008:   'papid'
}
def search_uname(db, uids):
    if (len(uids) != 0):
        sql = "select id,username from user where id in (" + ','.join(uids) + ")"
        data = db.query_all(sql)
        return data
    else:
        return ()

#subject是主体 全部时为0 object是客体
def search(g, p):
    type_g = int(p['subject']) if 'subject' in p else -1
    sub_type = p['object'] if 'object' in p else [-1]
    if type(sub_type) is not list:
        sub_type = [(int)(sub_type)]
    id = utils.parse_id(p['id']) if 'id' in p and p['id'] != '' else []
    name = str(p['name']) if 'name' in p else ''
    confirmType = int(p['confirmType']) if 'confirmType' in p else -1
    mode = int(p['mode']) if 'mode' in p else -1
    cid = utils.parse_category(p)
    # cids = [cid] + categoryModel.get_sub_category(g, cid) if cid > 0 else []
    edge_ids = get_edge_ids()
    vetex_ids, vetex_names = vetexManager.get_vetex_ids()
    type_g = front_type_ref[type_g] if type_g in front_type_ref else type_g
    sub_type_list = utils.parse_subtype(sub_type,front_type_ref)
    if type_g >= 0 and type_g not in vetex_type_ref:
        return {'code': 1}
    for sub_type_i in sub_type_list:
        if sub_type_i >= 0 and sub_type_i not in vetex_type_ref:
            return {'code': 1}
    t = g.V()
    origin_type = type_g
    use_category = False
    if type_g >= 0:
        table = vetex_type_ref[type_g]
        config = graph_vetex_config[table]
        id_key, name_key, key_list, return_key_list, category_relation, *add_args = list(config)
    if not (type_g != -1 and (id != [] and name != '')):
        use_category = True
        if cid > 0:
            t = t.has('cid', cid).union(__.identity(),__.repeat(inE('categoryParent').outV()).until(__.not_(inE('categoryParent')))).as_('category')
            if origin_type == 0:
                t = t.union(both('categoryKeyword', 'categoryBrand'), select('category'))
            elif  type_g != -1 and vetex_type_ref[type_g] != 'category':
                if not isinstance(category_relation, list):
                    category_relation = [category_relation]
                for relation in category_relation:
                    t = t.both(relation)
    # p['like'] = True
    if type_g >= 0:
        if id == [] and name == '':
            t = t.has('type', type_g)
        if id != []:
            t = t.has(id_key, within(id))
        if name != '':
            if 'like' in p and p['like']:
                name = '{}'.format(name)
                t = t.has(name_key, textContainsFuzzy(name))
            else:
                t = t.has(name_key, name)

    if not use_category and cid > 0 and category_relation != '':
        t = t.filter(vetexManager.get_category_filter(__, category_relation, cid, vetex_type_ref[type_g]))
    t = t.as_('s','sid','sname','sconfirmtype').union(inE().as_('relation').constant('in').as_('direction'),outE().as_('relation').constant('out').as_('direction')).select('relation')
    if confirmType != -1:
        t = t.has('confirmType', confirmType)
    if mode != -1:
        t = t.has('mode', mode)
    t = t.as_('id','type','confirmType','mode','source','uid').bothV()
    if -1 not in sub_type_list:
        t = t.has('type', within(sub_type_list))
    t = t.where(neq('s')).as_('o','oid','oname','oconfirmtype')
    t = t.select('s','sid','sname','sconfirmtype','id','type','direction','confirmType','mode','source','uid','o','oid','oname','oconfirmtype').by(__.values('type')).by(__.values(*id_key_list))
    t = t.by(__.values(*vetex_names)).by(__.values('confirmType'))
    t = t.by(__.values(*edge_ids)).by(__.values('type')).by().by(choose(__.has('confirmType'),__.values('confirmType'),__.constant(0))).by(vetexManager.get_value_iter(__, 'mode')).by(vetexManager.get_value_iter(__, 'source')).by(vetexManager.get_value_iter(__, 'uid', -1)).by(__.values('type')).by(__.values(*id_key_list))
    t = t.by(__.values(*vetex_names)).by(__.values('confirmType'))
    t = t.dedup().as_('t').group().by().project('count', 'data')
    count = -1
    if 'count' in p:
        t = t.by(__.count(local))
    else:
        t = t.by(constant(-1))
    print('t0:', t)
    limit = int(p['limit']) if 'limit' in p else 0
    if limit > 0:
        start = int(p['current']) if 'current' in p else 0
        end = start + limit
        t = t.by(range(local, start, end))
    print(t)
    data = t.toList()
    print(data)
    count = int(data[0]['count'])
    data = data[0]['data']
    l = []
    for row in data:
        l.append(data[row][0])
    data = l
    print('data:', data)
    h_cid = {}
    uids = set()
    uid_username_dict = {}
    for row in data:
        if isinstance(row['sname'], list):
            row['sname'] = ' > '.join(row['sname'][1:])
        if isinstance(row['oname'], list):
            row['oname'] = ' > '.join(row['oname'][1:])
        if row['s'] == 1:
            h_cid[row['sid']] = 1
        if row['o'] == 1:
            h_cid[row['oid']] = 1
        uids.add(str(row['uid']))
    db = app.connect_db('graph')
    userdata = search_uname(db, uids)
    for row in userdata:
        uid_username_dict[row[0]] = row[1]
    for row in data:
        row['username'] = uid_username_dict.get(row['uid']) if uid_username_dict.get(row['uid']) else -1
    if len(h_cid.keys()) > 0:
        h_cid_name = categoryModel.get_full_name(g, list(h_cid.keys()))
        for row in data:
            if row['s'] == 1:
                cid = row['sid']
                if cid in h_cid_name:
                    row['sname'] = h_cid_name[cid]
            if row['o'] == 1:
                cid = row['oid']
                if cid in h_cid_name:
                    row['oname'] = h_cid_name[cid]
    # print('data:', data)
    return {
        'data': data,
        'count': count
    }

def confirm(g, p):
    type = int(p['type'])
    ids = p['ids'] if 'ids' in p else []
    uid = int(p['uid']) if 'uid' in p else -1
    if len(ids) == 0:
        return
    confirmType = int(p['confirmType']) if 'confirmType' in p else 0
    if confirmType not in [0, 1, 2, 3]:
        return

    type = front_type_ref[type] if type in front_type_ref else type
    table = edge_type_ref[type]
    config = graph_edge_config[table]
    id_key = config[0]
    if table in ('categorySemantic', 'categoryPart', 'brandSemantic', 'brandPart', 'partRelation','categoryKeyword','brandKeyword','makerKeyword','categoryPropNameKeyword','propValueKeyword','semanticKeyword','partKeyword','skuKeyword','categoryPropNamePart'):
        mode = p['mode'] if 'mode' in p else 0
        g.E().has(id_key, within(ids)).property('confirmType', confirmType).property('mode', mode).property('uid', uid).toList()
    else:
        g.E().has(id_key, within(ids)).property('confirmType', confirmType).property('uid', uid).toList()

def add_relation_byid(g,p,db,table,type,i1,i2,id1_key,id2_key,p_id2_key,uid,confirmType):
    id1 = int(i1)
    id2 = int(i2)
    l = graph_edge_config[table]
    id_key = l[0]
    max_id = utils.get_next_id(db, table)
    t = g.V().has(id2_key, id2).as_('g2').V().has(id1_key, id1).as_('g1')
    if table in ('categorySemantic', 'categoryPart', 'brandSemantic', 'brandPart', 'partRelation','categoryKeyword','brandKeyword','makerKeyword','categoryPropNameKeyword','propValueKeyword','semanticKeyword','partKeyword','skuKeyword','categoryPropNamePart'):
        mode = int(p['mode']) if 'mode' in p else 0
        t = t.choose(__.out(table).where(eq('g2')), __.select('g1'), __.addE(table).to('g2').property('type', type).property(id_key, max_id).property(id1_key, id1).property(p_id2_key, id2).property('confirmType', confirmType).property('mode', mode).property('uid', uid))
    else:
        t = t.choose(__.out(table).where(eq('g2')), __.select('g1'), __.addE(table).to('g2').property('type', type).property(id_key, max_id).property(id1_key, id1).property(p_id2_key, id2).property('confirmType', confirmType).property('uid', uid))
    print(t)
    t.toList()

def add_relation(g, p, db):
    for k in ['type1', 'id1', 'type2', 'id2', 'type', 'direction']:
        if k not in p:
            return 1
    uid = int(p['uid']) if 'uid' in p else -1
    confirmType = int(p['confirmType']) if 'confirmType' in p else 1
    type1, id1, type2, id2, type, direction = int(p['type1']), p['id1'], int(p['type2']), p['id2'], int(p['type']), int(p['direction'])
    if p['direction'] == 1:
        type1, id1, type2, id2 = type2, id2, type1, id1
    type1 = front_type_ref[type1] if type1 in front_type_ref else type1
    type2 = front_type_ref[type2] if type2 in front_type_ref else type2
    type = front_type_ref[type] if type in front_type_ref else type
    if type1 not in graph_relation_ref or type2 not in graph_relation_ref[type1]:
        return 2
    relation_list = graph_relation_ref[type1][type2]
    table = edge_type_ref[type]
    if table not in relation_list:
        return 3

    table1 = vetex_type_ref[type1]
    table2 = vetex_type_ref[type2]
    l1 = graph_vetex_config[table1]
    l2 = graph_vetex_config[table2]
    id1_key = l1[0]
    id2_key = l2[0]
    p_id2_key = edge_duplicate_vetex[type] if type in edge_duplicate_vetex else id2_key

    if p['direction'] == 1 and isinstance(id1,list):
        for i1 in id1:
            add_relation_byid(g,p,db,table,type,i1,id2,id1_key,id2_key,p_id2_key,uid,confirmType)
    elif p['direction'] == 0 and isinstance(id2,list):
        for i2 in id2:
            add_relation_byid(g,p,db,table,type,id1,i2,id1_key,id2_key,p_id2_key,uid,confirmType)
    elif not isinstance(id2,list) and not isinstance(id1,list):
        add_relation_byid(g,p,db,table,type,id1,id2,id1_key,id2_key,p_id2_key,uid,confirmType)
    else:
        return 4
    return 0

def get_edge_ids():
    ids = []
    for k in graph_edge_config:
        ids.append(graph_edge_config[k][0])
    return ids

def Merge(dict1, dict2):
    return(dict2.update(dict1))

def get_all(g, p):
    vetex_type_list = []
    category_id = -2
    for i in vetex_type_ref:
        if vetex_type_ref[i] == 'category':
            category_id = i
        vetex_type_list.append(i)
    if 'gid' not in p:
        return {'code': 1}
    gid = p['gid']
    cid = utils.parse_category(p)
    pg_size = p['pg_size'] if 'pg_size' in p else [0,30]
    if len(pg_size) != 2:
        print("pg_size error")
        return {'code': 2}
    start = pg_size[0]
    end = pg_size[1]
    if category_id not in vetex_type_ref:
        print("category_id error")
        return {'code': 3}
    if 'v_type' not in p:
        v_type = vetex_type_list
    elif -1 in p['v_type']:
        v_type = vetex_type_list
    else:
        v_type = p['v_type']
    v_status = p['v_status'] if 'v_status' in p else [0,1]
    e_status = p['e_status'] if 'e_status' in p else [0,1]
    t = g.V(gid).bothE().has('confirmType',within(e_status)).bothV().has('type',category_id)
    if category_id in v_type:
        # v_type.remove(category_id)
        v_type2 = category_id
        if cid > 0:
            t = t.has('cid', cid).union(__.identity(), __.repeat(inE('categoryParent').outV()).until(__.not_(inE('categoryParent'))))
        else:
            pass
    else:
        t = g.V(gid)

    ids, names = vetexManager.get_vetex_ids()
    edge_ids = get_edge_ids()
    t_v = g.V(gid).as_('origin')
    t_v = t_v.project('vetex').by(union(identity(), __.select('origin').identity(), __.select('origin').bothE().has('confirmType',within(e_status)).bothV().has('type',within(v_type)).has('confirmType',within(v_status))).dedup().as_('v').project('gid', 'type','name','confirmType','id','origin','target','isShop','mode','memo','alias_bid0').by(__.select('v').id()).by(__.select('v').values('type')).by(__.select('v').values(*names)).by(__.select('v').values('confirmType')).by(__.select('v').values(*id_key_list)).by(vetexManager.get_value_iter(__.select('v'), 'origin')).by(vetexManager.get_value_iter(__.select('v'), 'target')).by(vetexManager.get_value_iter(__.select('v'), 'isShop')).by(vetexManager.get_value_iter(__.select('v'), 'mode')).by(vetexManager.get_value_iter(__.select('v'), 'memo')).by(vetexManager.get_value_iter(__.select('v'), 'alias_bid0')).range(start,end).fold())
    t_e = g.V(gid).as_('origin')
    t_e = t_e.project('edge').by(union(inE().has('confirmType',within(e_status)).as_('relation').constant('in').as_('direction'),outE().has('confirmType',within(e_status)).as_('relation').constant('out').as_('direction')).project('source', 'target', 'type', 'direction', 'gid','confirmType', 'mode').by(__.select('relation').outV().id()).by(__.select('relation').inV().id()).by(__.select('relation').values('type')).by().by(__.select('relation').id()).by(__.select('relation').values('confirmType')).by(choose(__.select('relation').has('mode'), __.select('relation').values('mode'), )).fold())
    ret_a = t_v.toList()
    ret_b = t_e.toList()
    r = Merge(ret_a[0] if len(ret_a) != 0 else {}, ret_b[0] if len(ret_b) != 0 else {})
    data = ret_b
    return {'code': 0, 'data': data}
