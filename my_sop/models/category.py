#coding=utf-8
import time
from extensions import utils
import application as app

sub_root_category = {
        '2011010111' : [40,50011665,99,50008907,50004958,50016891],
        '2011010112' : [14,50008090,50012164,50007218,1201,1512,50023904,11,1101,20,50011972,50012082,50012100,50018004,50018222,50018264,50019780,50022703,124242008],
        '2011010113' : [50010788,1801,50002768,50023282,126762001],
        '2011010114' : [50006842,30,50011740,16,50006843,1625,50010404],
        '2011010115' : [21,50016349,50016348,50025705],
        '2011010116' : [50008164,27,50020485,50020579,50020611,50008163,50020808,50020857,50020332,126700003,124050001],
        '2011010117' : [35,50014812,25,50008165,50022517,122650005],
        '2011010118' : [50002766,50020275,50016422,50026800,50023717,50026316,50050359,50008141,124458005],
        '2011010119' : [50012029,50011699,50010728,50013886,50510002],
        '2011010120' : [26,50024971,50074001,122684003],
        '2011010121' : [33,34,50802001],
        '2011010122' : [50468001,50011397,50013864],
        '2011010123' : [23,28],
        '2011010124' : [50007216,29,2813,50011949,50008075,50014811,50018252,50014927,50019095,50023575,50023804,50024451,50024612,50025004,50025110,50025111,50025707,50026523,50026555,50050471,50158001,50454031,50690010,121536003,122718004],
        '2011010110' : [98, 1410, 1705, 2128, 124484008, 122852001, 122950001, 122952001, 126762001, 122928002, 50024099, 126602002, 50005998, 50020670, 124044001, 50008825, 50010388, 50023724, 50012206, 50012472, 50013698, 50014442, 50488001, 50016350, 50017133, 50017300, 50017633, 50017637, 50017652, 50017759, 50017760, 50017908, 50018320, 50026535, 50019231, 50019253, 50019379, 50020179, 127450004, 50020907, 127484003, 50022649, 50023722, 50023731, 50023732, 50023733, 50023734, 50023735, 50023736, 50023737, 50023738, 50023739, 50023740, 50023741, 50023742, 50023743, 50023764, 50023765, 50023766, 50023869, 50023878, 50023897, 50024186, 50024424, 50024449, 50025280, 50025490, 50025616, 50025618, 50025651, 50025706, 50025881, 50025968, 50026481, 50026488, 50026513, 50230002, 50602001, 120894001, 50734010, 120886001, 120950002, 120950001, 121266001, 121380001, 121536007, 127588002, 121938001, 121940001, 122222008, 122248006, 122244003, 122256002, 124354002, 122918002, 122966004, 123500005, 123536002, 123680001, 123690003, 124024001, 124110010, 124116010, 124466001, 124468001, 124470001, 124470006, 124568010, 124750013, 124698018, 124844002, 124852003, 124868003, 124912001, 125022006, 125102006, 125406001, 126040001, 126252002, 126488005, 126488008, 127076003, 127442006, 127452002, 127458007, 127492006, 127508003, 127876007, 127878006, 127882008, 127924022, 201136401, 201149009],
}

root_category_names = {
        '2011010111' : '游戏/话费',
        '2011010112' : '3C数码',
        '2011010113' : '美容护理',
        '2011010114' : '服饰鞋包',
        '2011010115' : '家居用品',
        '2011010116' : '家装家饰',
        '2011010117' : '母婴',
        '2011010118' : '食品/保健',
        '2011010119' : '运动/户外',
        '2011010120' : '汽车配件',
        '2011010121' : '书籍音像',
        '2011010122' : '珠宝/首饰',
        '2011010123' : '玩乐/收藏',
        '2011010124' : '生活服务',
        '2011010110' : '其它行业'
}

    # 'top_category_name' => array(
    #     '2011010111' => '游戏/话费',
    #     '2011010112' => '3C数码',
    #     '2011010113' => '美容护理',
    #     '2011010114' => '服饰鞋包',
    #     '2011010115' => '家居用品',
    #     '2011010116' => '家装家饰',
    #     '2011010117' => '母婴',
    #     '2011010118' => '食品/保健',
    #     '2011010119' => '运动/户外',
    #     '2011010120' => '汽车配件',
    #     '2011010121' => '书籍音像',
    #     '2011010122' => '珠宝/首饰',
    #     '2011010123' => '玩乐/收藏',
    #     '2011010124' => '生活服务',
    #     '2011010110' => '其它行业'
    # ),

sub_root_category_jd = {
        '2011010113' : [1316, 16750],
}

#cid_list中必须是lv0cid或者lv1cid
def get_all_by_id(cid_list, withAll=False):
    db = app.connect_db('1_apollo')
    l = []
    for cid in cid_list:
        cid = str(cid)
        if cid not in sub_root_category:
            l.append(cid)
            continue
        l += sub_root_category[cid]
    length = len(l)
    if length == 0:
        return []

    add_sql = ',lv1cid,lv2cid,lv3cid,lv4cid,lv5cid ' if withAll else ''
    sql = 'select cid, concat_ws(" > ", lv1name,lv2name,lv3name,lv4name, lv5name) as name ' + add_sql + ' from item_category_backend_all where lv1cid in (%s)'
    data = utils.easy_query(db, sql, l, True)
    return data

def get_all_by_id_for_jd(cid_list):
    db = app.connect_clickhouse('chmaster')
    l = []
    for cid in cid_list:
        cid = str(cid)
        if cid not in sub_root_category_jd:
            l.append(cid)
            continue
        l += sub_root_category[cid]
    length = len(l)
    if length == 0:
        return []

    sql = 'select cid, name from jd.cached_item_category where lv1cid in ({}) order by month desc limit 1 by cid'.format(','.join([str(x) for x in l]))
    return db.query_all(sql)

def get_all_by_id_all(db, cid_list, source='ali'):
    l = []
    for cid in cid_list:
        cid = str(cid)
        if cid not in sub_root_category_jd:
            l.append(cid)
            continue
        l += sub_root_category[cid]
    length = len(l)
    if length == 0:
        return []

    sql = 'select cid, name from {source}.cached_item_category where lv1cid in ({lv1cid}) order by month desc limit 1 by cid'.format(
        source=source,
        lv1cid=','.join([str(x) for x in l])
    )
    return db.query_all(sql)

def get_default_lv1cid(db):
    h = dict()
    for lv0cid in sub_root_category:
        for cid in sub_root_category[lv0cid]:
            h[cid] = 1

    l = []
    sql = 'select distinct lv1cid from item_category_backend_all'
    data = db.query_all(sql)
    r = []
    for row in data:
        cid = row[0]
        if cid in h:
            continue
        r.append(cid)
    return r

def get_all_category(db):
    if db is None:
        db = app.connect_db('1_apollo')
    h_lv1cid_lv0cid = dict()
    for lv0cid in sub_root_category:
        for lv1cid in sub_root_category[lv0cid]:
            h_lv1cid_lv0cid[lv1cid] = int(lv0cid)
    sql = 'select cid, lv1cid from item_category_backend_all'
    data = db.query_all(sql)
    h = dict()
    for row in data:
        cid, lv1cid = list(row)
        if lv1cid not in h_lv1cid_lv0cid:
            continue
        lv0cid = h_lv1cid_lv0cid[lv1cid]
        h[cid] = [lv0cid, lv1cid]
    return h

def get_all_by_cid(db, ids, only_get_name=False):
    if db is None:
        db = app.connect_db('1_apollo')
    if len(ids) == 0:
        return []
    sql_select = 'cid, concat_ws(" > ", lv1name,lv2name,lv3name,lv4name, lv5name) as name' if only_get_name else '*'
    sql = 'select {sql_select} from item_category_backend_all where cid in (%s)'.format(sql_select=sql_select)
    return utils.easy_query(db, sql, ids)

def get_all_lv1_cinfo(db):
    if db is None:
        db = app.connect_db('1_apollo')
    l = []
    for lv0cid in sub_root_category:
        l += sub_root_category[lv0cid]
    data = get_all_by_cid(db, l)
    h = dict()
    for row in data:
        h[row[0]] = row
    return h

#循环取品类
def get_category_with_tree(db, l, dbname):
    if len(l) == 0:
        return []
    id_str = ','.join([str(x) for x in l])
    sql = 'select cid,name,tree from {dbname}.cached_item_category where cid in ({id_str}) order by month desc limit 1 by cid'.format(dbname=dbname, id_str=id_str)
    data = db.query_all(sql)
    h_cid = {}
    h_need = {}
    for row in data:
        cid, name, tree = list(row)
        h_cid[cid] = 1
        for temp in tree:
            h_need[temp] = 1
    l_cid = []
    for cid in h_need:
        if cid not in h_cid:
            l_cid.append(cid)
    if len(l_cid) > 0:
        data2 = get_category_with_tree(db, l_cid, dbname)
        return list(data) + list(data2)
    return data

def get_category_keyword(db, ids, dbname):
    l = get_category_with_tree(db, ids, dbname)
    h_cid_name = {}
    for cid, name, tree in l:
        h_cid_name[str(cid)] = name
    h_cid_keyword = {}
    for cid, name, tree in l:
        h_keyword = utils.get_or_new(h_cid_keyword, [str(cid)], {})
        for temp in tree:
            temp = str(temp)
            name2 = h_cid_name[temp]
            for keyword in utils.parse_keyword_all(name2):
                h_keyword[keyword] = 1
    return h_cid_name, h_cid_keyword



