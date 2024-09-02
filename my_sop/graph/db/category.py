from models import category
from gremlin_python import statics
from extensions import gremlin_full_text
statics.load_statics(globals())
from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import __
from extensions import utils

root_category = category.sub_root_category
root_category_names = category.root_category_names
category_db_ref = {2011010110: 25594,2011010111: 25595,2011010112: 25596,2011010113: 25597,2011010114: 25598,2011010115: 25599,2011010116: 25600,2011010117: 25601,2011010118: 25602,2011010119: 25603,2011010120: 25604,2011010121: 25605,2011010122: 25606,2011010123: 25607,2011010124: 25608}
chash = dict()
tree = dict()

def add_nodes_using_path(root, path):
    if len(path) > 0:
        node = path[0]
        #print(node)
        if node not in root:
            root[node] = [node, chash[node][2], dict()]        
        path = path[1:]
        add_nodes_using_path(root[node][2], path)

def add_child(tree_node, cid, cname):
    if cid not in tree_node[2]:
        tree_node[2][cid] = [cid, cname, dict()]

def get_all_category(db):    
    global tree
    tree = dict()

    for cid, cname in root_category_names.items():
        tree[int(cid)] = [int(cid), cname, dict()]
    
    h_lv1cid_lv0cid = dict()
    for lv0cid in root_category:
        for lv1cid in root_category[lv0cid]:
            h_lv1cid_lv0cid[lv1cid] = int(lv0cid)
    sql = 'select categoryId, parent, name from category where deleteFlag=0'
    data = db.query_all(sql)

    #chash = dict()
    for cid, parent, name in data:
        chash[cid] = (cid, parent, name)

    for cid, parent, name in data:
        path = [cid]        
        p = parent
        n = cid        
        invalid_parent = False
        while True:            
            if p == 0:
                if n in h_lv1cid_lv0cid:
                    p = h_lv1cid_lv0cid[n]
                    path.append(p)                
                break                    
            else:
                path.append(p)
                if p in chash:
                    n = p                
                    p = chash[p][1]
                else:
                    invalid_parent = True   #parent is deleted
                    break
        
        if invalid_parent:
            continue

        path = [ n for n in reversed(path) ]
        
        #print('->'.join([ str(n) for n in path ]))
        
        add_nodes_using_path(tree, path)
            
    return tree

def get_sub_category(g, cid):
    r = g.V().has('cid', cid).repeat(__.inE('categoryParent').outV()).until(__.not_(__.inE('categoryParent'))).values('cid').dedup().toList()
    return r

def get_direct_sub_category(g, cid):
    cid = int(cid)
    if cid == 0:
        r = []
        for k in root_category_names.keys():
            r.append((category_db_ref[int(k)], root_category_names[k]))
    else:
        r = g.V().has('cid',cid).inE('categoryParent').has('confirmType', within(0,1)).outV().has('confirmType', within(0,1)).local(union(__.values('cid'),union(__.values('cname'), choose(__.has('source', 1), constant('（渠道专用）'), constant('')), choose(__.has('source', 2), constant('（天猫）'), constant('')), choose(__.has('source', 3), constant('（京东）'), constant('')),choose(__.inE('categoryParent').has('confirmType', within(0,1)).outV().has('confirmType', within(0,1)), constant('1'), constant('0')) ).fold()).fold()).toList()
    return r

def get_full_name(g, cids):
    t = g.V().has('cid', within(cids)).where(out('categoryParent')).project('id', 'name').by(__.values('cid')).by(repeat(out('categoryParent').simplePath()).until(__.not_(out('categoryParent'))).path().by('cname').map(lambda: ("it.get().objects().reverse()", "gremlin-groovy")))
    d = t.toList()
    h_cid_name = {}
    for row in d:
        print(row)
        h_cid_name[row['id']] = ' > '.join(row['name'])
    return h_cid_name

def get_children(db, root):
    pcid_to_cid = {}
    sql = 'select a.cid,a.pcid from categoryParent a join graph.category b on \
    b.cid=a.cid  where b.confirmType!=2 and a.confirmType!=2'
    for row in db.query_all(sql):
        cid, pcid = list(row)
        cid = int(cid)
        pcid = int(pcid)
        if pcid not in pcid_to_cid:
            pcid_to_cid[pcid] = []
        if cid != pcid:
            pcid_to_cid[pcid].append(cid)
    if root in pcid_to_cid:
        children = get_children_sub(root, pcid_to_cid, [])
        children = list(set(children))
        count = {}
        for cid in children:
            count[cid] = 0
        sql = 'select b.alias_bid0,b.alias_bid0_status,a.cid from categoryBrand a join \
        brand b on b.bid = a.bid where a.cid in (%s)'
        for row in utils.easy_query(db, sql, children):
            alias_bid0, alias_bid0_status, cid = list(row)
            alias_bid0 = int(alias_bid0)
            alias_bid0_status = int(alias_bid0_status)
            cid = int(cid)
            if alias_bid0 != 0 and alias_bid0_status == 0:
                count[cid] = count[cid] + 1

        return children, count
    else:
        return [], {}


def get_children_sub(parent, pcid_to_cid, children):
    for each_child in pcid_to_cid[parent]:
        # print(each_child)
        if each_child in pcid_to_cid:
            children.append(each_child)
            get_children_sub(each_child, pcid_to_cid, children)
        else:
            children.append(each_child)
    return children

def get_category_tree(g, root, remove_invalid=True):
    """
    获取品类树（从root到叶节点的路径，可选择删除/保留无效路径以及结点）
    注：当remove_invalid=False时 返回路径个数与get_sub_category返回结点个数相同
    """
    from graph.kgraph import g_confirmedE, g_outV

    if remove_invalid:
        p = g.V().has('cid', root).repeat(g_outV(g_confirmedE(__.inE('categoryParent')))).until(
            __.not_(g_outV(g_confirmedE(__.inE('categoryParent'))))).dedup().path().by('cid').toList()
        return [list(_)[::2] for _ in p]
    else:
        p = g.V().has('cid', root).repeat(__.in_('categoryParent')).until(
            __.not_(__.inE('categoryParent'))).dedup().path().by('cid').toList()
        return [list(_) for _ in p]
