from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from colorama import Fore
import application as app
from graph.kgraph import g_confirmedE, g_inV, g_outV
from graph.vetex import vetex_type_ref
from models.analyze.trie import Trie
from models.nerl import *

g = app.connect_graph('default')

layer_index = [1]
check_trie = Trie()
for w in ['单品', '品类', '分类', '规格', '品种', '单位', '大小', '尺寸']:
    check_trie.insert(w)


def printpath(path):
    """打印知识图谱path路径"""
    count = -1
    for index, p in enumerate(path):
        if index == 0:
            print('┌', Fore.YELLOW, p, Fore.RESET, sep='')
        elif index == len(path) - 1:
            print(' ' * (count * 2), '└──', Fore.RED, p, Fore.RESET, sep='')
        elif isinstance(path[index]['type'], list):
            print(' ' * (count * 2), '└─┬', p, Fore.RESET, sep='')
        else:
            continue
        count += 1


def category_layer_check(c_list, init=1):
    """品类层查询"""
    global layer_index
    if init:
        layer_index = [1]
    if isinstance(c_list, int):
        c_list = [c_list]

    t = g_outV(g_confirmedE(g.V().has('cid', P.within(c_list)).inE('categoryParent'))).cid.dedup().toList()
    if t:
        layer_index.append(layer_index[-1] + len(t))
        return t + category_layer_check(t, 0)
    else:
        return []


def query_category(cids):
    query = g.V().has('cid', P.within(cids)).as_('category')
    query = query.optional(g_inV(g_confirmedE(__.outE('categoryKeyword')).where(__.values('mode').is_(0)))).as_('keyword')
    category_keyword_paths = query.dedup('category', 'keyword').path().by(__.valueMap(True)).toList()
    return category_keyword_paths


def query_brand(bids):
    query = g.V().has('bid', P.within(bids)).as_('brand')
    query = query.optional(g_inV(g_confirmedE(__.outE('brandKeyword')).where(__.values('mode').is_(0)))).as_('keyword')  # brand后的keyword nodes
    brand_keyword_paths = query.dedup('brand', 'keyword').path().by(__.valueMap(True)).toList()
    return brand_keyword_paths


def query_categoryPropName(cpnids):
    # categoryPropName查询
    query = g.V().has('cpnid', P.within(cpnids)).as_('cpn')
    query = g_inV(g_confirmedE(query.outE('categoryPropValue'))).as_('pv')
    query = query.optional(g_inV(g_confirmedE(__.outE('propValueKeyword')))).as_('keyword')
    cpn_pv_keyword_paths = query.dedup('cpn', 'pv', 'keyword').path().by(__.valueMap(True)).toList()
    return cpn_pv_keyword_paths


def query_propname(cpnids):
    # PropName查询
    query = g.V().has('cpnid', P.within(cpnids)).as_('cpn')
    query = g_outV(g_confirmedE(query.inE('categoryPropNameL'))).as_('pn')
    cpnid2pn = query.select('cpn', 'pn').by('cpnid').by('pnname').toList()
    return {relation['cpn']: relation['pn'] for relation in cpnid2pn}


def query_sinking_category(cids):
    # sinking_category查询
    query = g.V().has('cid', P.within(cids)).as_('category')
    query = g_inV(g_confirmedE(query.outE('categoryPropNameR'))).has('cpnname', '类别').as_('cpn')
    query = g_inV(g_confirmedE(query.outE('categoryPropValue'))).as_('pv')
    more_category_keyword_paths = query.dedup('category', 'pv').path().by(__.valueMap(True)).toList()
    return more_category_keyword_paths


def limit_category(all_cid, limit_category_layer):
    """限制品类层数>limit_category_layer"""
    return all_cid[layer_index[limit_category_layer]:]


def get_cid2bid(all_cid):
    all_bid = g_inV(g_confirmedE(g.V().has('cid', P.within(all_cid)).outE('categoryBrand'))).bid.dedup().toList()
    return all_bid


def get_cid2cpnid(all_cid):
    all_cpnid = g_inV(g_confirmedE(
        g.V().has('cid', P.within(all_cid)).outE('categoryPropNameR'))).has(
        'cpnname', P.without('类别', '品名', '系列', '高级选项', '件数', '型号', '分类')).cpnid.dedup().toList()
    return all_cpnid


def get_entity2keywords(all_paths, cpnid2pn=None):
    if cpnid2pn is None:
        cpnid2pn = dict()
    entities, keywords = dict(), dict()

    def add_token2gid(tokens, gid):
        if isinstance(tokens, str):
            tokens = [tokens]

        for token in tokens:
            if len(token) > 0:
                if keywords.get(token) is None:
                    keywords[token] = set()
                keywords[token].add(gid)

    for path in all_paths:
        printpath(path)

        entity, propname = None, None
        for n in path:
            # print(n)
            if isinstance(n['type'], int):
                continue
            gid = list(n.values())[0]
            label = vetex_type_ref[n['type'][0]]

            if label == KG_label_brand:
                if entities.get(gid) is None:
                    entities[gid] = BrandEntity(gid, n['bid'][0], n['bname'][0])
                    add_token2gid([n['name_cn'][0], n['name_en'][0]], gid)
                entity = entities[gid]
            elif label == KG_label_category:
                if entities.get(gid) is None:
                    entities[gid] = CategoryEntity(gid, n['cid'][0], n['cname'][0])
                    add_token2gid(n['cname'][0], gid)
                entity = entities[gid]
            elif label == KG_label_category_property:
                cpnid = n['cpnid'][0]
                propname = cpnid2pn.get(cpnid, n['cpnname'][0])  # 能查询到通用属性则由通用属性代替，否则为品类属性
                if check_trie.search_entity(propname):
                    break
            elif label == KG_label_property_val:
                if propname != '类别':
                    if entities.get(gid) is None:
                        if propname is None:
                            raise ValueError('No propname for init PropEntity')
                        entities[gid] = PropEntity(gid, n['pvid'][0], n['pvname'][0], propname)
                        add_token2gid(n['pvname'][0], gid)
                    entity = entities[gid]
                else:
                    add_token2gid(n['pvname'][0], entity.gid)
            elif label == KG_label_keyword:
                if entity:
                    add_token2gid(n['name'][0], entity.gid)
    return entities, keywords


def get_all_keywords(all_paths, cpnid2pn):
    keywords = set()
    for path in all_paths:
        printpath(path)

        entity, propname = None, None
        for n in path:
            # print(n)
            if isinstance(n['type'], int):
                continue
            gid = list(n.values())[0]
            label = vetex_type_ref[n['type'][0]]

            if label == KG_label_brand:
                for name in '/'.split(n['bname'][0]) + [n['name_cn'][0], n['name_en'][0]]:
                    if name:
                        keywords.add(name.lower())
            elif label == KG_label_category:
                name = n['cname'][0]
                keywords.add(name.lower())
            elif label == KG_label_category_property:
                cpnid = n['cpnid'][0]
                propname = cpnid2pn.get(cpnid, n['cpnname'][0])  # 能查询到通用属性则由通用属性代替，否则为品类属性
                if check_trie.search_entity(propname):
                    break
            elif label == KG_label_property_val:
                if propname != '类别':
                    name = n['pvname'][0]
                    keywords.add(name.lower())
            elif label == KG_label_keyword:
                name = n['name'][0]
                keywords.add(name.lower())
    return keywords

# if __name__ == '__main__':
#     cids = category_layer_check(25602)
#     print(len(cids))
#     # all_path = query_category(cids)
#     # for path in all_path:
#     #     printpath(path)
#
#     entities, keywords = get_entity2keywords(query_category(cids), dict())
#     print(entities)
#     input()
#     print(keywords)
