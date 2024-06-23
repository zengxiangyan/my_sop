import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from graph.kgraph import g_confirmed, g_confirmedE, g_inV
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P

g = app.get_graph('default')
db = app.get_db('graph')
db_clickhouse = app.get_clickhouse('chmaster')
db_sop = app.get_clickhouse('chsop')
db.debug = db_sop.debug = db_clickhouse.debug = False

# all_brand = dict()
# all_category = dict()
all_cpn = dict()
all_pv = dict()
# all_ip = dict()
type2id = {'category': 'cid', 'brand': 'bid', 'propValue': 'pvid'}
type2name = {'category': 'cname', 'brand': 'bname', 'propValue': 'pvname'}


class Node:
    def __init__(self, ids, type):
        self.name = ''
        self.type = type
        self.keywords = set()
        if len(ids) == 0:
            return

        query = g_confirmed(g.V().has(type2id[type], P.within(ids))).as_('name')  # 寻找nodes
        query = query.optional(g_inV(g_confirmedE(__.outE(type + 'Keyword')).has('mode', 0))).as_(
            'keyword')  # nodes后的keyword nodes

        for all_path in query.dedup('name', 'keyword').path().by(__.valueMap(True)).toList():
            for path in all_path:
                if path['label'] == type:
                    name = path[type2name[type]][0]
                    self.keywords.add(name.lower())
                    if type == 'brand':
                        self.keywords |= {path['name_en'][0].lower(), path['name_cn'][0].lower()}
                    # 最长为名字
                    if len(name) > len(self.name):
                        self.name = name
                elif path['label'] == 'keyword':
                    self.keywords.add(path['name'][0].lower())
        self.keywords = list(filter(lambda x: len(x), self.keywords))

    @property
    def keywords2name(self):
        return {k: self.name for k in self.keywords}

    @property
    def name2keyword(self):
        return dict([(self.name, self.keywords)])


class BrandNode(Node):
    def __init__(self, alias_bid0):
        bid = list(map(lambda x: x[0], db.query_all(
            f'select bid from brand where alias_bid0={alias_bid0} and confirmType!=2 and deleteFlag=0 and alias_bid0!=0;')))
        self.bids = bid
        super().__init__(bid, 'brand')
        # if self.name == '':
        #     data = db_sop.query_all(f'select name, name_cn, name_en from sop.all_brand where bid={alias_bid0}')[0]
        #     self.name = data[0].lower()
        #     self.keywords = list(set(name.lower() for name in data if len(name) > 0))

    def __str__(self):
        return f'bids    :{self.bids}\n' \
               f'name    : {self.name}\n' \
               f'keywords: {self.keywords}\n'


class CategoryNode(Node):
    def __init__(self, cid0):
        cid = list(map(lambda x: x[0], db.query_all(
            f'select cid from category where cid0={cid0} and confirmType!=2 and deleteFlag=0;')))
        self.cids = cid
        super().__init__(cid, 'category')
        self.props = []
        # if self.name == '':
        #     data = db_clickhouse.query_all(f'select name from ali.item_category where cid={cid0}')
        #     if len(data) > 0:
        #         self.name = data[0][0].lower()
        #         self.keywords = [self.name]

        if cid:
            cpnids = g.V().has('cid', P.within(self.cids)).out('categoryPropNameR').has('cpnname',
                                                                                        P.without('系列')).cpnid.toList()
            for cpnid in cpnids:
                if not all_cpn.get(cpnid):
                    all_cpn[cpnid] = CatPropNameNode(cpnid)
                self.props.append(all_cpn[cpnid])

    def __str__(self):
        return f"cids    : {self.cids}\n" \
               f"name    : {self.name}\n" \
               f"keywords: {self.keywords}\n" \
               f"props   : {self.props}\n"


class PropValueNode(Node):
    def __init__(self, pvid, type):
        self.pvid = pvid
        super().__init__(pvid, 'propValue')
        self.type = type

    def __repr__(self):
        return self.name

    def __str__(self):
        return f"pvid    : {self.pvid}\n" \
               f"name    : {self.name}\n" \
               f"keywords: {self.keywords}\n" \
               f"type    : {self.type}\n"


class CatPropNameNode:
    def __init__(self, cpnid):
        self.cpnid = cpnid
        self.keywords = g.V().has('cpnid', cpnid).cpnname.toList()
        self.name = sorted(self.keywords, key=lambda x: -len(x))[0]
        self.value = []
        pvids = g.V().has('cpnid', cpnid).out('categoryPropValue').pvid.toList()
        for pvid in pvids:
            if not all_pv.get(pvid):
                all_pv[pvid] = PropValueNode([pvid], self.name)
            self.value.append(all_pv[pvid])

    def __repr__(self):
        return f"{self.name}: {self.value}"


class IPNode:
    def __init__(self, ip_id, name):
        self.ipid = ip_id
        self.name = name
        self.type = 'ip'
        self.keywords = set(map(lambda x: x[0], db.query_all(
            f"select distinct name from keyword where kid in "
            f"(select kid from ipkeyword where ip_id={ip_id} and confirmType!=2 and deleteFlag=0);")))
        self.keywords = list(self.keywords | {self.name})

    def __str__(self):
        return f"ipid    : {self.ipid}\n" \
               f"name    : {self.name}\n" \
               f"keywords: {self.keywords}\n"

    @property
    def keywords2name(self):
        return {k: self.name for k in self.keywords}

    @property
    def name2keyword(self):
        return dict([(self.name, self.keywords)])


def pv2pn(all_cat, all_prop):
    word2type = dict()
    cid = [_cid for cat in all_cat.values() for _cid in cat.cids]
    for propvalue in all_prop.values():
        pvname = propvalue.name
        propName = g.V().has('type', 5).has('pvname', pvname).in_('categoryPropValue').where(
            __.in_('categoryPropNameR').has('cid', P.within(cid))).in_(
            "categoryPropNameL").as_('pn').dedup('pn').pnname.toList()
        for keyword in propvalue.keywords:
            if word2type.get(keyword) is not None:
                word2type[keyword] |= set(propName)
            else:
                word2type[keyword] = set(propName)
    return word2type
