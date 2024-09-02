import time
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))
import application as app
from nlp.pnlp import brand_special_str_pattern
import models.analyze.analyze_tool as tool


def process_propName(pn):
    pn = {'科技': '运动鞋科技'}.get(pn, pn)
    return brand_special_str_pattern.sub('', pn)


class Node:
    def __init__(self, tid, data, type):
        self.tid = tid
        self.data = data
        self.type = type
        self.children = []


class CompleteParser:
    debug = False

    g = app.get_graph('default')
    db_graph = app.get_db('graph')
    db_clickhouse = app.get_clickhouse('chmaster')

    remove_pn = {'子品类', '款号', '品牌', 'IP联名', 'IP合作类型', '明星同款', '是否套包', '一级类目', '上市时间', '上架时间',
                 '辅助-洗是否商场同款', '是否无效链接', '销售渠道', '颜色'}

    @tool.used_time
    def __init__(self, eid: int, cid: int):
        self.eid = eid
        self.cid = cid

        self.g.connect()
        self.db_graph.connect()
        self.db_clickhouse.connect()
        self.db_clickhouse.debug = self.debug
        self.graph_tree = self.init_graph_tree(self.cid)

    def graph_check(self, label, value, relations, way='in'):
        from graph.kgraph import g_confirmedE, g_inV, g_outV

        assert isinstance(relations, list), 'Wrong type for relations, please input a list.....'
        res = []
        if way == 'in':
            data = g_outV(g_confirmedE(self.g.V().has(label, value).inE(*relations))).valueMap(True).toList()
        else:
            data = g_inV(g_confirmedE(self.g.V().has(label, value).outE(*relations))).valueMap(True).toList()

        tid = {'category': 'cid', 'brand': 'bid', 'categoryPropName': 'cpnid', 'propValue': 'pvid'}
        tname = {'category': 'cname', 'brand': 'bname', 'categoryPropName': 'cpnname', 'propValue': 'pvname'}
        for d in data:
            res.append(Node(d[tid[d['label']]][0], d[tname[d['label']]][0], d['label']))
        return res

    def init_graph_tree(self, cid):
        graph_tree = self.graph_check('cid', cid, ['categoryParent'])
        for category in graph_tree:
            category.children = self.graph_check('cid', category.tid, ['categoryBrand', 'categoryPropNameR'], 'out')
            for brand_propName in category.children:
                if brand_propName.type == 'categoryPropName':
                    brand_propName.children = self.graph_check('cpnid', brand_propName.tid, ['categoryPropValue'],
                                                               'out')
        if self.debug:
            self.print_tree(graph_tree)
        return graph_tree

    @staticmethod
    def check_on_node(nodes, tid=None, name=None, type=None) -> Node or int:
        if tid is None and name is None and type is None:
            raise ValueError('Wrong input....')

        for node in nodes:
            if (tid is None or node.tid == tid) and (name is None or node.data == name) and (
                    type is None or node.type == type):
                return node
        return -1

    @tool.used_time
    def check_all(self, modify=False):
        cc_sub = list(filter(lambda d: d[1] != 'Others',
                             self.db_clickhouse.query_all(f"select cid, name from artificial.category_{self.eid};")))
        # self.print_tree(self.graph_tree)

        for clean_cid, subcategory in cc_sub:
            # check subcategory
            category_node = self.check_on_node(self.graph_tree, name=subcategory, type='category')

            status = 'exists'
            if category_node == -1 and modify:
                self.insert_category(subcategory)
                status = 'insert'
            self.log_print(clean_cid, category_node.tid, category_node.data, category_node.type, status=status)

            # check brand
            alias_bid0_ETable = list(map(lambda x: str(x[0]), self.db_clickhouse.query_all(
                f'select distinct alias_all_bid from sop_e.entity_prod_{self.eid}_E where clean_cid={clean_cid} and alias_all_bid!=0;')))
            bid_ETable = list(map(lambda x: x[0], self.db_graph.query_all(
                f"select distinct bid from brand where alias_bid0 in ({','.join(alias_bid0_ETable)}) and deleteFlag=0 and confirmType!=2;")))

            insert_count = 0
            for bid in bid_ETable:
                brand_node = self.check_on_node(category_node.children, tid=bid, type='brand')
                if brand_node == -1 and modify:
                    self.insert_brand(category_node.tid, bid)
                    insert_count += 1
            self.db_graph.commit()

            status = 'insert' if insert_count else 'exists'
            self.log_print(f'insert {insert_count} lines to categorybrand',
                           status=status)

            # check prop
            prop_sql = f"select distinct cpn, pv from sop_e.entity_prod_{self.eid}_E " \
                       f"\narray join `clean_props.name` as cpn, `clean_props.value` as pv" \
                       f"\nwhere clean_cid={clean_cid} and pv not in ('', '其他', '其它', '不适用');"
            pn2pv_ETable = {}
            for pn, pv in self.db_clickhouse.query_all(prop_sql):
                pn = process_propName(pn)
                if not pn2pv_ETable.get(pn):
                    pn2pv_ETable[pn] = set()
                pn2pv_ETable[pn].add(pv)

            for pn, pvs in pn2pv_ETable.items():
                if pn in self.remove_pn:
                    continue
                propName_node = self.check_on_node(category_node.children, name=pn, type='categoryPropName')

                if propName_node == -1 and modify:
                    cpnid = self.insert_categoryPropName(category_node.tid, pn)
                    self.log_print(f'insert {pn} categoryPropName', status='insert')
                    propName_node = Node(cpnid, pn, 'categoryPropName')

                for pv in pvs:
                    propValue_node = self.check_on_node(propName_node.children, name=pv, type='propValue')
                    if propValue_node == -1 and modify:
                        self.insert_propValue(propName_node.tid, pv)
                        self.log_print(f'insert {pv} propValue to {pn}', status='insert')
                self.db_graph.commit()

            # complete categorypropname l/r
            self.insert_complete_categoryPropNameLR()
            print('=' * 100)

    def insert_category(self, subcategory):
        # 或添加self.graph_tree添加节点
        raise NotImplementedError('按照上层cid, 添加下层子品类, 以及下面的所有节点, 未完成')

    def insert_brand(self, cid, bid):
        insert_cb_sql = f"INSERT INTO categorybrand (bid, cid, createTime, source)" \
                        f"\nSELECT {bid}, {cid}, {time.time()}, 6 FROM dual " \
                        f"\nWHERE NOT EXISTS (SELECT * FROM categorybrand WHERE cid={cid} and bid={bid});"
        self.db_graph.execute(insert_cb_sql)

    def insert_categoryPropName(self, cid, propName):
        # 1. propName
        insert_pn_sql = f"INSERT INTO propname (name, createTime, source)" \
                        f"\nSELECT '{propName}', {time.time()}, 6 FROM dual " \
                        f"\nWHERE NOT EXISTS (SELECT * FROM propname WHERE name='{propName}');"
        self.db_graph.execute(insert_pn_sql)
        self.db_graph.commit()

        # 2. categoryPropName
        pnid = self.db_graph.query_all(
            f"select pnid from propname where name='{propName}' and deleteFlag=0 and confirmType!=2 limit 1;")[0][0]
        insert_cpn_sql = f"INSERT INTO categorypropname (cid, pnid, name, createTime, source)" \
                         f"\nSELECT {cid}, {pnid}, '{propName}', {time.time()}, 6 FROM dual " \
                         f"\nWHERE NOT EXISTS (SELECT * FROM categorypropname WHERE pnid={pnid} and cid={cid});"
        self.db_graph.execute(insert_cpn_sql)
        self.db_graph.commit()

        return self.db_graph.query_all(f"select id from categorypropname where pnid={pnid} and cid={cid} limit 1")[0][0]

    def insert_complete_categoryPropNameLR(self):
        # 3. categoryPropNameR/categoryPropNameL
        insert_cpnl_sql = "insert into categorypropnamel select * from categorypropname where id > (select max(id) from categorypropnamel);"
        insert_cpnr_sql = "insert into categorypropnamer select * from categorypropname where id > (select max(id) from categorypropnamer);"
        self.db_graph.execute(insert_cpnl_sql)
        self.db_graph.execute(insert_cpnr_sql)
        self.db_graph.commit()

    def insert_propValue(self, cpnid, propValue):
        insert_pv_sql = f"INSERT INTO propvalue (name, createTime, source)" \
                        f"\nSELECT '{propValue}', {time.time()}, 6 FROM dual" \
                        f"\nWHERE NOT EXISTS (SELECT * FROM propvalue WHERE name='{propValue}');"
        self.db_graph.execute(insert_pv_sql)
        self.db_graph.commit()

        pvid = self.db_graph.query_all(f"select pvid from propvalue where name='{propValue}' limit 1")[0][0]
        insert_cpv_sql = f"INSERT INTO categorypropvalue (cpnid, pvid, createTime, source)" \
                         f"\nSELECT {cpnid}, {pvid}, {time.time()}, 6 FROM dual " \
                         f"\nWHERE NOT EXISTS (SELECT * FROM categorypropvalue WHERE cpnid={cpnid} and pvid={pvid});"
        self.db_graph.execute(insert_cpv_sql)

    def log_print(self, *args, status='EXISTS'):
        if self.debug:
            print(f'[{status.upper()}]\t', *args)

    def print_tree(self, heads, level=0):
        # todo modify
        for i, head in enumerate(heads):
            if level == 0:
                symbol = '┌'
            else:
                if i == 0 and level == 1:
                    symbol = '|' * (level - 1) + '├┬'
                elif i == len(heads) - 1:
                    symbol = '|' * level + '└'
                else:
                    symbol = '|' * level + '├'
                if head.children:
                    symbol += '┬'
                else:
                    symbol += '─'
            print(symbol, head.tid, head.data, head.type)
            if head.children:
                self.print_tree(head.children, level + 1)

    def __del__(self):
        self.g.close()
        self.db_graph.close()
        self.db_clickhouse.close()


if __name__ == '__main__':
    print(time.time())
    completer = CompleteParser(eid=91194, cid=2537)
    completer.debug = True
    completer.check_all(modify=True)
    # completer.insert_category('123')
    del completer
