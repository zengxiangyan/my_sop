import json
import re
import jieba
import shutil
import requests
import platform
import argparse
import pandas as pd
from bert_base.client import BertClient
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __

import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from graph.kgraph import g_confirmedE, g_inV, g_outV
from models.analyze.trie import Trie
from models.nerl import *


def match_answer(string, result):
    """
    匹配ner结果

    :param string:
    :param result:
    :return:
    """
    try:
        word_type = None
        offset = -1
        word = ''
        all_words = dict()
        for i, _ in enumerate(string):
            if not word:
                if result[i][0] == 'B':
                    word_type = result[i][2:]
                    offset = i
                    word = string[i]
                elif result[i][0] == 'I':
                    continue
                    # raise ValueError(f'返回错误, {result}')
            else:
                if result[i][0] == 'B':
                    all_words[(word, offset)] = word_type
                    word_type = result[i][2:]
                    offset = i
                    word = string[i]
                elif result[i][0] == 'I':
                    try:
                        assert word_type == result[i][2:]
                        word += string[i]
                    except AssertionError:
                        # todo fix
                        word += string[i]
                elif result[i][0] == 'O':
                    all_words[(word, offset)] = word_type
                    word_type = None
                    offset = -1
                    word = ''
        if word and word_type:
            all_words[(word, offset)] = word_type
        return all_words
    except:
        return dict()


class NERLParser:
    g = app.get_graph('default')
    db = app.get_db('graph')
    bc = None
    entities = dict()  # Dict[gid, ~KGEntity], KGEntity and it's sub-class
    keywords = dict()  # Dict[token, Set[gid]]
    # relations = set()  # List[(cgid, bgid)]
    init_flag = False
    en_Trie = Trie()

    def __init__(self):
        utils.easy_call([self.g, self.db], 'connect')
        self.other_words = [w for (w,) in self.db.query_all('select word from kadis.hotword_otherword;')]

    def connect_bert_server(self, ip='10.21.200.128', port=5557, port_out=5558, timeout=2000):
        self.bc = BertClient(ip=ip, port=port, port_out=port_out, timeout=timeout,
                             show_server_config=False, check_version=False, check_length=False, mode='NER')

    @used_time
    def init_entity_relation(self, lv0cids):
        for lv0cid in lv0cids:
            entities, keywords = pd.read_pickle(KGLoader.file_path(lv0cid))

            self.entities.update(entities)
            for k in keywords:
                self.keywords[k] = self.keywords.get(k, set()) | keywords[k]
        print('got {} entities and {} keywords'.format(len(self.entities), len(self.keywords)))

    def load_dictionary(self):
        more_words = ['贵妇丹', '贵妇膏', '水光', '润颜水', '润颜乳', '美妆板', '玻尿酸', '水光肌', '精华露', '骆驼奶', '亮颜']
        for w in self.other_words + more_words:
            jieba.add_word(w, 1000)
        for word in self.keywords:
            if utils.is_chinese(word):
                jieba.add_word(word)
            if not utils.is_only_chinese(word):
                self.en_Trie.insert(word)

    @used_time
    def analyze_text(self, text, top=False):
        text = re.split('[\v\r\n\t]+', text)[0]
        data = TextData(text.lower(), self.en_Trie)

        ner_result = self.get_ner_result(text)
        # print('[NER]', ner_result[0])
        # 检查分词结果和ner结果是否有重合，
        # 重合的话无视ner结果，否则重新规划分词路径，即重做data.cut
        effect_ner_result = self.compare_ner_mentions(ner_result[0], data.mentions)
        # print('[EFF NER]', effect_ner_result)
        if len(effect_ner_result) > 0:
            data.mentions = data.condition_cut(text.lower(), effect_ner_result)
        # 重分后重刷
        effect_ner_result = self.compare_ner_mentions(ner_result[0], data.mentions)

        # token2candidates
        for mention in data.mentions:
            if self.keywords.get(mention.token) is not None:
                mention.candidates = self.keywords.get(mention.token)

        # 计算所有的candidate的entity的权值 return: Dict[gid, confidence]
        all_candidate = data.get_all_candidate()
        # gid2confidence = data.cal_confidences_by_graph()
        # if self.g.graph_type == 'hugegraph':
        gid2confidence = self.cal_confidence_by_graph_new(all_candidate)
        # data.print_mention(self.entities, effect_ner_result, gid2confidence)
        return data.return_mention(self.entities, effect_ner_result, gid2confidence, top=top)

    def get_ner_result(self, text):
        processed_text = encode_string(text)
        string_list = [list(processed_text)]
        try:
            rst = self.bc.encode(string_list, is_tokenized=True)
        except:
            return [{}]
        answers = [match_answer(string, result) for string, result in zip([list(text)], rst)]
        return answers

    def compare_ner_mentions(self, ner_result, mentions):
        effect_segment = set()
        for mention in mentions:
            if self.keywords.get(mention.token) is not None:
                # mention.candidates = self.keywords.get(mention.token)
                effect_segment |= set(list(range(mention.offset, mention.offset + len(mention.token))))

        effect_ner_result = {}
        for (ner, offset), label in ner_result.items():
            if set(list(range(offset, offset + len(ner)))) & effect_segment:
                continue
            else:
                effect_ner_result[(ner, offset)] = label
        return effect_ner_result

    def cal_confidence_by_graph(self, all_candidate):
        location_weight = generate_location_weight(len(all_candidate)) * 2
        way = {'category': 1, 'brand': -1}
        relation_score, appearance_score = dict(), dict()

        for i, mention in enumerate(all_candidate):
            for gid in mention:
                label = self.entities[gid].label
                weight = location_weight[::way[label]][i] if label in way else 1
                appearance_score[gid] = appearance_score.get(gid, 0) + weight

        for i in range(len(all_candidate) - 1):
            for j in range(i + 1, len(all_candidate)):
                # i组和j组 计算概率 同组不重复计算
                for _i in all_candidate[i]:
                    for _j in all_candidate[j]:
                        if _i != _j:
                            if relation_score.get(tuple(sorted([_i, _j]))):
                                continue

                            # v1: 1v1 循环
                            # rank = self.g.V(_i).repeat(
                            #     # __.both('categoryBrand', 'categoryPropNameR', 'categoryPropValue',
                            #     #         'categoryParent').simplePath().timeLimit(2000)).until(
                            #     __.both().simplePath().timeLimit(2000)).until(
                            #     __.hasId(_j).or_().loops().is_(P.gte(3))).path().limit(1).toList()

                            # v2: 1v1 提前弹出限制时间
                            rank = self.g.V(_i).as_('start').repeat(
                                __.timeLimit(1000).both()).until(__.loops().is_(1)).as_('a').choose(
                                __.not_(__.hasId(_j)),
                                __.repeat(
                                    __.timeLimit(1000).both()).until(__.loops().is_(1)).as_('b').choose(
                                    __.not_(__.hasId(_j)),
                                    __.repeat(__.timeLimit(1000).both()).until(__.loops().is_(1))),
                                __.identity().as_('b')).hasId(_j).limit(1).as_('end').select('a', 'b', 'end').by(
                                __.id()).toList()
                            # relation_score[tuple(sorted([_i, _j]))] = 1 / len(rank[0]) if rank else 0
                            if rank and rank[0]['end'] == _j:
                                res_rank = [rank[0]['a'], rank[0]['b'], rank[0]['end']]
                                relation_score[tuple(sorted([_i, _j]))] = 1 / (res_rank.index(_j) + 1)

        # relation_k2v = dict()
        # for i in range(len(all_candidate) - 1):
        #     for j in range(i + 1, len(all_candidate)):
        #         # i组和j组 计算概率 同组不重复计算
        #         for _i in all_candidate[i]:
        #             for _j in all_candidate[j]:
        #                 k, v = sorted([_i, _j])
        #                 if relation_k2v.get(k) is None:
        #                     relation_k2v[k] = set()
        #                 relation_k2v[k].add(v)
        #
        # for k, vs in relation_k2v.items():
        #     query = self.g.V(14012536).as_('start').repeat(__.timeLimit(2000).both()).until(__.loops().is_(1)).as_('a').sideEffect(
        #         __.hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880).aggregate('r1')).choose(
        #         __.hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880),
        #         __.identity().as_('b'),
        #         __.repeat(__.timeLimit(2000).both()).until(__.loops().is_(1)).as_('b').sideEffect(
        #             __.hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880).aggregate('r2')).choose(
        #             __.hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880),
        #             __.identity(),
        #             __.barrier().choose(__.union(__.select('r1'), __.select( 'r2')).dedup().count().is_(P.eq(6)),
        #                                 __.identity(),
        #                                 __.repeat(__.timeLimit(2000).both()).until(
        #                                     __.loops().is_(1)).hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880).dedup().limit(
        #                                     __.union(__.select('r1'),
        #                                              __.select( 'r2')).dedup().count().map(6-T.it.get()).toList()))).
        #     hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880).dedup().limit(
        #         __.select('r1').dedup().count().map(6-T.it.get()).toList())
        #     ).hasId(288854240, 295903304, 290861088, 576618544, 276328680, 301047880).dedup().limit(6).as_('end').select('a','b','end')

        gid2confidence = dict()

        for (_i, _j), r in relation_score.items():
            gid2confidence[_i] = gid2confidence.get(_i, 0) + r * appearance_score[_i]
            gid2confidence[_j] = gid2confidence.get(_j, 0) + r * appearance_score[_j]
        return {g: round(c, 3) for g, c in gid2confidence.items()}

    def cal_confidence_by_graph_new(self, all_candidate):
        print(all_candidate)
        location_weight = generate_location_weight(len(all_candidate)) * 2
        way = {'category': 1, 'brand': -1}
        relation_score, appearance_score = dict(), dict()

        for i, mention in enumerate(all_candidate):
            for gid in mention:
                label = self.entities[gid].label
                weight = location_weight[::way[label]][i] if label in way else 1
                appearance_score[gid] = appearance_score.get(gid, 0) + weight

        for i in range(len(all_candidate) - 1):
            for j in range(i + 1, len(all_candidate)):
                paths = self.multinodeshortestpath_api(list(all_candidate[i]), list(all_candidate[j]))
                for _i in all_candidate[i]:
                    for _j in all_candidate[j]:
                        if _i == _j:
                            continue
                        if paths:
                            rank = paths[tuple(sorted([_i, _j]))]
                        else:
                            rank = self.shortestpath_api(_i, _j)
                        relation_score[tuple(sorted([_i, _j]))] = 1 / (rank + 1)

        gid2confidence = dict()
        for (_i, _j), r in relation_score.items():
            gid2confidence[_i] = gid2confidence.get(_i, 0) + r * appearance_score[_i]
            gid2confidence[_j] = gid2confidence.get(_j, 0) + r * appearance_score[_j]
        return {g: round(c, 3) for g, c in gid2confidence.items()}

    def shortestpath_api(self, start_id, end_id):
        server = 'http://119.3.132.13:8083'
        graph_name = 'huge'
        headers = {'content-type': 'application/json'}
        url = '{server}/graphs/{graph}/traversers/shortestpath?source="{start}"&target="{end}"&max_depth=10'.format(
            server=server, graph=graph_name, start=start_id, end=end_id)
        r = requests.get(url, headers=headers)

        return len(r.json()['paths']) if r.json().get('paths') else 10

    def multinodeshortestpath_api(self, start_id, end_id):
        # print(start_id, end_id)
        server = 'http://119.3.132.13:8083'
        graph_name = 'huge'
        headers = {'content-type': 'application/json'}
        url = '{server}/graphs/{graph}/traversers/multinodeshortestpath'.format(server=server, graph=graph_name)
        d = {
            "vertices": {
                "ids": start_id + end_id
                # "ids": ["3:26L", "6:39XN"]
            },
            "step": {
                "direction": "BOTH",
                "properties": {
                }
            },
            "max_depth": 6,
            "capacity": 100000000,
            "with_vertex": False
        }
        r = requests.post(url, json.dumps(d), headers=headers)

        if r.json().get('paths'):
            paths = {tuple(sorted([path['objects'][0], path['objects'][-1]])): len(path['objects'])
                     for path in r.json()['paths']}
            paths = {tuple(sorted([s, e])): paths.get(tuple(sorted([s, e])), 10)
                     for s in start_id for e in end_id if s != e}
            return paths
        else:
            return dict()

class KGLoader:
    g = app.get_graph('default')
    limit_category_layer = 2  # 不限制为0, 限制lv0cid的root层为1, 以此类推
    entities = dict()  # Dict[gid, ~KGEntity], KGEntity and it's sub-class
    keywords = dict()  # Dict[token, Set[gid]]

    # relations = set()  # List[(cgid, bgid)] 由查询度代替

    def __init__(self, lv0cid):
        self.g.connect()
        self.lv0cid = lv0cid
        cid = lv0cid2cid(lv0cid)
        # 叶节点cid, 去重
        self.layer_index = [1]
        self.all_cid = [cid] + self.layer_check([cid])
        self.limit_cid = self.all_cid[self.layer_index[self.limit_category_layer]:]
        self.all_bid = []
        self.file_path = self.file_path(lv0cid)

    @classmethod
    def file_path(cls, lv0cid):
        return join(app.output_path('nerl'), 'entity_relation_{}_new_2.pkl'.format(lv0cid))

    def layer_check(self, c_list):
        t = g_outV(g_confirmedE(self.g.V().has('cid', P.within(c_list)).inE('categoryParent'))).cid.dedup().toList()
        if t:
            self.layer_index.append(self.layer_index[-1] + len(t))
            return t + self.layer_check(t)
        else:
            return []

    def preload_entity(self):
        # 叶节点cid所连bid, 去重
        self.all_bid = g_inV(g_confirmedE(
            self.g.V().has('cid', P.within(self.all_cid)).outE('categoryBrand'))).bid.dedup().toList()
        # 叶节点cid所连cpnid, 去重
        all_cpnid = g_inV(g_confirmedE(
            self.g.V().has('cid', P.within(self.all_cid)).outE('categoryPropNameR'))).has(
            'cpnname', P.without('类别', '品名', '系列', '高级选项', '件数', '型号', '分类')).cpnid.dedup().toList()
        check_trie = Trie()
        for w in ['单品', '品类', '分类', '规格', '品种', '单位', '大小', '尺寸']:
            check_trie.insert(w)

        @used_time
        def query_brand():
            # brand查询
            query = self.g.V().has('bid', P.within(self.all_bid)).as_('brand')
            query = query.optional(g_inV(g_confirmedE(__.outE('brandKeyword')).where(__.values('mode').is_(0)))).as_(
                'keyword')  # brand后的keyword nodes
            brand_keyword_paths = query.dedup('brand', 'keyword').path().by(__.valueMap(True)).toList()
            return brand_keyword_paths

        @used_time
        def query_category():
            # category查询
            query = self.g.V().has('cid', P.within(self.limit_cid)).as_('category')
            query = query.optional(g_inV(g_confirmedE(__.outE('categoryKeyword')))).as_('keyword')
            category_keyword_paths = query.dedup('category', 'keyword').path().by(__.valueMap(True)).toList()
            return category_keyword_paths

        @used_time
        def query_categoryPropName():
            # categoryPropName查询
            query = self.g.V().has('cpnid', P.within(all_cpnid)).as_('cpn')
            query = g_inV(g_confirmedE(query.outE('categoryPropValue'))).as_('pv')
            query = query.optional(g_inV(g_confirmedE(__.outE('propValueKeyword')))).as_('keyword')
            cpn_pv_keyword_paths = query.dedup('cpn', 'pv', 'keyword').path().by(__.valueMap(True)).toList()
            return cpn_pv_keyword_paths

        @used_time
        def query_propname():
            # PropName查询
            query = self.g.V().has('cpnid', P.within(all_cpnid)).as_('cpn')
            query = g_outV(g_confirmedE(query.inE('categoryPropNameL'))).as_('pn')
            cpnid2pn = query.select('cpn', 'pn').by('cpnid').by('pnname').toList()
            return {relation['cpn']: relation['pn'] for relation in cpnid2pn}

        @used_time
        def query_sinking_category():
            # sinking_category查询
            query = self.g.V().has('cid', P.within(self.all_cid[self.layer_index[self.limit_category_layer]:])).as_(
                'category')
            query = g_inV(g_confirmedE(query.outE('categoryPropNameR'))).has('cpnname', '类别').as_('cpn')
            query = g_inV(g_confirmedE(query.outE('categoryPropValue'))).as_('pv')
            more_category_keyword_paths = query.dedup('category', 'pv').path().by(__.valueMap(True)).toList()
            return more_category_keyword_paths

        cpnid2pn = query_propname()
        for path in query_brand() + query_category() + query_categoryPropName() + query_sinking_category():
            # printpath(path)
            entity, propname = None, None
            for n in path:
                # print(n)
                gid = n['id']
                if isinstance(gid, dict):  # edge
                    continue
                label = n['label']
                if label == KG_label_brand:
                    if self.entities.get(gid) is None:
                        self.entities[gid] = BrandEntity(gid, n['bid'][0], n['bname'][0])
                        self.add_token2gid([n['name_cn'][0], n['name_en'][0]], gid)
                    entity = self.entities[gid]
                elif label == KG_label_category:
                    if self.entities.get(gid) is None:
                        self.entities[gid] = CategoryEntity(gid, n['cid'][0], n['cname'][0])
                        self.add_token2gid(n['cname'][0], gid)
                    entity = self.entities[gid]
                elif label == KG_label_category_property:
                    cpnid = n['cpnid'][0]
                    propname = cpnid2pn.get(cpnid, n['cpnname'][0])  # 能查询到通用属性则由通用属性代替，否则为品类属性
                    if check_trie.search_entity(propname):
                        break
                elif label == KG_label_property_val:
                    if propname != '类别':
                        if self.entities.get(gid) is None:
                            if propname is None:
                                raise ValueError('No propname for init PropEntity')
                            self.entities[gid] = PropEntity(gid, n['pvid'][0], n['pvname'][0], propname)
                            self.add_token2gid(n['pvname'][0], gid)
                        entity = self.entities[gid]
                    else:
                        self.add_token2gid(n['pvname'][0], entity.gid)
                elif label == KG_label_keyword:
                    if entity:
                        self.add_token2gid(n['name'][0], entity.gid)

    def add_token2gid(self, tokens, gid):
        if isinstance(tokens, str):
            tokens = [tokens]

        for token in tokens:
            if len(token) > 0:
                if self.keywords.get(token) is None:
                    self.keywords[token] = set()
                self.keywords[token].add(gid)

    def save_entity(self):
        if platform.system() == 'Linux':
            tmp_path = '/tmp/entity_relation_{}_v2.pkl'.format(self.lv0cid)
            pd.to_pickle((self.entities, self.keywords), tmp_path)
            shutil.copy(tmp_path, self.file_path)
        else:
            pd.to_pickle((self.entities, self.keywords), self.file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-lv0cids', nargs='+', type=int, default=[2011010118], help="lv0cid")
    parser.add_argument('-port', type=int, default=5557, help="port for service")
    parser.add_argument('-port_out', type=int, default=5558, help="port_out for service")

    # switch
    parser.add_argument('-nerl', action='store_true', help='NERL test')
    parser.add_argument('-conf', action='store_true', help='download conf file')

    args = parser.parse_args()

    if args.conf:
        for lv0 in args.lv0cids:
            loader = KGLoader(lv0)
            loader.preload_entity()
            loader.save_entity()

    if args.nerl:
        NERL = NERLParser()
        if platform.platform()[:5] == 'Linux':
            NERL.connect_bert_server(ip='203.156.218.106', port=args.port, port_out=args.port_out)
        else:
            NERL.connect_bert_server(ip='10.21.200.128', port=args.port, port_out=args.port,)
        NERL.init_entity_relation(args.lv0cids)  # 加载entity和relation
        NERL.load_dictionary()  # 加载词典
        # print(NERL.other_words)

        Text = input('input a text:')
        while Text != '':
            result = NERL.analyze_text(Text, top=True)
            print('\n', json.dumps(result, ensure_ascii=False, indent=4))
            Text = input('input a text:')

"""
|  lv0cid  |port |port_out|
|-------------------------|
|2011010113|15555|15556   |
|2011010118|15557|15558   |

bert-base-ner-train -data_dir ./DataCleaner/src/output/nerl/91081/dataset -output_dir ./DataCleaner/src/output/nerl/91081/ckpt_model -init_checkpoint ./bert/chinese_L-12_H-768_A-12/bert_model.ckpt -bert_config_file ./bert/chinese_L-12_H-768_A-12/bert_config.json -vocab_file ./bert/chinese_L-12_H-768_A-12/vocab.txt -batch_size 16 -device_map 1
bert-base-serving-start -model_dir ./DataCleaner/src/output/nerl/91081/ckpt_model -bert_model_dir ./bert/chinese_L-12_H-768_A-12 -model_pb_dir ./DataCleaner/src/output/nerl/91081/pb_model -mode NER -num_worker 3 -device_map 1

"""