import random
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from bert_base.client import BertClient
from gremlin_python.process.graph_traversal import __

import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app
from extensions import utils
from graph.nerl_beta import BrandEntity, CategoryEntity, PropEntity, KG_label_category
from models.analyze.trie import Trie
from models.analyze.analyze_tool import used_time
from graph.kgraph import g_confirmed, g_confirmedE, g_inV

space_replace_symbol = "_"


def encode_string(s):
    return s.replace(' ', space_replace_symbol).lower()


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


def connect_bert_server(ip='10.21.200.128', port=5555, port_out=5556, timeout=-1):
    return BertClient(ip=ip, port=port, port_out=port_out, timeout=timeout,
                      show_server_config=False, check_version=False, check_length=False, mode='NER')


def get_ner_result(bc, text_list):
    string_list = [list(encode_string(name)) for name in text_list]
    try:
        rst = bc.encode(string_list, is_tokenized=True)
    except:
        return [{} for _ in range(len(text_list))]
    answers = [match_answer(string, result) for string, result in zip([list(text) for text in text_list], rst)]
    return answers


class FakeDataGenerator:
    project_name = 'nerl'
    g = app.get_graph('default')
    db = app.get_db('graph')
    db_sop = app.get_clickhouse('chsop')
    entities = dict()  # Dict[gid, ~KGEntity], KGEntity and it's sub-class
    keywords = dict()  # Dict[token, Set[gid]]

    def __init__(self, eid, lv0cid=None):
        self.eid = eid
        utils.easy_call([self.g, self.db, self.db_sop], 'connect')

        self.lv0cids = self.__get_lv0cid_by_eid if lv0cid is None else lv0cid
        self.__get_all_info()

        self.base_dir = Path(app.output_path(self.project_name))
        self.__init_file()

    def __init_file(self):
        app.mkdirs(self.base_dir / str(self.eid) / 'dataset')
        app.mkdirs(self.base_dir / str(self.eid) / 'ckpt_model')
        app.mkdirs(self.base_dir / str(self.eid) / 'pb_model')

    @property
    def __get_lv0cid_by_eid(self):
        sql = 'select distinct lv0cid from kadis.etbl_map_config where eid={};'.format(self.eid)
        return list(map(lambda i: i[0], self.db.query_all(sql)))

    def __get_all_info(self):
        for lv0cid in self.lv0cids:
            file_path = join(app.output_path('nerl', nas=True), 'entity_relation_{}.pkl'.format(lv0cid))
            if exists(file_path):
                entities, keywords, _ = pd.read_pickle(file_path)
                self.entities.update(entities)
                for k in keywords:
                    self.keywords[k] = self.keywords.get(k, set()) | keywords[k]
            else:
                raise OSError('No file <{}> for init entity and relation.'.format(file_path))
        print('got {} entities and {} keywords'.format(len(self.entities), len(self.keywords)))

    @staticmethod
    def __create_data_file(data, filepath):
        """
        创建保存data的临时csv文件

        :param data:
        :param filepath:
        :return:
        """
        f = open(filepath, 'w', encoding='utf-8')
        for item, res in data:
            pr = 0
            for index, word in enumerate(list(item)):
                if pr < len(res) and index >= res[pr][2]:
                    pr = pr + 1
                if pr < len(res):
                    if index == res[pr][1]:
                        f.write(word + f' B-{res[pr][3]}\n')
                    elif res[pr][1] < index <= res[pr][2]:
                        f.write(word + f' I-{res[pr][3]}\n')
                    elif word == ' ':  # !!!字符与标注以空格为间隔，空格不能作为字符，产生干扰
                        f.write(space_replace_symbol + ' O\n')
                    else:
                        f.write(word + ' O\n')
                else:
                    if word == ' ':
                        f.write(space_replace_symbol + ' O\n')
                    else:
                        f.write(word + ' O\n')
            f.write('\n')
        f.close()

    def __search_from_graph(self, type, id):
        type2id = {'category': 'cid', 'brand': 'alias_bid0', 'propValue': 'pvid'}
        type2name = {'category': 'cname', 'brand': 'bname', 'propValue': 'pvname'}
        # P.within(ids)
        query = g_confirmed(self.g.V().has(type2id[type], id)).as_('name')  # 寻找nodes
        query = query.optional(g_inV(g_confirmedE(__.outE(type + 'Keyword')).has('mode', 0))).as_('keyword')

        gid = 0
        name = ''
        keywords = set()

        for all_path in query.dedup('name', 'keyword').path().by(__.valueMap(True)).toList():
            for path in all_path:
                if path['label'] == type:
                    _name = path[type2name[type]][0]
                    keywords.add(_name.lower())
                    if type == 'brand':
                        keywords |= {path['name_en'][0].lower(), path['name_cn'][0].lower()}
                    # 最长为名字
                    if '/' not in name:
                        if '/' in _name or len(_name) > len(name):
                            name = _name
                            gid = path['id']
                elif path['label'] == 'keyword':
                    keywords.add(path['name'][0].lower())
        if '' in keywords:
            keywords.remove('')
        return gid, name, keywords

    def generate_data(self):
        output = []
        word_trie = Trie()
        for token, gids in self.keywords.items():
            for gid in gids:
                label = self.entities[gid].label
                if label == KG_label_category:
                    word_trie.insert(token, label)

        data_sql = """
            select distinct name, alias_all_bid from sop.entity_prod_{eid}_A 
            where alias_all_bid != 0 
            order by alias_all_bid limit 100 by alias_all_bid;
        """
        _alias_all_bid, brand_trie = 0, None
        data = self.db_sop.query_all(data_sql.format(eid=self.eid))
        for (name, alias_all_bid) in tqdm(data):
            if _alias_all_bid != alias_all_bid:
                gid, brand_name, keywords = self.__search_from_graph('brand', alias_all_bid)
                _alias_all_bid = alias_all_bid
                brand_trie = Trie()
                for word in keywords:
                    brand_trie.insert(encode_string(word.lower()), 'brand')

            processed_name = encode_string(name.lower())
            category_res = word_trie.search_entity(processed_name)
            brand_res = brand_trie.search_entity(processed_name)
            if category_res and brand_res:
                # print(processed_name, category_res + brand_res)
                output.append((processed_name, sorted(category_res + brand_res, key=lambda r: r[1])))

        # pd.to_pickle(output, self.base_dir / str(self.eid) / 'dataset' / 'output.pkl')
        random.shuffle(output)
        train_items = output[:int(0.6 * len(output))]
        dev_items = output[int(0.6 * len(output)):int(0.8 * len(output))]
        test_items = output[int(0.8 * len(output)):int(1.0 * len(output))]

        self.__create_data_file(train_items, self.base_dir / str(self.eid) / 'dataset' / 'train.txt')
        self.__create_data_file(dev_items, self.base_dir / str(self.eid) / 'dataset' / 'dev.txt')
        self.__create_data_file(test_items, self.base_dir / str(self.eid) / 'dataset' / 'test.txt')


@used_time
def demo_ner():
    bc = connect_bert_server()
    data = ['舒肤佳｜舒肤佳祛痘白茶洁面沐浴露540ml*2 水杨酸 后背',
            '满婷（MANTING）清满海盐沐浴露550ml',
            '彩姿莱净螨沐浴露，除螨净痘，嫩肤修护，超大瓶/500ml，全国包邮']
    rst = get_ner_result(bc, data)
    for name, answer in zip(data, rst):
        print(name, answer)

    Text = input('input a text:')
    while Text != '':
        processed_text = encode_string(Text)
        string_list = [list(processed_text)]
        rst = bc.encode(string_list, is_tokenized=True)
        # print(result)
        answers = [match_answer(string, result) for string, result in zip([list(Text)], rst)]
        print(answers)
        Text = input('input a text:')


@used_time
def demo_generate_fake_data():
    generator = FakeDataGenerator(91073, [2011010113])
    generator.generate_data()


if __name__ == '__main__':
    demo_ner()
    # demo_generate_fake_data()
