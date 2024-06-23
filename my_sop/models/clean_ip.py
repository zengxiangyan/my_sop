import sys
from os.path import abspath, join, dirname, exists
import logging
import re
import jieba
import time

jieba.setLogLevel(logging.INFO)

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
from models.analyze.trie import Trie
from models.analyze.word import Word
import application as app
from extensions import utils
from graph.kgraph import g_confirmed, g_confirmedE, g_inV
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from colorama import Fore
from graph.vetex import vetex_type_ref

def used_time(func):
    def wrapper(*args, **kwargs):
        print('#' * 10 + f'task <{func.__name__.upper()}>' + '#' * 10)
        start_time = time.time()
        f = func(*args, **kwargs)
        end_time = time.time()
        print(f'task <{func.__name__.upper()}> done used:{end_time - start_time}s')
        return f

    return wrapper


class CleanIP:
    g = app.get_graph('default')
    graph_db = app.get_db('graph')
    db_clickhouse = app.get_clickhouse('chsop')
    flag = True

    en_pattern = '(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'
    ip_sql = "select distinct i.ip_id as ip_id, i.name as ip_name, i.`type` as type, k.name as keyword " \
             "\nfrom (ip i left join ipkeyword ik on i.ip_id = ik.ip_id) " \
             "\nleft join keyword k on ik.kid = k.kid" \
             "\nwhere i.confirmType!=2 and ik.confirmType!=2" \
             "\norder by type;"
    ipn_sql = 'select * from ipn;'
    type_name = {0: '文创', 1: '品牌跨界', 2: '明星', 3: '专家'}
    add_word = ['哈果超人', '悟空姐姐', '悟空粉丝', "大白鞋", '超人气', '超人気', '米卡其', '小丑鞋',
                '男女同款', '专柜同款', '商场同款', 'ins同款', '明星同款', '走秀同款', '抖音同款', '情侣同款', '网红同款',
                '热卖推荐', '掌柜推荐', '达能小王子', '中华老字号', '布朗尼', '亲爱的泰迪', '品冠园', '麦蒂什克', 'qq表情',
                '黑雷神', '粉雷神', '白雷神', '黑色雷神', '卡马龙', '解放桥', '贝塔嗞', '牛奶和爱丽丝', '比逗仕小飞侠', '媛媛大富翁',
                '布朗巴顿', '布朗风味', '布朗味', '米奇妈家', '米奇宝贝', '尤朵拉', '布朗巧克力', '考拉超人', '蜜蜂超人', '萌超人',
                '汤姆叔叔', '汤姆大叔', '麦小丑', '米奇妈', '绿巨人生物', '上海专供', '上海保供', '仅上海区域发货', '限上海区域发货']

    def __init__(self, eid, flag=False):
        self.flag = flag
        self.graph_db.connect()
        if self.flag:
            self.g.connect()
            self.db_clickhouse.connect()
        else:
            self.db_clickhouse.connect()
        data = self.graph_db.query_all(self.ip_sql)

        self.eid = eid
        self.en_trie = Trie()
        self.id2keyword = dict()
        self.word2id = dict()
        self.bid2keyword = dict()
        self.init_brand_keyword()
        for ip_id, ip, type, keyword in data:
            if self.id2keyword.get(ip_id) is None:
                self.id2keyword[ip_id] = Word(ip, f'PRV-{self.type_name[type]}', keyword, subject_id=ip_id)
                self.word2id[ip] = ip_id
            else:
                self.id2keyword[ip_id].add_alias(keyword)
            self.word2id[keyword] = ip_id
        del data
        self.graph_db.close()

        # jieba
        for word in self.word2id:
            if utils.is_chinese(word):
                jieba.add_word(word.lower())
            if not utils.is_only_chinese(word):
                self.en_trie.insert(word)
        for word in self.add_word:
            jieba.add_word(word.lower())
        jieba.add_word('可可乐')
        jieba.add_word('态极')
        jieba.add_word('韦德之道')
        jieba.add_word('韦德悟道')

    # @used_time
    def get_ip(self, name: str, trade_props: dict, alias_all_bid: int) -> dict:
        # return {'明星同款': [], 'IP联名': [], 'IP合作类型': []}
        # props = ','.join(trade_props.values())
        # name += ',' + props
        # brand_keywords = self.get_brand_keyword(alias_all_bid) if alias_all_bid not in [0, 26] else set()
        brand_keywords = self.bid2keyword.get(alias_all_bid, set())
        # print(brand_keywords)
        name = name.lower()
        for k in brand_keywords:
            jieba.add_word(k)
            name = name.replace(k, '')

        segment = jieba.lcut(name)

        ip_ids = set()
        en_string = ''
        space_flag = False

        # name中文检查
        for word in segment:
            if utils.is_only_chinese(word):
                if self.word2id.get(word):
                    ip_ids.add(self.word2id[word])
                space_flag = True
            else:
                if space_flag and en_string:
                    en_string += ' '
                en_string += word.lower()
                space_flag = False

        # print(en_string)
        # name英文检查
        for match, start, end, _ in self.en_trie.search_entity(en_string):
            if re.search(self.en_pattern.format(match), en_string):
                ip_ids.add(self.word2id[match])
        # print(ip_ids)

        process_result = self.__process_2(name, ip_ids, brand_keywords)
        # print(process_result)
        return self.__output_result(process_result)

    def get_brand_keyword(self, alias_all_bid):
        if not self.bid2keyword.get(alias_all_bid):
            keywords = set()
            if self.flag:
                query = g_confirmed(self.g.V().has('alias_bid0', alias_all_bid)).as_('brand')
                query = query.optional(g_inV(g_confirmedE(__.outE('brandKeyword')))).as_('keyword')
                for all_path in query.dedup('brand', 'keyword').path().by(__.valueMap(True)).toList():
                    for index, path in enumerate(all_path):
                        if isinstance(path['type'], int):
                            continue
                        label = vetex_type_ref[path['type'][0]]

                        if label == 'brand':
                            keywords |= {path['bname'][0].lower(), path['name_en'][0].lower(),
                                         path['name_cn'][0].lower()}
                        elif label == 'keyword':
                            keywords.add(path['name'][0].lower())
                if '' in keywords:
                    keywords.remove('')
                self.bid2keyword[alias_all_bid] = keywords
            else:
                sql = f"select name, name_cn, name_en, name_cn_front, name_en_front from artificial.all_brand " \
                      f"where bid = {alias_all_bid} limit 1;"
                keywords = set(word for word in self.db_clickhouse.query_all(sql)[0] if len(word) > 0)
                self.bid2keyword[alias_all_bid] = keywords

        return self.bid2keyword[alias_all_bid]

    @used_time
    def init_brand_keyword(self):
        all_alias_all_bid = [bid[0] for bid in
                             self.db_clickhouse.query_all(f'select distinct(alias_all_bid) from sop.entity_prod_{self.eid}_A;')
                             if bid[0] not in [0, 26]]
        if self.flag:
            all_gid = set()
            for batch_alias_all_bid in self.__get_batch_data(all_alias_all_bid):
                all_gid |= set(g_confirmed(self.g.V().has('alias_bid0', P.within(batch_alias_all_bid))).bid.toList())
            all_gid = sorted(all_gid)

            for batch_gid in self.__get_batch_data(all_gid):
                query = self.g.V().has('bid', P.within(batch_gid)).as_('brand')
                query = query.optional(g_inV(g_confirmedE(__.outE('brandKeyword')))).as_('keyword')
                alias_all_bid, keywords = 0, set()
                for all_path in query.dedup('brand', 'keyword').path().by(__.valueMap(True)).toList():
                    for index, path in enumerate(all_path):
                        if isinstance(path['type'], int):
                            continue
                        label = vetex_type_ref[path['type'][0]]
                        if label == 'brand':
                            alias_all_bid = path['alias_bid0'][0]
                            if self.bid2keyword.get(alias_all_bid) is not None:
                                break
                            keywords = {path['bname'][0].lower(), path['name_en'][0].lower(), path['name_cn'][0].lower()}
                        elif label == 'keyword':
                            keywords.add(path['name'][0].lower())
                    if '' in keywords:
                        keywords.remove('')
                    self.bid2keyword[alias_all_bid] = keywords
        else:
            sql = f"select bid, name, name_cn, name_en, name_cn_front, name_en_front from artificial.all_brand " \
                  f"where bid in ({','.join([str(bid) for bid in all_alias_all_bid])}) limit 1;"
            for bid, *raw_keywords in self.db_clickhouse.query_all(sql):
                keywords = set(word for word in raw_keywords if len(word) > 0)
                self.bid2keyword[bid] = keywords

    @staticmethod
    def __get_batch_data(all_data, batch_size=1000):
        start = 0
        while start < len(all_data):
            yield all_data[start: min(start + batch_size, len(all_data))]
            start += batch_size

    def __process(self, name, ip_ids, brand_keywords):
        type = {t for t in ['同款', '推荐', '合作', '订制', '联名'] if t in name}
        same = {'同款', '推荐'} & type
        corp = {'合作', '订制', '联名'} & type
        res = {'明星同款': [], 'IP联名': []}
        for ip_id in ip_ids:
            ip = self.id2keyword[ip_id]
            if ip.alias & brand_keywords:
                continue
            # if ip.type[4:] == '人名IP' and (same or not (same or corp)):
            if ip.type[4:] == '人名IP':
                res['明星同款'].append(ip.subject)
            elif ip.type[4:] == '品牌名IP' and not corp:
                continue
            else:
                res['IP联名'].append(ip.subject)
        res['IP合作类型'] = [] if not res['IP联名'] else list(corp) if corp else ['联名']
        # res = {'明星同款': [], 'IP联名': [], '品牌合作': []}
        # for ip_id in ip_ids:
        #     ip = self.id2keyword[ip_id]
        #     if alias_all_bid != 0 and ip.alias & self.__get_brand_keyword(alias_all_bid):
        #         continue
        #
        #     if ip.type[4:] == '人名IP':
        #         res['明星同款'].append(ip.subject)
        #     elif ip.type[4:] == '品牌名IP':
        #         res['品牌合作'].append(ip.subject)
        #     elif ip.type[4:] == '其他IP':
        #         res['IP联名'].append(ip.subject)
        return res

    def __process_2(self, name, ip_ids, brand_keywords):
        result = []

        type = {t for t in ['同款', '推荐', '合作', '订制', '联名', '代言'] if t in name}
        # same = {'同款', '推荐'} & type
        # corp = {'合作', '订制', '联名'} & type

        for ip_id in ip_ids:
            res = {'IP名': '', 'IP联名': '', 'IP类型': ''}
            ip = self.id2keyword[ip_id]
            if ip.alias & brand_keywords:
                continue

            res['IP名'] = ip.subject
            if res['IP名'] == '辛巴' and '推荐' in type:
                res['IP 类型'] = '明星'
                res['IP 联名'] = '×××推荐'
                result.append(res)
                continue
            if res['IP名'] in ['qq', 'teddybear/泰迪熊', '金莎', 'wey(汽车)', '葫芦娃', '小王子', '小飞侠', '山治', '高达',
                              '辛巴', '明哥', '樱桃小丸子', '加勒比海盗', '特种部队(g.i.joe)', '食物语'] and not type:
                continue
            # if res['IP名'] == '小王子' and '三牛' in name:
            #     continue
            if res['IP名'] == '小王子' and 'le petit prince' in name.lower():
                continue
            if res['IP名'] == 'ig' and '糖尿' in name:
                continue
            if res['IP名'] == '雷神' and '日本' in name:
                continue
            if res['IP名'] == '波克比' and '台湾' in name:
                continue
            if res['IP名'] == '蓝精灵' and ('斯拉夫' in name or '俄罗斯' in name):
                continue
            if res['IP名'] == '赤木刚宪' and '灌篮高手' not in name:
                continue

            res['IP类型'] = ip.type[4:]
            if ip.type[4:] == '文创':
                res['IP联名'] = '×××合作款' if '合作' in type else '×××联名'
            elif ip.type[4:] == '品牌跨界':
                if not type:
                    continue
                res['IP联名'] = '×××联名'
            elif ip.type[4:] == '明星':
                res['IP联名'] = '×××代言' if '代言' in type else '×××推荐' if '推荐' in type else '×××同款'
            elif ip.type[4:] == '专家':
                res['IP联名'] = '×××订制' if {'订制', '定制'} & type else '×××联名'
            result.append(res)
        return result

    @staticmethod
    def __output_result(res):
        if res:
            return {'IP名': [r['IP名'] for r in res], 'IP联名': [r['IP联名'] for r in res], 'IP类型': [r['IP类型'] for r in res]}
        else:
            return {'IP名': [], 'IP联名': [], 'IP类型': []}


@used_time
def test():
    import pandas as pd
    import xlsxwriter

    filename = r'C:\Users\DELL\Desktop\ip_test_2.xlsx'
    item_df = pd.read_excel(filename, dtype={'name': str, 'alias_all_bid': int})
    clean_ip = CleanIP(91410)

    # item_df = item_df[:5]
    item_df[['sp1', 'sp2', 'sp3']] = item_df.apply(
        lambda line: clean_ip.get_ip(line['name'], {}, int(line['alias_all_bid'])),
        axis=1, result_type='expand')

    item_df.to_excel(r'C:\Users\DELL\Desktop\ip_test_result_2.xlsx', index=False, engine='xlsxwriter')
    print(item_df.head())

    """
    测试时间：
    1576766条    561.3s
    10000条    71s

    """


if __name__ == '__main__':
    clean_ip = CleanIP(91410)
    res = clean_ip.get_ip(name='【京东joy联名款】 达利客  奶盐味 苏打 饼干560g 袋装',
                          trade_props={},
                          alias_all_bid=135194)
    print(res)
    # test()
