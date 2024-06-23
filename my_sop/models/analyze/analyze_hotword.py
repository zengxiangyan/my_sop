import re
import jieba
import logging
import numpy as np
import pandas as pd
from gensim import corpora
from gensim.models.tfidfmodel import TfidfModel
import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))
import application as app
from extensions import utils
from models.analyze.trie import Trie
import models.analyze.analyze_tool as tool
from models.analyze.word_detection import WordDetector
from models.analyze.word import build_words_from_json_file
from dateutil.relativedelta import relativedelta

jieba.setLogLevel(logging.INFO)


class AnalyzeHotWord:
    g = app.get_graph('default')
    graph_db = app.get_db('graph')
    db_clickhouse = app.get_clickhouse('chmaster')

    base_dir = app.output_path('hotword_analyze_{eid}')

    en_pattern = '(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'
    del_words = {'男青', '脚板', '码大码', '鞋女', '底板', '女潮', '鞋男', '网虫', '踏板', '白板', '官方网', '网网', "喷喷",
                 "子复古", "款女", "邮包", "儿童网", "子女", "女网", "男网", "少女网", "耐克鞋", "夏帕克", "涂鸦板", "跑跑步",
                 "跑速", "舞女"}
    _stop_front_word = ['年', '鞋', '款', '靴', '码']
    _stop_back_word = ['男', '女']

    def __init__(self, eid, f_log=True):
        # 初始化参数
        self.g.connect()
        self.graph_db.connect()
        self.db_clickhouse.connect()
        self.eid = eid
        self.log = True if f_log else False
        self.detector = WordDetector(eid=self.eid)  # 新词发现工具
        self.new_words = set()  # 新词
        self.alias_keywords = {'韦德之道', '皇贝足球', '黑曼巴', '中考', '莆田'}  # 同义词表关键词
        self.graph_keywords = set()  # 知识图谱关键词
        self.topK_items = 3  # 统计表top宝贝数

        # 清洗子品类
        self.sc_category = {clean_cid: sub for clean_cid, sub in
                            self.db_clickhouse.query_all(f"select cid, name from artificial.category_{self.eid};")}

        # 文件处理
        self.base_dir = self.base_dir.format(eid=self.eid)
        self.dictionary_filename = join(self.base_dir, 'word_detection_keywords.pkl')
        self.extract_phrase_filename = join(self.base_dir, 'new_words_{topk}.txt')
        self.alias_brand_filename = join(self.base_dir, 'alias_brand_merged.json')
        self.alias_category_filename = join(self.base_dir, 'alias_category_merged.json')
        self.alias_prop_filename = join(self.base_dir, 'alias_prop_merged.json')
        self.alias_ip_filename = join(self.base_dir, 'alias_ip_merged.json')
        self.statistics_dir = join(self.base_dir, 'statistics')
        self.result_dir = join(self.base_dir, 'result')
        app.mkdirs(self.base_dir)
        app.mkdirs(self.statistics_dir)
        app.mkdirs(self.result_dir)

        # 同义词表关键词与知识图谱关键词初始化
        # self._init_graph_keywords()
        self._init_alias_keywords()

        if self.log:
            self.log_dir = join(self.base_dir, 'log')
            app.mkdirs(self.log_dir)
            self.logger = tool.Logger(self.eid, self.log_dir)

    def print_log(self, *args):
        print(*args)
        if self.log:
            self.logger.write(str(*args) + '\n')

    def _init_graph_keywords(self):
        from graph.kgraph import g_confirmed, g_confirmedE, g_inV, g_outV
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import P

        # alias_all_bid
        chmaster = app.connect_clickhouse('chmaster')
        brand_sql = f"select DISTINCT alias_all_bid from sop.entity_prod_{self.eid}_A" \
                    f"\nwhere pkey BETWEEN '2020-12-01' and '2020-12-31';"
        alias_bid0 = chmaster.execute(brand_sql)
        alias_bid0 = [bid[0] for bid in alias_bid0 if bid[0] != 0]

        # cid
        query = g_confirmed(self.g.V().has('cid', 2537))
        query = g_outV(g_confirmedE(query.inE('categoryParent')))
        c_gid = query.id().toList()

        # cpvid
        query = self.g.V(c_gid).as_('category')
        query = g_inV(g_confirmedE(query.outE('categoryPropNameR'))).has('cpnname', P.without('系列')).as_('cpn')
        query = g_inV(g_confirmedE(query.outE('categoryPropValue'))).as_('cpv')
        cpvid = query.id().toList()

        # category (category->keyword)
        category_word = set()
        query = self.g.V(c_gid).as_('category')
        query = query.optional(g_inV(g_confirmedE(__.outE('categoryKeyword')))).as_('keyword')

        for category_path in query.dedup('category', 'keyword').path().by(__.valueMap(True)).toList():
            for index, path in enumerate(category_path):
                if path['label'] == 'category':
                    category_word |= self._split_word(path['cname'][0])
                elif path['label'] == 'keyword':
                    category_word |= self._split_word(path['name'][0])
        self.print_log('category keywords :', category_word.__len__())

        # keyword (propValue->keyword)
        prop_word = set()
        query = self.g.V(cpvid).as_('cpv')
        query = query.optional(g_inV(g_confirmedE(__.outE('propValueKeyword')))).as_('keyword')

        for prop_path in query.dedup('cpv', 'keyword').path().by(__.valueMap(True)).toList():
            for index, path in enumerate(prop_path):
                if path['label'] == 'propValue':
                    prop_word |= self._split_word(path['pvname'][0])
                elif path['label'] == 'keyword':
                    prop_word |= self._split_word(path['name'][0])
        self.print_log('property keywords :', prop_word.__len__())

        # brand (brand->keyword) 使用batch
        def batch_bid_iter(bid, batch_size=100):
            data_len = len(bid)
            num_batch = int((data_len - 1) / batch_size) + 1

            for i in range(num_batch):
                start_id = i * batch_size
                end_id = min((i + 1) * batch_size, data_len)
                # print(bid[start_id:end_id])
                yield bid[start_id:end_id]

        brand_word = set()
        for _alias_bid0 in batch_bid_iter(alias_bid0):
            query = g_confirmed(self.g.V().has('alias_bid0', P.within(list(_alias_bid0)))).as_('brand')
            query = query.optional(g_inV(g_confirmedE(__.outE('brandKeyword')))).as_('keyword')

            for brand_path in query.dedup('brand', 'keyword').path().by(__.valueMap(True)).toList():
                for index, path in enumerate(brand_path):
                    if path['label'] == 'brand':
                        brand_word |= self._split_word(path['bname'][0])
                    elif path['label'] == 'keyword':
                        brand_word |= self._split_word(path['name'][0])
        self.print_log('brand keywords :', brand_word.__len__())

        # ip (ip, ipkeyword, keyword)[table]
        ip_word = set()
        sql = "select distinct i.ip_id as ip_id, i.name as ip_name, i.`type` as type, k.name as keyword " \
              "\nfrom (ip i left join ipkeyword ik on i.ip_id = ik.ip_id) " \
              "\nleft join keyword k on ik.kid = k.kid" \
              "\nwhere i.confirmType!=2 and ik.confirmType!=2" \
              "\norder by type;"
        data = self.graph_db.query_all(sql)
        _, ips, _, keywords = zip(*data)
        for ip in set(ips):
            ip_word |= self._split_word(ip)
        for keyword in set(keywords):
            ip_word |= self._split_word(keyword)
        self.print_log('ip keywords :', ip_word.__len__())

        self.graph_keywords |= category_word | brand_word | prop_word | ip_word
        self.g.close()
        self.graph_db.close()

    def _init_alias_keywords(self):
        brd2index, merge_brand_list = build_words_from_json_file(self.alias_brand_filename)
        cat2index, merge_category_list = build_words_from_json_file(self.alias_category_filename)
        prp2index, merge_prop_list = build_words_from_json_file(self.alias_prop_filename)
        _ip2index, merge_ip_list = build_words_from_json_file(self.alias_ip_filename)

        self.alias_to_subject = dict()  # 词与标签映射
        self.en_trie = Trie()  # 英文字典树
        self.merge_all_dict = dict()  # 主词与词类映射
        for merge_list in [merge_ip_list, merge_prop_list, merge_brand_list, merge_category_list]:
            for word in merge_list:
                self.alias_to_subject.update(word.alias_to_subject)
                self.merge_all_dict[word.subject] = word
                for w in word.alias:
                    if not utils.is_only_chinese(w):
                        self.en_trie.insert(w)
                    if utils.is_chinese(w):
                        self.alias_keywords.add(w.lower())
                        jieba.add_word(w.lower())

    @staticmethod
    def _split_word(word: str, sp='/'):
        word = re.sub(r'[\+\(\)]', r'/', word)
        output = set()
        for w in set(word.split(sp)):
            if utils.is_chinese(w) and len(w) > 1:
                output.add(w)
        return output

    def _start_end_month(self):
        """获取最大最小月"""
        month_sql = f"select min(pkey), max(pkey) from sop.entity_prod_{self.eid}_A;"
        start_month, end_month = self.db_clickhouse.query_all(month_sql)[0]
        return start_month, end_month

    @staticmethod
    def _single_month_process(word_info, topK_items):
        total_info = word_info.pop(0)
        for word in word_info:
            for subcat in word_info[word]:
                word_info[word][subcat]['item_id'] = len(
                    {tmp_id for tmp_id, tmp_name in word_info[word][subcat]['topK_items']})
                # word_info[word][subcat]['num_sales'] = word_info[word][subcat]['num_sales']
                word_info[word][subcat]['topK_items'] = sorted(word_info[word][subcat]['topK_items'].items(),
                                                               key=lambda x: -x[1][1])[:topK_items]
        for subcat in total_info:
            total_info[subcat]['item_id'] = len(total_info[subcat]['item_id'])
        word_info[0] = total_info
        return word_info

    @tool.used_time
    def extract_phrase(self, topk=50, month='2020-12', check=True):
        # 拉取新词发现数据集
        detection_sql = f"select alias_all_bid, name, count(item_id) from sop_e.entity_prod_{self.eid}_E" \
                        f"\nwhere " \
                        f"\n    pkey BETWEEN '{month}-01' and '{month}-31' and " \
                        f"\n    clean_cid in ({','.join([str(i) for i in self.sc_category.keys()])})" \
                        f"\ngroup by alias_all_bid, name;"
        data = self.db_clickhouse.query_all(detection_sql)
        columns = tool.get_columns_from_sql(detection_sql)
        item_df = pd.DataFrame(data, columns=columns)
        self.print_log(
            f'{item_df.__len__()} lines in <sop.entity_prod_{self.eid}> at {month} used for new word detection...')

        # jieba
        for w in self.graph_keywords | self.alias_keywords:
            jieba.add_word(w)

        # 对于每个章节文本执行新词发现任务并分词后加入章节，tf-idf备用
        self.detector.re_init_dictionary(self.alias_keywords | self.graph_keywords)

        dictionary = corpora.Dictionary([[]])
        bid_list, all_corpus, bid_new_words = [], [], []
        for _, Series in item_df.groupby('alias_all_bid'):
            bid_list.append(Series.iloc[0]['alias_all_bid'])
            context = self.detector.context_generator(Series, ['name'])
            new_words = self.detector.extract_phrase(context, topk, min_n=2, max_n=4)
            del_words = {w for w in new_words if w[0] in self._stop_front_word or w[-1] in self._stop_back_word}
            new_words = set(new_words) - del_words
            bid_new_words.append(new_words)

            if new_words:
                self.print_log(new_words)

            for w in new_words:
                jieba.add_word(w)
            for w in del_words:
                jieba.del_word(w)

            paragraph, weight = zip(*Series[['name', 'item_id']].values.tolist())

            segments = [word for sentence in paragraph
                        for word in set(jieba.lcut(sentence.lower())) if utils.is_chinese(word) and len(word) > 1]
            dictionary.add_documents([segments])
            corpus = dictionary.doc2bow(segments)
            all_corpus.append(corpus)

        # 计算tf-idf
        tf_idf_model = TfidfModel(all_corpus, normalize=False)
        word_tf_idf = list(tf_idf_model[all_corpus])

        # 筛选发现新词, tf-idf阈值top50%
        all_keywords = {w for w_set in bid_new_words for w in w_set}
        bid_new_words_selected = {}
        explainer = {v: p for p, v in dictionary.token2id.items()}
        for bid, tf_idf, new_words in zip(bid_list, word_tf_idf, bid_new_words):
            sorted_tf_idf = sorted(tf_idf, key=lambda x: -x[1])[:int(0.5 * len(tf_idf))]
            top50_words_id, _ = zip(*sorted_tf_idf)
            top50_words = [explainer[wid] for wid in top50_words_id]

            new_words_selected = {w for w in set(new_words) & set(top50_words)}
            bid_new_words_selected[bid] = new_words_selected

        accept_words = {w for w_set in bid_new_words_selected.values() for w in w_set}
        deny_words = all_keywords - {w for w_set in bid_new_words_selected.values() for w in w_set}
        self.print_log('accept :')
        self.print_log(accept_words)
        self.print_log(' deny  :')
        self.print_log(deny_words)
        self.new_words = accept_words
        for w in deny_words:
            jieba.del_word(w)

        if check:
            alias_all_bid = [str(bid) for bid in bid_new_words_selected.keys()]
            brand_sql = f"select bid, name from all_brand where bid in ({','.join(alias_all_bid)});"
            data = self.db_clickhouse.query_all(brand_sql)
            brand_df = pd.DataFrame(data, columns=['bid', 'name'])

            outline = [[bid] + list(new_words) + [''] * (topk - 2 - len(new_words)) for bid, new_words in
                       bid_new_words_selected.items()]
            new_words_df = pd.DataFrame(outline, columns=['bid'] + [''] * (topk - 2))
            new_words_df = pd.merge(brand_df, new_words_df, on='bid', how='outer', sort=True)
            new_words_df.to_csv(join(self.result_dir, "new_words.csv"), index=False, encoding='gb18030')

    @tool.used_time
    def statistics(self):
        month, end_month = self._start_end_month()
        while month <= end_month:
            _month = month.strftime('%Y-%m')

            word_info = {0: dict()}
            print("=" * 50)
            item_sql = "select item_id, name, trade_props.value as trade_props, alias_all_bid, clean_cid, num, sales " \
                       f"\nfrom sop_e.entity_prod_{self.eid}_E " \
                       "\nwhere " \
                       f"\n    `source` = 1 and (shop_type > 20 OR shop_type < 10) and" \
                       f"\n    pkey BETWEEN '{_month}-01' and '{_month}-31' and" \
                       f"\n    clean_cid in ({','.join([str(i) for i in self.sc_category.keys()])});"

            data = self.db_clickhouse.query_all(item_sql)
            columns = tool.get_columns_from_sql(item_sql)
            item_df = pd.DataFrame(data, columns=columns)
            if len(item_df) == 0:
                break
            for index, item_info in item_df.sort_values(by=['item_id']).iterrows():
                item_id, name, trade_props, alias_all_bid, clean_cid, num, sales = item_info

                if word_info[0].get(clean_cid) is None:
                    word_info[0][clean_cid] = {'item_id': set(), 'num_sales': np.array([0, 0.0])}
                word_info[0][clean_cid]['item_id'].add(item_id)
                word_info[0][clean_cid]['num_sales'] += np.array([num, sales / 100])

                def single_Word_process(w, _name):
                    _word = self.alias_to_subject[w] if self.alias_to_subject.get(w) else w
                    if _word not in segment_set:
                        # 跳过重复字
                        segment_set.add(_word)
                        if word_info.get(_word) is None:
                            word_info[_word] = dict()
                        if word_info[_word].get(clean_cid) is None:
                            word_info[_word][clean_cid] = {'alias_all_bid': dict(), 'item_id': None,
                                                           'num_sales': np.array([0, 0.0]), 'topK_items': dict()}

                        # brand数据
                        word_info[_word][clean_cid]['alias_all_bid'][alias_all_bid] = \
                            word_info[_word][clean_cid]['alias_all_bid'].get(
                                alias_all_bid, np.array([0, 0.0])) + np.array([num, sales / 100])
                        # 销量销售额 计算带货力度
                        word_info[_word][clean_cid]['num_sales'] += np.array([num, sales / 100])
                        # item_id计数 计算曝光力度 以及 topK宝贝信息
                        word_info[_word][clean_cid]['topK_items'][(item_id, _name)] = \
                            word_info[_word][clean_cid]['topK_items'].get(
                                (item_id, _name), np.array([0, 0.0])) + np.array([num, sales / 100])

                # name中文检查
                segment_set = set()
                try:
                    segment = jieba.lcut(name.lower())
                except AttributeError as e:
                    self.print_log(name)
                    continue
                en_string = ''
                for w_index, word in enumerate(segment):
                    if utils.is_only_chinese(word):
                        if len(word) > 1:
                            single_Word_process(word, name)
                    else:
                        en_string += word.lower() + ' '

                    # name英文检查
                    for match, start, end in self.en_trie.search_entity(en_string):
                        if re.search(self.en_pattern.format(match), en_string):
                            single_Word_process(match, name)

            res = self._single_month_process(word_info, self.topK_items)
            pd.to_pickle(res, join(self.statistics_dir, f'month_word_info_{_month}.pkl'))
            del res
            month = month + relativedelta(months=1)

    @tool.used_time
    def generate_table(self, only_new_word=False):
        """生成结果表"""
        # jieba
        if not self.new_words:
            self.get_word_from_check_file()

        for w in self.graph_keywords | self.alias_keywords | self.new_words:
            jieba.add_word(w)

        output_word_columns = ["月份", "二级品类名称", "分词", "词性", "同义词", "曝光力度", "带货力度"]
        output_word_columns += [f"{j}{i}" for i in range(1, self.topK_items + 1) for j in ['链接', '宝贝名', '销量', '销售额']]

        output_word_df = pd.DataFrame()

        month, end_month = self._start_end_month()
        while month <= end_month:
            _month = month.strftime('%Y-%m')
            self.print_log(_month)
            if not exists(join(self.statistics_dir, f'month_word_info_{_month}.pkl')):
                month = month + relativedelta(months=1)
                continue
            res = pd.read_pickle(join(self.statistics_dir, f'month_word_info_{_month}.pkl'))

            total_info = res.pop(0)
            outline_word = []

            for word in res.keys():
                if only_new_word:
                    if word not in self.new_words:
                        continue
                else:
                    if word in tool.get_cached_txt(app.output_path('shop_words.txt')):
                        continue

                for subcat in res[word].keys():
                    def artificial_modify(word_type):
                        res_key = {'品牌': '品牌', '品类': '品类', '适用季节': '适用季节'}
                        return res_key.get(word_type[4:], word_type[4:])

                    # 热词表输出
                    outline_word.append([_month, self.sc_category[subcat][7:],
                                         word, artificial_modify(self.merge_all_dict[word].type)
                                         if self.merge_all_dict.get(word) else '',
                                         '/'.join(w for w in self.merge_all_dict[word].alias if w != word)
                                         if self.merge_all_dict.get(word) else '',
                                         res[word][subcat]['item_id'] / total_info[subcat]['item_id'],
                                         res[word][subcat]['num_sales'][1] / total_info[subcat]['num_sales'][1]])
                    for i, ((item_id, name), (num, sales)) in enumerate(
                            res[word][subcat]['topK_items']):
                        outline_word[-1] += [f'http://detail.tmall.com/item.htm?id={item_id}',
                                             name.replace('\r\n', ' '),
                                             num, sales]

            output_word_df = output_word_df.append(outline_word, ignore_index=True)
            month = month + relativedelta(months=1)

        if only_new_word:
            output_filename = f'{self.eid}_hotword_analyze_only_new_word.csv'
        else:
            output_filename = f'{self.eid}_hotword_analyze.csv'
        output_word_df.set_axis(output_word_columns, axis='columns', inplace=True)
        output_word_df.to_csv(join(self.result_dir, output_filename), index=False,
                              encoding='gb18030')

    def get_word_from_check_file(self, artificial_check=None):
        if artificial_check is None:
            artificial_check = dict()
        new_words_df = pd.read_csv(join(self.result_dir, "new_words.csv"), encoding='gb18030')
        new_words_df.drop(['bid', 'name'], axis=1, inplace=True)
        self.new_words = {w for line in new_words_df.values.tolist() for w in line if isinstance(w, str)}
        for wrong_word, fix_word in artificial_check.items():
            if wrong_word in artificial_check:
                self.new_words.remove(wrong_word)
            if not isinstance(fix_word, set):
                raise TypeError('人工修正数据类型错误')
            self.new_words |= fix_word


if __name__ == '__main__':
    analyzer = AnalyzeHotWord(90473)
    # 限制子品类
    analyzer.sc_category = {k: v for k, v in analyzer.sc_category.items() if v[:7] == '运动鞋new-'}
    # analyzer.print_log(analyzer.sc_category)
    # # 新词发现
    # analyzer.extract_phrase(month='2020-12')
    # # 统计
    # analyzer.statistics()
    for k, v in analyzer.sc_category.items():
        print(f'{k} : {v}')
    analyzer.get_word_from_check_file({'型综训': {'综训'},
                                       '猪八': {'猪八革'},
                                       '考达标': {'中考', '高考', '体考', '达标'},
                                       '莆田牛': {'莆田'},
                                       '真爆': {'爆米花'},
                                       '油果奇异': {'牛油果', '奇异果'},
                                       '色翻毛': {'翻毛'},
                                       '速鹰二': {'速鹰'},
                                       '能测试鞋': {'体能测试鞋'},
                                       '鱼刺二': {'鱼刺'},
                                       '美产高端': {'美产', '高端'},
                                       '一号小白': {'空军一号', '小白鞋'},
                                       '中考体': {'中考体育'},
                                       '悦跑五': {'悦跑'},
                                       '真爆米花': {'爆米花'},
                                       '之道': {'韦德之道'},
                                       '改桃子熟': {'爆改', '桃子熟了'},
                                       '平之路': {'和平之路'},
                                       '迪达斯': {'阿迪达斯'},
                                       '尾小白': {'绿尾', '小白鞋'},
                                       '贸美国撕': {'外贸', '美国'},
                                       '仔丹宁': {'牛仔', '丹宁'},
                                       '红脚趾': {'黑红脚趾'},
                                       '闪电': {'小闪电', '闪电突袭'},
                                       '正品小白': {'正品'}})
    # 出报表
    analyzer.generate_table(only_new_word=True)
