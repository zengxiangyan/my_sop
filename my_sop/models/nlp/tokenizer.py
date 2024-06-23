import re
import jieba
from html.parser import HTMLParser

import sys
from os.path import join, abspath, dirname, exists

import pandas as pd

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# from models.nerl import lv0cid2cid
from models.graph.common import *
from models.nlp.NER import NERParser
from extensions import utils

jieba.setLogLevel(jieba.logging.INFO)
brackets_signal = "「」（〔［｛《【〖〈([{<）〕］｝》】〗〉)]{>"


class Tokenizer:
    def __init__(self, ner=''):
        self.NER = NERParser(ner, ['brand']) if ner else None  # 判断是否加载ner模型
        self.en_Trie = Trie()

    def load_graph_info_by_lv0cid(self, lv0cid):
        """加载lv0cid下所有关键词"""
        f = f'{lv0cid}.pkl'
        if exists(f):
            self.gid2entity, self.keyword2gids, self.en_Trie = pd.read_pickle(f)
        else:
            cid = lv0cid2cid(lv0cid)
            all_cid = category_layer_check(cid)
            limit_cid = limit_category(all_cid, 2)  # 限制2层一下品类
            all_bid = get_cid2bid(all_cid)
            all_cpnid = get_cid2cpnid(limit_cid)
            cpnid2pn = query_propname(all_cpnid)

            # keywords = get_all_keywords(
            #     all_paths=query_brand(all_bid) + query_category(limit_cid) +
            #               query_categoryPropName(all_cpnid) + query_sinking_category(limit_cid),
            #     cpnid2pn=cpnid2pn)

            self.gid2entity, self.keyword2gids = get_entity2keywords(
                all_paths=query_brand(all_bid) + query_category(limit_cid) +
                          query_categoryPropName(all_cpnid) + query_sinking_category(limit_cid),
                cpnid2pn=cpnid2pn)

            for word in self.keyword2gids:
                if utils.is_chinese(word):
                    jieba.add_word(word)
                if not utils.is_only_chinese(word):
                    self.en_Trie.insert(word)
            print(f'load {len(self.keyword2gids)} keywords')
            pd.to_pickle((self.gid2entity, self.keyword2gids, self.en_Trie), f)

    @staticmethod
    def load_jieba_dict(file):
        """jieba加载用户字典"""
        jieba.load_userdict(file)

    @staticmethod
    def add_jieba(word_list):
        for word in word_list:
            jieba.add_word(word.lower())

    def tokenize(self, text):
        """分词"""
        text = HTMLParser().unescape(text)
        text = re.sub('[ ]+', ' ', re.sub('[\v\r\n\t]+', ' ', text)).lower()  # 去除制表符
        ner_result = self.NER.ner(text) if self.NER else dict()
        # 检查分词结果和ner结果是否有重合，
        # 重合的话无视ner结果，否则重新规划分词路径，即重做data.cut
        tokens = self.condition_cut(text, ner_result)
        labels = [sorted(set(self.gid2entity[gid].label for gid in self.keyword2gids[token])) if self.keyword2gids.get(token) else [''] for token in tokens ]
        return tokens, labels

    def is_symbol(self):
        ...

    def condition_cut(self, text, ner_result):
        i = 0
        all_mentions = []
        # ner_result_list = [(ner, offset, label) for (ner, offset), label in sorted(ner_result.items(),
        #                                                                            key=lambda i:i[0][1])]
        for (ner, offset), label in sorted(ner_result.items(), key=lambda i: i[0][1]):
            if offset != 0:
                part_mention = self.cut(text[i:offset])
                for mention in part_mention:
                    mention.offset += i
                all_mentions += part_mention

            m = Mention()
            m.token = ner
            m.offset = offset
            all_mentions.append(m)
            i = offset + len(ner)
        if i < len(text):
            part_mention = self.cut(text[i:])
            for mention in part_mention:
                mention.offset += i
            all_mentions += part_mention
        return [m.token for m in all_mentions]

    def cut(self, text):
        en_pattern = '(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'

        def single_process(word, offset):
            m = Mention()
            m.token = word
            m.offset = offset
            mentions.append(m)

        mentions = []

        tokenize_result = jieba.tokenize(text.lower())
        en_segment = []
        for word, st, ed in tokenize_result:
            if utils.is_only_chinese(word):
                # 中文
                single_process(word, st)
            else:
                # 英文
                if len(en_segment) > 0 and len(en_segment[-1][0]) + en_segment[-1][1] == st and en_segment[-1][0] not \
                        in brackets_signal:
                    # 与之前的英文字段相连
                    en_segment[-1][0] += word
                elif word != ' ':
                    # 避免词段首为空格
                    if len(en_segment) > 0:
                        # 避免词段末为空格
                        add_signal = []
                        en_segment[-1][0] = en_segment[-1][0].rstrip()
                        while len(en_segment[-1][0]) > 0 and en_segment[-1][0][-1] in '/)）】]':
                            _word, _st = en_segment.pop()
                            en_segment.append([_word[:-1], _st])
                            add_signal.append([_word[-1], _st + len(_word) - 1])
                        if len(en_segment[-1][0]) == 0:
                            en_segment.pop()
                        en_segment += add_signal[::-1]
                    en_segment.append([word, st])
            if len(en_segment) > 0:
                add_signal = []
                en_segment[-1][0] = en_segment[-1][0].rstrip()
                while len(en_segment[-1][0]) > 0 and en_segment[-1][0][-1] in '/)）】]':
                    _word, _st = en_segment.pop()
                    en_segment.append([_word[:-1], _st])
                    add_signal.append([_word[-1], _st + len(_word) - 1])
                if len(en_segment[-1][0]) == 0:
                    en_segment.pop()
                en_segment += add_signal[::-1]

        def process_escapes(context):
            new_context = ''
            char = r'*.?+$^[](){}|\/'
            for ch in context:
                if ch in char:
                    new_context += "\\" + ch
                else:
                    new_context += ch
            return new_context

        # 处理英文分词
        for seg, offset in en_segment:
            res = self.en_Trie.search_entity(seg)
            rel_p = 0
            while res:
                en = res.pop(0)

                # 复查，是否链接出来的和其他字母相连，即非完整字段
                if not re.search(en_pattern.format(process_escapes(en[0])), seg):
                    continue
                if rel_p != en[1]:
                    single_process(seg[rel_p:en[1]].rstrip(), offset + rel_p)
                single_process(en[0], offset + en[1])
                rel_p = en[2]
            if rel_p < len(seg):
                single_process(seg[rel_p:], offset + rel_p)
        return sorted(mentions, key=lambda m: m.offset)


if __name__ == '__main__':
    tokenizer = Tokenizer()
    # tokenizer.load_graph_info_by_lv0cid(2011010118)
    # print(tokenizer.tokenize('【宠粉节专享】美宝莲小金笔\t眼线笔 0.5ml'))
    # print(tokenizer)
    # print(tokenizer.en_Trie.all_entity())

    Text = input('input a text:')
    while Text != '':
        result = tokenizer.tokenize(Text)
        print(result)
        # print('\n', json.dumps(result, ensure_ascii=False, indent=4))
        Text = input('input a text:')
