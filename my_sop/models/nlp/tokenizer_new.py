import re
import jieba
import unicodedata
# from html.parser import HTMLParser
from html import unescape
import warnings

warnings.filterwarnings("ignore")

import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
from extensions import utils
from models.analyze.trie import Trie
from models.nerl.aliyun_api import get_client, get_ner_api_result
from models.nlp.common import text_normalize

jieba.setLogLevel(jieba.logging.INFO)
brackets_signal = "「」（〔［｛《【〖〈([{<）〕］｝》】〗〉)]}>"


class Tokenizer(object):
    """
    分词器

    基础分词器: jieba / aliyun
    优化分词器：eng / ner模型
    分词结果为 [[token, offset, end, label], ...]
    """

    def __init__(self, base='jieba', optimize=['eng']):
        """初始化分词器和优化分词工具"""
        print(f"基础分词器: {base}分词", f"优化器：{','.join(optimize) if len(optimize) else '无'}", sep='\t')
        if base == 'aliyun':
            self.client = get_client()
        basic_tokenizers = {
            'jieba': lambda text: [[word, offset, end, ''] for word, offset, end in jieba.tokenize(text)],
            'aliyun': lambda text: get_ner_api_result(self.client, text)
        }
        fill_label = {
            'jieba': '',
            'aliyun': '普通词'
        }
        self.base_tokenizer = basic_tokenizers[base]
        self.optimizers = optimize
        self.fill_label = fill_label[base]

    # @staticmethod
    # def jieba_tokenizer(text):
    #     """基础jieba分词"""
    #     tokens = [[word, offset, end, ''] for word, offset, end in jieba.tokenize(text)]
    #     return tokens
    #
    # @staticmethod
    # def aliyun_tokenizer(text):
    #     """基础aliyun分词"""
    #     tokens = get_ner_api_result(text)
    #     return tokens

    @staticmethod
    def english_optimizer(result, trie, fill_label=''):
        """英文字典树优化jieba中的英文分词"""
        en_segment, ch_segment = [], []  # List[word, start, end, label]

        # 分离中英文
        while result:
            word, st, ed, label = result.pop(0)
            if utils.is_only_chinese(word):
                ch_segment.append([word, st, ed, label])
            else:
                if len(en_segment) == 0:
                    en_segment.append([word, st, ed, ''])
                elif en_segment[-1][2] == st:
                    en_segment[-1][0] += word
                    en_segment[-1][2] = ed
                else:
                    en_segment.append([word, st, ed, ''])

        # 剥离首末的符号
        en_segment_new = []
        while en_segment:
            en_word, st, ed, label = en_segment.pop(0)
            if len(en_word) > 1:
                r = []
                if en_word[0] in brackets_signal:
                    r.append([en_word[0], st, st + 1, ''])
                    en_word = en_word[1:]
                    st += 1
                if en_word[-1] in brackets_signal:
                    r.append([en_word[-1], ed - 1, ed, ''])
                    en_word = en_word[:-1]
                    ed -= 1
                if st != ed:
                    r.append([en_word, st, ed, ''])
                en_segment_new += r
            else:
                en_segment_new.append([en_word, st, ed, label])

        def process_escapes(context):
            new_context = ''
            char = r'*.?+$^[](){}|\/'
            for ch in context:
                if ch in char:
                    new_context += "\\" + ch
                else:
                    new_context += ch
            return new_context

        eng_result = []
        for seg, st, ed, label in en_segment_new:
            trie_result = trie.search_entity(seg)
            rel_p = 0
            while trie_result:
                t, s, e, l = trie_result.pop(0)

                # 复查，是否链接出来的和其他字母相连，即非完整字段
                if not re.search('(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'.format(process_escapes(t)), seg):
                    continue
                if rel_p != s:
                    eng_result.append([seg[rel_p: s], st + rel_p, st + s, fill_label])
                eng_result.append([t, st + s, st + e, l])
                rel_p = e
            if rel_p < len(seg):
                eng_result.append([seg[rel_p:], st + rel_p, ed, fill_label])

        result = sorted(ch_segment + eng_result, key=lambda m: m[1])
        return result

    def tokenize(self, text, eng_trie=None, token2label=None):
        """分词主函数"""
        # 文本标准化
        normalized_text = text_normalize(text)
        # 基础分词
        tokens = self.base_tokenizer(normalized_text)

        # 英文优化
        if 'eng' in self.optimizers:
            if eng_trie is None:
                raise ValueError("eng_trie must be input when <eng> is in optimizers")
            if not isinstance(eng_trie, Trie):
                raise TypeError("eng_trie must be <Trie> type.")

            tokens = self.english_optimizer(tokens, eng_trie, self.fill_label)

        # 用token2label覆盖label
        if token2label is None:
            return tokens

        return [[token.strip(), start, end, token2label.get(token.strip(), label)] for token, start, end, label in tokens]
        return [[token, start, end, '/'.join(token2label[token]) if token2label.get(token) else label] for
                token, start, end, label in tokens]
        # token, start, end, label = tokens.pop(0)
        # while tokens[0][1] != 0:
        #     if token2label.get(token):
        #         label = '/'.join(token2label[token])`
        #     tokens.append([token, start, end, label])
        #     token, start, end, label = tokens.pop(0)
        # if token2label.get(token):
        #     label = '/'.join(token2label[token])
        # tokens.append([token, start, end, label])
        # return tokens


def fill_jieba_dict(word_list):
    for w in word_list:
        if len(w)==1:
            continue
        jieba.add_word(text_normalize(w))


if __name__ == '__main__':
    tokenizer = Tokenizer(base='aliyun')
    trie = Trie()
    trie.insert("ying yun's", 'brand')
    print(tokenizer.tokenize("街档酱香金钱肚广式早茶点心（Ying yu's）速冻半成品肚新鲜冷冻熟食速食卤牛肚", trie))
