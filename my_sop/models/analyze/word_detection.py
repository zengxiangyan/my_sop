import re
import ast
from models.analyze.trie import Trie
from smoothnlp.algorithm.phrase import extract_phrase
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))

import application as app


class TrieTokenizer:
    def __init__(self, vocab):
        self.trie = Trie()
        for w in vocab:
            self.trie.insert(w.lower())

    def tokenize(self, text):
        tokens = []
        search_entity = self.trie.search_entity(text)
        p = 0
        for en, st, ed in search_entity:
            if p != st:
                tokens.append(text[p:st])
            p = ed
        if p != len(text):
            tokens.append(text[p:])

        return tokens


class BasicTokenizer:
    def __init__(self, do_lower_case=True):
        self.do_lower_case = do_lower_case

    def tokenize(self, data_item):
        re_ = re.compile(u"([\u4E00-\u9FD5a-zA-Z0-9#&\._%]+)")

        tokens = []
        for text in data_item:
            if not isinstance(text, str):
                continue
            if self.do_lower_case:
                text = text.lower()
            try:
                blocks = re_.split(text)
            except:
                continue
            tokens += [blk for blk in blocks if len(blk) > 1]
        return tokens


class FullTokenizer:
    remove_en = True

    def __init__(self, vocab, do_lower_case=True):
        self.basic_tokenizer = BasicTokenizer(do_lower_case=do_lower_case)
        self.trie_tokenizer = TrieTokenizer(vocab=vocab)

    def tokenize(self, data):
        tokens = []
        for token in self.basic_tokenizer.tokenize(data):
            for new_token in self.trie_tokenizer.tokenize(token):
                if self.remove_en:
                    for ch_token in re.split('[a-z0-9#&\._%]+', new_token):
                        if len(ch_token) > 1:
                            tokens.append(ch_token)
                else:
                    if len(new_token) > 1:
                        tokens.append(new_token)
        return tokens


class WordDetector:
    def __init__(self, eid):
        self.eid = eid
        self.keywords = set(
            line.strip() for line in open(app.output_path('shop_words.txt'), 'r', encoding='utf-8').readlines())
        self.tokenizer = FullTokenizer(vocab=self.keywords)

    def re_init_dictionary(self, keywords):
        self.keywords |= set(keywords)
        self.tokenizer = FullTokenizer(vocab=self.keywords)

    def context_generator(self, df, columns=None):
        context = []

        if columns is None:
            columns = df.columns
        for column in columns:
            check_data = df[column].iloc[0]
            if isinstance(check_data, str):
                try:
                    # df[column] : str
                    ast.literal_eval(check_data)
                    data = {p for d in df[column].tolist() for p in ast.literal_eval(d)}
                except:
                    data = set(df[column].tolist())
            elif isinstance(check_data, list):
                data = set()
                for line in df[column].tolist():
                    data |= set(line)
            else:
                raise ValueError('Wrong data type...')
            context += self.tokenizer.tokenize(data)
            context = list(set(context))

        context = list(set(context))
        return context

    @staticmethod
    def extract_phrase(context, top_k=200, min_n=2, max_n=4):
        res_new_words = []
        if len(context) < 10 * top_k:
            top_k = int(0.1 * len(context))
            if top_k < 10:
                return res_new_words

        raw_new_words = extract_phrase(context, top_k=top_k, min_n=min_n, max_n=max_n)
        for w in raw_new_words:
            # 若有重合, 保留最长串, 暂不支持部分重叠((abc, bcd)->abcd)
            add_flag = False
            for _w in res_new_words:
                if _w in w:
                    _w = w
                    add_flag = True
                    break
                if w in _w:
                    add_flag = True
                    break
            if not add_flag:
                res_new_words.append(w)
            # if not self.new_words_trie.search_entity(w):
            #     self.new_words_trie.insert(w)
        return res_new_words
