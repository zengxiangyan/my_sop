from extensions import jieba_text
from os.path import abspath, join, dirname
import application as app
import os, jieba
from operator import itemgetter
from extensions import utils
import json
from nlp import pnlp

user_dict_file = join(abspath(dirname(__file__)), '../output/extract_words.csv')
cached_brand_keyword_file = join(abspath(dirname(__file__)), '../output/cached_brand_keyword_file.csv')

class BrandKeywordMatch:
    db = None
    keyword_ref = None
    cached_brand = dict()

    def __init__(self, table='brandKeyword'):
        if os.path.exists(user_dict_file):
            jieba.load_userdict(user_dict_file)
        db = app.connect_db('default')
        self.db = db
        self.init(table)

    def init(self, table='brandKeyword'):
        if os.path.exists(cached_brand_keyword_file):
            print('read from cached brand keyword file')
            self.keyword_ref = json.loads(''.join(utils.read_all_from_file(cached_brand_keyword_file)))
            return

        sql = 'select keywordId,name from keyword where keywordId in (select keywordId from %s)' %(table, )
        data = self.db.query_all(sql)
        h = dict()
        for row in data:
            h[row[1]] = row[0]
        self.keyword_ref = h
        utils.write_all_to_file(cached_brand_keyword_file, json.dumps(h, ensure_ascii=False))

        #此处需预先缓存brand

    #l [[keywordId,name,num]]
    #h2 else words{name:num}
    def match(self, name):
        if isinstance(name, list):
            words_list = name
        else:
            name = pnlp.unify_character(name)
            words_list = jieba_text.convert_doc_to_wordlist(name, cut_all=True)
        print(words_list)

        h = dict()
        h2 = dict()
        for w in words_list:
            if w in self.keyword_ref:
                h[w] = h[w] if w in h else 0
                h[w] += 1
            else:
                h2[w] = h2[w] if w in h2 else 0
                h2[w] += 1
        l = []
        for w in h:
            row = self.keyword_ref[w]
            l.append((row, w, h[w]))
        return l, h2