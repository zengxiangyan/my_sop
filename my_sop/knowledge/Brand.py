from extensions import jieba_text
from os.path import abspath, join, dirname
import application as app
import os, jieba
from operator import itemgetter
from extensions import utils
import json
from extensions import hanlp_text

user_dict_file = join(abspath(dirname(__file__)), '../output/extract_words.csv')
cached_brand_file = join(abspath(dirname(__file__)), '../output/cached_brand_file.csv')

class BrandMatch:
    db = None
    keyword_brand_ref = None
    cached_brand = dict()

    def __init__(self):
        if os.path.exists(user_dict_file):
            jieba.load_userdict(user_dict_file)
        db = app.connect_db('default')
        self.db = db
        self.init_brand()

    def init_brand(self):
        if os.path.exists(cached_brand_file):
            print('read from cached brand file')
            self.keyword_brand_ref = json.loads(''.join(utils.read_all_from_file(cached_brand_file)))
            return

        sql = 'select a.brandId,b.name from brandKeyword a left join keyword b on (a.keywordId=b.keywordId)'
        data = self.db.query_all(sql)
        h = dict()
        for row in data:
            h[row[1]] = row[0]
        self.keyword_brand_ref = h
        utils.write_all_to_file(cached_brand_file, json.dumps(h, ensure_ascii=False))

        #此处需预先缓存brand

    def match(self, name):
        if isinstance(name, list):
            words_list = name
        else:
            words_list = hanlp_text.convert_doc_to_wordlist(name, cut_all=True)
        print(words_list)
        h = dict()
        for w in words_list:
            if not w in self.keyword_brand_ref:
                continue
            brand_id = self.keyword_brand_ref[w]
            if brand_id in h:
                h[brand_id] += 1
            else:
                h[brand_id] = 1
        l = []
        for k in h:
            l.append((k, h[k]))
        if len(l) == 0:
            return 0, '', 0
        sorted_list = sorted(l, key=itemgetter(1), reverse=True)
        print(sorted_list)
        brand_id = sorted_list[0][0]
        if brand_id in self.cached_brand:
            data = self.cached_brand[brand_id]
        else:
            sql = 'select bid,name from brand where brandId=%s'
            data = self.db.query_one(sql, (brand_id,))
            self.cached_brand[brand_id] = data
        #todo: 此处要找原因
        if data is None:
            return 0, '', 0
        bid, brand_name = data
        print('brand_id:', brand_id, '&brand_name:', brand_name, 'bid:', bid)
        return brand_id, brand_name, bid

def get_tmall_brand(db, l):
    sql = 'select a.cid,b.bid,if(d.alias_bid=0,d.bid,d.alias_bid) as brand_id from all_site.all_brand_hot a left join dw_entity.brand b on (a.bid=b.bid_new) left join all_site.all_brand d on (a.bid=d.bid) where a.source="tmall" and a.cid in (%s)'
    return utils.easy_query(db, sql, l)