#coding=utf-8
import application as app

class AllBrand:

    def __init__(self):
        self.db = app.connect_db('default')
        self.cache = {0: ('', '', '', '', '', 0)}

    def search(self, all_bid):
        all_bid = int(all_bid)
        if all_bid not in self.cache:
            sql = 'SELECT name, name_cn, name_en, name_cn_front, name_en_front, alias_bid FROM brush.all_brand WHERE bid = %s;'     # zhou.wenjun: 0.18用all_site.all_brand，0.26用brush.all_brand
            ret = self.db.query_all(sql, (all_bid,))
            self.db.commit()
            if ret:
                self.cache[all_bid] = ret[0]
        return self.cache.get(all_bid)