import gc
import re
import sys
import json
import time
import datetime
import platform
import csv
from functools import cmp_to_key
from os import listdir, mkdir, chmod, getpid
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app

class Cache():
    def __init__(self):
        self.cache = {}

    def hmset(self, k, d):
        self.cache[k] = d

    def hgetall(self, k):
        return self.cache[k] if k in self.cache else None

    def expire(self, k, s):
        pass


class main():
    def __init__(self, cleaner):
        self.cleaner = cleaner
        self.init()

        self.cache = {}

    def init(self):
        pass

    def clear_cache(self):
        del self.cache
        gc.collect()
        self.cache = {}

    # 原始数据
    def get_entity_tbl(self):
        return ''
        # return '废弃artificial_local.entity_{}_origin_parts'.format(self.cleaner.eid)

    # 清洗
    def get_clean_tbl(self):
        return ''
        # return '废弃artificial_local.entity_{}_clean_new'.format(self.cleaner.eid)

    def get_a_tbl(self):
        return 'chsop', 'sop.entity_prod_{}_A'.format(self.cleaner.eid)

    def get_c_tbl(self):
        return self.get_a_tbl()

    def get_all_tbl(self):
        # if self.cleaner.bid in (44,210):
        return self.get_a_tbl()
        # return 'chsop', 'sop_c.entity_prod_{}_ALL'.format(self.cleaner.eid)

    def get_unique_tbl(self):
        return 'chsop', 'sop_c.entity_prod_{}_unique_items'.format(self.cleaner.eid)

    def get_sku_stat_tbl(self):
        return 'chsop', 'sop_c.entity_prod_{}_ALL_skustat'.format(self.cleaner.eid)

    def get_sku_price_tbl(self):
        return 'chsop', 'sop.entity_prod_{}_skuprice'.format(self.cleaner.eid)

    def get_b_tbl(self):
        return 'chsop', 'sop_b.entity_prod_{}_B'.format(self.cleaner.eid)

    def get_d_tbl(self):
        return 'chsop', 'sop_d.entity_prod_{}_D'.format(self.cleaner.eid)

    def get_e_tbl(self, db='sop_e'):
        return 'chsop', '{}.entity_prod_{}_E'.format(db, self.cleaner.eid)

    # 出数cid库
    def get_category_tbl(self):
        return 'chsop', 'artificial.category_{}'.format(self.cleaner.eid)

    # 出数sku库
    def get_product_tbl(self):
        return 'chsop', 'artificial.product_{}'.format(self.cleaner.eid)

    # 答题sku库
    def get_brush_product_tbl(self):
        return '26_apollo', 'product_lib.product_{}'.format(self.cleaner.eid)

    # 临时文件存储
    def get_cache_dir(self, file=''):
        return '/nas/upload/'+file

    def get_cache(self):
        if platform.system() != "Linux":
            return Cache()
        else:
            return app.get_cache('graph')

    def get_unique_key(self):
        return [ 'source','item_id','sku_id','trade_props.name','trade_props.value','brand','name' ]

    def get_brand_info(self, source, bid=0, refresh=False):
        cacheK = 'brand_'
        r = self.get_cache()

        k = '{}{}:{}'.format(cacheK, source, bid)
        if str(bid) != '0' or str(bid) != '':
            v = r.hgetall(k)
            if not v:
                refresh = True
            else:
                return v

        cols = ['bid','all_bid']

        if refresh or bid == 0:
            tbl = {
                '1': 'ali.brand_has_alias_bid_month',
                '2': 'jd.brand_has_alias_bid_month',
                '3': 'gome.brand_has_alias_bid_month',
                '4': 'jumei.brand_has_alias_bid_month',
                '5': 'kaola.brand_has_alias_bid_month',
                '6': 'suning.brand_has_alias_bid_month',
                '7': 'vip.brand_has_alias_bid_month',
                '8': 'pdd.brand_has_alias_bid_month',
                '9': 'jx.brand_has_alias_bid_month',
                '10': 'tuhu.brand_has_alias_bid_month',
                '11': 'dy.brand_has_alias_bid_month',
                '12': 'cdf.brand_has_alias_bid_month',
                '13': 'lvgou.brand_has_alias_bid_month',
                '14': 'dewu.brand_has_alias_bid_month',
                '15': 'hema.brand_has_alias_bid_month',
                '16': 'sunrise.brand_has_alias_bid_month',
                '17': 'test17.brand_has_alias_bid_month',
                '18': 'test18.brand_has_alias_bid_month',
                '19': 'test19.brand_has_alias_bid_month',
                '24': 'ks.brand_has_alias_bid_month',
                '999': '',
            }[str(source)]

            dba = self.cleaner.get_db('chmaster')
            sql = 'SELECT {} FROM {}{}'.format(','.join(cols), tbl, ' WHERE bid = {}'.format(bid) if bid > 0 else '')
            ret = dba.query_all(sql)
            ret = [{kk:v[i] for i,kk in enumerate(cols)} for v in ret]

            if bid == 0:
                return ret

            for v in ret:
                k = '{}{}:{}'.format(cacheK, source, bid)
                r.hmset(k, v)
                r.expire(k, 86400)

        if str(bid) == '0' or str(bid) == '':
            return

        k = '{}{}:{}'.format(cacheK, source, bid)
        v = r.hgetall(k)
        if not v:
            v = {kk:'' for i,kk in enumerate(cols)}
            v['bid'] = bid
            v['all_bid'] = 0
            r.hmset(k, v)
            r.expire(k, 3600)

        return v


    def get_allbrand_info(self, bid=0, refresh=False):
        cacheK = 'allbrand_'
        r = self.get_cache()

        k = '{}{}'.format(cacheK, bid)
        if bid > 0:
            v = r.hgetall(k)
            if not v:
                refresh = True
            else:
                return v

        cols = ['bid','name','name_cn','name_en','name_cn_front','name_en_front','alias_bid']

        if refresh:
            dba = self.cleaner.get_db('18_apollo')
            sql = 'SELECT {} FROM all_site.all_brand{}'.format(','.join(cols), ' WHERE bid = {}'.format(bid) if bid > 0 else '')
            ret = dba.query_all(sql)

            for v in ret:
                v = {kk:v[i] for i,kk in enumerate(cols)}
                v['alias_bid'] = v['alias_bid'] or v['bid']
                k = '{}{}'.format(cacheK, v['bid'])
                r.hmset(k, v)
                r.expire(k, 86400)

        if bid == 0:
            return

        k = '{}{}'.format(cacheK, bid)
        v = r.hgetall(k)
        if not v:
            v = {kk:'' for i,kk in enumerate(cols)}
            v['bid'] = v['alias_bid'] = bid
            r.hmset(k, v)
            r.expire(k, 3600)

        return v


    def get_subbrand_info(self, source, sbid=0, refresh=False):
        cacheK = 'subbrand_'
        r = self.get_cache()

        k = '{}{}:{}'.format(cacheK, source, sbid)
        if sbid > 0:
            v = r.hgetall(k)
            if not v:
                refresh = True
            else:
                return v

        mp = {
            '1': ['47_apollo', 'apollo.sub_brand'],
        }

        cols = ['sbid', 'full_name']
        if str(source) not in mp:
            return {kk:'' for i,kk in enumerate(cols)}
        db, tbl = mp[str(source)]

        if refresh:
            dba = self.cleaner.get_db(db)
            sql = 'SELECT {} FROM {}{}'.format(','.join(cols), tbl, ' WHERE sbid = {}'.format(sbid) if sbid > 0 else '')
            ret = dba.query_all(sql)

            for v in ret:
                v = {kk:v[i] for i,kk in enumerate(cols)}
                k = '{}{}:{}'.format(cacheK, source, v['sbid'])
                r.hmset(k, v)
                r.expire(k, 86400)

        if sbid == 0:
            return

        k = '{}{}:{}'.format(cacheK, source, sbid)
        v = r.hgetall(k)
        if not v:
            v = {kk:'' for i,kk in enumerate(cols)}
            v['sbid'] = sbid
            r.hmset(k, v)
            r.expire(k, 3600)

        return v


    def get_shop_info(self, source, sid=-1, refresh=False):
        cacheK = 'shop_'
        r = self.get_cache()

        k = '{}{}:{}'.format(cacheK, source, sid)
        if sid > 0:
            v = r.hgetall(k)
            if not v:
                refresh = True
            else:
                return v

        if refresh:
            dba = self.cleaner.get_db('chsop')
            sql = 'SELECT {} FROM artificial.all_shop WHERE source = {}'.format(','.join(cols), source, ' AND sid = {}'.format(sid) if sid >= 0 else '')
            ret = dba.query_all(sql)

            for v in ret:
                v = {k:v[i] for i,k in enumerate(['sid', 'name', 'nick'])}
                k = '{}{}:{}'.format(cacheK, source, v['sid'])
                r.hmset(k, v)
                r.expire(k, 86400)

        if sid == -1:
            return

        k = '{}{}:{}'.format(cacheK, source, sid)
        v = r.hgetall(k)
        if not v:
            v = {k:'' for i,k in enumerate(['sid', 'name', 'nick'])}
            v['sid'] = sid
            r.hmset(k, v)
            r.expire(k, 3600)

        return v

    def get_category_info(self, source, cid=0, refresh=False):
        if ('category', source, cid) in self.cache:
            return self.cache[('category', source, cid)]

        tbl = {
            '1': 'ali.cached_item_category',
            '2': 'jd.cached_item_category',
            '3': 'gome.cached_item_category',
            '4': 'jumei.cached_item_category',
            '5': 'kaola.cached_item_category',
            '6': 'suning.cached_item_category',
            '7': 'vip.cached_item_category',
            '8': 'pdd.cached_item_category',
            '9': 'jx.cached_item_category',
            '10': 'tuhu.cached_item_category',
            '11': 'dy.cached_item_category',
            '12': 'cdf.cached_item_category',
            '13': 'lvgou.cached_item_category',
            '14': 'dewu.cached_item_category',
            '15': 'hema.cached_item_category',
            '16': 'sunrise.cached_item_category',
            '17': 'test17.cached_item_category',
            '18': 'test18.cached_item_category',
            '19': 'test19.cached_item_category',
            '24': 'ks.cached_item_category',
            '999': '',
        }[str(source)]

        cols = ['cid','name','full_name','tree','child_cids_with_self']

        a,b = tbl.split('.')
        dba = self.cleaner.get_db('chsop')
        dbb = self.cleaner.get_db('chmaster')
        sql = '''
            SELECT argMax(`{}`,`month`) FROM remote('192.168.40.195','{}','{}','{}','{}') GROUP BY cid
        '''.format('`,`month`),argMax(`'.join(cols), a, b, dbb.user, dbb.passwd)
        ret = dba.query_all(sql)

        ret = {('category', source, vv[0]): {kk:vv[i] for i,kk in enumerate(cols)} for vv in ret}
        self.cache.update(ret)

        if ('category', source, cid) not in self.cache:
            v = {kk:'' for i,kk in enumerate(cols)}
            v['cid'] = cid
            v['tree'] = [cid]
            v['child_cids_with_self'] = [cid]
            self.cache[('category', source, cid)] = v
        return self.cache[('category', source, cid)]

        # cacheK = 'categoryxx_'
        # r = self.get_cache()

        # k = '{}{}:{}'.format(cacheK, source, cid)
        # if str(cid) != '0' or str(cid) != '':
        #     v = r.hgetall(k)
        #     if not v:
        #         refresh = True
        #     else:
        #         v['tree'] = eval(v['tree'])
        #         v['child_cids_with_self'] = eval(v['child_cids_with_self'])
        #         return v

        # cols = ['cid','name','full_name','tree','child_cids_with_self']

        # if refresh:
        #     tbl = {
        #         '1': 'ali.cached_item_category',
        #         '2': 'jd.cached_item_category',
        #         '3': 'gome.cached_item_category',
        #         '4': 'jumei.cached_item_category',
        #         '5': 'kaola.cached_item_category',
        #         '6': 'suning.cached_item_category',
        #         '7': 'vip.cached_item_category',
        #         '8': 'pdd.cached_item_category',
        #         '9': 'jx.cached_item_category',
        #         '10': 'tuhu.cached_item_category',
        #         '11': 'dy.cached_item_category',
        #         '12': 'cdf.cached_item_category',
        #         '13': 'lvgou.cached_item_category',
        #         '14': 'dewu.cached_item_category',
        #         '15': 'hema.cached_item_category',
        #         '16': 'sunrise.cached_item_category',
        #         '17': 'test17.cached_item_category',
        #         '18': 'test18.cached_item_category',
        #         '19': 'test19.cached_item_category',
        #         '20': 'ks.cached_item_category',
        #         '999': '',
        #     }[str(source)]

        #     dba = self.cleaner.get_db('chmaster')
        #     sql = 'SELECT {} FROM {}{}'.format(','.join(cols), tbl, ' WHERE cid = {} ORDER BY `month` DESC LIMIT 1'.format(cid) if cid > 0 else '')
        #     ret = dba.query_all(sql)

        #     for v in ret:
        #         v = {kk:v[i] for i,kk in enumerate(cols)}
        #         v['tree'] = str(v['tree'])
        #         v['child_cids_with_self'] = str(v['child_cids_with_self'])
        #         k = '{}{}:{}'.format(cacheK, source, cid)
        #         r.hmset(k, v)
        #         r.expire(k, 86400)

        # if str(cid) == '0' or str(cid) == '':
        #     return

        # k = '{}{}:{}'.format(cacheK, source, cid)
        # v = r.hgetall(k)
        # if not v:
        #     v = {kk:'' for i,kk in enumerate(cols)}
        #     v['cid'] = cid
        #     v['tree'] = str([cid])
        #     v['child_cids_with_self'] = str([cid])
        #     r.hmset(k, v)
        #     r.expire(k, 3600)

        # v['tree'] = eval(v['tree'])
        # v['child_cids_with_self'] = eval(v['child_cids_with_self'])

        # return v

    def get_allbrand(self, all_bid=-1):
        cols = ['bid','name','name_cn','name_en','name_cn_front','name_en_front','alias_bid']

        if 'allbrand' not in self.cache:
            aname, atbl = self.get_a_tbl()
            adba = self.cleaner.get_db(aname)

            sql = 'SELECT distinct(all_bid) FROM {}'.format(atbl)
            ret = adba.query_all(sql)
            bids = [str(v[0]) for v in ret]+['0']

            bdba = self.cleaner.get_db('18_apollo')

            sql = 'SELECT {} FROM all_site.all_brand WHERE bid IN ({})'.format(','.join(cols), ','.join(bids))
            ret = bdba.query_all(sql)

            res, bids = {}, ['0']
            for v in ret:
                v = {kk:v[i] for i,kk in enumerate(cols)}
                res[int(v['bid'])] = v
                if v['alias_bid'] > 0:
                    bids.append(str(v['alias_bid']))

            sql = 'SELECT {} FROM all_site.all_brand WHERE bid IN ({})'.format(','.join(cols), ','.join(list(set(bids))))
            ret = bdba.query_all(sql)

            for v in ret:
                v = {kk:v[i] for i,kk in enumerate(cols)}
                res[int(v['bid'])] = v

            self.cache['allbrand'] = res

        if all_bid ==-1:
            return self.cache['allbrand']
        if int(all_bid) not in self.cache['allbrand']:
            tmp = {col:'' for col in cols}
            tmp['bid'] = all_bid
            tmp['alias_bid'] = 0
            return tmp
        return self.cache['allbrand'][int(all_bid)]


    def get_subbrand(self, brand, sbid):
        if not sbid:
            return ''

        if 'subbrand' not in self.cache:
            aname, atbl = self.get_a_tbl()
            adba = self.cleaner.get_db(aname)

            sql = 'SELECT distinct(sub_brand) FROM {} WHERE source = 1'.format(atbl)
            ret = adba.query_all(sql)
            bids = [str(v[0]) for v in ret]

            bdba = self.cleaner.get_db('47_apollo')

            if len(bids) > 0:
                sql = 'SELECT sbid, full_name FROM apollo.sub_brand WHERE sbid IN ({})'.format(','.join(bids))
                ret = bdba.query_all(sql)

                res = {}
                for v in ret:
                    res[str(v[0])] = v[1]

                self.cache['subbrand'] = res
            else:
                self.cache['subbrand'] = {}

        k = str(sbid)
        if k not in self.cache['subbrand']:
            return ''
        return self.cache['subbrand'][k]


    def get_shopkeeper(self, source, sid=-1):
        cachek = 'shopinfo_{}'.format(source)
        if cachek not in self.cache:
            dba, tbl = self.get_a_tbl()
            dba = self.cleaner.get_db(dba)

            sql = '''
                SELECT sid, title, nick FROM artificial.all_shop WHERE `source` = {s} AND sid IN (
                    SELECT toUInt32(sid) FROM {} WHERE `source` = {s}
                )
            '''.format(tbl, s=source)
            ret = dba.query_all(sql)
            self.cache[cachek] = {v[0]:v[1:] for v in ret}

        if sid ==-1:
            return self.cache[cachek]
        if int(sid) not in self.cache[cachek]:
            return '', ''
        return self.cache[cachek][int(sid)]


    def get_source(self, source=None):
        mp = {
            'ali': 1,
            'tb': 1,
            'tmall': 1,
            'jd': 2,
            'gome': 3,
            'jumei': 4,
            'kaola': 5,
            'suning': 6,
            'vip': 7,
            'pdd': 8,
            'jx': 9,
            'tuhu': 10,
            'dy': 11,
            'cdf': 12,
            'lvgou': 13,
            'dewu': 14,
            'hema': 15,
            'sunrise': 16,
            'test17': 17,
            'test18': 18,
            'test19': 19,
            'ks': 24,
            '': 999,
        }
        if source is not None:
            return mp[source]
        return mp

    def get_source_en(self, source=None, shop_type=None):
        mp = {
            0: 'tb',
            1: 'tmall',
            2: 'jd',
            3: 'gome',
            4: 'jumei',
            5: 'kaola',
            6: 'suning',
            7: 'vip',
            8: 'pdd',
            9: 'jx',
            10: 'tuhu',
            11: 'dy',
            12: 'cdf',
            13: 'lvgou',
            14: 'dewu',
            15: 'hema',
            16: 'sunrise',
            17: 'test17',
            18: 'test18',
            19: 'test19',
            24: 'ks',
            999: '',
        }
        if source is not None:
            if int(source) == 1 and shop_type and int(shop_type) < 20 and int(shop_type) > 10:
                source = 0
            return mp[int(source)]
        return mp

    def get_source_cn(self, source=None, shop_type=None):
        mp = {
            0: '淘宝',
            1: '天猫',
            2: '京东',
            3: '国美',
            4: '聚美',
            5: '考拉',
            6: '苏宁',
            7: '唯品会',
            8: '拼多多',
            9: '酒仙',
            10: '途虎',
            11: '抖音',
            12: 'cdf',
            13: '旅购日上',
            14: '得物',
            15: '盒马',
            16: '新旅购',
            17: 'test17',
            18: 'test18',
            19: 'test19',
            24: '快手',
            999: '',
        }
        if source:
            if int(source) == 1 and shop_type and int(shop_type) < 20 and int(shop_type) > 10:
                source = 0
            return mp[int(source)]
        return mp

    # 底层shoptype定义
    def get_shoptype(self, source=None, shop_type=None):
        mp = {
            1: {11: '淘宝', 12: '淘宝', 21: '天猫', 22: '天猫国际', 23: '天猫超市', 24: '天猫国际', 25: '天猫', 26: '猫享自营', 27: '猫享自营国际', 28:'阿里健康国内', 9:'阿里健康国际'},
            2: {11: '京东国内自营', 12: '京东国内POP', 21: '京东海外自营',22: '京东海外POP'},
            3: {11: '国美国内自营', 12: '国美国内POP', 21: '国美海外自营',22: '国美海外POP'},
            4: {11: '聚美国内自营', 12: '聚美海外自营'},
            5: {11: '考拉国内自营', 12: '考拉国内POP', 21: '考拉海外自营',22: '考拉海外POP'},
            6: {11: '苏宁国内自营', 12: '苏宁国内POP', 21: '苏宁海外自营',22: '苏宁海外POP'},
            7: {11: '唯品会国内自营', 12: '唯品会海外自营'},
            8: {11: '拼多多国内自营', 12: '拼多多国内POP', 21: '拼多多海外自营',22: '拼多多海外POP'},
            9: {11: '酒仙自营', 12: '酒仙非自营'},
            10: {11: '途虎自营'},
            11: {11: '抖音国内普通',12: '抖音国内自营',21: '抖音国际普通',22: '抖音国际自营'},
            12: {11: 'cdf'},
            13: {11: '旅购日上优选',12: '旅购日上上海'},
            14: {11: '得物'},
            15: {11: '盒马'},
            16: {11: '新旅购国内购',12: '新旅购免税预定',21: '新旅购跨境购',},
            17: {11: 'test17'},
            18: {11: 'test18'},
            19: {11: 'test19'},
            24: {11: '快手'},
            999: {},
        }
        if source:
            return mp[int(source)][int(shop_type)]
        return mp

    def get_link(self, source=None, item_id=None):
        mp = {
            'tb': "http://item.taobao.com/item.htm?id={}",
            'tmall': "http://detail.tmall.com/item.htm?id={}",
            'jd': "http://item.jd.com/{}.html",
            'beibei': "http://www.beibei.com/detail/00-{}.html",
            'gome': "http://item.gome.com.cn/{}.html",
            'jumei': "http://item.jumei.com/{}.html",
            'kaola': "http://www.kaola.com/product/{}.html",
            'suning': "http://product.suning.com/{}.html",
            'vip': "http://archive-shop.vip.com/detail-0-{}.html",
            'yhd': "http://item.yhd.com/item/{}",
            'tuhu': "https://item.tuhu.cn/Products/{}.html",
            'jx': "http://www.jiuxian.com/goods-{}.html",
            'dy': "https://haohuo.jinritemai.com/views/product/detail?id={}",
            'cdf': "https://www.cdfgsanya.com/product.html?productId={}&goodsId={}",
            'dewu': "https://m.dewu.com/router/product/ProductDetail?spuId={}&skuId={}",
            'sunrise': "-",
            'lvgou': "-",
        }

        if source in ['cdf', 'dewu']:
            id_array = item_id.split("/")
            return mp[source].format(id_array[0], id_array[1])

        if source:
            return mp[source].format(item_id)
        return mp


    def get_extend(self, item):
        return None
    # 划线价(origin_price),促销价(promo_price),成交价(price)


    # 月销售额统计表 取销售额topN%
    def imported_item(self):
        dbch, tbl = self.get_entity_tbl()

        sql = '''
            DROP TABLE IF EXISTS artificial.entity_{}_sales
        '''.format(self.cleaner.eid)
        dbch.execute(sql)

        sql = '''
            CREATE TABLE artificial.entity_{}_sales (
                `tb_item_id` String CODEC(ZSTD(1)), `source` String CODEC(ZSTD(1)), `month` Date, `p1` String CODEC(ZSTD(1)), `sales` UInt64
            ) ENGINE = MergeTree() ORDER BY sales
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(self.cleaner.eid)
        dbch.execute(sql)

        sql = '''
            INSERT INTO artificial.entity_{eid}_sales
            SELECT tb_item_id, source, month, p1, sum(sales)
            FROM {}
            GROUP BY source, tb_item_id, month, p1
        '''.format(tbl, eid=self.cleaner.eid)
        dbch.execute(sql)

        # sql = '''
        #     select groupArray(sales)[arrayFirstIndex(i->i>{rate}, arrayCumSum(groupArray(c)))]
        #     from (
        #         select sales, sales / {total} c
        #         from artificial.entity_90325_sales
        #         order by sales desc
        #     )
        # '''.format(rate='0.8', total=1197024726057)
        # minsales = .query_all(sql)
        # select * from xxx where sales > minsales


    def export_csv(self, d, file, result):
        try:
            mkdir(app.output_path(d))
        except Exception as e:
            print(e.args)

        file = d + '/' + file
        with open(app.output_path(file), 'w', encoding = 'gbk', errors = 'ignore', newline = '') as output_file:
            writer = csv.writer(output_file)
            for row in result:
                writer.writerow(row)
        output_file.close()
        chmod(app.output_path(file), 0o777)


    def get_toplist(self, rate=0.8, where=1, selimit=200000):
        dba, tbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        s = time.time()
        sql = 'SELECT sum(sales*sign)*{} FROM {} WHERE ({})'.format(rate, tbl, where)
        ret = dba.query_all(sql)

        if ret[0][0] == 0:
            return []

        rr1 = ret[0][0]

        brush_p1, filter_p1 = self.get_plugin().filter_brush_props()

        sql = '''
            WITH groupArray(ss) AS arr
            SELECT arrayFilter((i, x) -> (x < {}), arr, arrayCumSum(arr))[-1]
            FROM (
                WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) AS p1
                SELECT sum(sales*sign) ss FROM {} WHERE ({})
                GROUP BY `source`, item_id , p1
                ORDER BY ss DESC
                LIMIT {}
            )
        '''.format(ret[0][0], filter_p1, tbl, where, selimit)
        ret = dba.query_all(sql)

        sql = '''
            SELECT sum(sales*sign), `source`, item_id ,
                arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{})))) AS p1
            FROM {} WHERE ({})
            GROUP BY `source`, item_id , p1
            HAVING sum(sales*sign) >= {}
        '''.format(filter_p1, tbl, where, ret[0][0])
        ret = dba.query_all(sql)

        return ret


    # 有发现交易属性名变了但值不变的情况
    def filter_brush_props(self, filter_keys=[], filter_vals=[]):
        filter_vals = [
            ['（|［|｛|＜|｟|《|【|〔|〖|〘|〚|「|〝|\{|\[|〈|⟦|⟨|⟪|⟬|⟮|⦅|⦗|«|‹', '('],
            ['）|］|｝|＞|｠|》|】|〕|〗|〙|〛|」|〞|\}|\]|〉|⟧|⟩|⟫|⟭|⟯|⦆|⦘|»|›', ')'],
            [' | | | |　|&nbsp;|\t|。|\.|，|,|、|！|\!|？|\?|—|-|…|_|——|；|\;|：|\:|[\x00-\x09]|[\x0B-\x0C]|[\x0E-\x1F]|\x7F', '']
        ] + filter_vals

        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all, item):
            r = self.cleaner.json_decode(trade_prop_all)
            if isinstance(r, dict):
                p1 = r
            else:
                p1 = trade_prop_all.split(',')
                p2 = trade_prop_all.split('|')
                p1 = p1 if len(p1) > len(p2) else p2
                p1 = {i:str(v) for i,v in enumerate(p1)}
            tmp = []
            for p in p1:
                if p in filter_keys:
                    continue

                v = p1[p].lower()
                for fr, fv, in filter_vals:
                    v = re.sub(re.compile(fr), fv, v)
                v = v.strip()
                if v != '':
                    tmp.append(v)

            p1 = list(set(tmp))
            p1.sort()
            return p1

        ss = 'lowerUTF8(x)'
        for fr, fv, in filter_vals:
            ss = '''replaceRegexpAll(
                {}, '{}', '{}'
                )'''.format(ss, fr.replace('\\', '\\\\'), fv)

        return format, '''
            arraySort(arrayFilter(y->trim(y)<>'',arrayDistinct(arrayMap((x, k)->IF(k NOT IN {}, trim(
                {}
            ), ''),trade_props.value,trade_props.name))))
        '''.format(filter_keys, ss)


    # 抽样 pkey
    def sample(self, where=1, limit=10000):
        dba, tbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        # group by交易属性 每个cid取topsales {limit}
        sql = '''
            SELECT cid, item_id, `trade_props.value`, sum(sales*sign) ss FROM {}
            WHERE ({})
            GROUP BY cid, item_id, `trade_props.value` HAVING ss > 0 ORDER BY ss DESC LIMIT {} BY cid
        '''.format(tbl, where, limit)
        ret = dba.query_all(sql)
        self.export_csv("batch{}new".format(self.cleaner.bid), "{}{}.csv".format(source, cid), ret)


    #################################################################
    ## rewrite
    def new_replace_info(self, item):
        pass


    def replace_info(self, item, smonth=None, emonth=None):
        return {}
        # return super().replace_info_x(smonth, emonth, item, ['snum', 'tb_item_id'], ['cid', 'all_bid'])


    # 按月，pkeys group by 分别取ckeys中出现过不同的返回
    def replace_info_x(self, smonth, emonth, item, pkeys, ckeys):
        aname, atbl = self.get_a_tbl()
        adba = self.cleaner.get_db(aname)

        k = 'cid cache'
        if k not in self.cache:
            self.cache[k] = {}
            for col in ckeys:
                sql = '''
                    SELECT {pkeys}, argMax({col}, date) from {} WHERE {}
                    GROUP BY {pkeys} HAVING(COUNT(DISTINCT({col}))) > 1
                '''.format(atbl, col=col, pkeys=','.join(pkeys).replace('tb_item_id', 'item_id').replace('snum', 'source'))
                ret = adba.query_all(sql)
                for v in ret:
                    kk = '-'.join([str(v) for v in v[0:-1]])
                    if kk not in self.cache[k]:
                        self.cache[k][kk] = {}
                    self.cache[k][kk][col] = v[-1]

        kk = '-'.join([str(item[col]) for col in pkeys])
        return {} if kk not in self.cache[k] else self.cache[k][kk]


    # 特殊出题
    def brush(self, smonth, emonth, logId=-1):
        # 不做特殊处理
        return False


    # 删除重复答题 优先保留sku答题
    def skip_brush(self, source, item_id, p1, add=True, sql=None, remove=True, if_flag=True, if_snum=False):
        tbl = self.cleaner.get_tbl()
        db26 = self.cleaner.get_db(self.cleaner.db26name)

        k = 'brush_cache'
        if k not in self.cache:
            name, type = self.cleaner.get_project()

            # sku答题中的老属性答题需要重新答
            where = ''
            if if_flag:
                where = 'where flag > 0'
            if not sql:
                if source in ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy']:
                    sql = 'SELECT source, tb_item_id, real_p1, flag, id FROM {} {}'.format(tbl, where)
                else:
                    sql = 'SELECT snum, tb_item_id, real_p1, flag, id FROM {} {}'.format(tbl, where)
            ret = db26.query_all(sql)
            mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):[v[3],v[4]] for v in ret}

            self.cache[k] = [type, mpp]

        type, mpp = self.cache[k]

        kk = '{}-{}-{}'.format(source, item_id, p1)
        if kk not in mpp:
            if add:
                mpp[kk] = [0, 0]
            return False

        flag, id = mpp[kk]

        # sku答题项目中的属性答题需要补答
        if flag == 1 and type == 5:
            if remove:
                sql = 'DELETE FROM {} WHERE id = {}'.format(tbl, id)
                db26.execute(sql)
            return False

        return True


    def skip_brush_new(self, process_top_ret, add=True, sql=None, remove=True, if_flag=True, if_snum=False):
        # k = 'skip_brush_cache'
        # if k not in self.cache:
        dba, atbl = self.cleaner.get_plugin().get_all_tbl()
        dba = self.cleaner.get_db(dba)
        db26 = self.cleaner.get_db('26_apollo')

        sql = 'SELECT id, snum, p1, tb_item_id, org_bid, flag FROM {}'.format(self.cleaner.get_tbl())
        bru_items = db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()

        tbl = 'sop_c.brush_temp_{}'.format(self.cleaner.eid)

        sql = 'DROP TABLE IF EXISTS {}'.format(tbl)
        dba.execute(sql)
        sql = 'CREATE TABLE {} AS sop_c.brush_log'.format(tbl)
        dba.execute(sql)

        data1 = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in process_top_ret:
            data1.append({'logid': 0,
                          'i': 0,
                          'batch_id': self.cleaner.bid,
                          'uuid2': uuid2,
                          'source': source,
                          'date': datetime.datetime(2021, 11, 11),
                          'item_id': tb_item_id,
                          'p1': str(p1),
                          'all_bid': alias_all_bid})
        # print(data1)
        if (len(data1) > 0):
            sql = 'INSERT INTO {} (logid, i, batch_id, uuid2, source, date, item_id, p1, all_bid) VALUES'.format(tbl)
            dba.execute(sql, data1)

        btbl = tbl + '_brush'
        dba.execute('DROP TABLE IF EXISTS {}'.format(btbl))
        sql = '''
            CREATE TABLE {} (
                `brush_id` UInt32, `source` UInt8, p1 Array(String), `item_id` String, `all_bid` UInt32, `flag` UInt8
            ) ENGINE = Log
        '''.format(btbl)
        dba.execute(sql)

        data = []
        for id, snum, p1, item_id, bid, flag, in bru_items:
            p1 = brush_p1(snum, p1, [])
            data.append([id, snum, p1, item_id, bid, flag])

        if len(data) > 0:
            sql = 'INSERT INTO {} VALUES'.format(btbl)
            dba.execute(sql, data)

        # sql = '''
        #     SELECT i, count(*), sum(total_new), sum(total0), sum(total1), sum(total2) FROM (
        #         SELECT a.i, source, item_id, p1, all_bid, sum(IF(isNull(b.flag),1,0)) total_new, sum(IF(b.flag=0,1,0)) total0, sum(IF(b.flag=1,1,0)) total1, sum(IF(b.flag=2,1,0)) total2
        #         FROM {} a LEFT JOIN {} b USING (source, item_id, p1, all_bid)
        #         GROUP BY a.i, source, item_id, p1, all_bid
        #     ) GROUP BY i
        # '''.format(tbl, btbl)
        # ret = dba.query_all(sql)

        sql = '''
            SELECT uuid2 FROM (
                SELECT any(uuid2) uuid2, a.i, source, item_id, p1, all_bid, sum(IF(isNull(b.flag),1,0)) total_new, sum(IF(b.flag=0,1,0)) total0, sum(IF(b.flag=1,1,0)) total1, sum(IF(b.flag=2,1,0)) total2
                FROM {} a LEFT JOIN {} b USING (source, item_id, p1, all_bid)
                GROUP BY a.i, source, item_id, p1, all_bid
            ) where total_new = 1
        '''.format(tbl, btbl)
        q = dba.query_all(sql)
        ret = []
        for row in q:
            ret.append(row[0])
        return ret


    # 取答题字段 默认取出题字段
    def brush_props(self, type):
        # 小五要求sku全部sp回填
        if str(type) == '2':
            poslist = self.cleaner.get_poslist()
            return [str(v) for v in poslist]

        # 属性答题
        sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {}'.format(self.cleaner.eid)
        ret = self.cleaner.db26.query_all(sql)
        return [str(v[0]) for v in ret]


    # type
    # 1 前后月覆盖
    # 0 不覆盖
    # type . rate
    def brush_similarity(self, v1, v2, where, item_ids, fix=False):
        if 'type900' not in self.cache:
            poslist = self.cleaner.get_poslist()
            self.cache['type900'] = ['sp{}'.format(pos) for pos in poslist if poslist[pos]['type'] == 900][0]

        if str(v1['real_p1']) != str(v2['p1']):
            k = '{}-{}-{}-{}'.format(v2['source'], v2['item_id'], v1['id'], v2['datenumber'])
            if k in fix and 'clean_'+self.cache['type900'] in v1 and v1['clean_'+self.cache['type900']] == v2[self.cache['type900']] and v2['avg_price'] > 0 and abs((v1['avg_price']-v2['avg_price'])/v2['avg_price']) <= 0.3:
                return 1.4-abs((v1['avg_price']-v2['avg_price'])/v2['avg_price'])
            return 0
        if str(v1['uuid2']) == str(v2['uuid2']):
            return 3
        if str(v1['pkey']) == str(v2['pkey']):
            return 2
        if v2['date'] >= v1['month']:
            return 1.6
        return 1.5

    # 需要join c表取清洗结果的情况
    def join_c(self,uuids):
        pass

    # 修改答题 比如121需要把拆过套包的还原回去
    def pre_brush_modify(self, v, products, prefix):
        return v

    # 修改spu回填顺序
    def pre_brush_spu(self, prod, spu, poslist):
        for c in spu:
            if c.find('spid') == 0 and prod[c] == '' and spu[c] != '':
                prod[c] = spu[c]
            if c in ['alias_all_bid'] and not prod[c] and spu[c] and spu[c] > 0:
                prod[c] = spu[c]

    # 机洗答题通用sp生成
    def calc_sp(self, v, is_brush=False):
        pass


    # 答题结果特殊修改
    def brush_modify(self, v, bru_items):
        pass


    def calc_splitrate_new(self, item, data):
        if len(data) <= 1:
            return data

        dbname, atbl = self.get_a_tbl()
        dbname, btbl = self.get_b_tbl()
        dba = self.cleaner.get_db(dbname)

        if 'sku_price' not in self.cache:
            sql = '''
                SELECT pid, sum(sales) / sum(num*number) FROM {}
                WHERE split_rate = 1 AND similarity >= 2 AND uuid2 NOT IN (SELECT uuid2 FROM {} WHERE sign = -1)
                GROUP BY pid
            '''.format(btbl, atbl)
            ret = dba.query_all(sql)
            self.cache['sku_price'] = {str(v[0]): v[1] for v in ret}

        ret = {str(v['pid']): self.cache['sku_price'][str(v['pid'])] for v in data if str(v['pid']) in self.cache['sku_price']}
        if len(ret.keys()) != len(data):
            total = sum([v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less
        else:
            total = sum([ret[str(v['pid'])]*v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = ret[str(v['pid'])]*v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less

        return data


    # # 拆套包 默认按照number均分 返回 [[rate, id], ...]
    # def calc_splitrate_new(self, item, data):
    #     total = sum([v['number'] for v in data])
    #     less  = 1
    #     for v in data:
    #         split_rate = v['number'] / total
    #         less -= split_rate
    #         v['split_rate'] = split_rate
    #     data[-1]['split_rate'] += less
    #     return data


    # 拆套包
    def calc_splitrate(self, aaa, data):
        pass


    def load_brush_finish(self, tbla, tblb, tblc):
        pass


    # 对需要出题的uuid定制，比如单价不同就不排重
    def process_brush_uuid(self, ver, flag=0, excl_visible=-1):
        tbl = self.cleaner.get_tbl()
        db26 = self.cleaner.get_db('26_apollo')
        dba = self.cleaner.get_db('chsop')

        # 已经出题的
        sql = '''
            SELECT snum, tb_item_id, real_p1, org_bid FROM {} WHERE flag >= {} AND visible NOT IN ({}) ORDER BY month
        '''.format(tbl, flag, excl_visible)
        rr1 = db26.query_all(sql)
        rr1 = {'{}-{}-{}'.format(v[0],v[1],v[2]): v[3] for v in rr1}

        # 要删除的
        sql = '''
            SELECT snum, tb_item_id, real_p1, id FROM {} WHERE flag < {} OR visible IN ({})
        '''.format(tbl, flag, excl_visible)
        rr2 = db26.query_all(sql)
        tmp = {}
        for v in rr2:
            k = '{}-{}-{}'.format(v[0],v[1],v[2])
            if k not in tmp:
                tmp[k] = []
            tmp[k].append(v[3])
        rr2 = tmp

        sql = '''
            SELECT uuid2, source, item_id, p1, all_bid, idx, key
            FROM sop_b.pre_brush WHERE eid={} AND ver={}
            ORDER BY idx LIMIT 1 BY source, item_id, p1, all_bid
        '''.format(self.cleaner.eid, ver)
        ret = dba.query_all(sql)

        uuids, rmids = [], []
        for v in ret:
            k = '{}-{}-{}'.format(v[1],v[2],v[3])

            if k in rr1 and rr1[k]==v[4]:
                # 已经出过题 且品牌没变
                continue

            if k in rr2:
                # 在删除的范围内
                rmids += rr2[k]

            uuids.append(v)

        return uuids, rmids


    def transform(self, tbl, dba):
        sql = '''
            SELECT col, groupArrayArray(`value.from`), col_change, groupArrayArray(`value.to`), `where` FROM (
                SELECT * FROM artificial.alter_update WHERE eid = {} ORDER BY ver DESC LIMIT 1 BY uuid
            ) WHERE deleteFlag = 0 GROUP BY col, col_change, `where`, `order` ORDER BY `order`
        '''.format(self.cleaner.eid)
        ret = dba.query_all(sql)

        cols = self.cleaner.get_cols(tbl, dba, ['name']).keys()
        for a, b, c, d, where, in ret:
            if c == '':
                dba.execute(where.format(tbl=tbl))
                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)
                continue

            if len(b) == 0:
                if a == 'ecshop':
                    self.hotfix_ecshop(tbl, dba, c)
                # elif a == 'isbrush':
                #     self.hotfix_isbrush(tbl, dba, c)
                else:
                    raise Exception('trans is empty')
                continue

            a = a.replace('\'', '\'\'')
            b = ['\'{}\''.format(bb.replace('\'', '\'\'')) for bb in b]
            c = c.replace('\'', '\'\'')
            d = ['\'{}\''.format(dd.replace('\'', '\'\'')) for dd in d]

            tmp = []
            for aa in a.split(','):
                if aa in cols:
                    pass
                elif aa == 'month':
                    aa = 'toYYYYMM(date)'
                else:
                    aa = '`c_props.value`[indexOf(`c_props.name`, \'{}\')]'.format(aa)
                tmp.append('toString({})'.format(aa))
            a = tmp

            if c in cols:
                fsql = '''
                    `{}` = transform(concat('',{}), [{}], [{}], toString(`{}`))
                '''.format(c, ',\',\','.join(a), ','.join(b), ','.join(d), c)
            else:
                fsql = '''
                    `{a}` = arrayConcat(
                        arrayFilter((x) -> x NOT IN [ '{k}' ], `{a}`), [ '{k}' ]
                    ),
                    `{b}` = arrayConcat(
                        arrayFilter((k, x) -> x NOT IN [ '{k}' ], `{b}`, `{a}`),
                        [transform(concat('',{}), [{}], [{}], `{b}`[indexOf(`{a}`, '{k}')])]
                    )
                '''.format(',\',\','.join(a), ','.join(b), ','.join(d), k=c, a='c_props.name', b='c_props.value')
            sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(tbl, fsql, where or 1)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)


    # 特殊修改 只能改清洗结果
    def hotfix(self, tbl, dba, prefix, params):
        self.hotfix_isbrush(tbl, dba, '是否人工答题')


    def hotfix_ecshop(self, tbl, dba, colname):
        dbname, atbl = self.get_a_tbl()
        adba = self.cleaner.get_db(dbname)

        sql = '''
            SELECT toString(groupArray(concat(toString(`source`),'_', toString(sid)))), toString(groupArray(shop_type))
            FROM (
                WITH transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0) AS source,
                    IF(chtype_h > 0, chtype_h, chtype_m) AS ch_type,
                    transform(ch_type, [1, 2, 3, 4], ['FSS', 'EKA', 'EDT', 'EKA_FSS'], toString(ch_type)) AS shop_type
                SELECT source, sid, shop_type FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm')
                WHERE sid IN (SELECT toUInt32(sid) FROM {} GROUP BY sid) AND shop_type != '0'
            )
        '''.format(atbl)
        ret = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name`  = arrayConcat(
                    arrayFilter((x) -> x NOT IN [ '{cname}' ], `c_props.name`),
                    [ '{cname}' ]
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN [ '{cname}' ], `c_props.value`, `c_props.name`),
                    [ IF( `source` = 1 AND (shop_type < 20 and shop_type > 10 ),
                            'C2C',
                            transform(
                                concat(toString(`source`),'_', toString(sid)),
                                {},
                                {},
                                `c_props.value`[indexOf(`c_props.name`, '{cname}')]
                            )
                    ) ]
                )
            WHERE 1
        '''.format(tbl, ret[0][0], ret[0][1], cname=colname)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def hotfix_isbrush(self, tbl, dba, colname):
        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name`  = arrayConcat(
                    arrayFilter((x) -> x NOT IN [ '{cname}' ], `c_props.name`),
                    [ '{cname}' ]
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN [ '{cname}' ], `c_props.value`, `c_props.name`),
                    [ IF(c_brush_id > 0, '前后月覆盖', '否') ]
                )
            WHERE 1
        '''.format(tbl, cname=colname)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        if self.cleaner.bid != 136:
            _, btbl = self.get_b_tbl()
            sql = '''
                ALTER TABLE {} UPDATE
                    `c_props.name`  = arrayConcat(
                        arrayFilter((x) -> x NOT IN [ '{cname}' ], `c_props.name`), [ '{cname}' ]
                    ),
                    `c_props.value` = arrayConcat(
                        arrayFilter((k, x) -> x NOT IN [ '{cname}' ], `c_props.value`, `c_props.name`), [ '断层填充' ]
                    )
                WHERE uuid2 IN (SELECT uuid2 FROM {} WHERE similarity >= 1 AND similarity < 1.5)
            '''.format(tbl, btbl, cname=colname)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

            sql = '''
                ALTER TABLE {} UPDATE
                    `c_props.name`  = arrayConcat(
                        arrayFilter((x) -> x NOT IN [ '{cname}' ], `c_props.name`), [ '{cname}' ]
                    ),
                    `c_props.value` = arrayConcat(
                        arrayFilter((k, x) -> x NOT IN [ '{cname}' ], `c_props.value`, `c_props.name`), [ '出题宝贝' ]
                    )
                WHERE uuid2 IN (SELECT uuid2 FROM {} WHERE similarity = 3)
            '''.format(tbl, btbl, cname=colname)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)


    # 特殊修改 可以改跟sales,num有关的
    def finish(self, tbl, dba, prefix):
        pass


    def update_trade_props(self, tbl, dba):
        self.cleaner.add_miss_cols(tbl, {'trade_props_arr':'Array(String)', 'clean_flag':'Int8'})
        # if self.cleaner.bid in (44,210):
        # update
        _, filter_p1 = self.filter_brush_props()

        # update price， p1
        dba.execute('ALTER TABLE {} UPDATE `trade_props_arr` = {}, `clean_flag` = 1 WHERE 1'.format(tbl, filter_p1))

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
        # else:
        #     # update
        #     _, filter_p1 = self.filter_brush_props()

        #     # update price， p1
        #     dba.execute('ALTER TABLE {} UPDATE `trade_props_arr` = {} WHERE 1'.format(tbl, filter_p1))

        #     while not self.cleaner.check_mutations_end(dba, tbl):
        #         time.sleep(5)


    def update_alias_bid(self, tbl, dba):
        bidsql, bidtbl = self.cleaner.get_aliasbid_sql()

        sql = 'ALTER TABLE {} UPDATE `alias_all_bid` = {} WHERE 1'.format(tbl, bidsql.format(v='all_bid'))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_other(self, tbl, dba):
        pass


    def update_e_alias_bid(self, tbl):
        dba = self.cleaner.get_db('chsop')
        sql = '''
            ALTER TABLE {} UPDATE alias_all_bid = dictGetOrDefault('all_brand', 'alias_bid', tuple(alias_all_bid), toUInt32(0))
            WHERE dictGetOrDefault('all_brand', 'alias_bid', tuple(alias_all_bid), toUInt32(0)) > 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def update_e_alias_pid(self, tbl, prefix):
        pdba, ptbl = self.get_product_tbl()
        pdba = self.cleaner.get_db(pdba)

        prod_cfg = self.cleaner.get_kadis(prefix)['prod']
        if prod_cfg:
            ptbl += '_'+prod_cfg

        try:
            sql = 'ALTER TABLE {} ADD COLUMN custom_pid Int32 DEFAULT 0 CODEC(ZSTD(1))'.format(ptbl)
            pdba.execute(sql)
        except:
            pass

        cols = self.cleaner.get_cols(tbl, pdba)
        if 'alias_pid' not in cols:
            sql = 'ALTER TABLE {} ADD COLUMN alias_pid UInt32 DEFAULT 0 CODEC(ZSTD(1))'.format(tbl)
            pdba.execute(sql)

        pdba.execute('DROP TABLE IF EXISTS {}_skujoin'.format(tbl))

        sql = '''
            CREATE TABLE {}_skujoin ENGINE = Join(ANY, LEFT, pid)
            AS
            WITH multiIf(custom_pid != 0, custom_pid, alias_pid != 0, alias_pid, pid) AS apid
            SELECT pid, IF(apid = -1, 0, apid) ppid FROM {}
        '''.format(tbl, ptbl)
        ret = pdba.query_all(sql)

        sql = '''
            ALTER TABLE {t} UPDATE alias_pid = ifNull(joinGet('{t}_skujoin', 'ppid', clean_pid), clean_pid)
            WHERE 1
        '''.format(t=tbl)
        pdba.execute(sql)

        while not self.cleaner.check_mutations_end(pdba, tbl):
            time.sleep(5)

        pdba.execute('DROP TABLE IF EXISTS {}_skujoin'.format(tbl))


    # 对e表做特殊处理后的 比如影藏某些sku不给客户看
    def process_exx(self, tbl, prefix, logId=0):
        pass


    # 跑月表用
    def process_month_tbl(self, tbl, prefix, logId=0):
        pass


    def generate_msearch_tbl(self, tbl):
        poslist = self.cleaner.get_poslist()
        p = ['sp{}'.format(poslist[p]['name']) for p in poslist if poslist[p]['multi_search']==1]

        if len(p) == 0:
            return

        dba = self.cleaner.get_db('chsop')
        sql = 'DROP TABLE IF EXISTS {}_specialmsearch'.format(tbl)
        dba.execute(sql)

        sql = 'CREATE TABLE {}_specialmsearch AS {}'.format(tbl, tbl)
        dba.execute(sql)

        sql = 'INSERT INTO {}_specialmsearch SELECT * FROM {}'.format(tbl, tbl)
        dba.execute(sql)

        tbl = tbl+'_specialmsearch'

        for pp in p:
            sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `{}`) AS p GROUP BY trim(p)'.format(tbl, pp)
            rrr = dba.query_all(sql)

            cols = ['ADD COLUMN `{}-{}` String CODEC(ZSTD(1))'.format(pp, col) for col, in rrr]
            sql = 'ALTER TABLE {} {}'.format(tbl, ','.join(cols))
            dba.execute(sql)

            sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
            dba.execute(sql)

            sql = '''
                CREATE TABLE {t}_set ENGINE = Set AS
                SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `{}`) AS p
            '''.format(pp, t=tbl)
            dba.execute(sql)

            cols = ['`{}-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(pp, tbl, c=col) for col, in rrr]
            sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

            sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
            dba.execute(sql)

        self.cleaner.update_eprops(tbl, dba)


    def process_st(self, tbl, smonth, emonth):
        pass

    # 出数属性检查
    def check_props(self, tbl):
        pass

    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        dba, etbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT min(pkey), max(pkey), sum(c_sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        sql = 'SELECT sum(sales*sign), sum(sign) FROM {} WHERE pkey >= \'{}\' AND pkey <= \'{}\' AND sales > 0 AND num > 0'.format(
            etbl, smonth, emonth
        )
        ret = dba.query_all(sql)
        salesb, countb = ret[0][0], ret[0][1]

        # if salesa != salesb:
        #     raise Exception("output failed salesa:{} salesb:{} counta:{} countb:{}".format(salesa, salesb, counta, countb))

        if salesa != salesb:
            self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)

    # sku答题检查
    def check_brush(self, tbl, dba, logId):
        # self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)
        pass

    # 出数数据检查
    def check(self, tbl, dba, logId):
        # 当月相同品牌是否会对应不同的品牌定位
        sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m'.format(tbl)
        ret = dba.query_all(sql)

        data = {}
        for m, in ret:
            sql = '''
                SELECT `source`, SUM(c) FROM (
                    WITH IF(`source`=5, [toString(sku_id)], [toString(trade_props.name), toString(trade_props.value)]) AS k
                    SELECT `source`, `date`, 1, countDistinct(uuid2) c FROM {} WHERE toYYYYMM(date) = {}
                    GROUP BY `source`, `date`, item_id, k HAVING c > 1
                ) GROUP BY `source`
            '''.format(tbl, m)
            rr1 = dba.query_all(sql)
            for s, c, in rr1:
                if s not in data:
                    data[s] = [[], []]
                data[s][0].append(c)

            sql = '''
                SELECT `source`, countDistinct(uuid2) FROM {} WHERE toYYYYMM(date) = {} GROUP BY `source`
            '''.format(tbl, m)
            rr2 = dba.query_all(sql)
            for s, c, in rr2:
                if s not in data:
                    data[s] = [[], []]
                data[s][1].append(c)

        ret = ['{}:{:.02f}%'.format(self.get_source_en(s), sum(data[s][0])/sum(data[s][1])*100) for s in data if len(data[s][0]) > 0]

        if len(ret) > 0:
            warn = ','.join(ret)
            self.cleaner.set_log(logId, {'warn':'重复宝贝占比'+warn})

    # 导市场洞察
    def market(self, smonth, emonth):
        pass


    #### check ####################################################
    # clean check
    def check_clean_repeat(self):
        sql = '''
            select toYYYYMM(month) m, uniq_k, count(*) c from artificial.entity_90501_clean group by m, uniq_k having c > 1
        '''


    def stat_create_table(self, finish=False):
        tbl = self.get_entity_tbl()
        tmp_tbl = '{}_stat_tmp'.format(tbl)

        if finish == True:
            sql = 'SELECT type, month, snum FROM {} GROUP BY type, month, snum'.format(tmp_tbl)
            ret = self.cleaner.dbch.query_all(sql)

            for type, month, snum, in ret:
                sql = 'ALTER TABLE {}_stat REPLACE PARTITION (\'{}\',{},{}) FROM {}'.format(tbl, type, month, snum, tmp_tbl)
                self.cleaner.dbch.execute(sql)

            sql = 'DROP TABLE {}'.format(tmp_tbl)
            self.cleaner.dbch.execute(sql)
            return


        sql = '''
            CREATE TABLE IF NOT EXISTS {}_stat (
                `type` String CODEC(ZSTD(1)),
                `month` UInt32 CODEC(ZSTD(1)),
                `snum` UInt8 CODEC(ZSTD(1)),
                `cid` UInt32 CODEC(ZSTD(1)),
                `sid` UInt32 CODEC(ZSTD(1)),
                `name` Array(String) CODEC(ZSTD(1)),
                `count` Int64 CODEC(ZSTD(1)),
                `sales` Int64 CODEC(ZSTD(1)),
                `num` Int64 CODEC(ZSTD(1))
            ) ENGINE = MergeTree() PARTITION BY (type, month, snum) ORDER BY (cid, sid, name)
            SETTINGS index_granularity = 8192,parts_to_delay_insert=50000,parts_to_throw_insert=1000000,max_parts_in_total=1000000,min_bytes_for_wide_part=0,min_rows_for_wide_part=0,storage_policy='disk_group_1'
        '''.format(tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'DROP TABLE IF EXISTS {}'.format(tmp_tbl)
        self.cleaner.dbch.execute(sql)

        sql = 'CREATE TABLE {} AS {}_stat'.format(tmp_tbl, tbl)
        self.cleaner.dbch.execute(sql)

        return tmp_tbl


    def get_stat_month_cid_sid(self):
        tbl = self.get_entity_tbl()
        sql = 'SELECT distinct(toYYYYMM(pkey)) FROM {}_parts'.format(tbl)
        rr1 = self.cleaner.dbch.query_all(sql)

        sql = 'SELECT snum, cid, sid FROM {}_parts GROUP BY snum, cid, sid'.format(tbl)
        rr2 = self.cleaner.dbch.query_all(sql)

        klist = []
        for month, in rr1:
            for snum, cid, sid, in rr2:
                klist.append([month, snum, cid, sid])
        return klist
        # 所有统计断掉的月份等维度都需要用0值填充 不然环比同比会异常


    # top前n的交易属性
    # select name[1] n, sum(count) c, sum(sales), sum(num) from artificial.entity_90776_origin_stat where type = 'item trade props' group by n order by c desc limit 10
    # 占总销售额x% select sum(sign), sum(sales*sign), sum(num*sign) from artificial.entity_90776_origin_parts

    # 交易属性x的前topn select name[2] n, sum(count) c, sum(sales), sum(num) from artificial.entity_90776_origin_stat where type = 'item trade props' and name[1]='颜色分类' group by n order by c desc limit 10
    def stat_entity_trade(self):
        tmp_tbl = self.stat_create_table()

        type = 'item trade props'
        tbl = self.get_entity_tbl()
        sql = '''
            INSERT INTO {} (type, month, snum, cid, sid, name, count, sales, num)
            SELECT \'{}\', toYYYYMM(pkey) m, snum, cid, sid, [n, v], sum(sign), sum(sales*sign), sum(num*sign)
            FROM {}_parts Array Join trade_props.name as n, trade_props.value as v
            GROUP BY m, snum, cid, sid, n, v
        '''.format(tmp_tbl, type, tbl)
        self.cleaner.dbch.execute(sql)

        self.stat_create_table(finish=True)


    def stat_entity_props(self):
        tmp_tbl = self.stat_create_table()

        type = 'item props'
        tbl = self.get_entity_tbl()
        sql = '''
            INSERT INTO {} (type, month, snum, cid, sid, name, count, sales, num)
            SELECT \'{}\', toYYYYMM(pkey) m, snum, cid, sid, [n, v], count(*), sum(sales*sign), sum(num*sign)
            FROM {}_parts Array Join props.name as n, props.value as v
            GROUP BY m, snum, cid, sid, n, v
        '''.format(tmp_tbl, type, tbl)
        self.cleaner.dbch.execute(sql)

        self.stat_create_table(finish=True)


    def stat_entity(self):
        tmp_tbl = self.stat_create_table()

        type = 'item'
        tbl = self.get_entity_tbl()
        sql = '''
            INSERT INTO {} (type, month, snum, cid, sid, name, count, sales, num)
            SELECT \'{}\', toYYYYMM(pkey) m, snum, cid, sid, [], count(*), sum(sales*sign), sum(num*sign)
            FROM {}_parts GROUP BY m, snum, cid, sid
        '''.format(tmp_tbl, type, tbl)
        self.cleaner.dbch.execute(sql)

        self.stat_create_table(finish=True)


    def stat_clean_sp(self):
        tmp_tbl = self.stat_create_table()

        type = 'clean'
        tbl = self.get_clean_tbl()
        poslist = self.cleaner.get_poslist()
        for pos in poslist:
            sql = '''
                INSERT INTO {} (type, month, snum, cid, sid, name, count, sales, num)
                SELECT \'{}\', toYYYYMM(pkey) m, snum, cid, sid, [\'{}\', sp{p}], count(*), sum(sales), sum(num)
                FROM {} GROUP BY m, snum, cid, sid, sp{p}
            '''.format(tmp_tbl, type, poslist[pos]['name'], tbl, p=pos)
            self.cleaner.dbch.execute(sql)

        self.stat_create_table(finish=True)


    def stat_output_sp(self):
        tmp_tbl = self.stat_create_table()

        type = 'output'
        tbl = self.get_output_tbl()
        poslist = self.cleaner.get_poslist()
        for pos in poslist:
            sql = '''
                INSERT INTO {} (type, month, snum, cid, sid, name, count, sales, num)
                SELECT \'{}\', toYYYYMM(pkey) m, snum, cid, sid, [\'{}\', sp{p}], count(*), sum(sales), sum(num)
                FROM {} GROUP BY m, snum, cid, sid, sp{p}
            '''.format(tmp_tbl, type, poslist[pos]['name'], tbl, p=pos)
            self.cleaner.dbch.execute(sql)

        self.stat_create_table(finish=True)


    ## 按照指定维度提取数据
    # type 报表类型
    # smonth emonth 时间范围
    # groupby ['month', 'snum', 'cid', 'sid', 'name']
    # where {'snum':1, 'cid':[1,2,3]}
    # test:
    #    ent.get_plugin().get_stat('item count', '2019-01-01', '2031-01-01', group=['month'], where={'snum':[1]})
    #    ent.get_plugin().get_stat('item count', '2019-01-01', '2031-01-01', group=['month', 'sid'], where={'snum':[1]})
    #    ent.get_plugin().get_stat('item count', '2019-01-01', '2031-01-01', group=['month', 'cid'])
    def get_stat(self, type, smonth, emonth, cols, where=None):
        tbl = self.get_entity_tbl()

        smonth = datetime.datetime.strptime(smonth, "%Y-%m-%d").strftime('%Y%m')
        emonth = datetime.datetime.strptime(emonth, "%Y-%m-%d").strftime('%Y%m')

        where = 'WHERE month>={} AND month<{} AND type={} AND ({})'.format(smonth, emonth, type, where or '1')


        group = list({'month', 'snum', 'cid', 'sid', 'name'}.difference(set(cols)))
        group = '' if len(group) == 0 else 'group by {}'.format(','.join(group))

        compare = ['0']
        for col in cols:
            if col == 'month':
                compare.append('{n}-neighbor({n}, -1) not in (1, 89)'.format(n=col))
            else:
                compare.append('{n}!=neighbor({n}, -1)'.format(n=col))

        sql = '''
            SELECT {cols},
                sc, if({compare}, 0, neighbor(sc, -1)) as pre_sc, (sc-pre_sc)/if(sc=0,1,sc)*100 ratem_sc,
                ss, if({compare}, 0, neighbor(ss, -1)) as pre_ss, (ss-pre_ss)/if(ss=0,1,ss)*100 ratem_ss,
                sn, if({compare}, 0, neighbor(sn, -1)) as pre_sn, (sn-pre_sn)/if(sn=0,1,sn)*100 ratem_sn
            FROM (
                SELECT {cols}, sum(sales) ss, sum(num) sn, sum(count) sc FROM {}_stat WHERE {} AND month >= {} AND month < {} {} ORDER BY {}
            )
        '''.format(tbl, ' AND '.join(f_where), smonth, emonth, groupby, ','.join(order), cols=','.join(cols), compare=' OR '.join(compare))
        ret = self.cleaner.dbch.query_all(sql, where_v)

        return ret


    def set_sku_default_val(self):
        tbl = self.cleaner.get_tbl()
        pname, ptbl = self.get_brush_product_tbl()
        db26 = self.cleaner.get_db(pname)

        bname, btbl = self.get_b_tbl()
        cname, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cname)

        sql = 'SELECT spid FROM dataway.clean_props WHERE eid = {} AND show_in_brush != 1'.format(self.cleaner.eid)
        ret = db26.query_all(sql)
        bpos_ids = [v for v, in ret]

        poslist = self.cleaner.get_poslist()
        pos_ids = [pos for pos in poslist if pos not in bpos_ids and poslist[pos]['sku_default_val'] == 1]

        sql = '''
            SELECT {}, pid, visible FROM {} WHERE (visible is NULL or visible = 0) AND alias_all_bid > 0 AND pid IN (SELECT pid FROM {t} WHERE flag = 2 UNION ALL SELECT pid FROM {t}_split)
        '''.format(','.join(['spid{}'.format(pos) for pos in pos_ids]), ptbl, t=tbl)
        ret = db26.query_all(sql)

        for v in ret:
            pid, visible = v[-2], v[-1]
            pids = [pos for i, pos in enumerate(pos_ids) if v[i] == '' or visible == 2]
            res = {}

            if len(pids) == 0:
                continue

            # 先取非拆套包的
            sql = '''
                SELECT {}
                FROM {} WHERE uuid2 IN (
                    SELECT uuid2 FROM {} WHERE similarity > 0 AND split_rate = 1 AND pid = {}
                )
            '''.format(
                ',\n'.join(['arraySort((x, y) -> y, groupArray(sp{p}), arrayEnumerateUniq(groupArray(sp{p})))[-1]'.format(p=pos) for pos in pids]),
                ctbl, btbl, pid
            )
            rrr = cdba.query_all(sql)
            res = {pos:rrr[0][i] for i, pos in enumerate(pids) if rrr[0][i] != ''}
            if len(res) > 0:
                visible = 1
            else:
                visible = 2

            # 没取到再用拆套包的
            pids = [pos for pos in pids if pos not in res]
            if len(pids) > 0:
                sql = '''
                    SELECT {}
                    FROM {} WHERE uuid2 IN (
                        SELECT uuid2 FROM {} WHERE similarity > 0 AND split < 1 AND pid = {}
                    )
                '''.format(
                    ',\n'.join(['arraySort((x, y) -> y, groupArray(sp{p}), arrayEnumerateUniq(groupArray(sp{p})))[-1]'.format(p=pos) for pos in pids]),
                    ctbl, btbl, pid
                )
                rrr = cdba.query_all(sql)
                rrr = {pos:rrr[0][i] for i, pos in enumerate(pids) if rrr[0][i] != ''}
                res.update(rrr)

            if len(res) > 0:
                ks = res.keys()
                sql = 'UPDATE {} SET visible={},{} WHERE pid = {}'.format(ptbl, visible, ','.join(['spid{}=%s'.format(k) for k in ks]), pid)
                db26.execute(sql, tuple([res[k] for k in ks]))


    def get_cachex(self):
        return {}


    def hotfix_new(self, dtbl, ddba, prefix):
        pass


    def transform_new(self, dtbl, ddba):
        pass


    def finish_new(self, dtbl, ddba, prefix):
        pass


    def update_sp(self, tbl, dba, poslist, prefix, ver):
        return True


def copy_data(part, atbl, btbl, colsa, colsb=None, other=None, witth=None):
    dba = app.get_clickhouse('chsop')
    dba.connect()

    colsa = '`'+'`,`'.join([k for k in colsa])+'`'
    colsb = colsa if not colsb else ','.join(colsb)

    print('Run task %s (%s)...' % (part, getpid()))
    start = time.time()
    sql = '''
        INSERT INTO {} ({}) {} SELECT {}
        FROM {} WHERE _partition_id='{}' {}
        SETTINGS max_threads=1, min_insert_block_size_bytes=512000000, max_insert_threads=1
    '''.format(btbl, colsa, witth or '', colsb, atbl, part, other or '')
    dba.execute(sql)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (part, (end - start)))


def copy_data2(source, pkey, cid, atbl, btbl, colsa, colsb=None, other=None, witth=None):
    dba = app.get_clickhouse('chsop')
    dba.connect()

    colsa = '`'+'`,`'.join([k for k in colsa])+'`'
    colsb = colsa if not colsb else ','.join(colsb)

    print('Run task %s %s %s (%s)...' % (source, pkey, cid, getpid()))
    start = time.time()
    sql = '''
        INSERT INTO {} ({}) {} SELECT {}
        FROM {} WHERE source={} AND pkey='{}' AND cid={} {}
        SETTINGS max_threads=1, min_insert_block_size_bytes=512000000, max_insert_threads=1
    '''.format(btbl, colsa, witth or '', colsb, atbl, source, pkey, cid, other or '')
    dba.execute(sql)
    end = time.time()
    print('Task %s %s %s runs %0.2f seconds.' % (source, pkey, cid, (end - start)))