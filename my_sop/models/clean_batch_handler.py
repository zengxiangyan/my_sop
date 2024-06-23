#coding=utf-8
import os
import sys
import csv
import time
import ujson
import hashlib
import multiprocessing

from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models.clean_batch import CleanBatch

class CleanBatchHandler:
    def __init__(self, batch_id):
        self.t1 = 0
        self.t2 = 0
        self.t3 = 0
        self.t4 = 0
        self.t5 = 0
        self.total = 0
        self.ctotal = 0
        self.cache = {}

        self.tbl = 'sop.entity_prod_90275_A'
        self.cache_root = '/nas/cache/'

        self.dbch = app.get_clickhouse('chmaster')
        self.dbch.connect()


    def process(v):
        return v


    def load_cache(self, sql, host):
        file = self.cache_root + hashlib.md5(sql.encode(encoding='utf-8')).hexdigest()
        os.system('''/bin/clickhouse-client -h{} -udefault --query="{} format CSV"  sed 's/"//g'> {}'''.format(host, sql, file))
        return file


    def set_cache(self, k, v):
        self.cache[k] = v


    def get_cache(self, v):
        keys = ['source', 'cid', 'sid', 'all_bid', 'item_id', 'p1', 'p2', 'name']
        key = '-'.join([str(v[k]) for k in keys])
        # print(key)
        return key, None if key not in self.cache else self.cache[key]


    # def safe_insert(self, key, val, cols):
    #     cols = self.get_cols(tbl or self.get_clean_tbl(), force=False)
    #     if key in cols:
    #         if cols[key].find('Int') != -1:
    #             val = int(val or 0)
    #         if cols[key].find('Float') != -1:
    #             val = float(val or 0)
    #         if cols[key] == 'LowCardinality(String)':
    #             val = str(val or '')
    #         if cols[key] == 'Date':
    #             val = datetime.datetime.strptime(val, "%Y-%m-%d")
    #         if cols[key] == 'DateTime':
    #             val = datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    #     #if isinstance(val, datetime.date):
    #     #    val = val.strftime('%Y-%m-%d')

    #     # if isinstance(val, int):
    #     #     val = str(val)
    #     return val


    def process_one(self, item):
        trade_prop_all, trade_props_name, trade_props_value = {}, eval(item['trade_props.name']), eval(item['trade_props.value'])
        for i, k in enumerate(trade_props_name):
            v = trade_props_value[i]
            if k not in trade_prop_all:
                trade_prop_all[k] = v
            elif not isinstance(trade_prop_all[k], list):
                trade_prop_all[k] = [trade_prop_all[k], v]
            else:
                trade_prop_all[k].append(v)
        item['trade_prop_all'] = trade_prop_all

        prop_all, props_name, props_value = {}, eval(item['props.name']), eval(item['props.value'])
        for i, k in enumerate(props_name):
            v = props_value[i]
            if k not in prop_all:
                prop_all[k] = v
            elif not isinstance(prop_all[k], list):
                prop_all[k] = [prop_all[k], v]
            else:
                prop_all[k].append(v)

        prop_all_full, trade_props_full_name, trade_props_full_value = {}, eval(item['trade_props_full.name']), eval(item['trade_props_full.value'])
        for i, k in enumerate(trade_props_full_name):
            v = trade_props_full_value[i]
            if k not in prop_all_full:
                prop_all_full[k] = v
            elif not isinstance(prop_all_full[k], list):
                prop_all_full[k] = [prop_all_full[k], v]
            else:
                prop_all_full[k].append(v)

        for k in prop_all_full:
            prop_all[k] = prop_all_full[k]
        item['prop_all'] = prop_all

        item['snum'] = int(item['source'])
        item['shop_type'] = int(item['shop_type'])
        item['source'] = ['', 'ali', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy'][item['snum']]
        if item['source'] == 'ali':
            item['source'] = 'tb' if item['shop_type'] < 20 and item['shop_type'] > 10 else 'tmall'


    def start(self, smonth, emonth):
        t = time.time()

        cols = [
            'source', 'item_id', 'name', 'sid', 'all_bid', 'uuid2', 'ver', 'pkey', 'date', 'cid', 'real_cid', 'sku_id', 'shop_type',
            'brand', 'alias_all_bid', 'region', 'region_str', 'price', 'org_price', 'promo_price', 'trade', 'num', 'sales', 'img',
            'trade_props.name', 'trade_props.value', 'trade_props_full.name', 'trade_props_full.value', 'props.name', 'props.value', 'created'
        ]
        sql = '''
            SELECT {},
                arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) p1,
                arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',props.value)))) p2
            FROM {tbl} WHERE uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE sign < 0)
        '''.format(','.join(cols), tbl=self.tbl)
        cols += ['p1', 'p2']

        t1s = time.time()
        file = self.load_cache(sql, self.dbch.host)
        self.t1 += time.time() - t1s

        filer = open(file, 'r')
        filew = open(file+'.res', 'w+')

        # pool = multiprocessing.Pool(processes=4)

        data, end, limit = [], False, 10000
        while not end:
            t2s = time.time()
            line = filer.readline()
            self.t2 += time.time() - t2s

            if line:
                data.append(line)
            else:
                end = True

            if len(data) >= limit or end:
                t3s = time.time()
                data = csv.reader(data, delimiter=',')
                self.t3 += time.time() - t3s

                # pool.apply_async(CleanBatchHandler.process, ([v for v in data], ))

                for v in data:
                    self.total += 1

                    v = {k: v[i] for i, k in enumerate(cols)}
                    key, cache = self.get_cache(v)
                    if cache is None:
                        self.ctotal += 1

                        v = self.process_one(v)

                        t4s = time.time()
                        cache = ujson.dumps(v, ensure_ascii=False)
                        self.t4 += time.time() - t4s

                        self.set_cache(key, cache)

                    t5s = time.time()
                    filew.write(cache+'\n')
                    self.t5 += time.time() - t5s

                data = []

        # pool.close()
        # pool.join()

        print("Done total:", self.total, 'ctotal', self.ctotal, 't', time.time()-t, 't1', self.t1, 't2', self.t2, 't3', self.t3, 't4', self.t4, 't5', self.t5)