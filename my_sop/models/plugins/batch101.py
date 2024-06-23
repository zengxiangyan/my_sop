import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import numpy as np

class main(Batch.main):

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sp5s = ['雅诗兰黛小棕瓶肌透修护眼部精华霜15ml', '雅诗兰黛即时修护特润精华露100ml', '雪花秀润致优活肌底精华露90ml']

        sales_by_uuid_all = {}
        uuids = []

        for sp5 in sp5s:
            sql = '''
            SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp5 = \'{}\'
            '''.format(self.cleaner.eid, smonth, emonth, sp5)
            uniq_k_ret = self.cleaner.dbch.query_all(sql)
            uniq_k = [str(v[0]) for v in uniq_k_ret]
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=120, where='uniq_k in ({})'.format(','.join(uniq_k)))
            sales_by_uuid_all.update(sales_by_uuid)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid_all)
        print('add brush: {}'.format(len(set(uuids))))
        return True


    def brush_3(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        bids = '97604,52297'
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v[0]) for v in ret])

        uuids = []

        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth,rate=0.9, where='all_bid in ({}) and source in (\'tb\',\'tmall\')'.format(bids))
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush: {}'.format(len(set(uuids))))
        return True


    def brush_6(self, smonth, emonth):
        # batch101补充出题规则：

        cids = '121410013,121422013,121454013,121468012,121472009,121484013'

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        bids = '52297'
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v[0]) for v in ret])

        uuids = []
        where = 'all_bid in ({}) and source in (\'tb\',\'tmall\') and cid in ({})'.format(bids, cids)
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush: {}'.format(len(set(uuids))))
        return True

    def brush(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        sales_by_uuid = {}
        bids = '97604, 52297'
        where = 'alias_all_bid in ({}) and source in (\'tmall\',\'tb\') and uuid2 in (select uuid2 from {} where alias_all_bid in ({}) and sp5 = \'其它SKU\' and source=1)'.format(bids,self.get_clean_tbl(),bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.90, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        uuids = []
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        where = 'alias_all_bid in ({}) and source in (\'tmall\',\'tb\')'.format(bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.90, where=where)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        print(len(ret))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        return True

