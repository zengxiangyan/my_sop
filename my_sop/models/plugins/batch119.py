import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app
import math


class main(Batch.main):

    def brush_default(self, smonth, emonth):
        # 20-08-14补充出题
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 1'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        uuids = []
        bids = '47220,326010,1856905,131009,112244'
        where = 'alias_all_bid in ({})'.format(bids)
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(set(uuids))))
        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        sql = 'select name from cleaner.clean_target where batch_id = 119 and pos_id = 5'
        sp5s = [v[0] for v in self.cleaner.db26.query_all(sql)]
        top = 2000
        rand = 1000

        uuid1 = []
        uuid2 = []
        sales_by_uuids = {}
        ret, sales_by_uuid1 = self.cleaner.process_top_new_default(smonth, emonth, limit=top*0.05, where='uuid2 in (select uuid2 from {} where sp1=\'其它\')'.format(self.get_clean_tbl()))
        sales_by_uuids.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuid1.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        ret = self.cleaner.process_rand_new_default(smonth, emonth, limit=rand*0.05, where='uuid2 in (select uuid2 from {} where sp1=\'其它\')'.format(self.get_clean_tbl()))
        print('rand ret {}'.format(len(ret)))
        for uuid2, source, tb_item_id, p1, cid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuid2.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        for sp5 in sp5s:
            ret, sales_by_uuid1 = self.cleaner.process_top_new_default(smonth, emonth, limit=(top*0.15 if sp5=='复合维生素' else top*0.05),
                                                        where='uuid2 in (select uuid2 from {} where sp5=\'{}\')'.format(self.get_clean_tbl(), sp5))
            sales_by_uuids.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuid1.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            ret = self.cleaner.process_rand_new_default(smonth, emonth,  limit=(rand*0.15 if sp5=='复合维生素' else rand*0.05),
                                                        where='uuid2 in (select uuid2 from {} where sp5=\'{}\')'.format(self.get_clean_tbl(), sp5))
            print('rand ret {}'.format(len(ret)))
            for uuid2, source, tb_item_id, p1, cid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuid2.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuid1, clean_flag, 1, sales_by_uuids)
        self.cleaner.add_brush(uuid2, clean_flag, 2)
        print('add new brush {}, {}'.format(len(set(uuid1)), len(set(uuid2))))
        return True

    def brush(self, smonth, emonth):
        uuids = []
        where = 'uuid2 in (select uuid2 from {} where sp1=\'维生素\') and source !=\'tb\''.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=99999999, rate=0.8, where=where)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            # if self.skip_brush(source,tb_item_id,p1):
            #     continue
            uuids.append(uuid2)
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('119 add new brush {}'.format(len(set(uuids))))
        print(len(uuids))
        return True




