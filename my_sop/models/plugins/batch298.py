import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth,logId=-1):
    ###月度新出题规则

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1

        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        sales_by_uuid = {}

        bid_list1 = [1624694, 6271097, 17435, 138719, 48251, 5946322, 333941, 48257, 435343, 505199]
        bid_list2 = [6228860, 6316190, 1375287, 217838, 6590160, 130062, 2845868, 5220058, 2538014, 176937, 793501]

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 11', 'source = 2']

        cc = {}

        ### V1 ##############
        uuids1 = []
        for each_source in sources:
            for each_bid in bid_list1:
                uuids1_ = []
                where = '''
                alias_all_bid = '{each_bid}' and {each_source} and c_sp1 = '芝士奶酪'
                '''.format(each_bid=each_bid, each_source=each_source)
                ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, rate=0.8, where=where)
                sales_by_uuid.update(sbs)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids1_.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids1.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}@{}@'.format(each_bid, str(each_source))] = [len(uuids1_)]

        self.cleaner.add_brush(uuids1, clean_flag, visible=1, sales_by_uuid=sales_by_uuid)

        ### V2 ##############
        uuids2 = []
        for each_source in sources:
            for each_bid in bid_list2:
                uuids2_ = []
                where = '''
                        alias_all_bid = '{each_bid}' and {each_source} and c_sp1 = '芝士奶酪'
                        '''.format(each_bid=each_bid, each_source=each_source)
                ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, rate=0.8, where=where)
                sales_by_uuid.update(sbs)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids2_.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids2.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}@{}@'.format(each_bid, str(each_source))] = [len(uuids2_)]

        self.cleaner.add_brush(uuids2, clean_flag, visible=2, sales_by_uuid=sales_by_uuid)

        ### V3 ##############
        uuids3 = []
        where = '''
        source = 1 and shop_type < 20 and shop_type > 10 and c_sp1 = '芝士奶酪' and  alias_all_bid in (1624694, 6271097, 17435, 138719, 48251, 5946322, 333941, 48257, 435343, 505199, 6228860, 6316190, 1375287, 217838, 6590160, 130062, 2845868, 5220058, 2538014, 326232, 793501)
        '''
        ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=3000, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuids3.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids3, clean_flag, visible=3, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print(i, cc[i])


        return True