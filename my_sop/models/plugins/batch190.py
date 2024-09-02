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

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()

        sales_by_uuid = {}
        db = self.cleaner.get_db(aname)
        rets = []
        cc = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []

        lists = [['605653750588', '4013558572'],
                ['52299050367', '1233569414'],
                ['68688100136', '1233569414'],
                ['32839049470', '1233569414'],
                ['601354897137', '3032547429'],
                ['601354897137', '2790577947'],
                ['592210879183', '147458640'],
                ['68729141304', '1233569414'],
                ['601487389386', '35405326'],
                ['601877067005', '3065386956'],
                ['601877067005', '2560056096'],
                ['601705180482', '2560056096'],
                ['52297193669', '1233569414'],
                ['52573904400', '1233569414']]

        for each_itemid, each_p1 in lists:
            where = '''
            item_id = '{each_itemid}' and arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value))))='{each_p1}'
            '''.format(each_itemid=each_itemid, each_p1=each_p1)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=100000, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        print(uuids)
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True