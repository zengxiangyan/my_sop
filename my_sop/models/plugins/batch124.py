import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):

    def brush(self, smonth, emonth):
        sids = [6111945,
                9292307,
                10147956,
                11116212,
                12360236,
                12815168,
                19975183,
                60733413,
                61699379,
                61704175,
                66272626,
                67205748,
                67268705,
                69719994,
                139730129,
                162988711,
                163647538,
                165297969,
                167095446,
                167805789,
                167891024,
                168546218,
                176881288,
                176987319,
                177033651,
                181932507,
                185497853,
                185958572,
                186043776,
                186725096,
                186804232,
                186821601,
                187018262,
                188131757,
                188301666,
                189596093,
                190872873,
                190883080,
                192194619,
                193230920,
                193277306,
                193803051,
                194641501]
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        uuids = []
        sales_by_uuids = {}
        c = []
        for sid in sids:
            cc = 0
            where = 'sid = {}'.format(sid)
            ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where)
            sales_by_uuids.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1 in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)
                cc = cc + 1
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            c.append([sid,cc])

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuids)
        print('add new brush {}'.format(len(set(uuids))))
        for i in c:
            print(i)
        return True




