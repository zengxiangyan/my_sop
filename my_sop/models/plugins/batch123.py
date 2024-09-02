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
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        where = 'uuid2 in (select uuid2 from {} where sp1=\'remifemin人工确认\')'.format(self.get_clean_tbl())
        uuid1 = []
        sales_by_uuids = {}
        ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, limit=999999, where=where)
        sales_by_uuids.update(sales_by_uuid1)

        for uuid2, source, tb_item_id, p1 in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuid1.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuid1, clean_flag, 1, sales_by_uuids)
        print('add new brush {}'.format(len(set(uuid1))))
        return True