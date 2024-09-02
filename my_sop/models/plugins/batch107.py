import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import numpy as np

class main(Batch.main):

    def brush(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sp5s = ['CHARLIE CARRYALL']

        sales_by_uuid_all = {}
        uuids = []

        for sp5 in sp5s:
            sql = '''
            SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp5 = \'{}\'
            '''.format(self.cleaner.eid, smonth, emonth, sp5)
            uniq_k_ret = self.cleaner.dbch.query_all(sql)
            uniq_k = [str(v[0]) for v in uniq_k_ret]
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=80, where='uniq_k in ({})'.format(','.join(uniq_k)))
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
