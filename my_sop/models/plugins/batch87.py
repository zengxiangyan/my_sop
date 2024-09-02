import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush(self, smonth, emonth):
        if self.cleaner.check(smonth, emonth) == False:
            return True

        clean_flag = self.cleaner.last_clean_flag() + 1

        uuids = []
        sql = '''
            SELECT sp3 FROM artificial.entity_{}_clean WHERE month >= '{}' AND month < '{}' GROUP BY sp3
        '''.format(self.cleaner.eid, smonth, emonth)
        ret = self.cleaner.dbch.query_all(sql)
        for sp3, in ret:
            where = 'SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp3 = \'{}\''.format(
                self.cleaner.eid, smonth, emonth, sp3
            )
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=100, where='uniq_k in ({})'.format(where))
            uuids += [v[0] for v in ret]

        ret = self.cleaner.process_rand(smonth, emonth, limit=1000)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush([v[0] for v in ret], clean_flag, 2)

        print('add new brush {} {}'.format(len(uuids), len(ret)))

        return True