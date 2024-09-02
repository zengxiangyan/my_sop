import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth):
        sales_by_uuid = {}
        uuids1 = []
        sql = 'select name from cleaner.clean_sub_batch where batch_id = {}'.format(self.cleaner.bid)
        sub_batch = [v[0] for v in self.cleaner.db26.query_all(sql)]
        if '其它' not in sub_batch:
            sub_batch.append('其它')
        count = dict.fromkeys(sub_batch, 0)
        for sb in sub_batch:
            where = '''
            uuid2 in (select uuid2 from {} where sp1 = '{}' and date >= '{}' AND date < '{}')
            and source in ('tmall','tb')
            '''.format(self.get_clean_tbl(), sb, smonth, emonth)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=False):
                    continue
                uuids1.append(uuid2)
                count[sb] = count[sb] + 1
        print('add brush 90%')
        print(count)
        print('total', len(uuids1))
        return True

