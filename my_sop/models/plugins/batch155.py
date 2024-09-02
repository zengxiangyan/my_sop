import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch

class main(Batch.main):

    def brush_0220(self, smonth, emonth):
        sources = [v[0] for v in self.cleaner.dbch.query_all('select distinct(source) from {}'.format(self.get_entity_tbl()+'_parts'))]
        sp2s = ['Makeup', 'Skin Care', 'Fragrance']
        sales_by_uuid = {}
        uuids = []
        for sp2 in sp2s:
            for source in sources:
                where = 'uuid2 in (select uuid2 from {} where sp2 = \'{}\') and source = \'{}\''.format(self.get_clean_tbl(), sp2, source)
                ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, limit=100, where=where)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        return True

    def brush(self, smonth, emonth, logId=-1):
        where = '''tb_item_id in ('68988573459','62755436054','68988573460','70082428484','69855670661','574890375836')'''
        ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, where=where)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid1)
        return True

