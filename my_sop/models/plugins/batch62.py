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
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=15000)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(str(uuid2))

        # sql = 'select uniq_k from entity_{}_clean where sp1 = \'乐高玩具\''.format(self.cleaner.eid)
        # uniq_ks = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        #
        # sql = '''
        #         select uuid2 from entity_{}_origin_parts where uuid2 in ({}) and uniq_k in ({})
        #       '''.format(self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in uuids]), ','.join([str(t) for t in uniq_ks]))
        sql = 'select distinct(b.uuid2) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid2 in ({}) and a.sp1 = \'乐高玩具\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in uuids]))
        uuids_lego = [v[0] for v in self.cleaner.dbch.query_all(sql)]

        uuids_600 = []
        c = 0
        for i in uuids_lego:
            if c < 600:
                if i not in uuids_600:
                    uuids_600.append(i)
                    c = c + 1

        self.cleaner.add_brush(uuids_600, clean_flag, 1, sales_by_uuid=sales_by_uuid)


    def brush_new(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=15000)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(str(uuid2))
        sql = '''
        select uuid2 from {} where sp1 = '乐高玩具' and uuid2 in ({})
        '''.format(self.get_clean_tbl(), ','.join(['\''+str(t)+'\'' for t in uuids]))
        uuids_lego = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_600 = []
        c = 0
        for i in uuids_lego:
            if c < 600:
                if i not in uuids_600:
                    uuids_600.append(i)
                    c = c + 1
        self.cleaner.add_brush(uuids_600, clean_flag, 1, sales_by_uuid=sales_by_uuid)