import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sales_by_uuid = {}
        cc = {}

        sp1 = [['祛疤', 600], ['其它', 200]]

        for each_sp1, each_limit in sp1:
            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1= '{each_sp1}')
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=each_limit, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]


            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        # sp2 = [['祛疤', 950], ['其它', 50]]
        #
        # for each_sp1, each_limit in sp2:
        #     uuids = []
        #     where = '''
        #             uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1= '{each_sp1}')
        #             '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=each_limit, orderBy='rand()', where=where)
        #     sales_by_uuid.update(sales_by_uuid1)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #             continue
        #         else:
        #             uuids.append(uuid2)
        #             mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #
        #     self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        return True