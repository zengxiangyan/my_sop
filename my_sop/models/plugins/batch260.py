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

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source =11']

        uuids_part1 = []
        for each_source in sources:
            where = '''
            c_sp5='其它' and {each_source}
            '''.format(each_source=each_source)
            ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=50, where=where)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids_part1.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part1, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids_part2 = []
        for each_source in sources:
            for each_sp5 in ['滚珠', '喷雾']:
                top_brand_sql = '''
                                select alias_all_bid,sum(sales*sign) as ss from {ctbl}  where c_sp5='{each_sp5}' and {each_source} and toYYYYMM(date) >= toYYYYMM(toDate(\'{smonth}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{emonth}\')) group by alias_all_bid order by ss desc limit 15
                                '''.format(atbl=atbl, ctbl=ctbl, each_sp5=each_sp5, smonth=smonth, emonth=emonth, each_source=each_source)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    where = '''
                            alias_all_bid = {each_bid} and c_sp5 = \'{each_sp5}\' and {each_source}
                            '''.format(ctbl=ctbl, each_bid=each_bid, each_sp5=each_sp5,  each_source=each_source, smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=15, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids_part2, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            SELECT DISTINCT sid FROM {} WHERE clean_alias_all_bid IN (
                SELECT IF(alias_bid=0,bid,alias_bid) FROM artificial.all_brand WHERE bid IN (11220)
            )
        '''.format(tbl)
        ret = dba.query_all(sql)
        ret = [v[0] for v in ret]

        if len(ret) > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp店铺分类` = 'EDT' WHERE sid IN {}
                SETTINGS mutations_sync = 1
            '''.format(tbl, ret)
            dba.execute(sql)