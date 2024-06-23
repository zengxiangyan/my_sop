import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):


    def brush(self, smonth, emonth, logId=-1):
        ###月度出题规则

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}

        # Part1



        sources = ['source = 1 and shop_type = 11', 'source = 1 and shop_type=12', 'source = 1 and shop_type in (21,23,25,26)','source = 1 and shop_type in (22,24,27)',
                   'source = 2 and shop_type in (11,12)', 'source = 2 and shop_type in (21,22)']

        uuids_part1 = []
        for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
            for each_source in sources:
                uuid_tg = []
                top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp1='浓缩咖啡' and pkey>'{ssmonth}' and pkey <'{eemonth}') and {each_source} and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\' group by alias_all_bid order by ss desc limit 7
                                            '''.format(atbl=atbl, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    where = '''
                            uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp1 = '浓缩咖啡' and pkey>'{ssmonth}' and pkey <'{eemonth}') and {each_source}
                            '''.format(ctbl=ctbl, each_source=each_source, ssmonth=ssmonth, eemonth=eemonth, each_bid=each_bid)
                    ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=5, where=where)
                    sales_by_uuid.update(sbs)
                    rets.append(ret)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids_part1.append(uuid2)
                        uuid_tg.append(uuid2)

                cc['{}~{}@{}'.format(ssmonth, eemonth,  str(each_source))] = [len(uuid_tg)]

        print(len(uuids_part1))
        self.cleaner.add_brush(uuids_part1, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids_part2 = []

        for each_sp1, each_limit in [['浓缩咖啡', 300], ['其它', 100]]:
            uuid_tg = []
            where = '''
                    uuid2 in (select uuid2 from {ctbl} where  c_sp1 = '{each_sp1}' and pkey>'2019-01-01' and pkey <'2022-01-01')
                    '''.format(ctbl=ctbl,  each_sp1=each_sp1)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=each_limit, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids_part2.append(uuid2)
                uuid_tg.append(uuid2)
            cc['{}~{}@{}'.format(smonth, emonth, str(each_sp1))] = [len(uuid_tg)]

        print(len(uuids_part2))
        self.cleaner.add_brush(uuids_part2, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print(i, cc[i])


        return True



