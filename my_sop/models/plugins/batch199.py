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

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()

        sales_by_uuid = {}
        db = self.cleaner.get_db(aname)
        rets = []
        cc = {}
        clean_flag = self.cleaner.last_clean_flag() + 1

        sources = ['source = 1 and shop_type<20', 'source = 1 and shop_type>20', 'source = 2']
        sp1s = ['可穿戴设备', '器械', '床品']

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources:
                    uuids = []
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and {each_source} and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\' group by alias_all_bid order by ss desc limit 12
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_source=each_source,
                                               ssmonth=ssmonth, eemonth=eemonth)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                            uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1='{each_sp1}' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and {each_source}
                                            '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1,
                                                       each_source=each_source, ssmonth=ssmonth, eemonth=eemonth)
                        ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=10, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if self.skip_brush(source, tb_item_id, p1):
                                continue
                            uuids.append(uuid2)

                    cc['{}~{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp1, each_source, 'top12brand')] = [len(uuids)]
                    self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)



        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources:
                    uuids = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and {each_source}
                                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, ssmonth=ssmonth,
                                               eemonth=eemonth)
                    ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=110, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)
                    cc['{}~{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp1, each_source, 'top110')] = [len(uuids)]
                    self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in [['2020-10-01', '2021-10-01']]:
            uuids = []
            where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'其它\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\')
                            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=50, where=where)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
            cc['{}~{}+{}+{}+{}'.format(ssmonth, eemonth, '其它', '所有渠道', 'top50')] = [len(uuids)]
            self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in [['2020-10-01', '2021-10-01']]:
            uuids = []
            where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'其它\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\')
                            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=50, where=where, orderBy='rand()')
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
            cc['{}~{}+{}+{}+{}'.format(ssmonth, eemonth, '其它', '所有渠道', 'Random50')] = [len(uuids)]
            self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)




        for i in cc:
            print (i, cc[i])

        exit()

        return True