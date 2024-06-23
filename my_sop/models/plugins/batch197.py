import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_0928(self, smonth, emonth,logId=-1):

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}


        clean_flag = self.cleaner.last_clean_flag() + 1

        lists = [['source=1 and shop_type>20', 192877849, 150266],
                 ['source=1 and shop_type>20', 12046952, 319425],
                 ['source=2', 1000001759, 319425],
                 ['source=1 and shop_type>20',	763058,	86061],
                 ['source=1 and shop_type>20',	9281929,	150266],
                 ['source=1 and shop_type>20',	9281929,	319425],
                 ['source=2',	1000003663,	150266],
                 ['source=1 and shop_type>20',	763058,	279434],
                 ['source=1 and shop_type>20',	192252097,	431526],
                 ['source=2',	1000004064,	86061],
                 ['source=2',	1000004064,	279434],
                 ['source=1 and shop_type>20',	9281929,	279434],
                 ['source=1 and shop_type>20',	9281929,	86061],
                 ['source=1 and shop_type>20',	9282014,	51946],
                 ['source=2',	43023,	431526],
                 ['source=1 and shop_type>20',	9281929,	51946],
                 ['source=2',	1000001811,	431526],
                 ['source=2',	10383714,	431526],
                 ['source=2',	43023,	51946],
                 ['source=1 and shop_type>20', 192168364, 51946]]

        for each_source, each_sid, each_bid in lists:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1=\'洗护凝珠\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source} and sid=\'{each_sid}\' and alias_all_bid=\'{each_bid}\'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid, each_bid=each_bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('*********',len(uuids))

        return True

    def brush(self, smonth, emonth,logId=-1):

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}


        clean_flag = self.cleaner.last_clean_flag() + 1

        # lists = [['source=1 and shop_type>20', 168612571, 53146],
        #          ['source=2', 1000002791, 53146],
        #          ['source=2',	197341,	53146]]

        lists = [['source=1 and shop_type>20', 9281929, 53146]]

        for each_source, each_sid, each_bid in lists:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1 in(\'洗衣液\',\'衣物除菌剂\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source} and sid=\'{each_sid}\' and alias_all_bid=\'{each_bid}\'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid, each_bid=each_bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('*********',len(uuids))

        return True

