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

        clean_flag = self.cleaner.last_clean_flag() + 1

        # uuids = []
        sales_by_uuid={}
        cc = {}

        sp1_lists = ['Body care', 'Creams', 'Emulsions & Fluids', '其它', 'Masks & Exfoliators', 'Women Fragrance', 'Men skincare', 'Other Fragrance', 'Men Fragrance', 'Eyes', 'Face', 'Lips', 'Skincare sets', 'Suncare', 'Essences & Serums', 'Lotions & Toners', 'Eye & Lip care', 'Cleansing', 'Nails', 'Palettes & Sets', 'Makeup Accessories']

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1_lists:
                uuids = []

                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source  =12
                '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)
                self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
                cc['{}+{}'.format(ssmonth, each_sp1)] = [len(uuids)]

        for i in cc:
            print (i, cc[i])

        # exit()

        return True
