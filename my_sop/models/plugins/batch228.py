import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_old(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sp1s = ['Accessories',	'Body care',	'Cleansing',	'Lotions & Toners',	'Essences & Serums',	'Creams',	'Masks & Exfoliators',	'Suncare',	'Eye & Lip care',	'Skincare sets',	'Men skincare',	'Emulsions & Fluids',	'Nails',	'Makeup Accessories',	'Face',	'Eyes',	'Lips',	'Palettes & Sets',	'Women Fragrance',	'Other Fragrance',	'Men Fragrance',	'其它']
        uuids = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where c_sp1 = '{each_sp1}' and pkey>='{ssmonth}' and pkey<'{eemonth}')  and source=14 and alias_all_bid in (219205,4791798,182627,266860,53268,20645,3779,156557,53384,52297,23253,2503,11092,8271,14362,53946,52238,181468,105167,171662,71334,493003,218566,246499,51962,106548,47858,245844,199607,52567,7012,218562,6758,529153,6404,31466,223274,180407,218502,218724,2169320,218970,202229,68367,5324,11697,218520,53312,16129,52188,218526,124790,52501,5316,53191,52711,97604,113650,113866,404633,44785,2458,218482,1052052)
                '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]


        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        return True

    ### 200 以后新接口默认出题
    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}



        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sp1s = ['Accessories',	'Body care',	'Cleansing',	'Lotions & Toners',	'Essences & Serums',	'Creams',	'Masks & Exfoliators',	'Suncare',	'Eye & Lip care',	'Skincare sets',	'Men skincare',	'Emulsions & Fluids',	'Nails',	'Makeup Accessories',	'Face',	'Eyes',	'Lips',	'Palettes & Sets',	'Women Fragrance',	'Other Fragrance',	'Men Fragrance',	'其它']
        uuids = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                where = '''
                c_sp1 = '{each_sp1}' and toYYYYMM(date) >= toYYYYMM(toDate('{ssmonth}')) and toYYYYMM(date) < toYYYYMM(toDate('{eemonth}'))  and source=14 and alias_all_bid in (219205,4791798,182627,266860,53268,20645,3779,156557,53384,52297,23253,2503,11092,8271,14362,53946,52238,181468,105167,171662,71334,493003,218566,246499,51962,106548,47858,245844,199607,52567,7012,218562,6758,529153,6404,31466,223274,180407,218502,218724,2169320,218970,202229,68367,5324,11697,218520,53312,16129,52188,218526,124790,52501,5316,53191,52711,97604,113650,113866,404633,44785,2458,218482,1052052)
                '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth)
                ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]


        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        return True