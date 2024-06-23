import sys
import time
import json
import math
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd
import csv

class main(Batch.main):

    # CREATE OR REPLACE VIEW artificial.all_brand_91130 AS
    # SELECT a.bid,IFNULL(b.brand_en_cn,'Others') name,IFNULL(b.brand_cn,a.name_cn) name_cn,IFNULL(b.brand_en,a.name_en) name_en,
    #        IFNULL(b.brand_cn,a.name_cn_front) name_cn_front,IFNULL(b.brand_en,a.name_en_front) name_en_front, IFNULL(b.manufacturer_cnen,'') factory,
    #        a.source,a.is_hot,IF(b.alias_bid IS NULL,a.bid,0) alias_bid,a.sales,a.modified,a.created
    # FROM brush.all_brand a LEFT JOIN (SELECT * FROM cleaner.clean_141_brand_link_dict WHERE del_flag = 0) b ON (a.bid=b.alias_bid);
    # CREATE OR REPLACE VIEW artificial.all_brand_91130 AS
    # SELECT a.bid,IFNULL(b.brand_en_cn,'Others') name,IFNULL(b.brand_cn,a.name_cn) name_cn,IFNULL(b.brand_en,a.name_en) name_en,
    #        IFNULL(b.brand_cn,a.name_cn_front) name_cn_front,IFNULL(b.brand_en,a.name_en_front) name_en_front, IFNULL(b.manufacturer_cnen,'') factory,
    #        a.source,a.is_hot,IF(b.alias_bid IS NULL OR b.alias_bid=0,a.bid,b.alias_bid) alias_bid,a.sales,a.modified,a.created
    # FROM artificial.all_brand a
    # LEFT JOIN (SELECT * FROM mysql('192.168.30.93', 'cleaner', 'clean_141_brand_link_dict', 'cleanAdmin', '6DiloKlm') WHERE del_flag = 0) b
    # ON (toUInt32(a.bid)=toUInt32(b.bid));
    def brush_4(self, smonth, emonth, logId=-1):
        sql = 'SELECT source, tb_item_id, real_p1, id FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}
        cc = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        sp1s = [['维生素',1], ['钙',2], ['氨糖软骨素',3]]
        for sp1,visible_check in sp1s:
            uuids = []
            uuids_update = []
            where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(self.get_clean_tbl(), sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
            cc[sp1] = len(ret)
            # for uuid2, source, tb_item_id, p1, cid in ret:
            #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
            #         uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
            #         continue
            #     uuids.append(uuid2)
            # sql = 'update {} set visible = 2, visible_check = {} where id in ({})'.format(self.cleaner.get_tbl(), visible_check, ','.join([str(v) for v in uuids_update]))
            # print(sql)
            # self.cleaner.db26.execute(sql)
            # self.cleaner.db26.commit()
            # cc.append([len(uuids_update), len(uuids)])
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=visible_check, visible=2, sales_by_uuid=sales_by_uuid1)
        print(cc)
        return True

    def brush_default_0413(self, smonth, emonth, logId=-1):
        sql = 'select distinct(sp1) from {}'.format(self.get_clean_tbl())
        dbch = self.cleaner.get_db(self.cleaner.dbchname)
        sp1s = [v[0] for v in dbch.query_all(sql)]
        # sql = 'select distinct(source) from {}'.format(self.cleaner.get_plugin().get_entity_tbl()+'_parts')
        sql = 'select distinct(source) from {}'.format(self.get_clean_tbl())
        dbch = self.cleaner.get_db(self.cleaner.dbchname)
        sources = [v[0] for v in dbch.query_all(sql)]

        where_template = 'alias_all_bid = 130885 and uuid2 in (select uuid2 from {} where sp1 = \'{}\' and source = {})'
        cc = {}
        uuids = []
        sales_by_uuid = {}
        uuids_update = []
        sql = 'SELECT source, tb_item_id, real_p1, id FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}
        for sp1 in sp1s:
            if sp1 != '其它':
                for each_source in sources:
                    c = 0
                    d = 0
                    where = where_template.format(self.get_clean_tbl(), sp1, each_source)
                    ret, sbs = self.cleaner.process_top(smonth, emonth, where=where,rate=0.9)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                            d = d + 1
                            continue
                        uuids.append(uuid2)
                        uuids.append(uuid2)
                        c = c + 1
                    cc[str(each_source) + '+' + str(sp1)] = [c,d]
        for i in cc:
            print(i, cc[i])
        sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
        print(sql)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible_check=4, visible=2, sales_by_uuid=sales_by_uuid)

    def brush_6(self, smonth, emonth, logId=-1):
        sql = 'SELECT source, tb_item_id, real_p1, id FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}
        cc = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        sp1s = [['蛋白粉',1]]
        for sp1,visible_check in sp1s:
            uuids = []
            uuids_update = []
            where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(self.get_clean_tbl(), sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, rate=0.7, limit=999999, where=where)
            print(len(ret))
            # for uuid2, source, tb_item_id, p1, cid in ret:
            #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
            #         uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
            #         continue
            #     uuids.append(uuid2)
            # sql = 'update {} set visible = 2, visible_check = {} where id in ({})'.format(self.cleaner.get_tbl(), visible_check, ','.join([str(v) for v in uuids_update]))
            # print(sql)
            # self.cleaner.db26.execute(sql)
            # self.cleaner.db26.commit()
            # cc.append([len(uuids_update), len(uuids)])
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=visible_check, visible=2, sales_by_uuid=sales_by_uuid1)
        # print(cc)
        print(len(uuids))
        return True

    def brush_5(self, smonth, emonth, logId=-1):
        count = {}
        bids = [402002,333391,130885,726329,5473733]
        clean_flag = self.cleaner.last_clean_flag() + 1
        sales_by_uuid = {}
        uuids = []
        for each_bid in bids:
            where = 'alias_all_bid = {}'.format(each_bid)
            ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, rate=0.95, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            c = 0
            for uuid2, source, tb_item_id, p1, cid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
                c = c + 1
            count[each_bid] = c
        print(len(uuids))
        for i in count:
            print(i,count[i])
        return True

    def brush_0423(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        cc = {}

        where_source = 'uuid2 not in (select uuid2 from {} where source = 1 and shop_type = 11)'.format(atbl)
        sp1s = [['钙', 1], ['氨糖软骨素', 2], ['维生素', 3], ['蛋白粉', 4]]
        for sp1, visible_check in sp1s:
            uuids = []
            uuids_update = []
            where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(ctbl, sp1, where_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.7, where=where)
            print(len(ret))
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                    continue
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
            sql = 'update {} set visible = 1, visible_check = {} where id in ({})'.format(self.cleaner.get_tbl(), visible_check, ','.join([str(v) for v in uuids_update]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()
            cc[sp1] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, visible_check=visible_check, visible=1, sales_by_uuid=sales_by_uuid1)
        print(cc)
        return True

    def brush_0427(self, smonth, emonth, logId=-1):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        where_source = 'uuid2 not in (select uuid2 from {} where source = 1 and shop_type = 11)'.format(atbl)
        bids = [402002,333391,130885,726329,5473733]
        for each_bid in bids:
            uuids = []
            uuids_update = []
            where = 'alias_all_bid = {} and {}'.format(each_bid,where_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.9, where=where)
            print(len(ret))
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                    continue
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
            if len(uuids_update) > 0:
                sql = 'update {} set visible = 2, visible_check = 1 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)

        return True

    def brush_0427_1(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        uuids = []
        where_source = 'concat(toString(source), toString(shop_type)) != \'111\' and uuid2 in (select uuid2 from {} where sp1 = \'氨糖软骨素\')'.format(ctbl)
        bids = [131009,5699306,326236,48678,401951,130885,1856905,845348,387249,393278,48221,326010,474173,333207,333159,1531173,393267,393304,347673,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,935500,819397,207212,21150,333391,474186,4649582,387336,273629,53021,54450,272287,3919777,6272674,387381,5969283,387255,387270,387332,6279975,387129,6045512,5462839,714073,392061,392074,387333,386477,5874301,5831186,5798604,506932,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,6157753,48704,93278,47220,48210,116183,95277,341271,3847776,1067497,4730279,475116,3158961,387454,5948788,402002,387121,387135,130428,110054,59590,474182,387334,89329,1856459,3158974,932108,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,4548333,1624048,561121,387450,48572,130554,2265513,472806,4888729,156841,347244,12741,2608328,48256,3107725,130225,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,6401714,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,3923119,4823115]
        for each_bid in bids:
            where = where_source + ' and alias_all_bid = {}'.format(each_bid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.9, where=where)
            print(len(ret))
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                # if self.skip_brush(source, tb_item_id, p1):
                #     continue
                uuids.append(uuid2)
        print(len(uuids))

    def brush_0507(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where visible = 6'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        cname, ctbl = self.get_c_tbl()

        cc = {}
        for each_sp1,each_visible_check in [['鱼油',1]]:
            uuids_update = []
            uuids = []
            c = 0
            where = 'concat(toString(source), toString(shop_type)) != \'111\' and uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(ctbl, each_sp1)
            bids = [131009,5699306,112244,6212705,6536485,326236,48678,401951,130885,1856905,845348,387249,393278,48221,326010,474173,333207,333159,1531173,393267,393304,347673,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,935500,819397,207212,21150,333391,474186,4649582,4452952,387336,273629,53021,54450,272287,3919777,6272674,4589889,5134375,387381,5969283,387255,387270,387332,6279975,1531329,387129,6045512,5975591,5462839,714073,392061,392074,387333,386477,5874301,5831186,6317296,5798604,506932,2381910,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,6157753,4896603,48704,93278,47220,48210,116183,95277,341271,3847776,1067497,4730279,475116,3158961,387454,5948788,5916287,402002,387121,387135,130428,110054,59590,474182,387334,89329,1856459,3158974,932108,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,6045628,4548333,1624048,77846,561121,387450,48572,130554,2265513,472806,4888729,156841,347244,12741,2608328,48256,3107725,130225,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,6401714,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,4813680,6050119,3923119,4823115,5432508,5699136,3923106,4327168    ]
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            print(len(ret))
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if int(alias_all_bid) in bids:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                        continue
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
            if len(uuids_update) > 0:
                sql = 'update {} set visible = 5, visible_check = {} where id in ({})'.format(self.cleaner.get_tbl(), each_visible_check, ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=each_visible_check, visible=5, sales_by_uuid=sales_by_uuid1)
            print(len(uuids_update), len(uuids))
            print('add', len(uuids))

    def brush_0508(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        uuids = []
        ss_by_bid = {}
        c_by_bid = {}
        # where = 'uuid2 in (select uuid2 from {} where sp1 = \'蛋白粉\') and source = 2'.format(ctbl)
        where = 'sid in (1000073704,165534664)'
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        # print(len(ret))
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
            if alias_all_bid not in ss_by_bid:
                ss_by_bid[alias_all_bid] = 0
                c_by_bid[alias_all_bid] = 0
            ss_by_bid[alias_all_bid] = ss_by_bid[alias_all_bid] + sales_by_uuid1[uuid2]
            c_by_bid[alias_all_bid] = c_by_bid[alias_all_bid] + 1
        sorted_ss_by_bid = sorted(ss_by_bid.items(), key=lambda item: item[1], reverse=True)
        cc = 0
        for i in sorted_ss_by_bid[0:5]:
            cc = cc + c_by_bid[i[0]]
        print(len(uuids), cc)

    def brush_0528(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'select distinct(source) from {}'.format(atbl)
        dba = self.cleaner.get_db(aname)
        sources = [v[0] for v in dba.query_all(sql)]

        cc = {}
        c = 0
        dd = {}
        # for each_sp1,each_visible_check in [['鱼油',3], ['氨糖软骨素',1]]:
        # for each_sp1,each_visible_check in [['益生菌', 2]]:
        # for each_sp1,each_visible_check in [['氨糖软骨素',1]]:
        for each_sp1, each_visible_check in [['钙', 1], ['维生素', 2], ['益生菌',3]]:
            for each_source in sources:
                uuids_update = []
                uuids = []
                c = 0
                where = 'concat(toString(source), toString(shop_type)) != \'111\' and uuid2 in ' \
                        '(select uuid2 from {} where sp1 = \'{}\') and source = {}'.format(ctbl, each_sp1, each_source)
                # bids = [131009,5699306,112244,6212705,6536485,326236,48678,401951,130885,1856905,845348,387249,393278,48221,326010,474173,333207,333159,1531173,393267,393304,347673,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,935500,819397,207212,21150,333391,474186,4649582,4452952,387336,273629,53021,54450,272287,3919777,6272674,4589889,5134375,387381,5969283,387255,387270,387332,6279975,1531329,387129,6045512,5975591,5462839,714073,392061,392074,387333,386477,5874301,5831186,6317296,5798604,506932,2381910,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,6157753,4896603,48704,93278,47220,48210,116183,95277,341271,3847776,1067497,4730279,475116,3158961,387454,5948788,5916287,402002,387121,387135,130428,110054,59590,474182,387334,89329,1856459,3158974,932108,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,6045628,4548333,1624048,77846,561121,387450,48572,130554,2265513,472806,4888729,156841,347244,12741,2608328,48256,3107725,130225,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,6401714,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,4813680,6050119,3923119,4823115,5432508,5699136,3923106,4327168]
                # bids = [89329,4383558,3328242,6398898,4945587,6424205,3060767,5872165,3868461,4674278,4412254,5169959,6270663,130547,130267,1243231,4250470,401934,639624,1707050,560974]
                # bids = [326026]
                # bids = [131009,5699306,112244,6212705,6536485,326236,48678,401951,241814,4783162,6467978,326026,130885,3923168,1210010,130231,1856905,845348,387249,393278,48221,326010,474173,333207,333159,1531173,393267,393304,347673,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,935500,819397,207212,21150,333391,474186,4649582,4452952,387336,273629,53021,54450,272287,3919777,6272674,4589889,5134375,387381,5969283,387255,387270,387332,6279975,1531329,387129,6045512,5975591,5462839,714073,392061,392074,387333,386477,5874301,5831186,6317296,5798604,506932,2381910,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,6157753,4896603,48704,93278,47220,48210,116183,95277,4725845,341271,3847776,1067497,4730279,475116,3158961,387454,5948788,5916287,402002,387121,387135,130428,3472327,110054,59590,474182,387334,89329,1856459,3158974,932108,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,6045628,4548333,1624048,77846,561121,387450,48572,130554,2265513,472806,4888729,156841,347244,12741,2608328,48256,3107725,130225,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,6401714,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,4813680,6050119,3923119,4823115,5432508,5699136,3923106,4327168]
                # bids = [131009,5699306,112244,6212705,6536485,326236,48678,401951,241814,4783162,6467978,326026,130885,3923168,1210010,130231,1856905,845348,387249,393278,48221,326010,474173,333207,333159,1531173,393267,393304,347673,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,935500,819397,207212,21150,333391,474186,4649582,4452952,387336,273629,53021,54450,272287,3919777,6272674,4589889,5134375,387381,5969283,387255,387270,387332,6279975,1531329,387129,6045512,5975591,5462839,714073,392061,392074,387333,386477,5874301,5831186,6317296,5798604,506932,2381910,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,6157753,4896603,48704,93278,47220,48210,116183,95277,4725845,341271,3847776,1067497,4730279,475116,3158961,387454,5948788,5916287,402002,387121,387135,130428,3472327,110054,59590,474182,387334,89329,1856459,3158974,932108,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,6045628,4548333,1624048,77846,561121,387450,48572,130554,2265513,472806,4888729,156841,347244,12741,2608328,48256,3107725,130225,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,6401714,966656,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,4813680,6050119,3923119,4823115,5432508,5699136,3923106,4327168]
                bids = [131009,112244,326236,48678,401951,241814,326026,130885,1210010,1856905,845348,387249,393278,48221,326010,474173,333207,333159,27591,103792,393304,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,558781,819397,207212,21150,333391,474186,4452952,387336,273629,53021,47220,272287,3919777,5134375,347699,5969283,387255,387270,387332,1531329,387129,5950105,5462839,714073,392061,392074,387333,386477,5874301,5831186,5798604,506932,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,3837273,48704,93278,48210,116183,95277,401688,3847776,1067497,4730279,475116,387577,387454,5916287,402002,387121,387135,130428,3472327,110054,59590,474182,387334,89329,1856459,3158974,3458059,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,4548333,77846,561121,387450,48572,130554,2265513,393970,2888376,156841,347244,12741,2608328,48256,3107725,85409,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,966656,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,3923119,4538428,3923106,1224207]
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                print('return', len(ret))
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if int(alias_all_bid) in bids:
                        # c = c + 1
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (2,5,6):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            c = c + 1
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            k = '{}-{}-{}'.format(each_sp1,each_source,alias_all_bid)
                            if k not in dd:
                                dd[k] = 0
                            dd[k] = dd[k] + 1
                cc['{}+{}'.format(each_sp1, str(each_source))] = c
                if len(uuids_update) > 0:
                    sql = 'update {} set visible = 6, visible_check = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible_check, ','.join([str(v) for v in uuids_update]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids, clean_flag, visible_check=each_visible_check, visible=6, sales_by_uuid=sales_by_uuid1)
                print(len(uuids_update), len(uuids))
                print('add', len(uuids))
                for i in dd:
                    print(i, dd[i])

        for i in cc:
            print(i,cc[i])
        # print('ccccc', c)
        return True

    def brush_5555(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        uuids = []
        ss_by_bid = {}
        c_by_bid = {}
        where = 'sid in (1000073704,165534664)'
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.99, where=where)
        # print(len(ret))
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source,tb_item_id,p1):
                continue
            uuids.append(uuid2)
        print(len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible=2, visible_check=1, sales_by_uuid=sales_by_uuid1)
        return True

    def brush_0520(self, smonth, emonth, logId=-1):
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        uuids = []
        uuids_update = []

        ss_by_bid = {}
        c_by_bid = {}
        # where = 'source = 5 and alias_all_bid = 130885'
        item_ids = [613755751018,100004502681,100003086983,618835986799,566448012930,597906901817,4816,550230916368,591818949423,555216430553,611868980717,593238498260]
        where = 'item_id in ({}) and (concat(toString(source), toString(shop_type)) != \'111\')'.format(','.join(['\'{}\''.format(v) for v in item_ids]))
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (2,5):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            uuids.append(uuid2)
        if len(uuids_update) > 0:
            sql = 'update {} set visible = 5, visible_check = 1, created = now() where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()
        print(len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible=5, visible_check=1, sales_by_uuid=sales_by_uuid1)
        return True

    def brush_0601(self, smonth, emonth, logId=-1):
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        cname, ctbl = self.get_c_tbl()
        uuids = []
        uuids_update = []
        # where = '''
        # item_id = '632176806761' and trade_props.value in (['姜黄素缓释止痛片【30粒】'] , ['高浓度姜黄素缓痛片【30粒】'])
        # '''
        where = 'alias_all_bid = 130885 and ((source = 1 and (shop_type > 20 or shop_type < 10 ) and sid in (187512609,165534664)) or (source = 2 and sid in (0,1000073704,1000010824)) )'
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if mpp.get('{}-{}-{}'.format(source, tb_item_id, p1), [0,0])[1] == 2:
                # uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                continue
            uuids.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        print(len(uuids), len(uuids_update))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible=2, visible_check=1, sales_by_uuid=sales_by_uuid1)
        return True

    def brush_0601x(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'select distinct(source) from {}'.format(atbl)
        dba = self.cleaner.get_db(aname)
        sources = [v[0] for v in dba.query_all(sql)]

        cc = {}
        c = 0
        dd = {}
        for each_sp1, each_visible_check in [['矿物质', 1]]:
            for each_source in sources:
                uuids_update = []
                uuids = []
                c = 0
                where = 'concat(toString(source), toString(shop_type)) != \'111\' and uuid2 in ' \
                        '(select uuid2 from {} where sp1 = \'{}\') and source = {}'.format(ctbl, each_sp1, each_source)
                bids = [131009,112244,326236,48678,401951,241814,326026,130885,1210010,1856905,845348,387249,393278,48221,326010,474173,333207,333159,27591,103792,393304,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,558781,819397,207212,21150,333391,474186,4452952,387336,273629,53021,47220,272287,3919777,5134375,347699,5969283,387255,387270,387332,1531329,387129,5950105,5462839,714073,392061,392074,387333,386477,5874301,5831186,5798604,506932,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,3837273,48704,93278,48210,116183,95277,401688,3847776,1067497,4730279,475116,387577,387454,5916287,402002,387121,387135,130428,3472327,110054,59590,474182,387334,89329,1856459,3158974,3458059,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,4548333,77846,561121,387450,48572,130554,2265513,393970,2888376,156841,347244,12741,2608328,48256,3107725,85409,1809728,3923130,2189056,199009,52052,401994,
                        1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,966656,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,3923119,4538428,3923106,1224207]
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                print('return', len(ret))
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if int(alias_all_bid) in bids:
                        # c = c + 1
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (2,5,6):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            c = c + 1
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            k = '{}-{}-{}'.format(each_sp1,each_source,alias_all_bid)
                            if k not in dd:
                                dd[k] = 0
                            dd[k] = dd[k] + 1
                cc['{}+{}'.format(each_sp1, str(each_source))] = c
                if len(uuids_update) > 0:
                    sql = 'update {} set visible = 7, visible_check = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible_check, ','.join([str(v) for v in uuids_update]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids, clean_flag, visible_check=each_visible_check, visible=7, sales_by_uuid=sales_by_uuid1)
                print(len(uuids_update), len(uuids))
                print('add', len(uuids))
                for i in dd:
                    print(i, dd[i])

        for i in cc:
            print(i,cc[i])
        # print('ccccc', c)
        return True

    def brush_0610(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        uuids_update = []
        uuids = []
        c = 0
        where = '''
        item_id in ('531742859330','580660288761','611768127166','530687190024','100000066801','575908069310','619603829683','610614497857','602675732941','567572882987','628410458456','45228762406','44144587821','570731449042','602853000510','603123889903','610614497857','3690351','549914207771','571283328672','587416469353','11222224069')
        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (2, 5, 6):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
            else:
                uuids.append(uuid2)
                c = c + 1
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        if len(uuids_update) > 0:
            sql = 'update {} set visible = 6, visible_check = 3, created = now() where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()
        self.cleaner.add_brush(uuids, clean_flag, visible_check=3, visible=6, sales_by_uuid=sales_by_uuid1)
        print(len(uuids_update), len(uuids))
        print('add', len(uuids))
        return True

    def brush_0714(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}

        # for each_sp1, each_visible_check in [['维生素', 1]]:
        for each_sp1, each_visible_check in [['鱼油', 2],['氨糖', 3],['益生菌', 4],['植物精华', 5],['钙', 6], ['矿物质', 7],['蛋白粉',8]]:
            for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 5']:
                uuids_update = []
                uuids = []
                c = 0
                # concat(toString(source), toString(shop_type)) != \'111\'
                where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} '.format(ctbl, each_sp1, each_source)
                bids = self.bids1
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                print('return', len(ret))
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if int(alias_all_bid) in bids:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            c = c + 1
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            k = '{}-{}-{}'.format(each_sp1,each_source,alias_all_bid)
                            if k not in dd:
                                dd[k] = 0
                            dd[k] = dd[k] + 1
                cc['{}+{}'.format(each_sp1, str(each_source))] = [len(uuids_update), len(uuids)]
                if len(uuids_update) > 0:
                    sql = 'update {} set visible = 13, visible_check = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible_check, ','.join([str(v) for v in uuids_update]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids, clean_flag, visible_check=each_visible_check, visible=13, sales_by_uuid=sales_by_uuid1)
                print(len(uuids_update), len(uuids))

        for i in cc:
            print(i,cc[i])
        return True

    bids1 = [14286,16835,21150,32649,47277,48221,48275,48347,48484,48505,52158,59905,60079,66371,77846,94398,94544,95277,95778,98511,104786,110800,116368,120436,128397,130071,130231,130246,130336,130359,130400,130840,130945,131061,131231,131236,132700,132956,133382,134265,134581,134597,134682,135231,135392,135956,136919,149500,149746,163301,182603,186722,189791,191309,200002,202991,204470,216790,218140,218325,218698,241814,242223,246387,246394,247000,248951,249161,251781,252907,257855,269716,274328,295228,295902,296633,297642,298346,298374,298498,298523,298653,308192,309680,318985,325915,325969,326030,326031,326074,326126,326167,326183,326186,327947,328113,328170,328342,333163,333205,333217,333224,333225,333231,333252,333268,333283,333297,333311,333347,333348,333354,333357,333359,333363,333369,333370,333379,333403,333410,333411,333416,333418,333437,333439,333444,333448,333450,333457,333460,333461,333466,333478,333507,333513,334068,341522,341568,347223,347226,347558,347561,347570,347581,347629,347635,347639,347656,347658,347666,347693,348177,359003,363701,369751,386501,387117,387208,387254,387268,387324,387382,387396,387400,387523,387526,387536,387541,387564,387574,387576,387578,387616,387622,387626,387629,387632,387799,387822,391996,391997,392016,392027,392032,392041,392062,392063,392069,392075,392080,392086,392091,392096,392104,392113,392115,393287,398137,398141,398143,401460,401477,401503,401531,401556,401567,401595,401800,401893,401936,401938,401939,401947,401948,401971,401973,401987,402003,402022,405766,407360,411549,413031,415985,416184,417407,419381,419421,419481,419799,419834,419912,419925,420004,420006,426379,429895,433464,438095,438111,439310,439324,439339,439341,439349,439359,439911,449132,449167,449179,449182,449196,449199,449200,449208,449220,452621,453608,459271,469488,472067,472069,472071,472081,472085,472094,472101,472105,472107,472109,472124,472138,472140,472145,472192,472340,472372,472377,472442,472639,472758,472918,472983,473097,474177,480560,483555,483586,483596,483618,494001,495705,495833,495908,496044,496268,496288,496522,496564,496800,504319,506226,506325,506409,506445,506472,506510,506575,506580,506583,506603,506605,506611,506613,506629,506630,506633,506645,506651,506652,506658,506659,506661,506868,506904,506913,506928,517705,520058,520091,520795,529079,535693,538098,540908,543260,543262,545980,546016,546020,546025,546026,547356,549152,552814,552824,552827,552830,552926,552943,563024,563026,563070,563079,563083,563136,570530,582929,585252,600955,602365,606883,612567,615100,618484,618493,618496,618523,618549,621057,630857,634710,634724,635153,635176,635211,635393,635410,637153,639408,639511,639530,639589,639682,639694,640842,653599,662027,672750,678805,690967,691006,694852,694868,694869,694870,694871,694873,694876,700326,701117,706338,706362,706378,706697,706699,712117,713931,713943,714004,714009,714012,725719,725728,727882,735823,745443,756961,766687,779400,793496,796376,796378,796385,796394,796430,798764,800007,801706,801760,801767,818105,818106,818126,818312,818508,818872,818875,818884,818888,818933,818943,819116,819194,819222,819225,819226,819617,819727,826295,826326,826494,826533,845828,850985,866144,880407,900095,908643,909240,909347,911398,911403,911409,911414,911437,916441,916448,916493,931887,931889,932108,932117,932127,932140,932149,932380,932465,932477,932480,932529,932533,932580,932581,932587,932632,932634,932638,932704,932930,939020,939035,939039,939050,939118,939183,939191,939201,941143,943578,966656,975112,978195,982414,982875,984443,988227,989401,989412,989414,994288,999163,1007228,1008840,1008841,1008843,1008846,1008847,1008855,1008859,1008865,1008867,1008874,1008894,1012821,1012851,1012866,1012870,1012872,1012873,1013010,1013090,1013105,1013114,1013222,1013242,1013438,1019315,1027429,1033785,1033787,1034027,1034044,1034501,1034502,1034510,1040782,1049648,1049943,1050148,1050498,1050568,1057098,1057105,1067285,1070785,1079593,1079598,1079608,1079616,1079617,1079619,1079620,1079627,1079629,1079632,1079637,1080681,1098165,1098167,1098168,1098169,1098171,1098174,1098177,1098179,1098180,1100405,1100518,1101039,1101075,1105979,1113656,1120285,1120288,1120311,1122212,1123856,1123904,1123918,1123920,1123921,1123936,1123940,1145121,1147056,1161849,1166773,1167183,1170852,1181471,1181510,1191454,1191456,1191458,1191460,1191484,1191487,1191491,1193870,1195470,1195472,1209236,1209237,1209242,1209246,1209247,1209249,1209250,1209252,1209262,1209471,1209483,1209516,1209530,1209803,1209816,1209889,1210010,1210032,1210089,1210093,1210095,1210189,1210387,1212453,1212459,1212460,1212471,1212474,1214333,1242159,1246644,1258000,1266344,1270586,1295199,1297528,1297549,1297554,1297556,1297560,1297568,1297573,1297576,1298376,1301666,1301667,1301677,1308288,1312476,1314767,1314772,1314774,1315057,1315062,1315088,1315121,1315155,1315252,1315366,1315367,1315378,1315422,1315500,1315506,1315556,1315565,1315566,1315569,1315572,1315573,1315579,1315582,1315650,1315678,1315686,1315688,1315733,1315889,1315921,1316020,1317920,1319616,1340057,1341234,1358029,1378763,1379415,1385637,1385978,1386179,1386198,1387207,1387210,1387219,1387225,1387232,1387237,1387247,1388292,1388632,1389643,1389645,1389648,1389652,1389654,1398411,1398419,1398420,1398424,1398606,1398687,1398689,1398726,1398799,1398816,1398817,1398818,1398821,1398828,1398833,1398879,1398930,1398944,1398946,1398951,1398972,1399093,1399137,1406364,1413420,1413467,1416532,1417724,1422609,1422613,1422744,1422763,1422765,1422795,1422831,1423802,1423818,1450606,1455822,1472801,1473002,1476729,1478486,1497498,1511932,1511936,1511938,1511948,1511949,1511956,1511969,1511973,1511974,1515685,1515686,1515688,1515690,1526152,1530428,1530457,1530767,1530798,1530888,1530995,1531019,1531023,1531030,1531031,1531034,1531042,1531044,1531046,1531056,1531173,1531174,1531177,1531191,1531225,1531249,1531250,1531320,1531339,1531340,1531343,1531662,1533321,1533574,1538510,1548892,1567629,1568050,1568121,1568123,1569149,1584567,1592485,1610977,1612214,1612230,1612283,1621958,1624048,1624262,1624282,1624292,1624299,1624309,1624311,1624313,1628407,1628410,1643946,1643952,1644392,1644427,1644522,1644605,1644704,1644706,1644729,1644731,1644735,1644740,1644756,1644905,1644983,1644998,1645097,1645104,1645106,1645203,1645327,1645343,1645354,1645478,1647564,1647898,1647900,1652522,1668289,1668563,1675180,1680538,1689775,1690232,1693260,1696808,1696810,1696812,1696819,1696823,1696826,1696830,1696833,1699140,1699155,1699156,1699157,1699161,1706576,1706586,1706593,1706791,1706834,1706858,1707032,1707038,1707055,1707061,1707138,1707139,1707142,1707153,1707165,1707171,1707172,1707180,1707239,1707240,1707241,1707249,1707252,1707403,1707478,1709074,1710074,1710768,1719265,1720347,1723823,1726621,1726628,1726645,1728915,1729323,1737172,1754912,1754922,1754923,1754928,1754936,1754941,1754945,1754946,1754951,1754961,1758391,1758395,1758397,1758398,1758435,1758437,1758438,1758440,1765326,1767565,1767573,1767576,1767578,1767586,1767753,1767841,1767860,1767958,1768042,1768045,1768053,1768058,1768074,1768165,1768185,1768217,1768223,1768306,1768343,1768363,1768422,1768478,1768528,1768551,1771200,1773691,1785422,1785441,1785445,1785512,1786743,1793280,1810855,1818117,1827030,1832396,1832662,1837539,1841182,1841187,1841200,1844133,1844503,1844543,1850642,1853292,1856375,1856378,1856385,1856389,1856390,1856469,1856537,1856577,1856638,1856726,1856766,1856767,1856774,1856782,1856787,1856850,1856913,1856949,1856955,1856958,1857001,1857017,1857239,1857293,1857365,1873701,1874810,1874828,1874829,1874831,1874834,1874839,1874846,1874854,1882595,1882627,1882641,1882649,1882803,1882804,1882805,1882813,1882814,1882820,1882854,1882863,1882900,1882902,1882934,1882991,1883006,1888039,1891749,1892079,1911775,1916787,1929243,1938226,1938563,1941855,1961900,1965219,1965240,1965242,1968231,1969543,1969556,1969590,1969591,1969595,1969597,1969599,1973999,1986094,1986215,1986250,1986253,1986255,1986260,1986275,1986290,1986299,1986316,1986357,1986368,1986620,1986624,1986632,1986635,1986636,1986646,1986657,1986658,1986665,1986675,1986724,1986826,1986828,1986832,1986834,1986900,1986903,1986909,1986914,1986921,1986931,1986934,1987025,1987039,1987215,1987225,1987229,1987236,1987300,1987303,1987331,1990734,1994028,1994890,2009953,2024095,2033041,2045086,2046754,2048880,2074189,2074191,2074194,2074221,2074222,2078159,2078899,2078943,2078949,2088138,2096049,2096055,2096061,2096068,2096074,2096199,2096203,2096226,2096451,2096464,2096465,2096467,2096472,2096477,2096483,2096490,2096495,2096509,2096666,2096669,2096670,2096733,2096740,2096749,2096802,2096831,2096853,2096859,2096864,2096954,2096997,2096998,2097140,2097295,2097304,2097312,2097350,2097382,2105908,2117465,2132341,2137970,2138515,2142308,2156341,2160070,2169367,2169370,2169371,2169400,2173459,2173461,2187659,2188373,2188465,2188705,2188713,2188716,2188720,2188788,2188895,2188908,2188970,2188983,2189056,2189082,2189086,2189223,2189233,2189438,2194464,2198970,2219870,2219872,2219892,2223150,2230231,2238638,2242989,2255933,2261058,2261064,2261066,2261078,2261094,2261096,2261103,2261104,2261107,2261110,2261116,2263972,2265497,2265498,2265504,2265508,2265510,2280855,2280859,2280860,2280864,2280865,2280876,2280970,2280977,2280985,2280987,2280992,2280993,2281147,2281149,2281157,2281163,2281169,2281331,2281389,2281390,2281433,2281452,2281460,2281544,2281553,2281559,2281575,2281582,2281588,2281592,2281742,2281834,2281893,2281909,2281928,2281937,2281941,2292595,2292597,2304052,2318788,2319026,2328166,2338708,2338816,2339402,2360255,2363361,2363370,2363379,2363380,2363381,2363388,2363389,2363399,2363401,2363405,2365930,2366506,2367475,2367507,2367512,2367519,2371353,2371367,2376277,2378024,2381183,2381683,2381685,2381686,2381689,2381690,2381693,2381860,2381869,2381870,2381873,2381879,2381984,2381990,2381996,2381998,2382002,2382005,2382009,2382084,2382102,2382172,2382177,2382181,2382187,2382205,2382218,2382249,2382256,2382259,2382267,2382357,2382369,2382549,2382709,2382751,2382858,2383485,2392863,2403074,2405365,2418287,2443925,2459308,2470440,2470444,2470448,2470450,2470461,2470470,2470483,2472466,2474610,2474614,2474652,2474654,2478598,2490475,2490486,2490634,2490636,2490794,2490802,2490808,2490813,2490822,2490823,2490827,2490839,2490842,2490843,2490863,2490972,2490980,2490983,2490986,2491039,2491048,2491056,2491058,2491062,2491143,2491150,2491333,2491347,2491350,2491482,2491583,2510673,2511899,2521207,2522296,2527164,2548299,2548470,2548765,2565278,2565987,2578660,2578671,2578678,2578680,2578689,2580277,2582848,2582864,2582906,2582909,2582911,2598703,2598705,2598718,2598866,2598886,2598893,2598908,2598928,2598934,2599013,2599025,2599026,2599028,2599037,2599044,2599049,2599171,2599264,2599286,2599297,2599301,2599302,2599306,2599308,2599318,2599394,2599397,2599399,2599404,2599408,2599412,2599569,2599645,2599671,2599718,2599763,2599830,2607745,2620201,2620229,2639229,2674287,2675239,2688689,2688692,2689183,2691136,2692822,2692829,2692832,2692876,2709227,2709236,2709237,2709238,2709249,2709404,2709446,2709448,2709457,2709473,2709485,2709527,2709534,2709552,2709569,2709597,2709692,2709768,2709771,2709791,2709799,2709854,2709855,2709883,2709898,2709966,2709984,2709995,2710081,2710134,2710311,2734791,2740749,2745484,2745782,2759277,2760831,2760837,2763287,2763289,2763314,2763316,2772294,2772398,2772437,2772440,2772450,2772451,2772457,2772531,2772576,2772586,2772594,2772631,2772637,2772676,2772875,2772924,2777044,2786178,2788767,2795281,2800077,2806258,2807019,2814643,2822460,2823370,2829332,2829475,2829692,2830141,2836168,2846019,2856132,2858654,2858661,2859791,2862237,2862244,2862246,2862270,2862873,2867605,2867609,2867671,2867675,2867680,2872117,2887999,2888003,2888004,2888013,2888014,2888233,2888237,2888257,2888296,2888326,2888417,2888453,2888466,2888470,2888488,2888491,2888493,2888495,2888517,2888735,2888738,2888764,2888841,2888844,2888848,2888852,2888871,2888874,2888895,2888934,2888961,2888963,2888964,2888967,2888969,2888971,2888976,2888979,2888988,2889053,2889143,2889161,2889252,2889384,2889423,2890412,2894524,2895966,2905394,2908723,2911085,2919264,2919265,2919266,2920158,2920178,2921674,2921789,2921819,2921832,2921851,2921868,2921876,2921877,2921881,2921882,2921883,2922005,2924872,2924880,2934157,2939484,2941235,2941237,2941271,2941274,2941482,2941508,2941572,2959197,2985666,2986454,2987733,2987746,2990804,2990805,2993481,2993495,2993530,2998523,3002257,3002258,3002265,3002374,3002392,3002419,3002425,3002434,3002467,3002469,3002473,3002475,3002482,3002484,3002486,3002590,3002659,3004110,3012544,3014416,3014420,3014434,3015309,3016840,3023646,3024504,3030044,3030052,3030055,3030062,3030070,3030071,3030072,3030120,3030121,3030127,3030140,3030150,3030152,3030161,3030168,3030289,3030351,3030358,3030359,3030463,3045058,3048336,3051813,3051817,3055427,3055472,3055573,3058556,3068584,3068931,3079510,3089308,3089320,3092962,3093006,3093007,3096494,3107557,3107558,3107671,3107676,3107680,3107703,3107714,3107753,3107758,3107767,3107775,3107984,3107986,3108009,3108012,3108043,3108044,3108046,3108049,3108089,3108121,3108128,3108142,3108145,3108263,3108328,3108406,3108449,3108465,3108485,3113759,3125606,3127005,3135353,3138793,3138794,3140373,3140983,3145594,3145644,3145647,3145748,3145750,3145807,3145817,3145925,3145950,3145951,3156317,3156318,3157010,3158405,3158979,3159069,3159216,3170327,3173646,3173647,3175004,3175023,3175024,3176016,3180197,3180450,3182056,3195929,3199611,3224569,3246939,3279900,3283324,3283352,3283989,3283992,3284007,3290584,3290586,3290599,3290600,3290602,3317658,3317813,3317817,3317837,3317897,3317917,3317920,3318002,3318030,3318402,3318434,3318442,3318444,3318503,3318506,3318538,3318585,3318599,3318657,3318686,3318699,3318806,3318823,3318875,3318958,3318972,3318984,3318993,3319011,3319084,3319104,3319139,3319152,3319192,3319244,3328182,3357321,3373598,3387317,3389495,3389508,3400057,3400061,3400076,3400077,3400080,3400083,3400133,3400173,3400302,3403832,3419239,3429310,3429350,3429352,3429403,3429443,3429452,3429484,3441294,3442313,3444962,3453149,3453151,3457950,3457951,3458066,3471939,3472327,3481858,3484073,3484106,3484108,3484284,3509253,3521773,3521778,3521935,3525665,3529409,3529413,3542943,3543679,3551577,3559854,3559875,3571103,3571107,3579118,3584060,3584171,3588136,3597105,3601028,3604844,3619078,3619152,3619170,3619171,3619185,3619356,3619366,3622084,3641323,3641645,3645523,3645631,3645641,3645656,3649406,3655026,3655542,3682321,3691738,3710324,3710566,3720107,3750557,3753826,3755104,3770975,3773261,3800832,3837273,3837275,3837304,3837311,3837312,3837326,3837328,3844859,3851239,3854942,3854956,3855576,3855579,3868416,3868439,3887432,3899430,3921770,3922476,3922595,3922608,3922904,3922910,3922943,3922998,3923017,3923045,3923068,3923117,3923145,3923168,3923189,3923196,3923198,3923208,3923229,3923427,3923430,3923473,3923475,3923483,3923491,3923504,3923552,3923593,3923653,3923743,3923748,3923765,3923829,3923863,3923884,3923923,3938482,3939417,3957400,3969505,3969617,3969717,3974059,3985911,3986115,3986180,3986215,3986282,3986308,3986314,3986319,3986403,3999301,3999902,4008699,4008723,4008733,4008750,4011427,4017691,4019564,4026620,4026621,4026639,4026646,4029467,4035688,4049427,4055124,4059227,4059253,4061852,4065577,4069741,4079250,4083111,4084661,4090896,4090901,4090909,4092243,4102955,4102957,4102974,4105720,4105721,4105763,4105792,4105873,4108769,4114512,4115869,4120298,4120305,4122385,4123868,4136159,4136172,4136177,4136234,4137340,4141156,4141160,4141164,4141205,4141224,4141240,4142296,4143622,4144432,4144439,4154965,4164159,4164217,4165219,4172881,4181494,4182569,4196551,4196553,4196554,4202973,4221468,4229212,4231176,4234686,4241106,4247826,4250257,4250354,4250368,4250439,4250444,4250449,4250450,4250485,4250494,4250497,4250571,4250585,4260374,4261078,4263396,4263451,4263470,4272197,4280024,4284937,4290622,4290625,4290653,4290661,4290691,4290968,4295064,4299219,4300193,4300194,4303480,4307723,4318673,4318690,4319433,4327081,4327082,4327104,4327113,4327117,4327143,4327234,4328130,4328131,4328164,4329103,4336760,4340423,4340425,4340427,4343380,4343381,4346262,4350291,4350303,4353278,4353282,4353285,4353292,4353323,4360504,4360553,4361344,4361874,4374318,4376338,4381585,4381602,4383429,4383508,4384391,4384421,4384436,4384442,4384487,4399273,4405105,4405107,4405171,4405172,4405184,4405199,4405245,4405247,4405248,4405260,4405272,4405282,4409545,4414837,4427098,4427100,4431991,4431996,4432000,4432004,4432052,4432053,4432068,4432081,4432114,4442998,4452956,4452957,4455853,4457028,4466730,4467776,4469120,4474084,4484582,4484585,4484589,4486241,4486242,4486246,4495221,4495253,4502074,4502241,4505759,4505781,4505785,4507434,4533348,4538376,4538384,4538386,4538388,4538395,4538418,4538427,4538428,4538430,4538470,4538473,4538478,4538479,4538498,4538505,4538528,4538557,4539611,4547726,4549241,4549260,4553634,4555881,4560598,4560824,4561266,4570177,4587197,4589886,4589889,4592292,4594144,4609758,4609762,4609907,4609974,4609986,4609992,4609997,4610003,4610103,4610133,4622258,4625342,4630587,4631446,4634402,4643197,4647324,4657462,4657489,4657532,4657538,4657539,4657548,4657575,4657586,4660271,4662950,4664889,4664924,4669509,4681241,4682551,4685926,4697992,4700091,4701934,4702509,4705774,4715028,4715663,4715752,4715757,4715760,4715767,4715794,4715795,4715797,4715804,4715846,4715848,4715860,4715864,4715872,4721713,4725712,4726994,4729143,4729741,4733129,4733135,4733512,4737637,4742374,4744154,4744155,4744156,4744399,4745421,4745634,4745636,4750176,4754264,4755439,4755913,4756100,4756883,4756886,4764268,4768853,4768855,4768856,4768857,4769005,4770837,4775013,4780925,4780995,4783162,4784652,4791578,4793927,4793980,4798900,4799729,4802996,4807061,4809081,4812735,4814971,4817352,4818283,4822043,4823551,4828278,4828279,4829391,4833939,4834590,4834704,4841109,4841110,4841555,4844292,4845701,4846266,4846701,4847129,4847141,4847207,4847475,4849992,4851286,4855176,4855850,4856552,4857499,4861485,4861944,4862140,4862248,4862483,4862561,4863255,4863498,4863576,4863607,4863870,4864007,4864011,4864096,4864123,4864134,4864149,4864279,4864447,4865307,4865341,4865684,4867327,4867420,4867457,4867638,4868177,4869740,4870086,4870213,4870406,4870432,4870439,4870482,4870485,4870487,4870503,4870524,4870595,4870647,4870656,4870658,4870683,4870687,4870740,4870745,4870771,4870803,4870916,4870920,4870974,4872188,4872189,4872190,4872191,4872364,4873644,4874189,4874538,4874610,4875004,4875373,4875741,4876838,4877015,4877022,4877024,4877531,4878049,4878916,4879250,4880281,4881640,4881885,4882622,4882631,4882657,4884155,4887774,4893699,4895450,4896603,4900098,4903618,4908043,4909623,4922053,4922058,4937802,4938915,4948405,4948410,4950916,4951560,4955688,4956236,4960637,4969428,4969434,4969514,4969545,4969557,4969572,4969598,4969688,4985531,5012063,5012113,5012126,5012166,5012181,5012264,5019937,5029331,5031231,5039907,5046056,5048600,5050655,5079814,5079821,5079852,5079877,5081867,5090693,5090704,5092886,5098907,5098968,5120694,5135730,5135755,5135762,5135767,5142849,5143708,5148426,5154876,5160928,5166948,5171115,5178611,5180278,5180294,5180297,5180317,5180335,5180343,5183978,5198291,5202403,5205479,5217817,5219322,5229180,5238747,5238755,5238759,5238766,5244422,5253250,5261443,5277133,5277645,5284632,5307262,5315260,5315278,5315304,5315369,5318770,5320788,5322566,5328471,5341230,5342424,5344135,5345427,5347766,5348986,5351487,5361694,5363159,5363244,5371205,5374539,5389986,5390006,5397557,5414558,5421384,5425720,5453139,5462212,5471803,5479186,5497020,5499755,5499822,5504413,5517634,5520590,5538328,5545251,5553517,5568842,5581403,5585026,5590849,5607690,5621429,5628449,5632770,5660886,5662563,5663239,5691320,5699141,5699306,5699757,5729652,5779245,5801035,5809297,5813161,5817479,5825444,5831187,5839874,5844897,5853855,5860574,5875066,5877184,5881666,5899367,5920735,5921595,5937435,5946898,5999588,6001652,6026852,6036455,6036456,6036457,6045628,6050788,6061373,6070753,6084731,6094513,6112242,6144092,6157753,6180147,6199287,6213231,6213517,6230654,6236841,6242723,6257087,6257525,6264184,6271951,6272674,6279817,6279827,6279831,6279850,6295735,6308519,6309800,6316983,6317296,6342289,6354670,6355830,6356259,6377906,6384689,6396264,6433856,6467978,6475189,6512647,6525260,6538170,6547174,6547175,6552761,6553099]
    bids_vitamin = [1245,14286,16835,21150,32649,47277,48221,48275,48347,48484,52158,59905,60079,66371,94398,94544,95277,95778,98511,104786,119784,120436,130071,130231,130246,130336,130359,130400,130840,130945,131231,131236,132535,132700,132956,133382,134265,134581,134597,134682,135231,135392,135956,153672,182603,191309,202991,204470,216790,218140,218325,218698,241814,242223,245085,246387,246394,247000,269716,295902,296633,297642,298346,298374,298498,298523,308192,309680,318985,325915,325969,326030,326031,326074,326167,326183,326186,328342,333163,333205,333217,333224,333225,333231,333252,333268,333283,333297,333311,333347,333348,333354,333357,333359,333363,333369,333370,333371,333379,333403,333411,333416,333418,333429,333437,333439,333444,333448,333450,333457,333460,333461,333466,333478,334068,347223,347226,347252,347558,347561,347570,347581,347629,347635,347639,347656,347658,347666,347693,348177,359003,363701,369751,386501,387117,387208,387268,387323,387324,387362,387382,387396,387400,387526,387536,387541,387564,387574,387576,387578,387616,387622,387626,387629,387632,387799,387822,391976,391996,391997,392016,392027,392032,392041,392062,392063,392069,392075,392076,392080,392081,392086,392091,392096,392104,392113,393287,398137,398141,398143,401460,401477,401503,401531,401556,401567,401595,401800,401893,401936,401938,401939,401947,401971,401973,401987,402003,402022,407360,411549,413031,415985,416184,419381,419421,419481,419799,419912,420004,420006,429895,438095,438111,439310,439324,439339,439341,439349,439359,439436,439911,449132,449167,449179,449182,449196,449199,449200,449208,449218,449220,453608,459271,469488,472067,472069,472071,472081,472085,472094,472101,472105,472107,472109,472124,472138,472140,472145,472192,472340,472372,472377,472442,472639,472758,472918,472983,473097,474177,480560,483555,483586,483618,495705,495833,495908,496044,496288,496522,496564,496800,504319,506226,506325,506472,506580,506583,506605,506611,506613,506629,506630,506633,506645,506658,506659,506904,506913,506928,517705,520058,520091,520795,535693,538098,543260,543262,545980,546016,546020,546025,546026,552814,552824,552827,552830,552926,552943,563070,563079,563083,563136,570530,582929,585252,606883,612567,618484,618493,618496,618523,618549,630857,634710,634724,634843,635153,635176,635211,635393,635410,639408,639511,639530,639589,639682,639694,662027,690967,691006,694852,694868,694869,694870,694871,694873,694876,701117,706338,706362,706378,706697,706699,712117,713931,713942,714004,714009,714012,725728,727882,735823,745443,746737,779400,793496,796376,796378,796385,796394,796430,798764,800007,801706,801760,801767,818105,818106,818126,818312,818508,818872,818875,818884,818888,818933,818943,819116,819194,819222,819225,819226,819617,819727,821336,826295,826494,826533,866144,871356,880407,883551,900095,908643,909240,911398,911403,911409,911414,911437,916441,916448,916493,931887,931889,932108,932117,932127,932140,932149,932380,932465,932477,932480,932529,932533,932580,932581,932587,932634,932638,932930,939020,939035,939039,939050,939118,939183,939191,939201,943578,966656,975112,982875,984443,988227,989401,989412,989414,994288,999163,1007228,1008840,1008841,1008843,1008846,1008847,1008855,1008859,1008865,1008867,1008874,1008894,1012821,1012851,1012866,1012870,1012872,1012873,1013010,1013090,1013105,1013114,1013222,1013242,1013438,1027429,1033785,1033787,1034027,1034501,1034502,1034510,1049943,1050148,1050498,1050568,1057098,1057105,1070785,1079593,1079598,1079608,1079616,1079617,1079619,1079620,1079627,1079629,1079632,1079637,1098165,1098167,1098168,1098169,1098171,1098174,1098177,1098179,1098180,1100405,1100518,1105979,1120288,1122212,1123856,1123904,1123921,1123936,1123940,1145121,1147056,1161849,1167183,1181471,1181510,1191454,1191456,1191458,1191460,1191484,1191487,1191491,1195470,1195472,1209236,1209237,1209242,1209246,1209247,1209249,1209250,1209252,1209262,1209471,1209483,1209530,1209803,1209816,1209889,1210010,1210089,1210093,1210095,1210189,1210387,1212453,1212459,1212460,1212471,1212474,1242159,1246644,1270586,1297528,1297549,1297554,1297556,1297568,1297573,1298376,1301666,1301667,1301677,1314767,1314772,1314774,1315057,1315062,1315088,1315121,1315155,1315252,1315366,1315367,1315378,1315500,1315506,1315556,1315565,1315566,1315569,1315572,1315573,1315579,1315582,1315650,1315688,1315733,1315889,1317920,1340057,1341234,1358029,1379415,1386198,1387207,1387210,1387219,1387225,1387232,1387237,1387247,1388292,1389643,1389645,1389648,1389652,1389654,1398411,1398420,1398424,1398606,1398687,1398726,1398816,1398817,1398818,1398821,1398828,1398833,1398879,1398930,1398944,1398946,1398951,1398972,1399093,1399137,1406364,1413420,1413467,1416532,1417724,1422613,1422763,1422765,1422795,1422831,1423802,1423818,1450606,1455822,1472801,1473002,1476729,1478486,1511932,1511936,1511938,1511948,1511949,1511956,1511969,1511973,1511974,1515685,1515686,1515688,1515690,1530457,1530888,1530995,1531019,1531023,1531030,1531034,1531042,1531044,1531046,1531056,1531173,1531174,1531177,1531191,1531225,1531249,1531250,1531320,1531339,1531340,1531343,1531662,1533321,1533574,1548892,1567629,1568050,1568121,1568123,1569149,1591826,1592485,1610977,1612214,1612230,1612283,1613242,1621958,1624262,1624282,1624299,1624309,1624311,1624313,1628407,1628410,1643946,1643952,1644427,1644522,1644605,1644704,1644706,1644731,1644735,1644740,1644905,1644983,1644998,1645097,1645104,1645106,1645203,1645327,1645343,1645354,1645478,1647564,1647898,1647900,1652522,1668289,1668563,1675180,1689775,1690232,1696808,1696810,1696812,1696819,1696823,1696826,1696830,1696833,1699140,1699155,1699156,1699157,1699161,1706576,1706586,1706593,1706791,1706834,1706858,1707032,1707038,1707055,1707061,1707138,1707139,1707142,1707153,1707165,1707171,1707172,1707180,1707239,1707240,1707249,1707252,1707403,1707478,1710074,1710768,1719251,1720347,1723823,1726621,1726645,1728915,1729323,1737172,1754912,1754922,1754923,1754928,1754936,1754941,1754945,1754946,1754951,1754961,1758391,1758395,1758397,1758398,1758435,1758437,1758438,1758440,1767565,1767573,1767576,1767578,1767586,1767753,1767841,1767958,1768042,1768058,1768074,1768165,1768170,1768185,1768217,1768223,1768306,1768343,1768363,1768422,1768528,1768551,1785441,1785445,1785512,1786743,1793280,1810855,1818117,1827030,1832396,1832662,1837539,1841182,1841187,1841200,1844133,1844503,1844543,1856375,1856378,1856385,1856389,1856390,1856469,1856537,1856638,1856726,1856766,1856767,1856774,1856782,1856787,1856850,1856913,1856949,1856955,1856958,1857001,1857017,1857239,1857293,1857365,1863390,1874810,1874828,1874829,1874831,1874834,1874839,1874846,1874854,1882627,1882649,1882803,1882804,1882805,1882813,1882820,1882863,1882900,1882902,1882934,1882991,1888039,1891749,1892079,1911775,1938226,1938563,1965219,1965240,1965242,1968231,1969543,1969556,1969590,1969591,1969595,1969597,1969599,1973999,1986094,1986215,1986250,1986260,1986275,1986290,1986299,1986316,1986368,1986620,1986624,1986632,1986635,1986636,1986646,1986657,1986658,1986665,1986675,1986826,1986828,1986829,1986832,1986900,1986903,1986909,1986914,1986921,1986931,1986934,1987025,1987039,1987215,1987225,1987229,1987300,1987303,1988387,1990734,2009953,2024095,2046754,2047098,2048880,2071853,2074189,2074191,2074194,2074221,2074222,2078159,2078899,2078949,2088138,2096049,2096055,2096061,2096068,2096199,2096203,2096226,2096451,2096464,2096465,2096467,2096472,2096477,2096483,2096490,2096495,2096666,2096669,2096670,2096733,2096740,2096749,2096831,2096853,2096954,2096997,2097140,2097295,2097304,2097312,2097350,2097382,2105908,2117465,2138515,2142308,2169367,2169370,2169371,2169400,2173459,2187659,2188373,2188465,2188705,2188713,2188716,2188720,2188788,2188895,2188908,2188913,2188970,2188983,2189056,2189082,2189223,2189233,2189438,2194464,2198970,2219870,2223150,2230231,2238638,2239390,2255933,2261058,2261064,2261066,2261078,2261094,2261096,2261103,2261104,2261107,2261110,2261116,2265497,2265498,2265504,2265508,2265510,2280855,2280859,2280860,2280864,2280865,2280876,2280977,2280985,2280987,2280992,2280993,2281147,2281149,2281157,2281163,2281169,2281389,2281390,2281433,2281452,2281460,2281553,2281559,2281575,2281582,2281588,2281592,2281834,2281909,2281928,2281937,2281941,2292595,2292597,2304052,2318788,2319026,2338816,2339402,2360255,2363361,2363370,2363379,2363380,2363381,2363388,2363389,2363399,2363401,2363405,2366506,2367475,2367507,2367512,2367519,2371353,2376277,2381183,2381683,2381685,2381686,2381689,2381690,2381693,2381860,2381869,2381984,2381990,2381996,2382002,2382005,2382172,2382177,2382181,2382187,2382205,2382249,2382256,2382259,2382267,2382357,2382369,2382549,2382709,2382720,2383485,2392863,2405365,2418287,2443122,2443925,2459308,2470440,2470444,2470448,2470450,2470461,2470470,2472466,2474610,2474614,2474652,2474654,2478598,2490475,2490486,2490634,2490636,2490794,2490802,2490808,2490813,2490822,2490823,2490863,2490980,2490983,2490986,2491039,2491048,2491056,2491058,2491062,2491143,2491333,2491347,2491350,2491482,2491583,2510673,2521215,2548299,2548470,2548765,2551684,2565278,2565987,2578660,2578671,2578678,2578680,2578689,2582848,2582864,2582906,2582909,2582911,2598703,2598705,2598718,2598866,2598886,2598893,2598908,2598928,2598934,2599013,2599025,2599026,2599028,2599044,2599049,2599264,2599286,2599297,2599301,2599302,2599306,2599308,2599318,2599394,2599397,2599399,2599404,2599408,2599412,2599569,2599645,2599671,2599718,2599763,2599830,2620229,2639229,2658209,2674287,2675239,2688689,2688692,2691136,2692822,2692829,2692832,2692876,2709227,2709236,2709237,2709238,2709249,2709404,2709446,2709448,2709457,2709485,2709534,2709552,2709569,2709597,2709692,2709768,2709771,2709791,2709799,2709854,2709855,2709883,2709898,2709966,2709984,2709995,2710081,2710134,2710311,2734791,2745484,2745782,2759277,2760831,2760837,2763287,2763289,2763314,2763316,2772294,2772398,2772437,2772440,2772451,2772457,2772576,2772586,2772594,2772631,2772637,2772676,2772924,2777044,2786178,2788767,2795281,2800077,2806258,2807019,2822460,2823370,2829332,2829475,2829692,2830141,2846019,2858654,2858661,2859791,2862237,2862244,2862246,2862270,2862873,2867605,2867609,2867671,2867675,2867680,2872117,2886520,2887999,2888003,2888004,2888013,2888014,2888233,2888257,2888296,2888326,2888417,2888453,2888466,2888470,2888491,2888493,2888517,2888735,2888738,2888841,2888844,2888848,2888852,2888871,2888874,2888895,2888934,2888961,2888963,2888964,2888967,2888969,2888976,2888988,2889053,2889143,2889161,2889252,2889423,2890412,2894524,2895966,2908723,2911085,2919264,2919265,2919266,2920158,2920178,2921674,2921789,2921819,2921832,2921851,2921868,2921876,2921877,2921881,2921882,2921883,2922005,2924872,2934157,2939484,2941235,2941237,2941271,2941274,2941482,2941508,2941572,2985666,2986454,2987733,2987746,2990804,2990805,2993481,2993495,2993530,3002257,3002265,3002374,3002392,3002419,3002425,3002434,3002467,3002469,3002473,3002475,3002482,3002484,3002486,3002659,3004110,3012544,3014416,3014420,3014434,3015309,3023646,3030044,3030052,3030055,3030062,3030070,3030071,3030072,3030120,3030121,3030127,3030140,3030150,3030161,3030168,3030289,3030351,3030358,3030463,3048336,3051813,3051817,3055427,3055472,3055573,3068584,3068931,3079510,3089308,3089320,3092962,3093006,3093007,3096494,3107558,3107671,3107676,3107680,3107714,3107767,3107775,3107984,3107986,3108009,3108012,3108043,3108044,3108046,3108049,3108089,3108121,3108142,3108145,3108263,3108328,3108406,3108449,3108465,3108485,3113759,3127005,3135353,3138793,3138794,3140373,3140983,3145594,3145644,3145647,3145748,3145750,3145807,3145925,3145950,3145951,3156317,3156318,3157010,3158979,3159069,3159216,3170327,3173646,3173647,3175004,3175023,3175024,3176014,3176016,3180197,3180450,3199611,3246939,3279900,3283324,3283352,3283989,3283992,3284007,3287875,3290584,3290586,3290599,3290600,3290602,3317658,3317813,3317837,3317897,3317917,3318002,3318030,3318434,3318442,3318444,3318487,3318503,3318506,3318538,3318585,3318599,3318657,3318686,3318699,3318806,3318823,3318871,3318875,3318958,3318972,3318984,3319011,3319084,3319104,3319139,3319192,3319244,3387317,3389508,3400057,3400061,3400076,3400077,3400080,3400083,3400133,3400173,3400302,3403832,3419239,3427485,3429310,3429350,3429403,3429452,3441294,3444962,3453149,3453151,3457950,3457951,3458066,3471939,3472327,3481858,3484073,3484106,3484108,3484284,3521773,3521778,3529413,3543679,3551577,3559854,3559875,3571103,3571107,3579118,3584060,3597105,3604844,3619078,3619152,3619170,3619171,3619185,3619356,3619366,3622084,3641323,3645523,3645641,3645656,3649406,3655026,3655542,3682321,3691738,3710324,3710566,3750557,3753826,3756598,3770975,3800832,3837273,3837275,3837304,3837311,3837312,3837326,3837328,3844859,3854942,3854956,3855576,3855579,3868416,3868439,3899430,3911798,3919686,3921770,3922476,3922595,3922608,3922904,3922943,3922998,3923017,3923045,3923068,3923117,3923145,3923168,3923189,3923196,3923198,3923229,3923427,3923430,3923473,3923475,3923483,3923491,3923552,3923593,3923653,3923748,3923765,3923829,3923884,3938482,3939417,3969505,3969617,3974059,3985911,3986115,3986180,3986215,3986282,3986314,3986319,3986403,3999301,3999902,4008723,4008733,4008750,4017691,4026620,4026621,4026646,4029467,4035688,4049427,4055124,4059227,4065577,4069741,4083111,4084661,4090896,4090909,4102955,4102957,4102974,4105720,4105721,4105763,4105792,4105873,4108769,4115869,4120298,4136159,4136172,4136177,4136234,4136325,4137340,4138931,4141160,4141205,4141224,4141240,4142296,4143622,4144432,4164159,4164217,4165219,4172881,4181494,4182569,4196551,4196553,4196554,4202973,4231176,4234686,4241106,4247826,4250257,4250354,4250368,4250439,4250444,4250449,4250450,4250494,4250497,4250571,4250585,4260374,4261078,4263396,4263451,4263470,4272197,4280024,4290622,4290661,4290691,4295064,4299219,4303480,4307723,4318673,4319433,4327081,4327082,4327104,4327113,4327117,4327143,4327172,4327234,4327911,4328130,4328131,4328164,4329103,4336760,4340423,4340425,4340427,4343380,4343381,4346262,4346531,4350291,4350303,4353278,4353285,4353292,4360504,4360553,4361344,4361874,4374318,4376338,4381585,4381602,4383429,4384391,4384421,4384436,4384442,4399273,4405105,4405107,4405171,4405172,4405184,4405199,4405245,4405248,4405260,4405272,4405282,4409545,4414837,4427100,4431991,4431996,4432000,4432028,4432052,4432053,4432081,4432114,4442998,4452956,4452957,4455853,4457028,4467776,4469120,4474084,4484582,4484585,4484589,4495221,4502074,4502241,4505759,4505781,4505785,4507434,4538376,4538386,4538388,4538427,4538428,4538430,4538470,4538473,4538478,4538479,4538505,4538557,4539611,4547726,4549241,4549260,4553634,4555881,4560824,4561266,4570177,4589886,4589889,4592292,4594144,4609758,4609762,4609907,4609974,4609992,4609997,4610003,4610103,4622258,4625342,4630587,4631446,4634402,4647324,4657462,4657489,4657532,4657538,4657539,4657548,4657586,4660271,4662950,4664889,4664924,4669509,4682551,4685926,4700091,4701934,4705774,4715028,4715663,4715757,4715760,4715767,4715794,4715795,4715797,4715804,4715846,4715848,4715860,4715861,4715864,4715872,4721713,4725712,4729143,4729741,4733129,4733135,4733512,4737637,4742616,4742617,4743986,4744399,4745421,4745634,4745636,4750176,4754264,4755439,4755913,4764268,4768857,4770837,4775013,4780925,4780995,4783162,4784652,4791578,4798900,4799729,4802996,4807061,4809081,4812735,4814971,4817352,4818283,4822043,4828278,4828279,4829391,4833939,4834704,4841109,4841110,4841555,4844292,4845701,4845922,4846701,4847129,4847141,4847207,4847475,4849992,4851286,4855176,4855850,4856552,4857499,4860151,4861485,4861944,4862248,4862483,4862561,4863255,4863498,4863607,4864011,4864096,4864123,4864134,4864149,4864279,4864447,4865341,4866792,4867327,4867420,4867457,4867638,4868177,4869740,4870086,4870213,4870406,4870432,4870439,4870482,4870485,4870487,4870503,4870524,4870595,4870647,4870656,4870658,4870683,4870687,4870740,4870745,4870803,4870916,4870920,4872188,4872189,4872190,4872191,4872364,4874189,4874538,4874610,4875004,4875373,4875741,4876838,4877015,4877022,4877024,4877531,4879250,4880281,4881640,4881885,4882622,4882657,4884155,4887774,4893699,4895450,4896603,4903618,4908043,4909623,4922053,4922058,4937802,4948405,4948410,4950916,4955688,4956236,4969428,4969434,4969514,4969557,4969560,4969572,4969598,4969688,4985531,5006819,5012063,5012113,5012126,5012166,5012181,5012264,5012318,5029331,5039907,5048459,5048600,5050655,5079821,5081867,5090693,5090704,5092886,5098907,5120694,5135730,5135762,5135767,5142849,5143708,5148426,5154876,5160928,5166948,5178611,5180278,5180294,5180297,5180317,5180335,5180343,5183978,5202403,5205479,5219322,5224062,5229180,5238755,5238759,5238766,5244422,5253250,5265243,5277133,5277645,5307262,5315260,5315278,5315304,5315369,5320788,5322566,5342424,5345427,5347766,5348986,5351487,5361694,5363244,5371205,5374539,5389986,5390006,5390027,5421384,5425720,5427603,5462212,5471803,5479186,5497020,5499755,5499822,5520590,5538328,5545251,5553517,5560035,5568842,5581403,5584197,5585026,5590849,5607690,5621429,5622867,5628449,5632770,5662563,5663239,5691320,5699306,5729652,5779245,5801035,5809297,5813161,5817479,5825444,5831187,5844897,5853855,5875066,5877184,5899367,5921595,5937435,5946898,5999588,6026852,6036455,6036456,6036457,6045628,6061373,6070753,6084731,6094513,6096592,6112242,6144092,6157753,6199287,6213231,6213517,6230654,6236841,6242723,6257087,6257525,6264184,6271951,6272674,6279817,6279827,6279831,6279850,6295735,6308519,6309800,6316983,6317296,6342289,6354670,6356259,6377906,6384689,6396264,6433856,6467978,6512647,6525260,6547174,6547175,6552761,6553099]

    def brush_0716(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}

        for each_sp1, each_visible, each_limit in [['钙', 14, 3000000],['益生菌',15,3000000],['矿物质',16,1000000],['氨糖软骨素',17,1000000],['鱼油',19,3000000]]:
        # for each_sp1, each_visible, each_limit in [['钙', 14, 3000000]]:
            for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 5']:
                uuids_update = []
                uuids = []
                c = 0
                where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(ctbl, each_sp1, each_source)
                # bids = self.bids1
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if sales_by_uuid1[uuid2] >= each_limit:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,3,4):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            c = c + 1
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            k = '{}-{}-{}'.format(each_sp1,each_source,alias_all_bid)
                            if k not in dd:
                                dd[k] = 0
                            dd[k] = dd[k] + 1
                cc['{}+{}'.format(each_sp1, str(each_source))] = [len(uuids_update), len(uuids)]
                if len(uuids_update) > 0:
                    sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible, ','.join([str(v) for v in uuids_update]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=each_visible, sales_by_uuid=sales_by_uuid1)
                print(len(uuids_update), len(uuids))

        for i in cc:
            print(i,cc[i])
        return True

    def brush_0516(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}

        uuids_update = []
        uuids = []
        c = 0
        # filename = 'C:/Users/Tuna/Desktop/20210716钙按照itemid新增出题.v2.csv'
        filename = 'C:/Users/Tuna/Desktop/20210719 按照itemid出题汇总表.xlsx'
        df = pd.read_excel(filename, sep=',',sheet_name=0,header=0,dtype=str, encoding='gb18030')
        items = ','.join(['\'{}\''.format(row[0]) for row in df.values])
        # items = []
        # with open(filename,'r') as f:
        #     reader = csv.reader(f)
        #     for row in reader:
        #         if reader.line_num == 0:
        #             continue
        #         items.append("'{}'".format(row[0]))
        # where = "item_id in ({})".format(','.join(items))
        # bids = self.bids1
        where = 'item_id in ({})'.format(items)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=20000, where=where)
        print('item',len(df.values),'ret',len(ret))
        xxx = 0
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,3,4):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
            else:
                if str(tb_item_id) == '619416051123':
                    xxx = xxx + 1
                uuids.append(uuid2)
                c = c + 1
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        # if len(uuids_update) > 0:
        #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 14, ','.join([str(v) for v in uuids_update]))
        #     print(sql)
        #     self.cleaner.db26.execute(sql)
        #     self.cleaner.db26.commit()
        # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=14, sales_by_uuid=sales_by_uuid1)
        print(len(uuids_update), len(uuids))
        print('xxxx', xxx)
        return True

    def skip_helper_141(self, smonth, emonth, uuids):

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # 找出当期之前所有出过的题
        sql = '''select uuid2 from {tbl}  where (snum, tb_item_id, real_p1,month)
        in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}' and uuid2!=0  group by snum, tb_item_id, real_p1)
        '''.format(tbl= self.cleaner.get_tbl(), smonth=smonth)
        ret = self.cleaner.db26.query_all(sql)
        # map = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        map = [v[0] for v in ret]


        # 找出当期前出题的最近一条机洗结果
        # sql_result_old_uuid = '''
        # select a.*,b.c_sp1,b.alias_all_bid bid_f from
        # (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        # SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2 FROM {atbl}
        # WHERE pkey  < '{smonth}'
        # GROUP BY source, item_id, p1,uuid2) a
        # JOIN
        # (select * from {ctbl}) b
        # on a.uuid2=b.uuid2
        # where a.uuid2 in({uuids})
        # '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, uuids=','.join(["'"+str(v)+"'" for v in map]))
        sql_result_old_uuid = '''
        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2,c_sp1 FROM {ctbl}
        WHERE pkey  < '{smonth}' and uuid2 in({uuids})
        GROUP BY source, item_id, p1,uuid2,c_sp1

        '''.format(ctbl=ctbl, smonth=smonth, uuids=','.join(["'"+str(v)+"'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5]] for v in db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, alias_all_bid,uuid2,c_sp1 FROM {ctbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0
        AND uuid2 NOT IN (SELECT uuid2 FROM {atbl}  WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sign = -1)
        and uuid2 in ({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth, uuids=','.join(["'"+str(v)+"'" for v in uuids]))

        map_help_new = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5], v[4]] for v in db.query_all(sql_result_new_uuid)}

        # 当期之后出的题目（包括当期）
        sql2 = "SELECT distinct snum, tb_item_id, real_p1, id, visible FROM {} where pkey >='{}'  ".format(
            self.cleaner.get_tbl(), smonth)
        ret2 = self.cleaner.db26.query_all(sql2)
        map2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret2}

        new_uuids = []
        old_uuids_update = []

        for new_key, new_value in map_help_new.items():
            # 当期之后有出题（包括当期），则忽略
            if new_key in map2.keys():
                continue
            else:
                # 当期的新题
                if new_key not in map_help_old.keys():
                    new_uuids.append(str(new_value[2]))
                else:
                    # 之前有出过题，但某些重要机洗值改变，重新出题
                    for old_key, old_value in map_help_old.items():
                        if new_key == old_key and (new_value[0] != old_value[0] or new_value[1] != old_value[1]):
                            old_uuids_update.append(str(new_value[2]))
                        else:
                            continue

        return new_uuids, old_uuids_update, map_help_new

    def brush_0429(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        uuids = []

        where = '''
                uuid2 in ('fd2a6ce8-e8ca-4a04-95d6-1f4882e42410'
                        )
                '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where,
                                                                   limit=100000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0513(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}
        uuid_add_value = []
        sales_by_uuid= {}

        uuids_update = []
        uuids = []

        where = '''
        sid in (23561143, 23362097)
        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=20000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_add_value.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            else:
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=55, sales_by_uuid=sales_by_uuid1)

        return True



    ###月度默认出题规则，注意更改visible和--已作废
    def brush_old(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}
        uuid_add_value = []
        sales_by_uuid= {}

        ##1 & 2
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for each_sp1, each_visible, each_limit in [['氨基酸', 40, 800000], ['动物精华', 41, 20000000]]:
            for each_sp1, each_visible, each_limit in [['鱼油', 56, 800000], ['钙', 57, 800000], ['益生菌', 58, 800000],
                                                       ['维生素', 59, 800000], ['植物精华', 60, 800000],
                                                       ['矿物质', 61, 800000], ['氨糖软骨素', 62, 800000], ['辅酶Q10', 63, 800000], ['氨基酸', 64, 800000], ['动物精华', 65, 20000000]]:
                for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11',
                                    'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 5', 'source = 11']:
                    uuids_update = []
                    uuids = []
                    c = 0
                    where = '''
                            uuid2 in (select uuid2 from {} where c_sp1 = \'{}\' ) and {}
                            '''.format(ctbl, each_sp1, each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=20000)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if sales_by_uuid1[uuid2] >= each_limit:
                            uuid_add_value.append(uuid2)
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            else:
                                uuids.append(uuid2)
                                c = c + 1
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                                k = '{}-{}'.format(each_source, alias_all_bid)
                                if k not in dd:
                                    dd[k] = 0
                                dd[k] = dd[k] + 1
                    print(len(uuids_update), len(uuids))
                    cc['{}~{}@{}@{}'.format(ssmonth, eemonth, each_sp1, str(each_source))] = [len(uuids_update), len(uuids)]
                    if len(uuids_update) > 0:
                        sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible, ','.join([str(v) for v in uuids_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=each_visible, sales_by_uuid=sales_by_uuid1)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_141(smonth, emonth, uuid_add_value)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=66, sales_by_uuid=sales_by_uuid)



        for i in cc:
            print(i, cc[i])
        # exit()


        return True


    def brush_xxxx(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        cc = {}
        dd = {}
        ccc = 0
        bids = [131009,112244,326236,48678,401951,241814,326026,130885,1210010,1856905,845348,387249,393278,48221,326010,474173,333207,333159,27591,103792,393304,393280,59463,472799,146506,130400,446738,393276,393274,393271,347680,326127,333285,391982,558781,819397,207212,21150,333391,474186,4452952,387336,273629,53021,47220,272287,3919777,5134375,347699,5969283,387255,387270,387332,1531329,387129,5950105,5462839,714073,392061,392074,387333,386477,5874301,5831186,5798604,506932,387412,47161,104447,48330,116368,563041,1768164,1882861,4715802,326893,17447,217336,2386941,3837273,48704,93278,48210,116183,95277,401688,3847776,1067497,4730279,475116,387577,387454,5916287,402002,387121,387135,130428,3472327,110054,59590,474182,387334,89329,1856459,3158974,3458059,2188446,5618981,134814,326203,1731655,227635,94398,387127,387269,4548333,77846,561121,387450,48572,130554,2265513,393970,2888376,156841,347244,12741,2608328,48256,3107725,85409,1809728,3923130,2189056,199009,52052,401994,1531296,2458436,3436421,6378376,6236534,3514536,1962703,380617,472087,11273,217042,185573,120436,131198,4838,52067,399125,4804356,3117148,6123206,3986403,939231,1511941,52323,472854,966656,4538470,333291,333250,52447,387343,1210332,554015,1882597,1986355,5909731,245082,5709097,387689,326238,2686529,111201,326186,48380,52711,59539,47277,4328158,3923119,4538428,3923106,1224207]
        cids = [50026464,50026713,1852,5379,5422]
        for each_cid in cids:
            for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 5']:
                uuids_update = []
                uuids = []
                c = 0
                where = '{} and cid = {}'.format(each_source, each_cid)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                print('return', len(ret))
                ccc = ccc + len(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if (int(alias_all_bid) in bids):
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (2,5,6,7,8):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            c = c + 1
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            # k = '{}-{}-{}'.format(each_sp1,each_source,alias_all_bid)
                            # if k not in dd:
                            #     dd[k] = 0
                            # dd[k] = dd[k] + 1
                cc[where] = len(uuids)

                if len(uuids_update) > 0:
                    sql = 'update {} set visible = 9, visible_check = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 4, ','.join([str(v) for v in uuids_update]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()

                self.cleaner.add_brush(uuids, clean_flag, visible_check=4, visible=9, sales_by_uuid=sales_by_uuid1)
                print(len(uuids_update), len(uuids))

                # print('add', len(uuids))
                # for i in dd:
                #     print(i, dd[i])
        print('aaaaaaa', ccc)
        for i in cc:
            print(i,cc[i])
        return True


    def brush_0822feiqi(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}
        uuid_add_value = []
        sales_by_uuid = {}

        ##1 & 2
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1, each_visible, each_limit in [['鱼油', 125, 800000], ['钙', 125, 800000], ['益生菌', 125, 800000],
                                                       ['维生素', 125, 800000], ['植物精华', 125, 800000],
                                                       ['矿物质', 125, 800000], ['氨糖软骨素', 125, 800000], ['辅酶Q10', 125, 800000], ['氨基酸', 125, 800000], ['动物精华', 125, 20000000]]:
                uuids_update_sp1 = []
                uuids_sp1 = []
                for each_source in ['source = 1 and shop_type < 20 and shop_type = 12',
                                    'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2',
                                    'source = 5', 'source = 11']:
                    uuids_update = []
                    uuids = []
                    c = 0
                    where = '''
                            c_sp1 = \'{}\' and  ((sales/100) / num)>=1 and c_sp33 !='无效链接'  and {}
                            '''.format(each_sp1, each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=20000)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if sales_by_uuid1[uuid2] >= each_limit:
                            uuid_add_value.append(uuid2)
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                                    uuids_update_sp1.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            else:
                                uuids.append(uuid2)
                                uuids_sp1.append(uuid2)
                                c = c + 1
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                                k = '{}-{}'.format(each_source, alias_all_bid)
                                if k not in dd:
                                    dd[k] = 0
                                dd[k] = dd[k] + 1
                    print(len(uuids_update), len(uuids))
                    cc['{}~{}@{}@{}'.format(ssmonth, eemonth, each_sp1, str(each_source))] = [len(uuids_update), len(uuids)]
                if len(uuids_update) > 0:
                    sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible, ','.join([str(v) for v in uuids_update_sp1]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids_sp1, clean_flag, visible_check=1, visible=each_visible, sales_by_uuid=sales_by_uuid)

        uuids = []
        uuids_update = []

        where = '''
                sid in (23561143, 23362097)
                '''
        ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=20000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_add_value.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=125, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_141(smonth, emonth, uuid_add_value)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=125, sales_by_uuid=sales_by_uuid)

        update_sql_dy = '''
               update {tbl} set visible = {visible_douyin} where cast(created as date) = cast(now() as date) and snum = 11 and flag = 0
               '''.format(tbl=self.cleaner.get_tbl(), visible_douyin=125)
        print(update_sql_dy)
        self.cleaner.db26.execute(update_sql_dy)
        self.cleaner.db26.commit()

        for i in cc:
            print(i, cc[i])
        # exit()

        return True



    ###月度默认出题规则，注意更改visible和
    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        visible_tmall = 43
        visible_dy = 44
        visible_jd = 45
        visible_tb = 46
        visible_kaola = 47

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        cc = {}
        dd = {}
        uuid_add_value = []
        sales_by_uuid = {}

        ##1 & 2
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1, each_visible, each_limit in [['鱼油', 127, 800000], ['钙', 127, 800000], ['益生菌', 127, 800000],
                                                       ['维生素', 127, 800000], ['植物精华', 127, 800000],
                                                       ['矿物质', 127, 800000], ['氨糖软骨素', 127, 800000], ['辅酶Q10', 127, 800000], ['氨基酸', 127, 800000], ['动物精华', 127, 20000000]]:
                uuids_update_sp1 = []
                uuids_sp1 = []
                for each_source in ['source = 1 and shop_type < 20 and shop_type = 12',
                                    'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2',
                                    'source = 5', 'source = 11']:
                    uuids_update = []
                    uuids = []
                    c = 0
                    where = '''
                            c_sp1 = \'{}\' and  ((sales/100) / num)>=1 and c_sp33 !='无效链接'  and {}
                            '''.format(each_sp1, each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=20000, filter_by='by_date')
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if sales_by_uuid1[uuid2] >= each_limit:
                            uuid_add_value.append(uuid2)
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                                    uuids_update_sp1.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            else:
                                uuids.append(uuid2)
                                uuids_sp1.append(uuid2)
                                c = c + 1
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                                k = '{}-{}'.format(each_source, alias_all_bid)
                                if k not in dd:
                                    dd[k] = 0
                                dd[k] = dd[k] + 1
                    print(len(uuids_update), len(uuids))
                    cc['{}~{}@{}@{}'.format(ssmonth, eemonth, each_sp1, str(each_source))] = [len(uuids_update), len(uuids)]
                if len(uuids_update_sp1) > 0:
                    sql = 'update {} set visible = {}, visible_check=2, created = now() where id in ({})'.format(self.cleaner.get_tbl(), each_visible, ','.join([str(v) for v in uuids_update_sp1]))
                    print(sql)
                    self.cleaner.db26.execute(sql)
                    self.cleaner.db26.commit()
                self.cleaner.add_brush(uuids_sp1, clean_flag, visible_check=2, visible=each_visible, sales_by_uuid=sales_by_uuid)

        uuids = []
        uuids_update = []

        where = '''
                sid in (23362097,34981989,23561143,30133712)
                '''
        ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=20000, filter_by='by_date')
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_add_value.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 3, 4):
                    uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids, clean_flag, visible_check=2, visible=127, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_141(smonth, emonth, uuid_add_value)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=2, visible=127, sales_by_uuid=sales_by_uuid)

        update_sql_tmall = '''
               update {tbl} set visible = {visible_tmall},visible_check=1 where cast(created as date) = cast(now() as date) and snum = 1 and (shop_type>20 or shop_type<10) and visible_check=2 and visible=127
               '''.format(tbl=self.cleaner.get_tbl(), visible_tmall=visible_tmall)
        print(update_sql_tmall)
        self.cleaner.db26.execute(update_sql_tmall)
        self.cleaner.db26.commit()

        update_sql_dy = '''
                       update {tbl} set visible = {visible_dy},visible_check=1 where cast(created as date) = cast(now() as date) and snum = 11 and visible_check=2 and visible=127
                       '''.format(tbl=self.cleaner.get_tbl(), visible_dy=visible_dy)
        print(update_sql_dy)
        self.cleaner.db26.execute(update_sql_dy)
        self.cleaner.db26.commit()

        update_sql_jd = '''
                       update {tbl} set visible = {visible_jd},visible_check=1 where cast(created as date) = cast(now() as date) and snum = 2 and visible_check=2 and visible=127
                       '''.format(tbl=self.cleaner.get_tbl(), visible_jd=visible_jd)
        print(update_sql_jd)
        self.cleaner.db26.execute(update_sql_jd)
        self.cleaner.db26.commit()

        update_sql_tb = '''
                       update {tbl} set visible = {visible_tb},visible_check=1 where cast(created as date) = cast(now() as date) and snum = 1 and shop_type = 12 and visible_check=2 and visible=127
                       '''.format(tbl=self.cleaner.get_tbl(), visible_tb=visible_tb)
        print(update_sql_tb)
        self.cleaner.db26.execute(update_sql_tb)
        self.cleaner.db26.commit()

        update_sql_kaola = '''
                       update {tbl} set visible = {visible_kaola},visible_check=1 where cast(created as date) = cast(now() as date) and snum = 5 and visible_check=2 and visible=127
                       '''.format(tbl=self.cleaner.get_tbl(), visible_kaola=visible_kaola)
        print(update_sql_kaola)
        self.cleaner.db26.execute(update_sql_kaola)
        self.cleaner.db26.commit()



        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        self.hotfix_ecshop(tbl, dba, '店铺分类')

        sql = '''
        ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
            k='店铺分类', multiIf(source=1 AND (shop_type<20 and shop_type>10), 'C2C', v='EKA_FSS', 'EKA', v),
        v), c_props.name, c_props.value)
        WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
        ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
            k='是否孕产哺乳保健', If(
                        multiSearchAny(`c_props.value`[indexOf(`c_props.name`, '人群')], ['孕妇','男士(备孕)'])
                    AND multiSearchAny(`c_props.value`[indexOf(`c_props.name`, '孕期阶段')], ['男性备孕期','女性备孕期','怀孕期','哺乳期']),
                '孕产哺乳保健', '其它'
            ),
            k='机洗答题子品类对不上', If(v != '' AND v != `c_props.value`[indexOf(`c_props.name`, '子品类')], '是', '否'),
            k='子品类', If(`c_props.value`[indexOf(`c_props.name`, '机洗答题子品类对不上')] != '' AND v != `c_props.value`[indexOf(`c_props.name`, '机洗答题子品类对不上')], `c_props.value`[indexOf(`c_props.name`, '机洗答题子品类对不上')], v),
        v), c_props.name, c_props.value)
        WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
        ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
            k='总销量', toString(ROUND(c_num * toUInt32OrZero(IF(`c_props.value`[indexOf(`c_props.name`, 'SKU件数')]='','1',`c_props.value`[indexOf(`c_props.name`, 'SKU件数')])))),
        v), c_props.name, c_props.value)
        WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # IF (
        #     LENGTH(arrayFilter((v, k) -> k = '人群' AND v IN ['婴幼儿','儿童','老年人','通用'], `c_props.value`, `c_props.name`)) == 0
        # AND LENGTH(arrayFilter((v, k) -> k = '孕期阶段' AND v IN ['备孕期','怀孕期','哺乳期'], `c_props.value`, `c_props.name`)) > 0
        # )

        db26 = self.cleaner.get_db('26_apollo')

        sql = 'SELECT bid, name, alias_bid FROM cleaner.`clean_141_brand_link_dict` WHERE del_flag = 0'
        ret = db26.query_all(sql)

        a, b, c = [], [], []
        for bid, name, alias_bid, in ret:
            a.append(str(bid))
            b.append(str(alias_bid))

        sql = '''
            ALTER TABLE {} UPDATE `c_alias_all_bid` = transform(
                `c_all_bid`, [{}], [{}], `c_alias_all_bid`
            ) WHERE 1
        '''.format(tbl, ','.join(a), ','.join(b))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `sp店铺分类` = 'EKA'
            WHERE `sp店铺分类` = 'EKA_FSS'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
        ALTER TABLE {} UPDATE
            `sp是否孕产哺乳保健` = If(
                multiSearchAny(`sp人群`, ['孕妇','男士(备孕)']) AND multiSearchAny(`sp孕期阶段`, ['男性备孕期','女性备孕期','怀孕期','哺乳期']),
                '孕产哺乳保健', '其它'
            ),
            `sp机洗答题子品类对不上` = If(`sp机洗答题子品类对不上`!='' AND `sp机洗答题子品类对不上`!=`sp子品类`, '是', '否'),
            `sp子品类` = If(`sp机洗答题子品类对不上`!='' AND `sp子品类`!=`sp机洗答题子品类对不上`, `sp机洗答题子品类对不上`, `sp子品类`)
        WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
        ALTER TABLE {} UPDATE
            `sp总销量` = toString(ROUND(clean_num * toUInt32OrZero(IF(`spSKU件数`='','1',`spSKU件数`))))
        WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp白皮书-功效`) AS p PREWHERE `sp白皮书-功效` != '' GROUP BY trim(p)'.format(tbl)
        # ret = dba.query_all(sql)

        # self.cleaner.add_miss_cols(tbl, {'sp白皮书-功效-{}'.format(v):'String' for v, in ret})

        # dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        # sql = '''
        #     CREATE TABLE {t}_set ENGINE = Set AS
        #     SELECT uuid2, trim(p) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp白皮书-功效`) AS p
        #     PREWHERE `sp白皮书-功效` != ''
        # '''.format(t=tbl)
        # dba.execute(sql)

        # cols = ['`sp白皮书-功效-{c}`=IF((uuid2, \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        # sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

        # sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
        # dba.execute(sql)

        # sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp白皮书-成分`) AS p PREWHERE `sp白皮书-成分` != '' GROUP BY trim(p)'.format(tbl)
        # ret = dba.query_all(sql)

        # self.cleaner.add_miss_cols(tbl, {'sp白皮书-成分-{}'.format(v):'String' for v, in ret})

        # dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        # sql = '''
        #     CREATE TABLE {t}_set ENGINE = Set AS
        #     SELECT uuid2, trim(p) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp白皮书-成分`) AS p
        #     PREWHERE `sp白皮书-成分` != ''
        # '''.format(t=tbl)
        # dba.execute(sql)

        # cols = ['`sp白皮书-成分-{c}`=IF((uuid2, \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        # sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

        # sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
        # dba.execute(sql)


    # # 豆子要加的
    def pre_brush_modify(self, v, products, prefix):
        if v['visible'] in [1]:
            v['flag'] = 0


    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            vv['sp46'] = vv['sp46'].replace('Ծ‸ Ծ', '|')
            vv['sp59'] = vv['sp59'].replace('Ծ‸ Ծ', '|')

        if v['flag'] != 2:
            return

        pack_name = []
        for vv in v['split_pids']:
            if v['pid'] == 1200:
                vv['sp43'] = '礼盒'
            elif v['pid'] in (1081,5826):
                vv['sp43'] = '套包'
            else:
                vv['sp43'] = '单品'
            # 用来判断机洗和人工是否不一致
            if vv['sp1'] not in ('', '其它', '其它保健品') and vv['sku_name'].find('___') == 0:
                vv['sp55'] = vv['sp1']
                vv['sp1'] = ''
            if vv['sku_name'].find('___') == -1:
                pack_name.append(vv['sku_name'])
        pack_name.sort()

        for vv in v['split_pids']:
            vv['sp83'] = '+'.join(pack_name)


    # def get_cachex(self):
    #     cache = {}

    #     dba, btbl = self.get_b_tbl()
    #     dba, ctbl = self.get_c_tbl()
    #     dba = self.cleaner.get_db(dba)

    #     cols = ['pid', 'all_bid_sp'] + ['sp{}'.format(i) for i in range(1,54)]
    #     sql = 'select CONCAT(toString(source), \'-\', item_id, sp54) k, {} from {} where clean_type >= 0 group by k'.format(
    #         ','.join(['any({})'.format(c) for c in cols]), ctbl
    #     )
    #     ret = dba.query_all(sql)
    #     c = {}
    #     for v in ret:
    #         c[v[0]] = {k:v[i+1] for i, k in enumerate(cols)}
    #     cache['clean_sp1'] = c

    #     cols = ['pid', 'all_bid_sp'] + ['sp{}'.format(i) for i in range(1,54)] + ['sp22']
    #     sql = '''
    #         SELECT concat(toString(source), '-', item_id, sp54) k, {} FROM {}
    #         WHERE similarity >= 2 AND split_rate = 1 AND sp21 NOT LIKE '\\\\_\\\\_\\\\_%'
    #         LIMIT 1 BY k
    #     '''.format(
    #         ','.join([c.replace('all_bid_sp', 'alias_all_bid') for c in cols]), btbl
    #     )
    #     ret = dba.query_all(sql)
    #     c = {}
    #     for v in ret:
    #         c[v[0]] = {k:v[i+1] for i, k in enumerate(cols)}
    #     cache['clean_pid'] = c

    #     return cache


    def update_sp54(self):
        cdba, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cdba)
        db26 = self.cleaner.get_db('26_apollo')

        sql = 'SELECT id, uuid2 FROM {} WHERE uuid2 != 0'.format(self.cleaner.get_tbl())
        ret = db26.query_all(sql)

        sql = 'SELECT uuid2, sp54 FROM {} WHERE uuid2 IN ({}) AND sp54!=\'\''.format(
            ctbl, ','.join(['\'{}\''.format(v[1]) for v in ret])
        )
        rrr = cdba.query_all(sql)
        rrr = {str(v[0]):v[1] for v in rrr}
        for id, uuid2, in ret:
            if uuid2 not in rrr:
                continue
            sql = 'UPDATE {} SET spid54=%s WHERE id = {}'.format(self.cleaner.get_tbl(), id)
            db26.execute(sql, (rrr[uuid2],))
            print(id)
        db26.commit()


    # def process_exx(self, tbl, prefix, logId=0):
    #     super().process_exx(tbl)

    #     dba = self.cleaner.get_db('chsop')
    #     db26 = self.cleaner.get_db('26_apollo')

    #     sql = 'SELECT bids, names, alias_bid, vitamins FROM cleaner.`clean_141_brand_link_dict` WHERE del_flag = 0'
    #     ret = db26.query_all(sql)

    #     a, b, c = [], [], []
    #     for bids, names, alias_bid, vitamins, in ret:
    #         name = json.loads(names)[0]
    #         for bid in bids.split(','):
    #             a.append(str(bid)+'_'+str(vitamins))
    #             b.append(str(alias_bid))
    #             c.append(name)

    #     sql = 'DROP TABLE IF EXISTS {}x'.format(tbl)
    #     dba.execute(sql)

    #     sql = 'CREATE TABLE {t}x AS {t}'.format(t=tbl)
    #     dba.execute(sql)

    #     sql = 'SELECT source, pkey, toYYYYMM(pkey) FROM {} GROUP BY source, pkey'.format(tbl)
    #     ret = dba.query_all(sql)

    #     for source, pkey, month, in ret:
    #         sql = '''
    #             INSERT INTO {t}x (
    #                 `uuid2`,`sign`,`ver`,`old_uuid`,`old_sign`,`old_ver`,`pkey`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,
    #                 `sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,
    #                 `num`,`trade_props.name`,`trade_props.value`,`trade_props_hash`,`source`,`clean_cid`,`clean_pid`,`alias_pid`,
    #                 `created`,`old_all_bid`,`old_alias_all_bid`,`old_price`,`old_num`,`old_sales`,`old_tip`,`old_rbid`,
    #                 `old_sub_brand`,`old_region`,`old_trade`,`clean_number`,`clean_cln_ver`,`clean_sku_id`,`clean_model_id`,
    #                 `clean_split_rate`,`clean_brush_id`,`clean_props.name`,`clean_props.value`
    #             ) SELECT
    #                 a.`uuid2`,a.`sign`,a.`ver`,a.`old_uuid`,a.`old_sign`,a.`old_ver`,a.`pkey`,a.`date`,a.`cid`,a.`real_cid`,a.`item_id`,a.`sku_id`,a.`name`,
    #                 a.`sid`,a.`shop_type`,a.`brand`,a.`all_bid`,a.`alias_all_bid`,a.`region_str`,a.`price`,a.`org_price`,a.`promo_price`,a.`img`,a.`sales`,
    #                 a.`num`,a.`trade_props.name`,a.`trade_props.value`,a.`trade_props_hash`,a.`source`,a.`clean_cid`,a.`clean_pid`,a.`alias_pid`,
    #                 a.`created`,a.`old_all_bid`,a.`old_alias_all_bid`,a.`old_price`,a.`old_num`,a.`old_sales`,a.`old_tip`,a.`old_rbid`,
    #                 a.`old_sub_brand`,a.`old_region`,a.`old_trade`,a.`clean_number`,a.`clean_cln_ver`,a.`clean_sku_id`,a.`clean_model_id`,
    #                 a.`clean_split_rate`,a.`clean_brush_id`,
    #                 arrayConcat(
    #                     arrayFilter((x) -> x NOT IN ['合并品牌id','宝贝品牌id'], a.`clean_props.name`), ['合并品牌id','宝贝品牌id']
    #                 ),
    #                 arrayConcat(
    #                     arrayFilter((k, x) -> x NOT IN ['合并品牌id','宝贝品牌id'], a.`clean_props.value`, a.`clean_props.name`),
    #                     [
    #                         toString(transform(CONCAT(toString(a.`all_bid`),'_',toString(a.sp1)), [{a}], [{b}], a.`alias_all_bid`)),
    #                         multiIf(
    #                             a.`clean_brush_id` > 0 AND CONCAT(toString(a.`alias_all_bid`),'_',toString(a.sp1)) IN [{a}], toString(a.`alias_all_bid`),
    #                             a.`clean_brush_id` > 0, '-1',
    #                             ifNull(b.`brand`, '0')
    #                         )
    #                     ]
    #                 )
    #             FROM (SELECT *, IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')]='维生素', 1, 0) AS sp1 FROM {t} WHERE `source`={s} AND pkey='{p}') a
    #             LEFT JOIN (SELECT * FROM sop_e.entity_prod_91130_E_brand WHERE `source`={s} AND toYYYYMM(`month`)={m} LIMIT 1 BY condition, item_id, name) b
    #             ON (
    #                 toYYYYMM(a.`date`) = toYYYYMM(b.`month`) AND
    #                 a.sp1 = b.`condition` AND
    #                 a.`source` = b.`source` AND
    #                 a.item_id = b.item_id AND
    #                 a.name = b.name
    #             )
    #         '''.format(t=tbl, a=','.join(['\'{}\''.format(v) for v in a]), b=','.join(b), s=source, p=pkey, m=month)
    #         dba.execute(sql)

    #     sql = 'DROP TABLE {}'.format(tbl)
    #     dba.execute(sql)

    #     sql = 'RENAME TABLE {t}x TO {t}'.format(t=tbl)
    #     dba.execute(sql)

    #     sql = '''
    #         ALTER TABLE {} UPDATE `clean_props.value` = arrayMap((k, v) -> IF(
    #             k='宝贝品牌id', transform(CONCAT(v, '_', IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')]='维生素', '1', '0')), [{}], [{}], v),
    #         v), clean_props.name, clean_props.value)
    #         WHERE 1
    #     '''.format(tbl, ','.join(['\'{}\''.format(v) for v in a]), ','.join(['\'{}\''.format(v) for v in b]))
    #     dba.execute(sql)

    #     while not self.cleaner.check_mutations_end(dba, tbl):
    #         time.sleep(5)

    #     sql = '''
    #         ALTER TABLE {} UPDATE
    #             `clean_props.name` = arrayConcat(
    #                 arrayFilter((x) -> x NOT IN ['合并品牌','宝贝品牌'], `clean_props.name`), ['合并品牌','宝贝品牌']
    #             ),
    #             `clean_props.value` = arrayConcat(
    #                 arrayFilter((k, x) -> x NOT IN ['合并品牌','宝贝品牌'], `clean_props.value`, `clean_props.name`),
    #                 [
    #                     transform(
    #                         CONCAT(clean_props.value[indexOf(clean_props.name, '合并品牌id')], '_', IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')]='维生素', '1', '0')),
    #                             ['-1_0','-1_1', '0_0', '0_1', {a}], ['Others', 'Others', '', '', {c}],
    #                         dictGetOrDefault('all_brand', 'name', tuple(toUInt32OrZero(clean_props.value[indexOf(clean_props.name, '合并品牌id')])), '')
    #                     ),
    #                     transform(
    #                         CONCAT(clean_props.value[indexOf(clean_props.name, '宝贝品牌id')], '_', IF(`clean_props.value`[indexOf(`clean_props.name`, '子品类')]='维生素', '1', '0')),
    #                             ['-1_0','-1_1', '0_0', '0_1', {a}], ['Others', 'Others', '', '', {c}],
    #                         dictGetOrDefault('all_brand', 'name', tuple(toUInt32OrZero(clean_props.value[indexOf(clean_props.name, '宝贝品牌id')])), '')
    #                     )
    #                 ]
    #             )
    #         WHERE 1
    #     '''.format(tbl, a=','.join(['\'{}\''.format(v) for v in a]), c=','.join(['\'{}\''.format(v.replace('\'', '\\\'')) for v in c]))
    #     dba.execute(sql)

    #     while not self.cleaner.check_mutations_end(dba, tbl):
    #         time.sleep(5)