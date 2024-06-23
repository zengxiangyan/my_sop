import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0317(self, smonth, emonth,logId=-1):

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



        ### condition1
        uuids1 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where1 = '''
            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where1, rate=0.3)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids1.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids1, clean_flag, visible=1, sales_by_uuid=sales_by_uuid)


        ### condition2
        uuids2 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where2 = '''
            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where2, rate=0.6)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if alias_all_bid in [139477, 133212, 104418]:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids2.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids2, clean_flag, visible=2, sales_by_uuid=sales_by_uuid)

        ### condition3
        uuids3 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where3 = '''
                    c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
                    '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where3, rate=0.7)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if alias_all_bid in [19357,	48380,	65113,	72362,	104418,	131356,	131638,	133184,	133212,	134714,	135290,	135302,	135309,	135428,	135466,	135497,	135741,	135752,	135821,	135880,	139477,	140498,	142142,	187374,	273949,	278144,	295339,	300051,	300814,	328298,	1258426,	1297347,	1331219,	1378787,	1378788,	2352046,	2527750,	2676254,	2818769,	3499542,	3786176,	3792773,	3793188,	4493804,	4521977,	4580088,	4770660,	5228560,	5414857,	5595814,	5782054,	5904471,	5948535,	6001901,	6004491,	6477989,	6541337,	6719999,	6722883,	6860720]:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids3.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids3, clean_flag, visible=3, sales_by_uuid=sales_by_uuid)

        ### condition4
        uuids4 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where4 = '''
                    c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
                    '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where4, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if alias_all_bid in [19357,	48380,	65113,	72362,	104418,	131356,	131638,	133184,	133212,	134714,	135290,	135302,	135309,	135428,	135466,	135497,	135741,	135752,	135821,	135880,	139477,	140498,	142142,	187374,	273949,	278144,	295339,	300051,	300814,	328298,	1258426,	1297347,	1331219,	1378787,	1378788,	2352046,	2527750,	2676254,	2818769,	3499542,	3786176,	3792773,	3793188,	4493804,	4521977,	4580088,	4770660,	5228560,	5414857,	5595814,	5782054,	5904471,	5948535,	6001901,	6004491,	6477989,	6541337,	6719999,	6722883,	6860720]:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids4.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids4, clean_flag, visible=4, sales_by_uuid=sales_by_uuid)

        print(len(uuids1), len(uuids2), len(uuids3), len(uuids4))

        return True

    def brush_0320(self, smonth, emonth,logId=-1):

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
        cc = {}



        ### condition1
        uuids1 = []
        sources = ['((source = 1 and  (shop_type > 20 or shop_type < 10)) or source = 2 )']
        # for each_sp8 in ['礼盒', '']:
        for each_source in sources:
            where1 = '''
            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where1, rate=0.3)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids1.append(uuid2)
                # if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                #     continue
                # else:
                #     uuids1.append(uuid2)
                #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

                # cc['{}~{}'.format(each_sp8, each_source)] = [len(uuids1)]

        print(len(uuids1))

        return True

    def brush(self, smonth, emonth,logId=-1):

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1

        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible, pid in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible, pid])
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in data}

        sales_by_uuid = {}

        ### condition1

        uuids_lihe = []
        uuids_feilihe = []

        delete_id = []

        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            uuids_total = []
            uuids_lihe_total = []
            uuids_feilihe_total = []
            where_t = '''
            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source} 
            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where_t, rate=0.3)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_total.append(uuid2)
            uuids_len = len(uuids_total)
            ### 礼盒部分
            where_lihe = '''
            {each_source} and c_sp8 = '礼盒' and (source, item_id, trade_props.value) in (select source, item_id, trade_props.value from {ctbl} where uuid2 in ({uuids_}) and toYYYYMM(date) >= toYYYYMM(toDate('{smonth}')) AND toYYYYMM(date) < toYYYYMM(toDate('{emonth}')))
            '''.format(each_source=each_source, ctbl=ctbl, uuids_=','.join([str(v) for v in uuids_total]), smonth=smonth, emonth=emonth)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where_lihe, rate=0.35)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_lihe_total.append(uuid2)
                uuids_lihe_len = len(uuids_lihe_total)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (480, ):
                        uuids_lihe.append(uuid2)
                        delete_id.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    uuids_lihe.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
            ### 非礼盒部分
            where_feilihe = '''
            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source} and c_sp8 != '礼盒'
            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where_feilihe, limit=uuids_len-uuids_lihe_len)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_feilihe_total.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (480,):
                        uuids_feilihe.append(uuid2)
                        delete_id.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    uuids_feilihe.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
            print(uuids_len, uuids_lihe_len, len(uuids_feilihe_total))

        self.cleaner.add_brush(uuids_lihe+uuids_feilihe, clean_flag, visible=4, sales_by_uuid=sales_by_uuid)

        ### condition2
        uuids2 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where2 = '''
                    c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
                    '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where2, rate=0.6)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if alias_all_bid in [139477, 133212, 104418]:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (480,):
                            uuids2.append(uuid2)
                            delete_id.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                    else:
                        uuids2.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids2, clean_flag, visible=4, sales_by_uuid=sales_by_uuid)

        if len(delete_id) > 0:
            sql = 'delete from  {}  where id in ({})'.format(self.cleaner.get_tbl(),','.join([str(v) for v in delete_id]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()

        ### condition3
        uuids3 = []
        sources = ['source = 1 and  (shop_type > 20 or shop_type < 10)', 'source = 2']
        for each_source in sources:
            where3 = '''
                            c_sp1 = '果干' and c_sp6 != '剔除' and c_sp3 = '有效链接' and {each_source}
                            '''.format(each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where3, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if alias_all_bid in [104418,	135428,	131356,	139477,	295339,	135466,	4770660,	6484146,	4610513,	6493073,	6243486,	142142,	133212,	6760068,	109225,	1413474,	5681354,	65113,	6541337,	4521977,	6708713,	439469,	134714,	3539448,	135302,	7163755,	131379,	6477989,	820689,	5782054,	48380,	3790227,	3786176,	5228560,	300051,	87122,	135821,	4493804,	328298,	157536,	2424300,	3685348,	133183,	279867,	6026254,	1160539,	5283317,	2818769,	135309,	6425563,	135880,	6324300,	1378787,	131636,	139560,	5874333,	2352046,	5171986,	5904471,	278144,	300814,	187374,	133184,	1378788,	135752,	3499542,	4580088,	5948535,	6001901,	2676254,	72362,	135290,	3792773,	6004491,	6860720,	6722883,	6719999,	135497,	273949,	1258426,	1297347,	2527750,	135372,	1331219,	300278,	5414857,	19357,	1108905,	5174013,	5513530,	135342,	6719642,	6228379,	5780428,	297266,	183525,	423509,	6478867,	6775774,	205726,	6101352,	87123,	130062,	3899308,	135755,	5663302,	5325085,	6709193,	4692169,	17749,	6034705,	6935196]:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids3.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    continue

        self.cleaner.add_brush(uuids3, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        print(delete_id)

        self.cleaner.set_default_val(type=1)

        return True