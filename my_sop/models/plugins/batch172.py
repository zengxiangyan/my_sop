import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0524(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        uuids_update = []

        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)

        for each_sp1, each_visible in [['婴儿牛奶粉',1], ['婴儿羊奶粉',2], ['婴儿特殊配方粉',3]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, rate=0.8, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)] in ():
                            #     uuids_update.append(uuid2)
                            continue
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                        uuids.append(uuid2)
                    self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)

        for each_sp1, each_visible in [['婴儿牛奶粉', 1], ['婴儿羊奶粉', 2], ['婴儿特殊配方粉', 3]]:
            top_brand_sql = '''
            select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{sp1}' and pkey >= '{smonth}'
            and pkey < '{emonth}') group by alias_all_bid order by ss desc limit 30
            '''.format(atbl=self.get_entity_tbl()(), ctbl=self.get_clean_tbl(), sp1=each_sp1, smonth='2018-01-01', emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            for each_bid in bids:
                uuids = []
                where = 'alias_all_bid = {} and uuid2 in (select uuid2 from {} where sp1=\'{}\')'.format(each_bid, self.get_clean_tbl(), each_sp1)
                ret, sbs = self.cleaner.process_top(smonth, emonth, where=where, limit=5)
                sales_by_uuid.update(sbs)
                for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                    uuids.append(uuid2)
                self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
        # if len(uuids_update) > 0:
        #     sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
        #     print(sql)
        #     self.cleaner.db26.execute(sql)
        #     self.cleaner.db26.commit()

        return True

    def brush_bak(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        uuidsxxx = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1, id FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)

        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
                            uuids.append(uuid2)
                    print (len(uuids))
                    # self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(uuids_update) > 0:
                        # sql = 'update {} set clean_flag = {} where id in ({})'.format(self.cleaner.get_tbl(), clean_flag, ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
                        uuids.append(uuid2)
                        uuidsxxx.append(uuid2)
                    # self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)

                    print(len(uuids_update), len(uuids))


        for each_sp1, each_visible in [['婴儿特殊配方粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
                            uuids.append(uuid2)
                    print (len(uuids))
                    # self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(uuids_update) > 0:
                        # sql = 'update {} set clean_flag = {} where id in ({})'.format(self.cleaner.get_tbl(), clean_flag, ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['婴儿特殊配方粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0
                        uuids.append(uuid2)
                        uuidsxxx.append(uuid2)
                    # self.cleaner.add_brush(uuids, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)

                    print(len(uuids_update), len(uuids))
                    # if len(uuids_update) > 0:
                    #     sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

        return True




    def brush_0719(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        uuidsxxx = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)

        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4,5):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    print (len(uuids), len(uuids_update), len(id_update))
                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(id_update) > 0:
                    #     sql = 'update {} set visible = 3 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4,5):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,0]
                            uuids.append(uuid2)
                            uuidsxxx.append(uuid2)
                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(id_update) > 0:
                    #     sql = 'update {} set visible = 3 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

                    print(len(uuids), len(uuids_update), len(id_update))


        for each_sp1, each_visible in [['婴儿特殊配方粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4,5):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    print(len(uuids), len(uuids_update), len(id_update))
                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(id_update) > 0:
                    #     sql = 'update {} set visible = 3 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['婴儿特殊配方粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4,5):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,0]
                            uuids.append(uuid2)
                            uuidsxxx.append(uuid2)
                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    # if len(id_update) > 0:
                    #     sql = 'update {} set visible = 3 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

                    print(len(uuids), len(uuids_update), len(id_update))
                    # if len(uuids_update) > 0:
                    #     sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

        return True



    def brush_bak3(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        uuidsxxx = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)




        for each_sp1, each_visible in [['其它', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    print(len(uuids), len(uuids_update), len(id_update))
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=4, sales_by_uuid=sales_by_uuid_1)
                    if len(id_update) > 0:
                        sql = 'update {} set visible = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['其它', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,0]
                            uuids.append(uuid2)
                            uuidsxxx.append(uuid2)
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=4, sales_by_uuid=sales_by_uuid_1)
                    if len(id_update) > 0:
                        sql = 'update {} set visible = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()

                    print(len(uuids), len(uuids_update), len(id_update))
                    # if len(uuids_update) > 0:
                    #     sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

        return True

    def brush_bb(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        uuidsxxx = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)




        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {}'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1]  in (2,50):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    print(len(uuids), len(uuids_update), len(id_update))
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=4, sales_by_uuid=sales_by_uuid_1)
                    if len(id_update) > 0:
                        sql = 'update {} set visible = 5 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()
                        # sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                        # print(sql)
                        # self.cleaner.db26.execute(sql)
                        # self.cleaner.db26.commit()

        for each_sp1, each_visible in [['婴儿牛奶粉', 1]]:
            for ssmonth, eemonth in [['2020-01-01', '2020-07-01'], ['2021-01-01', '2021-07-01']]:
                for each_source in ['source=\'tmall\' and shop_type in (22,24)', 'source = \'jd\' and shop_type in (21,22)']:
                    uuids = []
                    uuids_update = []
                    id_update = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1]  in (2,50):
                                uuids_update.append(uuid2)
                                id_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,0]
                            uuids.append(uuid2)
                            uuidsxxx.append(uuid2)
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=4, sales_by_uuid=sales_by_uuid_1)
                    if len(id_update) > 0:
                        sql = 'update {} set visible = 5 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in id_update]))
                        print(sql)
                        self.cleaner.db26.execute(sql)
                        self.cleaner.db26.commit()

                    print(len(uuids), len(uuids_update), len(id_update))
                    # if len(uuids_update) > 0:
                    #     sql = 'update {} set visible = 2, visible_check = 4 where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()

        return True

    def brush_0803(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}

        uuidsxxx = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)


        #source1 = ['source = \'tmall\'', 'source = \'tb\'', 'source = \'jd\'']
        source1 = ['source = \'jd\'']
        sp1 = ['婴儿牛奶粉', '婴儿特殊配方粉']
        uuids = []
        uuids_update = []
        for each_sp1 in sp1:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                for each_source in source1:

                    where = '''
                    uuid2 in (select uuid2 from {} where sp1 = \'{each_sp1}\') and {each_source}
                    '''.format(self.get_clean_tbl(), each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1,3,4,5):
                                uuids_update.append(uuid2)
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    #print('----------------', len(uuids + uuids_update))
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=1,sales_by_uuid=sales_by_uuid_1)

        # source1 = ['source = \'tmall\'', 'source = \'tb\'', 'source = \'jd\'']
        source1 = ['source = \'jd\'']
        sp1 = ['婴儿牛奶粉', '婴儿特殊配方粉']
        uuids2 = []
        uuids_update2 = []
        for each_sp1 in sp1:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                for each_source in source1:

                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(self.get_clean_tbl(), each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, limit=10000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1, 3, 4, 5):
                                uuids_update2.append(uuid2)
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids2.append(uuid2)
        print('----------------', len(uuids + uuids_update))
        print('----------------', len(uuids2 + uuids_update2))
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, 1, visible=1,sales_by_uuid=sales_by_uuid_1)
        return True


    def brush_0804(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}


        clean_flag = self.cleaner.last_clean_flag() + 1

        cc = {}

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = \'tb\' and shop_type=11', 'source = \'tb\' and shop_type=12', 'source = \'tmall\' and shop_type in(21,23)', 'source = \'tmall\' and shop_type in(22,24)', 'source = \'jd\' and shop_type in(11,12)', 'source = \'jd\' and shop_type in(21,22)']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉']
        for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01']]:
            for each_source in sources:
                for each_sp1 in sp1_list:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {tbl} where sp1=\'{each_sp1}\' ) and {each_source} and alias_all_bid not in (48199,4885410)
                    '''.format(tbl=self.get_clean_tbl(), each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1, 3, 4, 5):
                                uuids_update.append(uuid2)
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}'.format(ssmonth, each_source, each_sp1)] = len(uuids + uuids_update)
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print (i, cc[i])

        return True

    def brush_0830(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}


        clean_flag = self.cleaner.last_clean_flag() + 1

        cc = {}

        sql = 'SELECT source, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = \'tb\' and shop_type=11', 'source = \'tb\' and shop_type=12', 'source = \'tmall\' and shop_type in(21,23)', 'source = \'tmall\' and shop_type in(22,24)', 'source = \'jd\' and shop_type in(11,12)', 'source = \'jd\' and shop_type in(21,22)']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉']
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in sources:
                for each_sp1 in sp1_list:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {tbl} where sp1=\'{each_sp1}\' ) and {each_source} and alias_all_bid not in (48199,4885410)
                    '''.format(tbl=self.get_clean_tbl(), each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (1, 3, 4, 5):
                                uuids_update.append(uuid2)
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}'.format(ssmonth, each_source, each_sp1)] = len(uuids + uuids_update)
                    # self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print (i, cc[i])

        return True

    def brush_fenyue(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type = 11']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.8, limit=100000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('80%出题', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids+uuids_update, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid_1)


        for each_sp1 in sp1_list:
            for each_source in sources:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids2 = []
                    uuids_update2 = []
                    where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\') and {} and alias_all_bid in (48199,4885410)'.format(
                        ctbl, each_sp1, each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update2.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids2.append(uuid2)

                    cc['{}+{}+{}+{}'.format('48199&4885410全量出题', ssmonth, each_source, each_sp1)] = len(uuids2), len(uuids_update2)
                    self.cleaner.add_brush(uuids2+uuids_update2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        return True

    def brush_0906(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = [['snum=1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11'],
                   ['snum=1 and (shop_type > 20 or shop_type < 10 )', 'source = 1 and (shop_type > 20 or shop_type < 10 )'],
                   ['snum=2', 'source = 2'],
                   ['snum=1 and (shop_type < 20 and shop_type > 10 ) and shop_type = 11', 'source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type = 11']]
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        for each_sp1 in sp1_list:
            for each_platform, each_source in sources:
                uuids2 = []
                uuids_update2 = []
                sql_itemid = '''
                select distinct tb_item_id from {tbl} where {each_platform} and created>='2021-09-01' and created<'2021-09-02'
                '''.format(tbl=self.cleaner.get_tbl(), each_platform=each_platform)
                ret_itemid = self.cleaner.db26.query_all(sql_itemid)
                itemids = [str(v[0]) for v in ret_itemid]

                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\') and {each_source} and item_id in ({itemids})
                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, itemids=','.join(["'"+str(v)+"'" for v in itemids]))
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids2.append(uuid2)
                cc['{}+{}'.format(each_source, each_sp1)] = len(uuids2), len(uuids_update2)
                self.cleaner.add_brush(uuids2 + uuids_update2, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid_1)


        for i in cc:
            print (i, cc[i])

        exit()

        return True


    def brush_bak0907(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type = 11']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        bids ="'48199',	'48222',	'17051',	'48224',	'48281',	'9788',	'48200',	'17435',	'48221',	'48219',	'48225',	'48228',	'48241',	'333489',	'48239',	'48284',	'202596',	'48201',	'5183122',	'19888',	'2158',	'4745507',	'17865',	'203510',	'1293363',	'5978025',	'401558',	'741275',	'6157535',	'207930',	'48282',	'48214',	'393276',	'48210',	'48660',	'133507',	'391982',	'341482',	'53212',	'274337',	'48472',	'69029',	'1001',	'387254',	'6468388',	'53021',	'6009068',	'17248',	'21179',	'3664',	'114191',	'11273',	'8980',	'6313736',	'5826248',	'6032825',	'3694199',	'6163',	'72571',	'279317',	'5339174',	'333207',	'122781',	'116181',	'333312',	'14297',	'1768164',	'134364',	'51962',	'4812507',	'495795',	'203426',	'387454',	'347634',	'784671',	'1319070',	'51972',	'11944',	'203787',	'234841',	'483563',	'69254',	'204132',	'6422526',	'52122',	'48251',	'302151',	'81874',	'3727031',	'48255',	'5303963',	'258003',	'3107673',	'20362',	'305706',	'47051',	'5839152',	'479940',	'90485',	'6530327',	'333376',	'5431864',	'70165',	'5531014',	'350576',	'296665',	'387689',	'6003624',	'115167',	'6074865',	'417420',	'112887',	'464770',	'57549',	'552889',	'60306',	'830463',	'727601',	'801154',	'59786',	'892498',	'245937',	'52711',	'4154',	'1312174',	'9763',	'751334',	'347287',	'81012',	'347699',	'4564655',	'137',	'2100873',	'52784',	'185254',	'114153',	'3006416',	'277903',	'274044',	'3154618',	'279805',	'405822',	'3317873',	'879',	'3481886',	'86892',	'3728859',	'123271',	'18708',	'176569',	'24821',	'130411',	'246358',	'52917',	'426722',	'96926',	'2971844',	'3137834',	'80847',	'11416',	'5123368',	'5195833',	'5363186',	'5363201',	'390455',	'5574386',	'5647682',	'5764935',	'5913333',	'5961699',	'71230',	'577711',	'6050033',	'6178502',	'6207479',	'6249294',	'6407717',	'6435872',	'6458117',	'6483323',	'6584323'"

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and (all_bid in({bids}) or alias_all_bid in({bids}))
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, bids=bids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=50000, rate=0.95)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('Key_bids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids+uuids_update, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and all_bid not in({bids}) and  alias_all_bid not in({bids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, bids=bids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('notKey_bids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids+uuids_update, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid_1)


        for i in cc:
            print (i, cc[i])

        exit()

        return True

    def brush_dakulinshi(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 5', 'source = 6', 'source = 7']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('notKey_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=102,sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        return True


    # def brush(self, smonth, emonth, logId=-1):
    def brush_1227daku(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        ## 21年使用的sids
        sids ="'1000002847',	'1000003568',	'1000003179',	'1000002520',	'1000002668',	'1000015026',	'1000001582',	'1000003570',	'1000002672',	'1000003112',	'1000002688',	'1000003111',	'1000076309',	'1000126429',	'65803253',	'186949317',	'1000014486',	'9281929',	'647925',	'192172837',	'176778182',	'189937050',	'62645733',	'172427629',	'10821407',	'13769176',	'69176931',	'9286775',	'188461104',	'3602532',	'1000015205',	'0',	'9370176',	'67302766',	'17078538',	'17961262',	'64449098',	'213653',	'1000084683',	'170685533',	'1000013503',	'54056',	'10089438',	'1278329',	'13640540',	'4407822',	'69500138',	'11506904',	'1000076024',	'68423191',	'1000078242',	'1000017162',	'18092113',	'1000102591',	'19825965',	'191323276',	'0',	'214651',	'18394179',	'170952',	'747880',	'13594370',	'169616109',	'1364310',	'19405629',	'23898',	'20542532',	'10020105',	'163296608',	'1000006644',	'186807361',	'172740',	'18599235',	'1000015266',	'70932',	'66424402',	'186814778',	'1353468',	'806728',	'7667918',	'193310965',	'184537697',	'17724332',	'126580',	'496660',	'19777247',	'70752993',	'176331448',	'30001607',	'1000076286',	'170830491',	'1000078270',	'188080410',	'30000205',	'9287189',	'181933151',	'190806051',	'190531284',	'8087184',	'10251198',	'187822014',	'171248961',	'89183',	'120324',	'194467513',	'192871508'"

        ## 19年使用的sids
        # sids = "'1000002847',	'1000002520',	'1000003179',	'1000002668',	'1000003568',	'1000015026',	'1000003112',	'1000003570',	'1000002672',	'1000002688',	'17961262',	'66424402',	'64449098',	'65803253',	'191323276',	'1000014486',	'170685533',	'176778182',	'647925',	'193310965',	'189674146',	'62645733',	'69500138',	'9281929',	'1000001582',	'1000015205',	'3602532',	'67302766',	'17078538',	'1000003111',	'1000126429',	'213653',	'8087184',	'19825965',	'1000187839',	'18394179',	'1278329',	'54056',	'168921976',	'211344',	'19405629',	'68423191',	'170830491',	'1000017162',	'10821407',	'1000076309',	'1353468',	'1000013503',	'1370209',	'17724332',	'189937050',	'0',	'172713423',	'641609',	'63388200',	'172696000',	'20542532',	'163296608',	'1000006644',	'18599235',	'1000078242',	'176787354',	'1000015266',	'70932',	'119636',	'4509648',	'35271',	'7667918',	'171892321',	'176404999',	'89183',	'187444883',	'181933286',	'747880',	'188461104',	'1353443',	'9370176',	'181902967',	'175392127',	'9287189',	'189859423',	'1000076242',	'181933151',	'192915913',	'12552288',	'201307',	'190531284',	'63873778',	'171248961',	'126580',	'612480',	'1681285',	'120324'"
        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and sid in({sids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('Key_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42,sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and sid not in ({sids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('notKey_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42,sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=5)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('kaola', ssmonth, 'kaola', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42,
                                       sales_by_uuid=sales_by_uuid_1)


        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=6)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('suning', ssmonth, 'suning', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=7)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('vip', ssmonth, 'vip', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42,
                                       sales_by_uuid=sales_by_uuid_1)


        for i in cc:
            print (i, cc[i])

        # exit()

        return True

    def brush_1215(self, smonth, emonth, logId=-1):

        print(1)

        return True

    def brush_daku(self, smonth, emonth, logId=-1):
    # def brush(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        dd = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sql_itemid = '''
                            select distinct tb_item_id from {tbl} where  created>='2022-03-01' and created<'2022-03-02' and visible=42
                            '''.format(tbl=self.cleaner.get_tbl())
        ret_itemid = self.cleaner.db26.query_all(sql_itemid)
        itemids = [str(v[0]) for v in ret_itemid]


        for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
            uuids_update = []
            uuids = []
            uuids_update_uuid = []
            c = 0
            where = '''
                    uuid2 in (select uuid2 from {ctbl} ) and {each_source} and item_id in({itemids})
                    '''.format(ctbl=ctbl,  each_source=each_source, itemids=','.join(["'"+str(v)+"'" for v in itemids]))
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=400000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                        uuids_update.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                else:
                    uuids.append(uuid2)
                    c = c + 1
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
                    if k not in dd:
                        dd[k] = 0
                    dd[k] = dd[k] + 1
            print(len(uuids_update), len(uuids))
            cc['{}~{}+{}'.format(smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=42, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print (i, cc[i])



        return True



    def brush_0927(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source=5', 'source=6', 'source=7']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        sql_itemid = '''
                    select distinct tb_item_id from {tbl} where  created>='2021-09-16' and created<'2021-09-17'
                    '''.format(tbl=self.cleaner.get_tbl())
        ret_itemid = self.cleaner.db26.query_all(sql_itemid)
        itemids = [str(v[0]) for v in ret_itemid]

        # print('************', len(itemids))
        # exit()

        for each_source in sources:
            uuids_update = []
            uuids = []
            uuids_update_uuid = []
            c = 0
            where = '''
            uuid2 in (select uuid2 from {ctbl} ) and {each_source} and item_id in({itemids})
            '''.format(ctbl=ctbl, each_source=each_source, itemids=','.join(["'" + str(v) + "'" for v in itemids]))
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=400000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                        uuids_update.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                else:
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids.append(uuid2)

            cc['{}+{}'.format(each_source, smonth)] = len(uuids), len(uuids_update)
            self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=1,sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        exit()

        return True

    def brush_now(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        ## 国内
        # sources = ['source = 1 and shop_type in (21,23,25)', 'source = 2 and shop_type in (11,12)']
        #
        # sids ="'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'62463392',	'64449098',	'65803253',	'19825965',	'763120',	'184880001',	'194273299',	'181933286',	'194913054',	'69500138',	'195418959',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'190806051',	'191323276',	'187028404',	'195491213',	'194778061',	'172696000',	'193198183',	'19405629',	'9370176',	'1353443',	'9287189',	'194235592',	'163753574',	'181933151',	'191784770',	'66745772',	'1353468',	'7667918',	'7709298',	'17650243',	'63978220',	'68251797',	'186807361',	'189859423',	'193559416',	'60570654',	'195203424',	'192974228',	'194149023',	'8087184',	'20619935',	'12144949',	'18196280',	'189938717',	'193238145',	'193856231',	'181904206',	'10729360',	'188933930',	'193050336',	'192544809',	'195429933',	'12039139',	'10592535',	'191284939',	'168298270',	'187444883',	'194962316',	'193445447',	'193142273',	'1000002847',	'1000003568',	'1000002668',	'1000003179',	'1000002520',	'1000003570',	'1000002672',	'1000003112',	'1000006644',	'1000002688',	'1000003111',	'1000126429',	'1000013503',	'1000014486',	'1000102591',	'647925',	'1000076309',	'10251198',	'1000187839',	'973338',	'170952',	'10343082',	'211344',	'120324',	'746561',	'23898',	'1000015205',	'1000001582',	'10020105',	'1000078242',	'172740',	'1000017162',	'1000015026',	'674014',	'70932',	'1000015587',	'1000084244',	'213653',	'1000348121',	'131560',	'214651',	'126580',	'818745',	'747880',	'30356',	'663225',	'673126',	'0',	'768735',	'10134444',	'42902',	'1000010405',	'10214507',	'207325',	'63471',	'698676',	'35271',	'652695',	'606696',	'70302',	'10139043',	'876497',	'851809',	'748616',	'10052923',	'598847',	'1000221821',	'10187738',	'10034275',	'208939',	'822328',	'158274',	'792301',	'10221701',	'1000337685',	'184556',	'10251317',	'10631028',	'10153381',	'158096',	'782345',	'709650',	'166535',	'1000281625',	'866096',	'19293',	'10138259',	'1000015266',	'63337',	'851789',	'887773',	'1000099746',	'724628',	'125117',	'194323',	'970088',	'88066',	'204899',	'10133327'"

        ## 海外
        # sources = ['source = 1 and shop_type in (22,24)', 'source = 2 and shop_type in (21,22)']
        #
        # sids = "'1000015205',	'1000015026',	'1000076024',	'1000076286',	'1000090221',	'1000076309',	'1000085822',	'1000348121',	'10251198',	'1000084244',	'1000076242',	'170952',	'10089438',	'1000365928',	'1000090626',	'120324',	'822328',	'1000313586',	'858790',	'1000124388',	'125117',	'1000076307',	'1000015266',	'1000187839',	'1000099746',	'713792',	'589629',	'126580',	'660051',	'970088',	'660152',	'119636',	'1000078270',	'1000344201',	'652695',	'0',	'149899',	'606696',	'130647',	'208738',	'10343082',	'140037',	'747880',	'10151263',	'988743',	'10021744',	'131560',	'1000091226',	'666051',	'583612',	'694299',	'795143',	'129166',	'1000334709',	'660297',	'817251',	'182139',	'1000015786',	'683293',	'184556',	'10288122',	'10924001',	'818745',	'215260',	'158096',	'10281422',	'1000291802',	'11011691',	'170685533',	'66424402',	'193962356',	'193310965',	'188637984',	'192915913',	'195498510',	'193171994',	'193716326',	'195491213',	'187512609',	'181871121',	'170830491',	'175248939',	'191592705',	'194750082',	'190531284',	'65696293',	'193539687',	'189674146',	'193195595',	'188655268',	'193823635',	'192918050',	'192889305',	'63873778',	'176787354',	'195627818',	'195529088',	'165282384',	'170918489',	'169997827',	'192511188',	'187627185',	'194583278',	'172448935',	'171892321',	'192886590',	'70243618',	'194437041',	'195382923',	'195119710',	'195320750'"

        ## 天猫淘宝国内
        sources = ['source = 1 and shop_type in (11,21,23,25)']




        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source}  and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=101,sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        exit()
        #

        return True

    # 月度增量默认规则
    def brush_1104(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and shop_type in (22,24)', 'source = 2 and shop_type in(21,22)']
        lists = [['婴儿牛奶粉', 1], ['婴儿特殊配方粉', 6]]

        for each_source in sources:
            for each_sp1, each_visible in lists:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids = []
                    uuids_update = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\')  and {each_source} and alias_all_bid in('48199','4885410')
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (2, 50, 100):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    cc['{}+{}+{}+{}'.format(each_source, ssmonth, 'key', each_sp1)] = len(uuids), len(uuids_update)

        for each_source in sources:
            for each_sp1, each_visible in lists:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    uuids = []
                    uuids_update = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\')  and {each_source} and alias_all_bid not in('48199','4885410')
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (2, 50, 100):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)
                    self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=each_visible, sales_by_uuid=sales_by_uuid_1)
                    cc['{}+{}+{}+{}'.format(each_source, ssmonth, 'not key', each_sp1)] = len(uuids), len(uuids_update)

        for i in cc:
            print (i, cc[i])

        ## exit()


        return True

    def brush_1209(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        uuids = []

        where = '''
        uuid2 in (select uuid2 from {ctbl} where sp1 in('婴儿牛奶粉','婴儿特殊配方粉','婴儿羊奶粉') and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source in (5,6,7)
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=8, sales_by_uuid=sales_by_uuid_1)
        print(len(uuids))

        return True

    def brush_1203B(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}


        sources = ['source = 1 and shop_type =12', 'source =1 and shop_type>20', 'source=2']
        ##21年sid
        # sids = "'1000013402',	'1000003568',	'1000002668',	'1000002847',	'1000002520',	'1000015026',	'1000002672',	'1000001582',	'1000003112',	'1000003111',	'1000002688',	'1000076309',	'1000126429',	'1000079866',	'65803253',	'64449098',	'10251198',	'1000014486',	'1000102591',	'1000013503',	'191323276',	'190229639',	'763120',	'184880001',	'195498510',	'1000090626',	'6681857',	'195191206',	'195418959',	'9286775',	'69176931',	'1000003570',	'3602532',	'1000015205',	'188461104',	'9281929',	'1000003179',	'1000006644',	'67302766',	'17961262',	'17078538',	'66424402',	'647925',	'10285207',	'213653',	'193310965',	'1000084244',	'190806051',	'7667918',	'826264',	'193716326',	'1000148782',	'195491213',	'165354961',	'195648339',	'11506904',	'170685533',	'10020105',	'1000002746',	'1000348121',	'17724332',	'724628',	'1353468',	'63471',	'181871121',	'69500138',	'62645733',	'18877',	'973338',	'165148174',	'195730062',	'164615368',	'1000078242',	'23898',	'163296608',	'746561',	'68423191',	'19405629',	'170830491',	'172740',	'20542532',	'1000010405',	'70932',	'1000337685',	'176778182',	'10179331',	'192915913',	'194149023',	'678460',	'946414',	'120324',	'1000365928',	'214651',	'1364310',	'10821407',	'172696000',	'1000076286',	'0',	'1000076242',	'1000076024',	'18599235',	'181933151',	'1000015266',	'125117',	'170952',	'188608070',	'10592535',	'186814778',	'10075881',	'19825965',	'35271',	'191284939',	'751058',	'10044004',	'660152',	'181933286',	'10416815'"
        ##20年sid
        sids = "'1000002847',	'1000003568',	'1000003179',	'1000002520',	'1000002668',	'1000015026',	'1000003570',	'1000002672',	'1000003112',	'1000002688',	'1000003111',	'1000076309',	'1000126429',	'65803253',	'1000014486',	'9281929',	'193310965',	'647925',	'1000187839',	'176778182',	'64449098',	'54056',	'62645733',	'69500138',	'69176931',	'9286775',	'188461104',	'3602532',	'1000001582',	'1000015205',	'170685533',	'67302766',	'17078538',	'17961262',	'66424402',	'213653',	'1000013503',	'10251198',	'10089438',	'189937050',	'214651',	'747880',	'211344',	'11506904',	'68423191',	'163296608',	'1000006644',	'1000078242',	'1000017162',	'1000015266',	'1000102591',	'19825965',	'191323276',	'10075881',	'0',	'171892321',	'18394179',	'1278329',	'973338',	'496660',	'1364310',	'19405629',	'23898',	'1000076024',	'10020105',	'170830491',	'10821407',	'172740',	'18599235',	'70932',	'822328',	'186814778',	'1353468',	'7667918',	'63873778',	'17724332',	'795143',	'120324',	'176331448',	'172696000',	'20542532',	'1000076286',	'62463392',	'188080410',	'1000076242',	'9287189',	'188637984',	'181933151',	'652695',	'190806051',	'190531284',	'8087184',	'70128719',	'171248961',	'170952',	'163611988',	'17543537'"
        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}' and sp1 ='婴儿羊奶粉' ) and {each_source} and sid in({sids})
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('Key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}' and sp1 ='婴儿羊奶粉') and {each_source} and sid not in({sids})
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000,
                                                                     rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2,
                                       sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        return True

    def brush_1203(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        uuids = []
        uuids_update = []

        item_ids="'589403409690',	'615893316359',	'10020946144929',	'621064096145',	'10026592929868',	'589716743586',	'625478442199',	'65583855776',	'49309203381',	'625769147234',	'10026225857026',	'641021958887',	'593498988327',	'10030147592646',	'10026227510836',	'644612242047',	'14437205612',	'27071640436',	'35519799782',	'10026227510837',	'613256054724',	'10030147477532',	'626064784722',	'11331771227',	'53928873647',	'10026224034797',	'10029889943058',	'617169199787',	'49494352850',	'614331259069',	'10025059606148',	'10027460595800',	'10027458259593',	'543052801223',	'49309203385',	'10026226959635',	'651526484533',	'69311301497',	'18795110246',	'10034082289730',	'632377300053',	'570399642617',	'10026652848697',	'632094372608',	'636449645490',	'49309203383',	'45896865406',	'646352791076',	'71573418917',	'10026099620842',	'630005886754',	'634412638432',	'589781407262',	'13609351418',	'10025738991262',	'650332564187',	'579502071696',	'645809690586',	'10026535735662',	'15496804346',	'10025520239905',	'10027460170514',	'57632307923',	'10031975781953',	'618721345972',	'10021720284334',	'557003099541',	'617982237149',	'10024454392787',	'58849801532',	'10026225857025',	'10737359180',	'49309203382',	'10031770790272',	'10032990535335',	'642057021786',	'649976920874',	'13725314267',	'10026319971279',	'10021437723098',	'10021727322071',	'651943741007',	'71885414626',	'635172384054'"
        where = '''
        item_id in ({item_ids}) and cid in('211104','201284105','7052')
        '''.format(item_ids=item_ids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, 2, 3, 4, 5, 6, 7, 8,9,10,11):
                    uuids_update.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid_1)
        print(len(uuids_update),len(uuids))



        return True

    def brush_0112Meisu(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        ## 国内
        sources = ['source = 1 and shop_type in (21,23,25)', 'source = 2 and shop_type in (11,12)', 'source=1 and shop_type=11']

        sids = "'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'62463392',	'64449098',	'65803253',	'19825965',	'763120',	'184880001',	'194273299',	'181933286',	'194913054',	'69500138',	'195418959',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'190806051',	'191323276',	'187028404',	'195491213',	'194778061',	'172696000',	'193198183',	'19405629',	'9370176',	'1353443',	'9287189',	'194235592',	'163753574',	'181933151',	'191784770',	'66745772',	'1353468',	'7667918',	'7709298',	'17650243',	'63978220',	'68251797',	'186807361',	'189859423',	'193559416',	'60570654',	'195203424',	'192974228',	'194149023',	'8087184',	'20619935',	'12144949',	'18196280',	'189938717',	'193238145',	'193856231',	'181904206',	'10729360',	'188933930',	'193050336',	'192544809',	'195429933',	'12039139',	'10592535',	'191284939',	'168298270',	'187444883',	'194962316',	'193445447',	'193142273',	'1000002847',	'1000003568',	'1000002668',	'1000003179',	'1000002520',	'1000003570',	'1000002672',	'1000003112',	'1000006644',	'1000002688',	'1000003111',	'1000126429',	'1000013503',	'1000014486',	'1000102591',	'647925',	'1000076309',	'10251198',	'1000187839',	'973338',	'170952',	'10343082',	'211344',	'120324',	'746561',	'23898',	'1000015205',	'1000001582',	'10020105',	'1000078242',	'172740',	'1000017162',	'1000015026',	'674014',	'70932',	'1000015587',	'1000084244',	'213653',	'1000348121',	'131560',	'214651',	'126580',	'818745',	'747880',	'30356',	'663225',	'673126',	'0',	'768735',	'10134444',	'42902',	'1000010405',	'10214507',	'207325',	'63471',	'698676',	'35271',	'652695',	'606696',	'70302',	'10139043',	'876497',	'851809',	'748616',	'10052923',	'598847',	'1000221821',	'10187738',	'10034275',	'208939',	'822328',	'158274',	'792301',	'10221701',	'1000337685',	'184556',	'10251317',	'10631028',	'10153381',	'158096',	'782345',	'709650',	'166535',	'1000281625',	'866096',	'19293',	'10138259',	'1000015266',	'63337',	'851789',	'887773',	'1000099746',	'724628',	'125117',	'194323',	'970088',	'88066',	'204899',	'10133327','10374731','10304410','10172529','10281422','3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'13657988',	'64449098',	'65803253',	'190806051',	'191323276',	'7667918',	'192172837',	'190229639',	'763120',	'184880001',	'9661694',	'4958318',	'195191206',	'195418959',	'9286775',	'14975123',	'62654366',	'191285215',	'164615368',	'68423191',	'10821407',	'19405629',	'20542532',	'18599235',	'62463392',	'18092113',	'10179331',	'1353468',	'19825965',	'181161372',	'187822014',	'13769176',	'189399394',	'194876717',	'195648339',	'6681857',	'2497119',	'70645417',	'172696000',	'193198183',	'1353443',	'194235592',	'181904206',	'9370176',	'9287189',	'163753574',	'181933151',	'189507102',	'194376602',	'1359061',	'10592535',	'194149023',	'7709298',	'191698870',	'191284939',	'66745772',	'59615852',	'186673609',	'70752993',	'62058104',	'173816544',	'193199339',	'195730062',	'193650050',	'1357077',	'68251797',	'1359088',	'189859423',	'186807361',	'193559416',	'192544809',	'1385823',	'195203424',	'76751',	'8197953',	'168298270',	'193739946',	'187444883',	'184537697',	'69500138',	'4717510',	'12558866',	'194747149',	'193238145',	'7738513',	'188933930',	'187028404',	'192937275',	'193050336',	'60570654',	'191784770',	'11380114',	'194273299',	'192974228',	'175136630',	'192005219',	'20619935',	'12180805',	'12144949',	'187155437',	'169616109',	'162801135',	'193803011',	'194797218',	'9759512'"

        for each_source in sources:
            for ssmonth, eemonth in [['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source}  and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                        #     uuids_update.append(uuid2)
                        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid_1)



        for i in cc:
            print (i, cc[i])


        return True


    def brush_1227Ameisu(self, smonth, emonth, logId=-1):
    # def brush(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        ## 国内
        sources = ['source = 1 and shop_type in (21,23,25)', 'source = 2 and shop_type in (11,12)']

        sids ="'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'62463392',	'64449098',	'65803253',	'19825965',	'763120',	'184880001',	'194273299',	'181933286',	'194913054',	'69500138',	'195418959',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'190806051',	'191323276',	'187028404',	'195491213',	'194778061',	'172696000',	'193198183',	'19405629',	'9370176',	'1353443',	'9287189',	'194235592',	'163753574',	'181933151',	'191784770',	'66745772',	'1353468',	'7667918',	'7709298',	'17650243',	'63978220',	'68251797',	'186807361',	'189859423',	'193559416',	'60570654',	'195203424',	'192974228',	'194149023',	'8087184',	'20619935',	'12144949',	'18196280',	'189938717',	'193238145',	'193856231',	'181904206',	'10729360',	'188933930',	'193050336',	'192544809',	'195429933',	'12039139',	'10592535',	'191284939',	'168298270',	'187444883',	'194962316',	'193445447',	'193142273',	'1000002847',	'1000003568',	'1000002668',	'1000003179',	'1000002520',	'1000003570',	'1000002672',	'1000003112',	'1000006644',	'1000002688',	'1000003111',	'1000126429',	'1000013503',	'1000014486',	'1000102591',	'647925',	'1000076309',	'10251198',	'1000187839',	'973338',	'170952',	'10343082',	'211344',	'120324',	'746561',	'23898',	'1000015205',	'1000001582',	'10020105',	'1000078242',	'172740',	'1000017162',	'1000015026',	'674014',	'70932',	'1000015587',	'1000084244',	'213653',	'1000348121',	'131560',	'214651',	'126580',	'818745',	'747880',	'30356',	'663225',	'673126',	'0',	'768735',	'10134444',	'42902',	'1000010405',	'10214507',	'207325',	'63471',	'698676',	'35271',	'652695',	'606696',	'70302',	'10139043',	'876497',	'851809',	'748616',	'10052923',	'598847',	'1000221821',	'10187738',	'10034275',	'208939',	'822328',	'158274',	'792301',	'10221701',	'1000337685',	'184556',	'10251317',	'10631028',	'10153381',	'158096',	'782345',	'709650',	'166535',	'1000281625',	'866096',	'19293',	'10138259',	'1000015266',	'63337',	'851789',	'887773',	'1000099746',	'724628',	'125117',	'194323',	'970088',	'88066',	'204899',	'10133327','10374731','10304410','10172529','10281422','3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'13657988',	'64449098',	'65803253',	'190806051',	'191323276',	'7667918',	'192172837',	'190229639',	'763120',	'184880001',	'9661694',	'4958318',	'195191206',	'195418959',	'9286775',	'14975123',	'62654366',	'191285215',	'164615368',	'68423191',	'10821407',	'19405629',	'20542532',	'18599235',	'62463392',	'18092113',	'10179331',	'1353468',	'19825965',	'181161372',	'187822014',	'13769176',	'189399394',	'194876717',	'195648339',	'6681857',	'2497119',	'70645417',	'172696000',	'193198183',	'1353443',	'194235592',	'181904206',	'9370176',	'9287189',	'163753574',	'181933151',	'189507102',	'194376602',	'1359061',	'10592535',	'194149023',	'7709298',	'191698870',	'191284939',	'66745772',	'59615852',	'186673609',	'70752993',	'62058104',	'173816544',	'193199339',	'195730062',	'193650050',	'1357077',	'68251797',	'1359088',	'189859423',	'186807361',	'193559416',	'192544809',	'1385823',	'195203424',	'76751',	'8197953',	'168298270',	'193739946',	'187444883',	'184537697',	'69500138',	'4717510',	'12558866',	'194747149',	'193238145',	'7738513',	'188933930',	'187028404',	'192937275',	'193050336',	'60570654',	'191784770',	'11380114',	'194273299',	'192974228',	'175136630',	'192005219',	'20619935',	'12180805',	'12144949',	'187155437',	'169616109',	'162801135',	'193803011',	'194797218',	'9759512'"


        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source} and sid in({sids}) and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                        #     uuids_update.append(uuid2)
                        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('Key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=41,sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source} and sid not in({sids}) and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                        #     uuids_update.append(uuid2)
                        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=41, sales_by_uuid=sales_by_uuid_1)



        for i in cc:
            print (i, cc[i])

        # exit()

        return True

    def brush_1201(self, smonth, emonth, logId=-1):
        print(1)
        return True

    # def brush(self, smonth, emonth, logId=-1):
    def brush_1227Cmeisu(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        ## 国内
        sources = ['source = 1 and shop_type in (21,23,25)', 'source = 2 and shop_type in (11,12)', 'source = 1 and shop_type =11']

        ## 海外
        # sources = ['source = 1 and shop_type in (22,24)', 'source = 2 and shop_type in (21,22)']

        ## 天猫淘宝国内
        # sources = ['source = 1 and shop_type =12', 'source =1 and shop_type>20', 'source=2']

        for each_source in sources:
            uuids2 = []
            uuids_update2 = []
            sql_itemid = '''
            select distinct tb_item_id from {tbl} where  created>='2022-03-01' and created<'2022-03-02' and visible = 41
            '''.format(tbl=self.cleaner.get_tbl())
            ret_itemid = self.cleaner.db26.query_all(sql_itemid)
            itemids = [str(v[0]) for v in ret_itemid]

            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and  pkey<'{emonth}') and {each_source} and item_id in ({itemids})
            '''.format(ctbl=ctbl, each_source=each_source, smonth=smonth, emonth=emonth, itemids=','.join(["'"+str(v)+"'" for v in itemids]))
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                    #     uuids_update2.append(uuid2)
                    #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    continue
                else:
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids2.append(uuid2)
            cc['{}'.format(each_source)] = [len(uuids2), len(uuids_update2)]
            self.cleaner.add_brush(uuids2 , clean_flag, visible_check=1, visible=41, sales_by_uuid=sales_by_uuid_1)


        for i in cc:
            print (i, cc[i])

        return True


    # def brush(self, smonth, emonth, logId=-1):
    def brush_1227Bmeisu(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        alias_all_bids = "'9788',	'48200',	'5918430',	'48443',	'48228',	'48241',	'3645511',	'4885410',	'48199',	'48282',	'48214',	'48222',	'48219',	'1660132',	'48224',	'651663',	'48281',	'17051',	'48221',	'48225',	'693001',	'793396',	'405721',	'5220058',	'17435',	'48284'"
        # alias_lists = ['9788',	'48200',	'5918430',	'48443',	'48228',	'48241',	'3645511',	'4885410',	'48199',	'48282',	'48214',	'48222',	'48219',	'1660132',	'48224',	'651663',	'48281',	'17051',	'48221',	'48225',	'693001',	'793396',	'405721',	'5220058',	'17435',	'48284']

        uuids = []
        uuids_update = []

        where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and  pkey<'{emonth}') and source=1 and shop_type=11 and alias_all_bid in ({alias_all_bids}) and cid in('211104','201284105','7052')
                        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, alias_all_bids=alias_all_bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300000, rate=0.8)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
                # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                #     uuids_update.append(uuid2)
                #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=41, sales_by_uuid=sales_by_uuid)

        return True

    def brush_1110C(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        uuids = []
        uuids_update = []

        where = '''
        uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and  pkey<'{emonth}') and source = 2 and shop_type in (11,12) and sid in ('10374731','10304410','10172529','10281422') and cid in('211104','201284105','7052')
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200000)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (2, 50, 100):
                    uuids_update.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=103,sales_by_uuid=sales_by_uuid_1)


        return True

    def brush_1110(self, smonth, emonth, logId=-1):

        sql = "select id from product_lib.entity_91357_item where visible > 100 and (snum,tb_item_id ,real_p1) not  in (select snum,tb_item_id ,real_p1 from product_lib.entity_91357_item where visible <= 100 )".format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        ids = [str(v[0]) for v in ret]

        sql2='''
        update product_lib.entity_91357_item set visible = 9 where id in ({})
        '''.format(','.join([str(v) for v in ids]))

        print(sql2)
        self.cleaner.db26.execute(sql2)
        self.cleaner.db26.commit()



        return True


    def brush_test_1011(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        lists = [['婴儿牛奶粉', 101], ['婴儿特殊配方粉', 102]]

        sids ="'188658722',	'11972539',	'3086801',	'167739814',	'9759512',	'176778182',	'188146985',	'186814778',	'189937050',	'192158473',	'1355840',	'1370209',	'62645733',	'179082411',	'68682932',	'163611988',	'69176931',	'65783966',	'10075881',	'193492457',	'173528035',	'62661428',	'64491757',	'17221557',	'59989877',	'600204',	'18394179',	'162989533',	'185138528',	'169838288',	'1278329',	'14385051',	'11954705',	'496660',	'3251818',	'179004644',	'12982563',	'11506904',	'66332907',	'186417010',	'194086185',	'186024435',	'192572327',	'17724332',	'173560724',	'905371',	'89183',	'6681857',	'1367781',	'3095354',	'17543537',	'3575067',	'1681285',	'1364310',	'1356981',	'1359088',	'1705813',	'2988163',	'64546397',	'8194490',	'171248961',	'12552288',	'13289267',	'179424799',	'188889516',	'4824786',	'5977465',	'168842745',	'6531801',	'5660910',	'176331448',	'175419901',	'174051462',	'7021622',	'63978349',	'13360027',	'189260401',	'173399119',	'9406960',	'10104847',	'3496267',	'5363767',	'641609',	'1391933',	'180130668',	'1000002847',	'1000003568',	'1000003179',	'1000002520',	'1000002668',	'1000003570',	'1000002672',	'1000003112',	'1000002688',	'1000006644',	'1000003111',	'1000126429',	'1000014486',	'1000013503',	'213653',	'647925',	'54056',	'1000337685',	'975874',	'10139043',	'211344',	'122082',	'851809',	'746561',	'23898',	'1000001582',	'10020105',	'598847',	'1000078242',	'172740',	'1000017162',	'42902',	'10214507',	'70932',	'698676',	'63471',	'1000102591',	'0',	'214651',	'10301047',	'10153381',	'88066',	'1000004706',	'663225',	'673126',	'29446',	'10034275',	'1000010405',	'674014',	'207325',	'814599',	'1000015587',	'35271',	'612480',	'973338',	'48863',	'10092965',	'159157',	'30356',	'709650',	'1000221821',	'768735',	'24029',	'866096',	'208939',	'158274',	'894218',	'1000148782',	'60305',	'10294492',	'10251317',	'126580',	'19293',	'10052923',	'166535',	'46055',	'659016',	'10134444',	'937299',	'203184',	'218401',	'790145',	'1000278536',	'201307',	'10100702',	'1000015026',	'1000015205',	'1000076024',	'1000076309',	'1000076286',	'1000085822',	'1000015266',	'1000090221',	'1000076242',	'1000187839',	'1000099746',	'10089438',	'10251198',	'170952',	'747880',	'795143',	'1000286203',	'942157',	'822328',	'119636',	'1000078270',	'858790',	'1000076307',	'1000124388',	'10103588',	'131560',	'1000102297',	'660051',	'589629',	'120324',	'970088',	'761132',	'988743',	'1000084683',	'817251',	'652695',	'182139',	'1000291802',	'177056',	'582183',	'10104418',	'606696',	'1000348121',	'1000092096',	'987283',	'583280',	'660152',	'762756',	'1000300761',	'660297',	'1000313586',	'125117',	'215260',	'1000155481',	'715438',	'1000084244',	'818745',	'129166',	'10181212',	'165083',	'793380',	'140921',	'877927',	'707844',	'184556',	'130647',	'147129',	'855388',	'137025',	'687544',	'743341',	'587807',	'187823',	'170685533',	'66424402',	'188637984',	'193962356',	'193310965',	'170918489',	'176787354',	'187512609',	'193617977',	'181871121',	'189674146',	'188655268',	'170830491',	'175248939',	'63873778',	'190531284',	'192918050',	'171892321',	'63502177',	'193171994',	'189751781',	'192915913',	'187627185',	'65696293',	'191592705',	'191549700',	'193539687',	'192511188',	'176973172',	'193587710',	'63695948',	'175392127',	'165282384',	'174617924',	'192886590',	'193532073',	'70128719',	'69662007',	'172626743',	'194680018',	'67499778',	'192505290',	'193823635',	'70201002',	'194281638',	'180577732',	'183267841',	'68410915',	'3602532',	'163296608',	'9281929',	'188461104',	'67302766',	'17078538',	'17961262',	'62463392',	'65803253',	'64449098',	'19825965',	'194148124',	'69500138',	'181933286',	'194273299',	'194449785',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'1353468',	'192974228',	'191323276',	'187444883',	'194962316',	'168572838',	'173068372',	'763120',	'172696000',	'9370176',	'1353443',	'19405629',	'9287189',	'188080410',	'163753574',	'181933151',	'191784770',	'190806051',	'12039139',	'7667918',	'7709298',	'66745772',	'189938717',	'193445447',	'68251797',	'193198183',	'186807361',	'184880001',	'189859423',	'193559416',	'187028404',	'192544809',	'10179331',	'8087184',	'12386679',	'194111584',	'17650243',	'181902967',	'181904206',	'10795284',	'188933930',	'193050336',	'60570654',	'60268731',	'10592535',	'191284939',	'20619935',	'163400789',	'18196280' "

        for each_source in sources:
            for each_sp1, each_visible in lists:
                for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and sid in({sids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (2, 50, 100):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('Key_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    self.cleaner.add_brush(uuids+uuids_update, clean_flag, visible_check=1, visible=each_visible,sales_by_uuid=sales_by_uuid_1)


        for i in cc:
            print (i, cc[i])

        exit()


        return True


    def brush_1012(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source=5', 'source=6', 'source=7']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        sql_itemid = '''
                    select distinct tb_item_id from {tbl} where  created>='2021-10-11' and created<'2021-10-12'
                    '''.format(tbl=self.cleaner.get_tbl())
        ret_itemid = self.cleaner.db26.query_all(sql_itemid)
        itemids = [str(v[0]) for v in ret_itemid]

        # print('************', len(itemids))
        # exit()

        for each_source in sources:
            uuids_update = []
            uuids = []
            uuids_update_uuid = []
            c = 0
            where = '''
            uuid2 in (select uuid2 from {ctbl} ) and {each_source} and item_id in({itemids})
            '''.format(ctbl=ctbl, each_source=each_source, itemids=','.join(["'" + str(v) + "'" for v in itemids]))
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=400000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (2, 50, 100):
                        uuids_update.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                else:
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids.append(uuid2)

            cc['{}+{}'.format(each_source, smonth)] = len(uuids), len(uuids_update)
            self.cleaner.add_brush(uuids + uuids_update, clean_flag, visible_check=1, visible=103,sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        exit()

        return True

    def brush_0930(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        self.helper_0930()
        exit()


        return True



    def brush_1227Ameisu_A(self, smonth, emonth, logId=-1):
    # def brush(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        ## 国内
        sources = ['source = 1 and shop_type in (21,23,25)', 'source = 2 and shop_type in (11,12)']

        sids ="'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'62463392',	'64449098',	'65803253',	'19825965',	'763120',	'184880001',	'194273299',	'181933286',	'194913054',	'69500138',	'195418959',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'190806051',	'191323276',	'187028404',	'195491213',	'194778061',	'172696000',	'193198183',	'19405629',	'9370176',	'1353443',	'9287189',	'194235592',	'163753574',	'181933151',	'191784770',	'66745772',	'1353468',	'7667918',	'7709298',	'17650243',	'63978220',	'68251797',	'186807361',	'189859423',	'193559416',	'60570654',	'195203424',	'192974228',	'194149023',	'8087184',	'20619935',	'12144949',	'18196280',	'189938717',	'193238145',	'193856231',	'181904206',	'10729360',	'188933930',	'193050336',	'192544809',	'195429933',	'12039139',	'10592535',	'191284939',	'168298270',	'187444883',	'194962316',	'193445447',	'193142273',	'1000002847',	'1000003568',	'1000002668',	'1000003179',	'1000002520',	'1000003570',	'1000002672',	'1000003112',	'1000006644',	'1000002688',	'1000003111',	'1000126429',	'1000013503',	'1000014486',	'1000102591',	'647925',	'1000076309',	'10251198',	'1000187839',	'973338',	'170952',	'10343082',	'211344',	'120324',	'746561',	'23898',	'1000015205',	'1000001582',	'10020105',	'1000078242',	'172740',	'1000017162',	'1000015026',	'674014',	'70932',	'1000015587',	'1000084244',	'213653',	'1000348121',	'131560',	'214651',	'126580',	'818745',	'747880',	'30356',	'663225',	'673126',	'0',	'768735',	'10134444',	'42902',	'1000010405',	'10214507',	'207325',	'63471',	'698676',	'35271',	'652695',	'606696',	'70302',	'10139043',	'876497',	'851809',	'748616',	'10052923',	'598847',	'1000221821',	'10187738',	'10034275',	'208939',	'822328',	'158274',	'792301',	'10221701',	'1000337685',	'184556',	'10251317',	'10631028',	'10153381',	'158096',	'782345',	'709650',	'166535',	'1000281625',	'866096',	'19293',	'10138259',	'1000015266',	'63337',	'851789',	'887773',	'1000099746',	'724628',	'125117',	'194323',	'970088',	'88066',	'204899',	'10133327','10374731','10304410','10172529','10281422','3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'13657988',	'64449098',	'65803253',	'190806051',	'191323276',	'7667918',	'192172837',	'190229639',	'763120',	'184880001',	'9661694',	'4958318',	'195191206',	'195418959',	'9286775',	'14975123',	'62654366',	'191285215',	'164615368',	'68423191',	'10821407',	'19405629',	'20542532',	'18599235',	'62463392',	'18092113',	'10179331',	'1353468',	'19825965',	'181161372',	'187822014',	'13769176',	'189399394',	'194876717',	'195648339',	'6681857',	'2497119',	'70645417',	'172696000',	'193198183',	'1353443',	'194235592',	'181904206',	'9370176',	'9287189',	'163753574',	'181933151',	'189507102',	'194376602',	'1359061',	'10592535',	'194149023',	'7709298',	'191698870',	'191284939',	'66745772',	'59615852',	'186673609',	'70752993',	'62058104',	'173816544',	'193199339',	'195730062',	'193650050',	'1357077',	'68251797',	'1359088',	'189859423',	'186807361',	'193559416',	'192544809',	'1385823',	'195203424',	'76751',	'8197953',	'168298270',	'193739946',	'187444883',	'184537697',	'69500138',	'4717510',	'12558866',	'194747149',	'193238145',	'7738513',	'188933930',	'187028404',	'192937275',	'193050336',	'60570654',	'191784770',	'11380114',	'194273299',	'192974228',	'175136630',	'192005219',	'20619935',	'12180805',	'12144949',	'187155437',	'169616109',	'162801135',	'193803011',	'194797218',	'9759512'"



        ## 海外
        # sources = ['source = 1 and shop_type in (22,24)', 'source = 2 and shop_type in (21,22)']
        #
        # sids = "'1000015205',	'1000015026',	'1000076024',	'1000076286',	'1000090221',	'1000076309',	'1000085822',	'1000348121',	'10251198',	'1000084244',	'1000076242',	'170952',	'10089438',	'1000365928',	'1000090626',	'120324',	'822328',	'1000313586',	'858790',	'1000124388',	'125117',	'1000076307',	'1000015266',	'1000187839',	'1000099746',	'713792',	'589629',	'126580',	'660051',	'970088',	'660152',	'119636',	'1000078270',	'1000344201',	'652695',	'0',	'149899',	'606696',	'130647',	'208738',	'10343082',	'140037',	'747880',	'10151263',	'988743',	'10021744',	'131560',	'1000091226',	'666051',	'583612',	'694299',	'795143',	'129166',	'1000334709',	'660297',	'817251',	'182139',	'1000015786',	'683293',	'184556',	'10288122',	'10924001',	'818745',	'215260',	'158096',	'10281422',	'1000291802',	'11011691',	'170685533',	'66424402',	'193962356',	'193310965',	'188637984',	'192915913',	'195498510',	'193171994',	'193716326',	'195491213',	'187512609',	'181871121',	'170830491',	'175248939',	'191592705',	'194750082',	'190531284',	'65696293',	'193539687',	'189674146',	'193195595',	'188655268',	'193823635',	'192918050',	'192889305',	'63873778',	'176787354',	'195627818',	'195529088',	'165282384',	'170918489',	'169997827',	'192511188',	'187627185',	'194583278',	'172448935',	'171892321',	'192886590',	'70243618',	'194437041',	'195382923',	'195119710',	'195320750'"

        ## 天猫淘宝国内
        # sources = ['source = 1 and shop_type in (11,21,23,25)']
        #
        # sids = "'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'13657988',	'64449098',	'65803253',	'190806051',	'191323276',	'7667918',	'192172837',	'190229639',	'763120',	'184880001',	'9661694',	'4958318',	'195191206',	'195418959',	'9286775',	'14975123',	'62654366',	'191285215',	'164615368',	'68423191',	'10821407',	'19405629',	'20542532',	'18599235',	'62463392',	'18092113',	'10179331',	'1353468',	'19825965',	'181161372',	'187822014',	'13769176',	'189399394',	'194876717',	'195648339',	'6681857',	'2497119',	'70645417',	'172696000',	'193198183',	'1353443',	'194235592',	'181904206',	'9370176',	'9287189',	'163753574',	'181933151',	'189507102',	'194376602',	'1359061',	'10592535',	'194149023',	'7709298',	'191698870',	'191284939',	'66745772',	'59615852',	'186673609',	'70752993',	'62058104',	'173816544',	'193199339',	'195730062',	'193650050',	'1357077',	'68251797',	'1359088',	'189859423',	'186807361',	'193559416',	'192544809',	'1385823',	'195203424',	'76751',	'8197953',	'168298270',	'193739946',	'187444883',	'184537697',	'69500138',	'4717510',	'12558866',	'194747149',	'193238145',	'7738513',	'188933930',	'187028404',	'192937275',	'193050336',	'60570654',	'191784770',	'11380114',	'194273299',	'192974228',	'175136630',	'192005219',	'20619935',	'12180805',	'12144949',	'187155437',	'169616109',	'162801135',	'193803011',	'194797218',	'9759512'"

        uuid_value_update = []

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source} and sid in({sids}) and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                        #     uuids_update.append(uuid2)
                        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('Key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                # self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=101,sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and  pkey<'{eemonth}') and {each_source} and sid not in({sids}) and cid in('211104','201284105','7052')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                        #     uuids_update.append(uuid2)
                        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                # self.cleaner.add_brush(uuids_update+uuids, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid_1)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_172(smonth, emonth, uuid_value_update)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=41, sales_by_uuid=sales_by_uuid)



        for i in cc:
            print (i, cc[i])

        # exit()

        return True

    # def brush(self, smonth, emonth, logId=-1):
    def brush_1227daku_B(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type < 20 and shop_type > 10 ) and shop_type != 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        ## 21年使用的sids
        sids ="'1000002847',	'1000003568',	'1000003179',	'1000002520',	'1000002668',	'1000015026',	'1000001582',	'1000003570',	'1000002672',	'1000003112',	'1000002688',	'1000003111',	'1000076309',	'1000126429',	'65803253',	'186949317',	'1000014486',	'9281929',	'647925',	'192172837',	'176778182',	'189937050',	'62645733',	'172427629',	'10821407',	'13769176',	'69176931',	'9286775',	'188461104',	'3602532',	'1000015205',	'0',	'9370176',	'67302766',	'17078538',	'17961262',	'64449098',	'213653',	'1000084683',	'170685533',	'1000013503',	'54056',	'10089438',	'1278329',	'13640540',	'4407822',	'69500138',	'11506904',	'1000076024',	'68423191',	'1000078242',	'1000017162',	'18092113',	'1000102591',	'19825965',	'191323276',	'0',	'214651',	'18394179',	'170952',	'747880',	'13594370',	'169616109',	'1364310',	'19405629',	'23898',	'20542532',	'10020105',	'163296608',	'1000006644',	'186807361',	'172740',	'18599235',	'1000015266',	'70932',	'66424402',	'186814778',	'1353468',	'806728',	'7667918',	'193310965',	'184537697',	'17724332',	'126580',	'496660',	'19777247',	'70752993',	'176331448',	'30001607',	'1000076286',	'170830491',	'1000078270',	'188080410',	'30000205',	'9287189',	'181933151',	'190806051',	'190531284',	'8087184',	'10251198',	'187822014',	'171248961',	'89183',	'120324',	'194467513',	'192871508'"

        uuid_value_update = []

        ## 19年使用的sids
        # sids = "'1000002847',	'1000002520',	'1000003179',	'1000002668',	'1000003568',	'1000015026',	'1000003112',	'1000003570',	'1000002672',	'1000002688',	'17961262',	'66424402',	'64449098',	'65803253',	'191323276',	'1000014486',	'170685533',	'176778182',	'647925',	'193310965',	'189674146',	'62645733',	'69500138',	'9281929',	'1000001582',	'1000015205',	'3602532',	'67302766',	'17078538',	'1000003111',	'1000126429',	'213653',	'8087184',	'19825965',	'1000187839',	'18394179',	'1278329',	'54056',	'168921976',	'211344',	'19405629',	'68423191',	'170830491',	'1000017162',	'10821407',	'1000076309',	'1353468',	'1000013503',	'1370209',	'17724332',	'189937050',	'0',	'172713423',	'641609',	'63388200',	'172696000',	'20542532',	'163296608',	'1000006644',	'18599235',	'1000078242',	'176787354',	'1000015266',	'70932',	'119636',	'4509648',	'35271',	'7667918',	'171892321',	'176404999',	'89183',	'187444883',	'181933286',	'747880',	'188461104',	'1353443',	'9370176',	'181902967',	'175392127',	'9287189',	'189859423',	'1000076242',	'181933151',	'192915913',	'12552288',	'201307',	'190531284',	'63873778',	'171248961',	'126580',	'612480',	'1681285',	'120324'"
        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and sid in({sids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_value_update.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('Key_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=37,sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' ) and {each_source} and sid not in ({sids})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=300000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_value_update.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('notKey_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(uuids_update)
                    # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=37,sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=5)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('kaola', ssmonth, 'kaola', each_sp1)] = len(uuids), len(uuids_update)
                # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=37, sales_by_uuid=sales_by_uuid_1)


        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=6)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('suning', ssmonth, 'suning', each_sp1)] = len(uuids), len(uuids_update)
                # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=37, sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=7)
                '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] not in (0, 1, 3, 4, 5):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 1
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('vip', ssmonth, 'vip', each_sp1)] = len(uuids), len(uuids_update)
                # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=37, sales_by_uuid=sales_by_uuid_1)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_172(smonth, emonth, uuid_value_update)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=42, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print (i, cc[i])

        # exit()

        return True

    # def brush(self, smonth, emonth, logId=-1):
    def brush_1227Bmeisu(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        uuid_value_update = []

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        alias_all_bids = "'9788',	'48200',	'5918430',	'48443',	'48228',	'48241',	'3645511',	'4885410',	'48199',	'48282',	'48214',	'48222',	'48219',	'1660132',	'48224',	'651663',	'48281',	'17051',	'48221',	'48225',	'693001',	'793396',	'405721',	'5220058',	'17435',	'48284'"
        # alias_lists = ['9788',	'48200',	'5918430',	'48443',	'48228',	'48241',	'3645511',	'4885410',	'48199',	'48282',	'48214',	'48222',	'48219',	'1660132',	'48224',	'651663',	'48281',	'17051',	'48221',	'48225',	'693001',	'793396',	'405721',	'5220058',	'17435',	'48284']

        uuids = []
        uuids_update = []

        where = '''uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and  pkey<'{emonth}') and source=1 and shop_type=11 and alias_all_bid in ({alias_all_bids}) and cid in('211104','201284105','7052')
                            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, alias_all_bids=alias_all_bids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300000, rate=0.8)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_value_update.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
                # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3,4,5,6,7,8):
                #     uuids_update.append(uuid2)
                #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] = 0
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=38, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_172(smonth, emonth, uuid_value_update)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=41, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0301(self, smonth, emonth, logId=-1):

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
        uuid2 in('fbe64597-2f01-40a6-9f82-682620316607','0ca61ff2-a3a6-45cb-bd9a-65d076eb09c6')
        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where, limit=100000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=40, sales_by_uuid=sales_by_uuid)



        return True

    def helper_0930(self):
        '''
        batch 141 91357 E 表
        根据给定的品牌名，找到对应的alias_all_bid, 2020-01~2020-12销额最高的5家店铺，旗舰店，以及对应的宝贝+交易属性数
        '''
        utils.easy_call([chsop, graph], 'connect')
        df = pd.read_excel(r'C:\Users\chen.weihong\Desktop\20210903 奶粉重点品牌列表(1).xlsx', header=0,
                           sheet_name='Sheet1')
        h_b = {}
        h_bidt = {}
        for index, row in df.iterrows():
            brd = row[0].replace("'", "\\'")
            h_b[brd] = {}
            h_bidt[brd] = {}
            for i in brd.split('/'):
                h_b[brd][i] = 1
        s = "%' or name like '%".join(i for brd in h_b for i in h_b[brd])
        sql = f'''SELECT bid, alias_bid, name FROM artificial.all_brand WHERE name like '%{s}%'; '''
        data = chsop.query_all(sql)
        for row in data:
            bid, abid, bname = row
            for brd in h_b:
                for i in h_b[brd]:
                    if i in bname:
                        if abid != 0:
                            h_bidt[brd][abid] = bname
                        else:
                            h_bidt[brd][bid] = bname
        print(h_bidt)
        sql = f'''
                 SELECT
                     distinct alias_all_bid
                 FROM
                     sop_e.entity_prod_91357_E
                 WHERE
                     pkey >= '2020-01-01'
                     AND pkey<'2021-01-01'
                     AND alias_all_bid in {str(tuple(str(i) for brd in h_bidt for i in h_bidt[brd]))}; '''
        data = chsop.query_all(sql)
        h_v = {}
        for row in data:
            h_v[row[0]] = 1
        h_bid = {}
        for brd in h_bidt:
            for bid in h_bidt[brd]:
                if h_v.get(bid) is not None:
                    if h_bid.get(brd) is None:
                        h_bid[brd] = {}
                    h_bid[brd][bid] = h_bidt[brd][bid]

        s = "CASE"
        for brd in h_bid:
            s += " WHEN alias_all_bid IN " + str(
                tuple(h_bid[brd].keys()) if tuple(h_bid[brd].keys()) else '(-1)') + " THEN " + "'" + brd + "' "
        print(s)
        sql = f'''
                 SELECT
                     transform(
                             multiIf(source!=1,source,(shop_type<20 and shop_type>10),0,1),
                             [0,1,2,3,4,5,6,7,8,9,10],
                             ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu'],
                         '' ) AS "plt",
                     sid,
                     {s} ELSE '其它品牌' END AS "brd",
                     sum(sales)/100 ss,
                     countDistinct(source, item_id, trade_props_hash)
                 FROM
                     sop_e.entity_prod_91357_E
                 WHERE
                     pkey >= '2020-01-01'
                     AND pkey<'2021-01-01'
                     and source=2 and shop_type in(21,22)
                 GROUP BY
                     plt,
                     sid,
                     brd
                 ORDER BY
                     ss DESC
                 LIMIT 5 BY brd; '''
        data = chsop.query_all(sql)
        h_d = {}
        # s_map = {1: 'tmall', 2: 'jd', 3: 'gome', 4: 'jumei', 5: 'kaola', 6: 'suning'}
        h_s = {}
        for row in data:
            src, sid, brd, sl, ic = row
            h_s[(src, sid)] = 1
            if h_d.get(brd) is None:
                h_d[brd] = {}
            h_d[brd][(src, sid)] = [sl, ic]
        sql = f'''SELECT source_origin, sid, name FROM graph.ecshop WHERE (source_origin, sid) IN ({','.join(str(i) for i in h_s.keys())}); '''
        data = graph.query_all(sql)
        for row in data:
            source_origin, sid, name = row
            h_s[(source_origin, sid)] = name
        res = []
        ml = 0
        for brd in h_d:
            if brd == '其它品牌':
                continue
            tmp = [brd, ]
            for srcsid in h_d[brd]:
                src, sid = srcsid
                tmp += [src, sid, h_s[srcsid]] + h_d[brd][srcsid]
            tmp += [''] * (26 - len(tmp))
            for bid in h_bid[brd]:
                tmp += [bid, h_bid[brd][bid]]
            ml = max(ml, len(h_bid[brd]))
            if len(h_bid[brd]) == 13683:
                print(brd)
            res.append(tmp)
        print(ml)
        for tmp in res:
            tmp += [''] * (ml - len(tmp))
        col_shop = ['平台', 'sid', '店名', '销额', '条数']
        col = ['品牌名'] + [k for j in [[c + str(i + 1) for c in col_shop] for i in range(5)] for k in j]
        col_brd = ['bid', '品牌']
        col += [k for j in [[c + str(i + 1) for c in col_brd] for i in range(ml)] for k in j]
        with pd.ExcelWriter(r'C:\Users\chen.weihong\Desktop\20210903 奶粉重点品牌列表1011_京东海外.xlsx',
                            engine='xlsxwriter', options={'strings_to_urls': False}) as writer:
            df = pd.DataFrame(data=res, columns=col)
            print(df.shape)
            df.to_excel(writer, index=False)


    def skip_helper_172(self, smonth, emonth, uuids):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # 找出当期之前所有出过的题
        sql = '''select uuid2 from {tbl}  where (snum, tb_item_id, real_p1,month)
        in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}'  group by snum, tb_item_id, real_p1)
        '''.format(tbl= self.cleaner.get_tbl(), smonth=smonth)
        ret = self.cleaner.db26.query_all(sql)
        # map = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        map = [v[0] for v in ret]


        # 找出当期前出题的最近一条机洗结果
        sql_result_old_uuid = '''
        select a.*,b.sp1, b.sp20, b.sp42 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2,sales/num vol FROM {atbl}
        WHERE pkey  < '{smonth}'
        GROUP BY source, item_id, p1,uuid2,sales,num) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where a.uuid2 in({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, uuids=','.join(["'"+str(v)+"'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5], v[6], v[7], v[8]] for v in db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
         select a.*,b.sp1, b.sp20, b.sp42 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, alias_all_bid,uuid2,sales/num vol FROM {atbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0
        AND uuid2 NOT IN (SELECT uuid2 FROM {atbl}  WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sign = -1)
        ) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where a.uuid2 in ({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth, uuids=','.join(["'"+str(v)+"'" for v in uuids]))

        map_help_new = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5], v[6], v[7], v[8], v[4]] for v in db.query_all(sql_result_new_uuid)}

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
                    new_uuids.append(str(new_value[5]))
                else:
                    # 之前有出过题，但某些重要机洗值改变，重新出题
                    for old_key, old_value in map_help_old.items():
                        if new_key == old_key and (new_value[2] != old_value[2] or new_value[3] != old_value[3] or new_value[4] != old_value[4]):
                        # if new_key == old_key and (
                        #         new_value[2] != old_value[2] or new_value[3] != old_value[3] or new_value[4] !=
                        #         old_value[4] or float(new_value[1]) > 3 * float(old_value[1]) or float(
                        #         new_value[1]) < (1 / 3) * float(old_value[1])):
                            old_uuids_update.append(str(new_value[5]))
                        else:
                            continue

        return new_uuids, old_uuids_update, map_help_new

    def abnormal_price_uuid(self, smonth, emonth):

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)



        sql_meisu = '''
                    select uuid2 from(
            select t2.source MBD,t2.item_id item_id,t2.p1 ,argMin(`date`,date) Date,argMin(uuid2,date) uuid2,argMin(name,date) name,
            argMin(`trade_props.value`,date) name2,argMin(sales,date) salesvalue,argMin(num,date) salesvolume,argMin(vol,date) sellingprice,
            argMin(zongguige_final,date) packsize,argMin(yuzhi,date) averageprice  from (
            with
            -- t1计算E表最近条答题宝贝的总规格
            t1 as(
            select source,item_id,p1,toInt32(case when chutibaobei_zongguige is null then geleifugai_zongguige else chutibaobei_zongguige end) zongguige_final from(
                    select source,item_id, p1
                    ,argMax(case when "sp是否人工"='出题宝贝' then zongguige end,date) chutibaobei_zongguige
                    ,argMax(case when "sp是否人工" in('前后月覆盖','是','断层填充') then zongguige end,date) geleifugai_zongguige
                    from
                    (select a.*,b.zongguige,a.p1 from
                                    (with arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
                                    select p1,* from   sop_e.entity_prod_91357_E_meisu) a
                                    inner join
                                    (select uuid2, sum(toInt32OrZero("sp总规格")*sign) as zongguige
                                    from sop_e.entity_prod_91357_E_meisu where /*pkey>='2022-01-01' and*/ pkey<'{smonth}' and "sp是否人工" !='否'
                                    and toInt32OrZero("sp总规格")>0
                                    group by uuid2) b
                                    on a.uuid2 = b.uuid2)
                    --sop_e.entity_prod_91357_E_meisu
                    where  pkey<'{smonth}'  -- 修改计算截止时间
                    --and clean_props.value[indexOf(clean_props.name,'是否人工')] !='否'
                    --and clean_props.value[indexOf(clean_props.name,'子品类')] NOT IN  ('其它','其它奶粉')
                    --and (name not Like '%补充剂%' or name not like '%强化剂%')
                    --and clean_props.value[indexOf(clean_props.name,'SKU-出数专用')] not like '\_%'
                    --and length(clean_props.value[indexOf(clean_props.name,'总规格')])>0
                    group by source,item_id,p1) a)
            -- t2计算A表各条宝贝信息
            ,t2 as(
            with arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
            select source,item_id,p1,uuid2,(sales/num)/100 vol,date,name,`trade_props.value`,sales,num from sop.entity_prod_91357_A where pkey>='{smonth}' and pkey<'{emonth}'  -- 修改统计时间
            and cid in('211104','201284105','7052') and ((source = 1 and shop_type in (21,23,25,26,28))
            or (source = 2 and shop_type in (11,12)) or (source = 1 and shop_type =11)) and uuid2 not in (select uuid2 from sop.entity_prod_91357_A where pkey>='{smonth}' and pkey<'{emonth}' and sign=-1)) -- 修改统计时间
            select t2.*,(t2.vol/t1.zongguige_final)*1000 yuzhi,t1.zongguige_final from t1 inner join t2
            on t1.source=t2.source and t1.item_id=t2.item_id and t1.p1=t2.p1
            where((t2.vol/t1.zongguige_final)*1000<=80 or (t2.vol/t1.zongguige_final)*1000>=1000)
            and (t2.name not Like '%补充剂%' and t2.name not like '%强化剂%')
             ) j
            /*where concat(toString(t2.source),toString(t2.item_id),toString(t2.p1))
            not in (select concat(toString(snum),toString(tb_item_id),toString(real_p1))  from  mysql('192.168.30.93', 'product_lib', 'entity_91357_item', 'cleanAdmin', '6DiloKlm') where pkey>='{smonth}' and pkey<'{emonth}')*/
             group by t2.source,t2.item_id,t2.p1)
             where toString(uuid2)
            not in (select toString(uuid2)  from  mysql('192.168.30.93', 'product_lib', 'entity_91357_item', 'cleanAdmin', '6DiloKlm') where pkey>='{smonth}' and pkey<'{emonth}')
        '''.format(smonth=smonth, emonth=emonth)
        abnormal_price_uuids_meisu = [str(v[0]) for v in db.query_all(sql_meisu)]

        sql_daku = '''
                    select uuid2 from(
            select t2.source MBD,t2.item_id item_id,t2.p1 ,argMin(`date`,date) Date,argMin(uuid2,date) uuid2,argMin(name,date) name,
            argMin(`trade_props.value`,date) name2,argMin(sales,date) salesvalue,argMin(num,date) salesvolume,argMin(vol,date) sellingprice,
            argMin(zongguige_final,date) packsize,argMin(yuzhi,date) averageprice  from (
            with
            -- t1计算E表最近条答题宝贝的总规格
            t1 as(
            select source,item_id,p1,toInt32(case when chutibaobei_zongguige is null then geleifugai_zongguige else chutibaobei_zongguige end) zongguige_final from(
                    select source,item_id, p1
                    ,argMax(case when "sp是否人工"='出题宝贝' then zongguige end,date) chutibaobei_zongguige
                    ,argMax(case when "sp是否人工" in('前后月覆盖','是','断层填充') then zongguige end,date) geleifugai_zongguige
                    from
                    (select a.*,b.zongguige,a.p1 from
                                    (with arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
                                    select p1,* from   sop_e.entity_prod_91357_E) a
                                    inner join
                                    (select uuid2, sum(toInt32OrZero("sp总规格")*sign) as zongguige
                                    from sop_e.entity_prod_91357_E where /*pkey>='2022-01-01' and*/ pkey<'{smonth}' and "sp是否人工" !='否'
                                    and toInt32OrZero("sp总规格")>0
                                    group by uuid2) b
                                    on a.uuid2 = b.uuid2)
                    --sop_e.entity_prod_91357_E
                    where  pkey<'{smonth}'  -- 修改计算截止时间
                    --and clean_props.value[indexOf(clean_props.name,'是否人工')] !='否'
                    --and clean_props.value[indexOf(clean_props.name,'子品类')] NOT IN  ('其它','其它奶粉')
                    --and (name not Like '%补充剂%' or name not like '%强化剂%')
                    --and clean_props.value[indexOf(clean_props.name,'SKU-出数专用')] not like '\_%'
                    --and length(clean_props.value[indexOf(clean_props.name,'总规格')])>0
                    group by source,item_id,p1) a)
            -- t2计算A表各条宝贝信息
            ,t2 as(
            with arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
            select source,item_id,p1,uuid2,(sales/num)/100 vol,date,name,`trade_props.value`,sales,num from sop.entity_prod_91357_A where pkey>='{smonth}' and pkey<'{emonth}'  -- 修改统计时间
            and cid not in('211104','201284105','7052') and ((source = 1 and shop_type =12)
            or (source = 1 and (shop_type >20 or shop_type<10)) or (source in (2,5,6,7,11))) and uuid2 not in (select uuid2 from sop.entity_prod_91357_A where pkey>='{smonth}' and pkey<'{emonth}' and sign=-1)) -- 修改统计时间
            select t2.*,(t2.vol/t1.zongguige_final)*1000 yuzhi,t1.zongguige_final from t1 inner join t2
            on t1.source=t2.source and t1.item_id=t2.item_id and t1.p1=t2.p1
            where((t2.vol/t1.zongguige_final)*1000<=80 or (t2.vol/t1.zongguige_final)*1000>=1000)
            and (t2.name not Like '%补充剂%' and t2.name not like '%强化剂%')
             ) j
            /*where concat(toString(t2.source),toString(t2.item_id),toString(t2.p1))
            not in (select concat(toString(snum),toString(tb_item_id),toString(real_p1))  from  mysql('192.168.30.93', 'product_lib', 'entity_91357_item', 'cleanAdmin', '6DiloKlm') where pkey>='{smonth}' and pkey<'{emonth}')*/
             group by t2.source,t2.item_id,t2.p1)
             where toString(uuid2)
            not in (select toString(uuid2)  from  mysql('192.168.30.93', 'product_lib', 'entity_91357_item', 'cleanAdmin', '6DiloKlm') where pkey>='{smonth}' and pkey<'{emonth}')
        '''.format(smonth=smonth, emonth=emonth)
        abnormal_price_uuids_daku = [str(v[0]) for v in db.query_all(sql_daku)]

        return abnormal_price_uuids_meisu, abnormal_price_uuids_daku



    ###奶粉默认月度报告版本_最新版
    def brush(self, smonth, emonth, logId=-1):


        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}




        ###每个月要改三个visible，由骏文提供*************************************************************************************************
        ###<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<千万别忘了改>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        visible_meisu = 90
        visible_daku = 91
        douyin_imp_brand = "'7193802','341477','796413','48443','48214','48241','48260','48282','6342697','4745507','796371','3645511','342584','116218','48200','2725134','48222','883322','48251','405721','6986865','7219498','3335846','651663','7261544','5220058','48257','1293363','207393','333490','17435','48224','9788','48390','48219','1660132','48284','48225','48281','48204','225031','48209','48211','6603250','693001','1718974','3735704','3726964','17051','48210','48228','48226','48221','7173272','4885410','48199','9788',	'17435',	'48199',	'48219',	'48221',	'48222',	'48225',	'48281',	'341477',	'405721',	'693001',	'1293363',	'1902536',	'5918430',7193814,46581,405706,5435378,5782318,7350684"


        ##############美素部分####################

        uuid_value_update_meisu = []

        ## 天猫和京东国内
        sources = ['source = 1 and shop_type in (21,23,25,26,28)', 'source = 2 and shop_type in (11,12)']
        ## 暂用sid
        sids_meisu = "'3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'62463392',	'64449098',	'65803253',	'19825965',	'763120',	'184880001',	'194273299',	'181933286',	'194913054',	'69500138',	'195418959',	'9286775',	'20542532',	'68423191',	'18599235',	'10821407',	'190806051',	'191323276',	'187028404',	'195491213',	'194778061',	'172696000',	'193198183',	'19405629',	'9370176',	'1353443',	'9287189',	'194235592',	'163753574',	'181933151',	'191784770',	'66745772',	'1353468',	'7667918',	'7709298',	'17650243',	'63978220',	'68251797',	'186807361',	'189859423',	'193559416',	'60570654',	'195203424',	'192974228',	'194149023',	'8087184',	'20619935',	'12144949',	'18196280',	'189938717',	'193238145',	'193856231',	'181904206',	'10729360',	'188933930',	'193050336',	'192544809',	'195429933',	'12039139',	'10592535',	'191284939',	'168298270',	'187444883',	'194962316',	'193445447',	'193142273',	'1000002847',	'1000003568',	'1000002668',	'1000003179',	'1000002520',	'1000003570',	'1000002672',	'1000003112',	'1000006644',	'1000002688',	'1000003111',	'1000126429',	'1000013503',	'1000014486',	'1000102591',	'647925',	'1000076309',	'10251198',	'1000187839',	'973338',	'170952',	'10343082',	'211344',	'120324',	'746561',	'23898',	'1000015205',	'1000001582',	'10020105',	'1000078242',	'172740',	'1000017162',	'1000015026',	'674014',	'70932',	'1000015587',	'1000084244',	'213653',	'1000348121',	'131560',	'214651',	'126580',	'818745',	'747880',	'30356',	'663225',	'673126',	'0',	'768735',	'10134444',	'42902',	'1000010405',	'10214507',	'207325',	'63471',	'698676',	'35271',	'652695',	'606696',	'70302',	'10139043',	'876497',	'851809',	'748616',	'10052923',	'598847',	'1000221821',	'10187738',	'10034275',	'208939',	'822328',	'158274',	'792301',	'10221701',	'1000337685',	'184556',	'10251317',	'10631028',	'10153381',	'158096',	'782345',	'709650',	'166535',	'1000281625',	'866096',	'19293',	'10138259',	'1000015266',	'63337',	'851789',	'887773',	'1000099746',	'724628',	'125117',	'194323',	'970088',	'88066',	'204899',	'10133327','10374731','10304410','10172529','10281422','3602532',	'163296608',	'188461104',	'9281929',	'67302766',	'17961262',	'17078538',	'13657988',	'64449098',	'65803253',	'190806051',	'191323276',	'7667918',	'192172837',	'190229639',	'763120',	'184880001',	'9661694',	'4958318',	'195191206',	'195418959',	'9286775',	'14975123',	'62654366',	'191285215',	'164615368',	'68423191',	'10821407',	'19405629',	'20542532',	'18599235',	'62463392',	'18092113',	'10179331',	'1353468',	'19825965',	'181161372',	'187822014',	'13769176',	'189399394',	'194876717',	'195648339',	'6681857',	'2497119',	'70645417',	'172696000',	'193198183',	'1353443',	'194235592',	'181904206',	'9370176',	'9287189',	'163753574',	'181933151',	'189507102',	'194376602',	'1359061',	'10592535',	'194149023',	'7709298',	'191698870',	'191284939',	'66745772',	'59615852',	'186673609',	'70752993',	'62058104',	'173816544',	'193199339',	'195730062',	'193650050',	'1357077',	'68251797',	'1359088',	'189859423',	'186807361',	'193559416',	'192544809',	'1385823',	'195203424',	'76751',	'8197953',	'168298270',	'193739946',	'187444883',	'184537697',	'69500138',	'4717510',	'12558866',	'194747149',	'193238145',	'7738513',	'188933930',	'187028404',	'192937275',	'193050336',	'60570654',	'191784770',	'11380114',	'194273299',	'192974228',	'175136630',	'192005219',	'20619935',	'12180805',	'12144949',	'187155437',	'169616109',	'162801135',	'193803011',	'194797218',	'9759512'"
        ## 23nian暂用sid
        # sids_meisu = "188461104, 9281929, 19405629, 172696000, 193198183, 62463392, 3602532, 20542532, 67302766, 186807361, 9370176, 195819241, 187028404, 190806051, 194149023, 195730062, 195850515, 192916063, 194321853, 168238957, 181902967, 64339464, 195952474, 17961262, 192544809, 181933151, 195821061, 194273299, 195824257, 185023263, 12660305, 194049357, 194594101, 192107457, 167526824, 65076191, 195821279, 194588636, 10592535, 195639923, 163753574, 189689358, 8087184, 192586056, 195357875, 193007871, 64230966, 181904206, 192634910, 68251797, 193238145, 128752971, 6698581, 10795284, 9987581, 194923474, 196098113, 64449098, 195418959, 194962316, 12039139, 67015874, 192855234, 12176792, 193550349, 193449672, 194826346, 195848090, 193032623, 191135705, 191197569, 193151131, 192700551, 18599235, 184880001, 9287189, 192937275, 188784491, 168964884, 195125031, 10821407, 194235592, 188933930, 1353443, 193045967, 7667918, 191284939, 194747149, 9863273, 189021715, 64558666, 194200123, 1353480, 181932884, 184351723, 191401186, 68423191, 10729360, 8109079, 164281380, 195762820, 195178688, 8796515, 193888985, 61477746, 10145044, 195681805, 195497312, 195746649, 194429148, 189018866, 195965328, 189859423, 163296608, 195824256, 194923715, 194280503, 194938666, 20120390, 193121032, 191299472, 65803253, 10179331, 195951936, 195599177, 193559416, 6696356, 164071505, 195832872, 67975080, 12361758, 188750428, 195847640, 194604956, 191928718, 195759070, 195933742, 9322956, 193579187, 1000002847, 10374731, 746561, 10304410, 782345, 1000006644, 1000003179, 1000002520, 1000003568, 1000370661, 1000001582, 1000221821, 1000395593, 798288, 1000356822, 1000102591, 10416815, 10205439, 10344443, 42902, 959315, 1000147401, 11773499, 207325, 10731035, 1000303181, 658438, 10110573, 1000003111, 70932, 63337, 19293, 1000337685, 10433918, 12220042, 1000350447, 11737388, 1000300485, 654200, 10320733, 10165418, 10439499, 1000440213, 10080073, 10251317, 10631028, 11490313, 10366382, 207624, 158274, 1000002670, 1000015587, 732518, 10256310, 640492, 10075214, 46055, 10135356, 10435508, 166535, 990653, 872571, 23898, 11454326, 663225, 10052923, 647786, 1000004706, 10293195, 11593704, 11486395, 218401, 1000126429, 647925, 88066, 10139043, 10158184, 1000002701, 1000090824, 10176562, 952781, 1000002710, 29835, 10192157, 815116, 663094, 11806583, 11849864, 1000367414, 10044004, 1000381925, 10429224, 1000312994, 10168781, 1000003112, 11519595, 1000010405, 208939, 10421454, 12044587, 11885269, 10906916, 863136, 201307, 10369962, 11420567, 598847, 620917, 1000361242, 1000003570, 112175, 10156866, 10187738, 10020105, 768735, 63471, 10221701, 1000013503, 11502679, 11330065, 21228, 1000104341, 894218, 174417, 1000209086, 1000008823, 180022, 801544, 1000333223, 11391378, 10286004, 1000003590, 10312645, 709650, 651850, 12739, 1000092944, 10166469, 690148, 10401435, 11818340, 776254, 996898, 1000295963, 998848, 1000013402, 1000078242, 1000002668, 673126, 11442110, 1000014602, 613998, 1000374886, 1000332823, 1000281625, 213653, 1000418105, 11918377, 724628, 1000002672, 172740, 10034275, 769921, 10134444, 10220909, 866096, 1000014803, 10806248, 1000076283, 12205013, 612480, 687475, 10240547, 10295953, 11499073, 10452137, 10417002, 11773240, 11796994, 11710765, 11591925, 10733146"


        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''{each_source} and sid in({sids}) and cid in('211104','201284105','7052','31762')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids_meisu)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=300000)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_meisu.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('Key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update + uuids, clean_flag, visible_check=1, visible=visible_meisu,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_source in sources:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01']]:
                uuids = []
                uuids_update = []
                where = '''{each_source} and sid not in({sids}) and cid in('211104','201284105','7052','31762')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, sids=sids_meisu)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=300000,
                                                                     rate=0.85)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_meisu.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format('No_key_sids', ssmonth, each_source)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids_update + uuids, clean_flag, visible_check=1, visible=visible_meisu,
                                       sales_by_uuid=sales_by_uuid_1)

        ###淘宝部分  (尽量不要同意历史分月补题，出题量太多且淘宝销售额占比只有5%)
        ##淘宝指定alias_all_bid
        alias_all_bids_meisutaobao = "'9788',	'48200',	'5918430',	'48443',	'48228',	'48241',	'3645511',	'4885410',	'48199',	'48282',	'48214',	'48222',	'48219',	'1660132',	'48224',	'651663',	'48281',	'17051',	'48221',	'48225',	'693001',	'793396',	'405721',	'5220058',	'17435',	'48284'"
        uuids = []

        where = '''source=1 and shop_type=11 and alias_all_bid in ({alias_all_bids}) and cid in('211104','201284105','7052','31762')
                                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth,
                                           alias_all_bids=alias_all_bids_meisutaobao)
        ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=300000, rate=0.85)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_value_update_meisu.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_meisu,
                               sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_172(smonth, emonth, uuid_value_update_meisu)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=visible_meisu,
                               sales_by_uuid=sales_by_uuid)

        ##item_id重新捞一遍

        ## 国内
        sources = ['source = 1 and shop_type in (21,23,25,26,28)', 'source = 2 and shop_type in (11,12)',
                   'source = 1 and shop_type =11']

        for each_source in sources:
            uuids2 = []
            uuids_update2 = []
            sql_itemid = '''
                    select distinct tb_item_id from {tbl} where  cast(created as date) = cast(now() as date) and visible = {visible}
                    '''.format(tbl=self.cleaner.get_tbl(), visible=visible_meisu)
            ret_itemid = self.cleaner.db26.query_all(sql_itemid)
            itemids = [str(v[0]) for v in ret_itemid]

            where = '''
                    {each_source} and item_id in ({itemids})
                    '''.format(ctbl=ctbl, each_source=each_source, smonth=smonth, emonth=emonth,
                               itemids=','.join(["'" + str(v) + "'" for v in itemids]))
            ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=200000)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids2.append(uuid2)
            cc['{}'.format(each_source)] = [len(uuids2), len(uuids_update2)]
            self.cleaner.add_brush(uuids2, clean_flag, visible_check=1, visible=visible_meisu,
                                   sales_by_uuid=sales_by_uuid_1)

        ##############大库部分####################

        uuid_value_update_daku = []

        sources_daku = ['source = 1 and shop_type < 20 and shop_type = 12',
                        'source = 1 and (shop_type > 20 or shop_type < 10 )',
                        'source = 2']
        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        ## 21年使用的sids
        sids_daku = "'1000002847',	'1000003568',	'1000003179',	'1000002520',	'1000002668',	'1000015026',	'1000001582',	'1000003570',	'1000002672',	'1000003112',	'1000002688',	'1000003111',	'1000076309',	'1000126429',	'65803253',	'186949317',	'1000014486',	'9281929',	'647925',	'192172837',	'176778182',	'189937050',	'62645733',	'172427629',	'10821407',	'13769176',	'69176931',	'9286775',	'188461104',	'3602532',	'1000015205',	'0',	'9370176',	'67302766',	'17078538',	'17961262',	'64449098',	'213653',	'1000084683',	'170685533',	'1000013503',	'54056',	'10089438',	'1278329',	'13640540',	'4407822',	'69500138',	'11506904',	'1000076024',	'68423191',	'1000078242',	'1000017162',	'18092113',	'1000102591',	'19825965',	'191323276',	'0',	'214651',	'18394179',	'170952',	'747880',	'13594370',	'169616109',	'1364310',	'19405629',	'23898',	'20542532',	'10020105',	'163296608',	'1000006644',	'186807361',	'172740',	'18599235',	'1000015266',	'70932',	'66424402',	'186814778',	'1353468',	'806728',	'7667918',	'193310965',	'184537697',	'17724332',	'126580',	'496660',	'19777247',	'70752993',	'176331448',	'30001607',	'1000076286',	'170830491',	'1000078270',	'188080410',	'30000205',	'9287189',	'181933151',	'190806051',	'190531284',	'8087184',	'10251198',	'187822014',	'171248961',	'89183',	'120324',	'194467513',	'192871508'"
        ## 23年使用的sids
        # sids_daku = "10104847, 168271691, 1356981, 194086185, 192939952, 178205790, 190148184, 16481966, 1353954, 179004644, 11972539, 17952388, 193212155, 65106895, 194005314, 172641347, 188382426, 10688845, 185330396, 194825136, 180114598, 190906768, 59604103, 191545180, 61568362, 195715380, 1474857, 195844638, 195047546, 188146985, 62661428, 3095354, 193987477, 193984627, 193492457, 193899504, 163868749, 189937050, 3086801, 13750171, 8742373, 193222671, 1355309, 19727199, 179082411, 3575067, 195093851, 195711866, 193267781, 16893567, 17909473, 192618924, 62433413, 166552398, 194903558, 173798028, 194739104, 194527795, 1354164, 59989877, 1353974, 10126741, 190801403, 187914087, 188658722, 162832869, 59615852, 3908912, 10075881, 64546397, 12662378, 173528035, 191938425, 12843700, 192128444, 1965363, 64491757, 12270756, 11920440, 190212354, 12365859, 1357801, 68657310, 1363567, 1358990, 164236185, 17221557, 192158473, 2988163, 165507656, 940833, 189793387, 68352549, 2032953, 7181415, 188596362, 8961046, 174519914, 193553883, 177603607, 186973225, 1357909, 6681857, 14385051, 1278329, 1354258, 59599021, 179064359, 192663473, 12590663, 60937062, 10010350, 8935071, 5686755, 20010344, 190609694, 17388733, 173560724, 162989533, 195039569, 5363767, 1388499, 2717695, 186814778, 187999543, 7021622, 14770224, 16159931, 165670047, 185559187, 186838549, 188461104, 9281929, 19405629, 170685533, 192915913, 170830491, 62463392, 191592705, 186807361, 9370176, 190806051, 194149023, 195730062, 195850515, 192916063, 194321853, 168238957, 181902967, 64339464, 195564566, 195952474, 17961262, 192544809, 181933151, 195821061, 66424402, 65696293, 194273299, 185023263, 12660305, 195504400, 194049357, 193171994, 192107457, 167526824, 65076191, 195821279, 10592535, 195639923, 163753574, 189689358, 8087184, 192586056, 193617977, 195357875, 193007871, 181871121, 181904206, 20542532, 3602532, 68251797, 193238145, 128752971, 172696000, 70128719, 195832872, 195823047, 192505290, 10795284, 195429091, 193310965, 64449098, 195418959, 67015874, 192855234, 12176792, 184037443, 193550349, 193449672, 193490265, 70201002, 186982835, 193032623, 191135705, 191197569, 165282384, 18599235, 192918050, 184880001, 195577676, 169469979, 12039139, 193823635, 192699126, 193971565, 195679503, 195925687, 195819241, 195125031, 10821407, 194235592, 188933930, 1353443, 7667918, 191284939, 194747149, 9863273, 189021715, 64558666, 194200123, 1353480, 181932884, 184351723, 191401186, 68423191, 8109079, 164281380, 195762820, 195178688, 193151131, 8796515, 193888985, 61477746, 10145044, 195681805, 195497312, 195746649, 194429148, 189018866, 195965328, 189859423, 163296608, 188655268, 192634910, 194938666, 20120390, 193121032, 191299472, 65803253, 10179331, 195951936, 67302766, 195599177, 193559416, 188637984, 63873778, 64737492, 12361758, 188750428, 195847640, 194604956, 195927178, 191928718, 195759070, 195933742, 9322956, 186949317, 64467009, 195472168, 195341747, 193579187, 1000002847, 1000085822, 10374731, 746561, 10304410, 1000015026, 1000006644, 1000003179, 1000002520, 1000003568, 1000001582, 1000221821, 1000395593, 1000313586, 798288, 1000102591, 10416815, 10205439, 10344443, 42902, 959315, 1000147401, 11773499, 207325, 10731035, 1000303181, 658438, 10110573, 1000356386, 1000348121, 1000003111, 70932, 63337, 19293, 1000076309, 1000337685, 125117, 666051, 10234590, 1000300485, 654200, 10320733, 10165418, 10439499, 170952, 126580, 1000440213, 10080073, 10251317, 10366382, 207624, 158274, 1000002670, 1000015587, 732518, 10256310, 640492, 10075214, 1000078270, 46055, 10135356, 1000076024, 166535, 990653, 872571, 23898, 11454326, 663225, 10052923, 215260, 194323, 723298, 10118778, 147129, 1000076307, 1000004706, 125128, 184556, 10213398, 1000126429, 10251198, 1000392887, 647925, 11721777, 1000002701, 1000090824, 10176562, 952781, 1000002710, 29835, 1000126677, 10192157, 815116, 1000170281, 119636, 589629, 183448, 11490313, 1000367414, 10044004, 1000381925, 1000312994, 1000003112, 11519595, 1000090221, 1000010405, 88066, 10423518, 10421454, 12044587, 861172, 10906916, 11589157, 10979277, 130647, 10337889, 1000361242, 1000003570, 112175, 10156866, 10187738, 1000076286, 10020105, 768735, 63471, 10221701, 1000013503, 11502679, 11330065, 21228, 1000104341, 894218, 174417, 1000209086, 1000008823, 180022, 801544, 1000333223, 11391378, 10286004, 1000003590, 10312645, 709650, 651850, 201307, 12739, 1000092944, 10166469, 11818340, 776254, 996898, 1000295963, 998848, 1000013402, 1000078242, 1000002668, 1000015205, 673126, 10168781, 1000014602, 613998, 1000374886, 1000332823, 1000281625, 213653, 1000418105, 11918377, 724628, 1000002672, 172740, 10034275, 1000015266, 10151263, 10134444, 10220909, 866096, 1000014803, 10806248, 1000076283, 10924001, 10158184, 12205013, 612480, 1000370661, 687475, 10240547, 10295953, 11499073, 10452137, 10417002, 1000084683, 10141242, 129166, 10500490"

        for each_source in sources_daku:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = ''' c_sp1=\'{each_sp1}\' and {each_source} and sid in({sids})
                            '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids_daku)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where,
                                                                         limit=300000)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_value_update_daku.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('Key_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(
                        uuids_update)
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                           sales_by_uuid=sales_by_uuid_1)

        for each_source in sources_daku:
            for each_sp1 in sp1_list:
                for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                    # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''c_sp1=\'{each_sp1}\' and {each_source} and sid not in ({sids})
                            '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, sids=sids_daku)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where,
                                                                         limit=300000,
                                                                         rate=0.8)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_value_update_daku.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids.append(uuid2)

                    cc['{}+{}+{}+{}'.format('notKey_sids', ssmonth, each_source, each_sp1)] = len(uuids), len(
                        uuids_update)
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                           sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''c_sp1=\'{each_sp1}\' and source=5
                        '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=400000,
                                                                     rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_daku.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('kaola', ssmonth, 'kaola', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''c_sp1=\'{each_sp1}\' and source=6
                        '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=400000,
                                                                     rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_daku.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('suning', ssmonth, 'suning', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = ''' c_sp1=\'{each_sp1}\' and source=7
                        '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=400000,
                                                                     rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_daku.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('vip', ssmonth, 'vip', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                       sales_by_uuid=sales_by_uuid_1)

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''c_sp1=\'{each_sp1}\' and source=11
                        '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=400000,
                                                                     rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_value_update_daku.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('douyin', ssmonth, 'douyin', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                       sales_by_uuid=sales_by_uuid_1)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_172(smonth, emonth, uuid_value_update_daku)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=visible_daku,
                               sales_by_uuid=sales_by_uuid)

        ##item_id重新捞一遍

        sql_itemid = '''
        select distinct tb_item_id from {tbl} where  cast(created as date) = cast(now() as date) and visible = {visible}
        '''.format(tbl=self.cleaner.get_tbl(), visible=visible_daku)
        ret_itemid = self.cleaner.db26.query_all(sql_itemid)
        itemids = [str(v[0]) for v in ret_itemid]

        for each_source in ['source = 1 and shop_type < 20 and shop_type = 12',
                            'source = 1 and (shop_type > 20 or shop_type < 10 )',
                            'source = 2', 'source=11']:
            uuids_update = []
            uuids = []
            where = '''
            {each_source} and item_id in({itemids})
            '''.format(ctbl=ctbl, each_source=each_source,
                       itemids=','.join(["'" + str(v) + "'" for v in itemids]))
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=400000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
            print(len(uuids_update), len(uuids))
            cc['{}~{}+{}'.format(smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=visible_daku,
                                   sales_by_uuid=sales_by_uuid1)

        #####每月11号跑一下，平时注释掉即可****************************************************************************

        abnormal_price_uuids_meisu, abnormal_price_uuids_daku = self.abnormal_price_uuid(smonth, emonth)

        if len(abnormal_price_uuids_meisu)>0:
            meisu_uuids = []
            where = '''
                    uuid2 in ({})
                    '''.format(','.join(["'" + str(v) + "'" for v in abnormal_price_uuids_meisu]))
            ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where, limit=100000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                meisu_uuids.append(uuid2)
            self.cleaner.add_brush(meisu_uuids, clean_flag, visible_check=1, visible=visible_meisu, sales_by_uuid=sales_by_uuid)

        if len(abnormal_price_uuids_daku) > 0:
            daku_uuids = []
            where = '''
                    uuid2 in ({})
                    '''.format(','.join(["'" + str(v) + "'" for v in abnormal_price_uuids_daku]))
            ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where, limit=100000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                daku_uuids.append(uuid2)
            self.cleaner.add_brush(daku_uuids, clean_flag, visible_check=1, visible=visible_daku, sales_by_uuid=sales_by_uuid)

        #####每月11号跑一下，平时注释掉即可end****************************************************************************

        ## --------------------------------------------------------------

        ## 源悦 特殊出题
        sql_yuanyue = '''SELECT snum, tb_item_id, p1, id, visible FROM {} where name like '%源悦%'  '''.format(
            self.cleaner.get_tbl())
        ret_yuanyue = self.cleaner.db26.query_all(sql_yuanyue)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret_yuanyue:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp_yuanyue = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        uuids_yuanyue = []
        uuids_yuanyue_feiqi = []

        where = '''
                name like '%源悦%'
                '''
        ret, sales_by_uuid_1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=300000)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_yuanyue:
                mpp_yuanyue['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids_yuanyue_feiqi.append(uuid2)
            else:
                mpp_yuanyue['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids_yuanyue.append(uuid2)

        self.cleaner.add_brush(uuids_yuanyue, clean_flag, visible_check=1, visible=visible_daku, sales_by_uuid=sales_by_uuid_1)





        update_sql = '''
        update {tbl} set visible = {visible_meisu} where cast(created as date) = cast(now() as date) and visible={visible_daku} and cid in('211104','201284105','7052','31762') and ((snum = 1 and shop_type in (21,23,25,26,28)) or (snum = 2 and shop_type in (11,12)) or (snum = 1 and shop_type =11))
        '''.format(tbl=self.cleaner.get_tbl(), visible_meisu=visible_meisu, visible_daku=visible_daku)
        print(update_sql)
        self.cleaner.db26.execute(update_sql)
        self.cleaner.db26.commit()


        # update_sql_dy = '''
        # update {tbl} set visible = {visible_douyin} where cast(created as date) = cast(now() as date) and snum = 11 and visible in ({visible_meisu}, {visible_daku})
        # '''.format(tbl=self.cleaner.get_tbl(), visible_douyin=visible_douyin, visible_meisu=visible_meisu,
        #            visible_daku=visible_daku)
        # print(update_sql_dy)
        # self.cleaner.db26.execute(update_sql_dy)
        # self.cleaner.db26.commit()

        update_sql_dy = '''
        update {tbl} set visible = {visible_meisu} where cast(created as date) = cast(now() as date) and snum = 11 and visible in ({visible_meisu}, {visible_daku})
        and shop_type in (11,12) and alias_all_bid  in ({douyin_imp_brand}) and cid in (20553,20557,33716,36550)
        '''.format(tbl=self.cleaner.get_tbl(), visible_meisu=visible_meisu, visible_daku=visible_daku, douyin_imp_brand=douyin_imp_brand)
        print(update_sql_dy)
        self.cleaner.db26.execute(update_sql_dy)
        self.cleaner.db26.commit()

        update_sql_dy127 = '''
        update {tbl} set visible = 127 where cast(created as date) = cast(now() as date) and snum = 11 and visible in ({visible_meisu}, {visible_daku})
        and (shop_type not in (11,12) or alias_all_bid not in ({douyin_imp_brand}) or cid not in (20553,20557,33716,36550))
        '''.format(tbl=self.cleaner.get_tbl(), visible_meisu=visible_meisu, visible_daku=visible_daku, douyin_imp_brand=douyin_imp_brand)
        print(update_sql_dy127)
        self.cleaner.db26.execute(update_sql_dy127)
        self.cleaner.db26.commit()

        update_sql_dy126 = '''
        update {tbl} set visible = 126 where cast(created as date) = cast(now() as date) and snum = 11 and visible =127
        and alias_all_bid in ({douyin_imp_brand})
        '''.format(tbl=self.cleaner.get_tbl(), visible_meisu=visible_meisu, visible_daku=visible_daku, douyin_imp_brand=douyin_imp_brand)
        print(update_sql_dy126)
        self.cleaner.db26.execute(update_sql_dy126)
        self.cleaner.db26.commit()

        # delete_sql_dy = '''
        # delete from {tbl} where cast(created as date) = cast(now() as date) and snum = 11 and visible in ({visible_meisu}, {visible_daku})
        # and (shop_type not in (11,12) or alias_all_bid not in ('7193802','341477','796413','48443','48214','48241','48260','48282','6342697','4745507','796371','3645511','342584','116218','48200','2725134','48222','883322','48251','405721','6986865','7219498','3335846','651663','7261544','5220058','48257','1293363','207393','333490','17435','48224','9788','48390','48219','1660132','48284','48225','48281','48204','225031','48209','48211','6603250','693001','1718974','3735704','3726964','17051','48210','48228','48226','48221','7173272','4885410','48199') or cid not in (20553,20557,33716,36550))
        # '''.format(tbl=self.cleaner.get_tbl(), visible_meisu=visible_meisu, visible_daku=visible_daku)
        # print(delete_sql_dy)
        # self.cleaner.db26.execute(delete_sql_dy)
        # self.cleaner.db26.commit()





        return True


    def brush_0510(self, smonth, emonth, logId=-1):
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sp1_list = ['婴儿牛奶粉', '婴儿特殊配方粉', '婴儿羊奶粉']

        for each_sp1 in sp1_list:
            for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
                # for ssmonth, eemonth in [['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                uuids = []
                uuids_update = []
                where = '''uuid2 in (select uuid2 from {ctbl} where sp1=\'{each_sp1}\' and source=11)
                        '''.format(ctbl=ctbl, each_sp1=each_sp1)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.8)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    # uuid_value_update_daku.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}+{}'.format('suning', ssmonth, 'suning', each_sp1)] = len(uuids), len(uuids_update)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=60, sales_by_uuid=sales_by_uuid_1)

        for i in cc:
            print (i, cc[i])

        return True

    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.hotfix_brush(tbl, dba, '是否人工')

        dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_b'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_c'.format(tbl))

        sql = '''
            CREATE TABLE {t}_a Engine Log AS
            SELECT uuid2, item_id, `sp子品类` a, toString(clean_alias_all_bid) b, `sp适用人群（段数）` c, clean_sales/clean_num price
            FROM {t} WHERE `sp单品规格` = '' AND b_id = 0
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {t}_b Engine Log AS
            SELECT item_id, `sp子品类` a, toString(clean_alias_all_bid) b, `sp适用人群（段数）` c, `sp单品规格` d, sum(clean_sales)/sum(clean_num) price
            FROM {t} WHERE d != '' AND b_id > 0 GROUP BY item_id, a, b, c, d
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {t}_c ENGINE = Join(ANY, LEFT, uuid2) AS
            SELECT uuid2, d FROM {t}_a a JOIN {t}_b b USING (item_id, a, b, c) ORDER BY ABS(a.price-b.price) LIMIT 1 BY uuid2
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE
                `sp单品规格` = ifNull(joinGet('{t}_c', 'd', uuid2),''),
                `sp总规格` = REPLACE(toString(ROUND(toFloat32(REPLACE(ifNull(joinGet('{t}_c', 'd', uuid2),'0'), 'g', ''))*toInt32OrZero(`sp件数`), 1)),'.0','')
            WHERE NOT isNull(joinGet('{t}_c', 'd', uuid2)) AND joinGet('{t}_c', 'd', uuid2) != ''
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_a'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_b'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_c'.format(tbl))


    def finish_new(self, tbl, dba, prefix):
        data = [
            ['Frieslandcampina/荷兰皇家菲仕兰','FRISO PRESTIGE/皇家美素佳儿','旺玥'],
            ['Frieslandcampina/荷兰皇家菲仕兰','Friso/美素佳儿','源悦'],
            ['Yili/伊利','金领冠Pro-Kido','珍护'],
            ['Danone/达能','Aptamil/爱他美','卓傲'],
            ['Danone/达能','Aptamil/爱他美','卓萃'],
            ['Danone/达能','Aptamil/爱他美',''],
            ['Junlebao/君乐宝','Banner Dairy/旗帜',''],
            ['H&Hgroup/健合集团','Biostime/合生元',''],
            ['Danone/达能','Cow&Gate',''],
            ['Abbott/雅培','Eleva/菁挚',''],
            ['Mead Johnson/美赞臣','Enfinitas/蓝臻',''],
            ['Frieslandcampina/荷兰皇家菲仕兰','FRISO PRESTIGE/皇家美素佳儿',''],
            ['Hero Group/玺乐集团','Hero Baby/天赋力',''],
            ['Hipp/喜宝','Hipp/喜宝',''],
            ['Wyeth/惠氏','Illuma/启赋',''],
            ['Danone/达能','Nutrilon/诺优能',''],
            ['Yili/伊利','金领冠Pro-Kido',''],
            ['Firmus/飞鹤','星飞帆',''],
            ['FIRMUS/飞鹤','',''],
            ['Wyeth/惠氏','',''],
            ['Yili/伊利','',''],
            ['Mead Johnson/美赞臣','',''],
            ['Junlebao/君乐宝','',''],
            ['A2/The A2 Milk Company','',''],
            ['Nestle/雀巢','',''],
            ['Beingmate/贝因美','',''],
            ['Abbott/雅培','',''],
            ['Bellamy’S/贝拉米','',''],
        ]

        self.cleaner.add_miss_cols(tbl, {'sp平台':'String'})
        self.cleaner.add_miss_cols('sop_e.entity_prod_91357_E_meisu', {'sp平台':'String'})
        dba.execute('''
            ALTER TABLE {} UPDATE `sp平台` = transform(`source`*100+shop_type,[121,125,128,123,126,211,212],['天猫去除猫超','天猫去除猫超','天猫去除猫超','猫超','猫超','京东国内','京东POP'],'')
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(tbl))
        dba.execute('''
            ALTER TABLE sop_e.entity_prod_91357_E_meisu UPDATE `sp平台` = transform(`source`*100+shop_type,[121,125,128,123,126,211,212],['天猫去除猫超','天猫去除猫超','天猫去除猫超','猫超','猫超','京东国内','京东POP'],'')
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(tbl))

        for a, b, c, in data:
            dba.execute('DROP TABLE IF EXISTS sop_e.entity_91357_fix')
            dba.execute('''
                CREATE TABLE sop_e.entity_91357_fix (
                    `sp适用人群（段数）` String,`sp平台` String,`v1` Float32,`v2` Float32
                ) ENGINE = Join(ANY, LEFT, `sp适用人群（段数）`, `sp平台`)
            ''')

            where = ' `sp厂商`=\''+a+'\'' if a!='' else '' + ' `sp产品品牌`=\''+b+'\'' if b!='' else '' + ' `sp子品牌`=\''+c+'\'' if c!='' else ''
            sql = '''
                INSERT INTO sop_e.entity_91357_fix
                WITH multiIf(`sp是否人工`='出题宝贝' AND `sp是否模型预测`!='模型应用',1,`sp是否人工`='前后月覆盖' AND `sp店铺分类`='FSS' AND `sp是否模型预测`!='模型应用',2,`sp是否人工`='前后月覆盖' AND `sp是否模型预测`!='模型应用',3,`sp店铺分类`='FSS' AND `sp总规格`!='',4,`sp总规格`!='',5,0) AS `sort`
                SELECT `sp适用人群（段数）`,`sp平台`,argMax(`sp单品规格`,`sales`) v1,argMax(round(price/toInt32(`sp件数`)/toFloat32(`sp单品规格`),2),`sales`) v2
                FROM {}
                WHERE `sp子品类` IN ['婴儿牛奶粉','婴儿特殊配方粉','儿童牛奶粉'] AND cid IN [211104,201284105,7052,31762] AND `sp是否无效链接` IN ['有效链接',''] AND `source`*100+shop_type IN (121,125,128,123,126,211,212)
                  AND {}
                GROUP BY `sp适用人群（段数）`,`sp平台`,`sort` HAVING `sort` > 0 ORDER BY `sort` LIMIT 1 BY `sp适用人群（段数）`,`sp平台`
            '''
            dba.execute(sql.format('sop_e.entity_prod_91357_E_meisu',where))
            dba.execute(sql.format(tbl,where))

            sql = '''
                ALTER TABLE {} UPDATE
                    `sp件数`=toString(ceil(price/toFloat32(`sp单品规格`)/joinGet('sop_e.entity_91357_fix', 'v2', `sp适用人群（段数）`,`sp平台`))),
                    `sp是否人工`='修正'
                WHERE cid IN [211104,201284105,7052,31762] AND `sp是否无效链接` IN ['有效链接',''] AND `source`*100+shop_type IN (121,125,128,123,126,211,212)
                  AND {} AND `sp是否人工` NOT IN ['否',''] AND `sp是否模型预测`='模型应用' AND NOT isNull(joinGet('sop_e.entity_91357_fix', 'v2', `sp适用人群（段数）`,`sp平台`))
                SETTINGS mutations_sync=1
            '''.format(tbl, where)
            dba.execute(sql)

            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单品规格`=joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`),
                    `sp件数`=toString(ceil(price/toFloat32(joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`))/joinGet('sop_e.entity_91357_fix', 'v2', `sp适用人群（段数）`,`sp平台`))),
                    `sp是否人工`='修正'
                WHERE cid IN [211104,201284105,7052,31762] AND `sp是否无效链接` IN ['有效链接',''] AND `source`*100+shop_type IN (121,125,128,123,126,211,212)
                  AND {} AND `sp是否人工` IN ['否',''] AND NOT isNull(joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`))
                SETTINGS mutations_sync=1
            '''.format(tbl, where)
            dba.execute(sql)

        dba.execute('''
            ALTER TABLE {} UPDATE `sp总规格` = toString(round(toFloat32(`sp单品规格`)*toInt32(`sp件数`),2))
            WHERE `sp单品规格` != '' AND `sp件数` != '' SETTINGS mutations_sync=1
        '''.format(tbl))

        dba.execute('DROP TABLE IF EXISTS sop_e.entity_91357_fix')
        dba.execute('''
            CREATE TABLE sop_e.entity_91357_fix (
                `sp适用人群（段数）` String,`sp平台` String,`v1` Float32,`v2` Float32
            ) ENGINE = Join(ANY, LEFT, `sp适用人群（段数）`, `sp平台`)
        ''')
        sql = '''
            INSERT INTO sop_e.entity_91357_fix
            SELECT `sp适用人群（段数）`,`sp平台`,round(sum(toFloat32(`sp总规格`))/sum(toInt32(`sp件数`)),2) v1, round(sum(price)/sum(toFloat32(`sp总规格`)),2) v2
            FROM {}
            WHERE `sp子品类` IN ['婴儿牛奶粉','婴儿特殊配方粉','儿童牛奶粉'] AND cid IN [211104,201284105,7052,31762] AND `sp是否无效链接` IN ['有效链接',''] AND `source`*100+shop_type IN (121,125,128,123,126,211,212)
            AND `sp是否人工` NOT IN ['否','']
            GROUP BY `sp适用人群（段数）`,`sp平台`
        '''
        dba.execute(sql.format('sop_e.entity_prod_91357_E_meisu'))
        dba.execute(sql.format(tbl))

        sql = '''
            ALTER TABLE {} UPDATE
                `sp单品规格`=toString(joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`)),
                `sp件数`=toString(ceil(price/joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`)/joinGet('sop_e.entity_91357_fix', 'v2', `sp适用人群（段数）`,`sp平台`))),
                `sp是否人工`='修正'
            WHERE cid IN [211104,201284105,7052,31762] AND `sp是否无效链接` IN ['有效链接',''] AND `source`*100+shop_type IN (121,125,128,123,126,211,212)
              AND `sp是否人工` IN ['否',''] AND NOT isNull(joinGet('sop_e.entity_91357_fix', 'v1', `sp适用人群（段数）`,`sp平台`))
            SETTINGS mutations_sync=1
        '''.format(tbl)
        dba.execute(sql)

        dba.execute('''
            ALTER TABLE {} UPDATE `sp总规格` = toString(round(toFloat32(`sp单品规格`)*toInt32(`sp件数`),2))
            WHERE `sp单品规格` != '' AND `sp件数` != '' SETTINGS mutations_sync=1
        '''.format(tbl))

        dba.execute('''
            ALTER TABLE {} UPDATE `sp总规格_2024` = `sp总规格`, `sp单品规格_2024` = `sp单品规格`, `sp件数_2024` = `sp件数`
            WHERE 1 SETTINGS mutations_sync=1
        '''.format(tbl))

        if prefix.find('_meisu') > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp产品品牌` = 'Friso/美素佳儿' WHERE `sp产品品牌` = 'Frisolac/美素力'
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)
            sql = '''
                ALTER TABLE {} UPDATE `sp产品品牌` = 'Friso Prestige/皇家美素佳儿' WHERE `sp产品品牌` = 'Frisolac Prestige/皇家美素力'
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)


    # def pre_brush_modify(self, v, products, prefix):
    #     if v['visible'] >= 90 and v['visible'] <= 100:
    #         v['flag'] = 0


    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if vv['sp42'] == '' and vv['sp18'] != '':
                # 总规格（sp42）为空，单品规格（sp18）不为空，则sp42=sp18*sp20；sp18去掉单位"g"再计算
                vv['sp42'] = '{:g}'.format(round(float(vv['sp18'].replace('g',''))*float(vv['sp20']),1))
            elif vv['sp42'] != '' and vv['sp18'] == '' and vv['sp42'].find('+') != -1:
                # 总规格（sp42）不为空，单品规格（sp18）为空 有加号("+")
                vv['sp42'] = '{:g}'.format(round(eval(vv['sp42']),1))
                vv['sp18'] = vv['sp42']
                vv['sp20'] = '1'
            elif vv['sp42'] != '' and vv['sp18'] == '' and vv['sp42'].find('*') != -1:
                # 总规格（sp42）不为空，单品规格（sp18）为空 只有乘号"*"
                vv['sp18'], vv['sp20'] = vv['sp42'].split('*')
                vv['sp42'] = '{:g}'.format(round(float(vv['sp18'].replace('g',''))*float(vv['sp20']),1))
            elif vv['sp42'] != '' and vv['sp18'] == '':
                # 总规格（sp42）不为空，单品规格（sp18）为空 既没有加号"+",也没有乘号"*"
                vv['sp18'] = vv['sp42']
                vv['sp20'] = '1'
            elif vv['sp42'] != '' and vv['sp18'] != '':
                # 总规格（sp42)为不为空，单品规格（sp18)也不为空，则sp42=sp18*sp20   sp18去掉单位"g"再计算
                vv['sp42'] = '{:g}'.format(round(float(vv['sp18'].replace('g',''))*float(vv['sp20']),1))
            vv['sp18'] = vv['sp18'].replace('g','')