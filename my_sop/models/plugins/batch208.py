import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    # 有发现交易属性名变了但值不变的情况
    def filter_brush_props(self):
        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all, item):
            if str(source) == '2':
                return []
            r = self.cleaner.json_decode(trade_prop_all)
            if isinstance(r, dict):
                p1 = r
            else:
                p1 = trade_prop_all.split(',')
                p2 = trade_prop_all.split('|')
                p1 = p1 if len(p1) > len(p2) else p2
                p1 = {i:str(v) for i,v in enumerate(p1)}
            p1 = [p1[p].replace(' ','') for p in p1 if p1[p].replace(' ','')!='']
            p1 = list(set(p1))
            p1.sort()
            return p1
        return format, '''IF( source=2, [], arraySort(arrayMap(y->replace(y,' ',''),arrayFilter(y->trim(y)<>'',arrayDistinct(trade_props.value)))) )'''


    def brush_0215B(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'"]


        ## V1
        for each_sp1 in sp1s:
            for each_source in sources:
                for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                    '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=250)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (1, 2, 3, 408, 545, 595, 626):
                                uuids_update.append(uuid2)
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)
                    cc['{}+{}+{}+{}'.format(each_source, ssmonth, each_sp1, 'V1')] = [len(uuids), len(uuids_update)]

        ## V2
        for each_sp1 in sp1s:
            for each_source in sources:
                for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
                    uuids = []
                    uuids_update = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                    '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (1, 2, 3, 408, 545, 595, 626):
                                uuids_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)
                    cc['{}+{}+{}+{}'.format(each_source, ssmonth, each_sp1, 'V2')] = [len(uuids), len(uuids_update)]

        for i in cc:
            print (i, cc[i])


        return True

    def brush_0304B(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sp1s = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'", "'Cleansing（洁面）'", "'Mask（面膜）'", "'防晒'", "'BodyCare（身体护理）'"]

        for each_sp1 in sp1s:
            uuids = []
            uuids_update = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1})) and source = 1 and shop_type>20
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (1,2,3,408,545,626,1954,1955,2087,2130,2131,2573,2574,2575,2576):
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
            cc['{}~{}@{}@{}'.format(smonth, emonth, each_sp1, 'Top200')] = [len(uuids_update), len(uuids)]
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=6, sales_by_uuid=sales_by_uuid1)
            if len(uuids_update) > 0:
                sql = 'update {} set visible = {}, created = now()  where id in ({})'.format(self.cleaner.get_tbl(), 6, ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()

        return True



    def brush_0215(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sp1s = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'", "'Cleansing（洁面）'", "'Mask（面膜）'", "'防晒'", "'BodyCare（身体护理）'"]

        for each_sp1 in sp1s:
            uuids = []
            uuids_update = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1})) and source = 1 and shop_type>20
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=200)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (2,):
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
            cc['{}~{}@{}@{}'.format(smonth, emonth, each_sp1, 'Top200')] = [len(uuids_update), len(uuids)]
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=6, sales_by_uuid=sales_by_uuid1)
            if len(uuids_update) > 0:
                sql = 'update {} set visible = {}, created = now(),pid=0,uid=0,flag=0 where id in ({})'.format(self.cleaner.get_tbl(), 6, ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()

        for each_sp1 in sp1s:
            uuids = []
            uuids_update = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1})) and source = 1 and shop_type>20
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (2,):
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
            cc['{}~{}@{}@{}'.format(smonth, emonth, each_sp1, 'Top80%')] = [len(uuids_update), len(uuids)]
            # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=7, sales_by_uuid=sales_by_uuid1)
            if len(uuids_update) > 0:
                sql = 'update {} set visible = {}, created = now(),pid=0,uid=0,flag=0 where id in ({})'.format(self.cleaner.get_tbl(), 7, ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()

        for i in cc:
            print(i, cc[i])


        return True

    def brush_0304(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\_\_\_%' and name like '%不建%' '''

        ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        pids = ','.join([str(v[0]) for v in ret_bujian])



        sales_by_uuid = {}
        cc = {}

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'"]

        uuids_delete = []


        ## V3


        for each_sp1 in sp1s:
            for each_source in sources:
                uuids3 = []
                uuids3_update = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.3)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408):
                            uuids3_update.append(uuid2)
                            uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                        else:
                            continue
                    else:
                        uuids3.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.3')] = [len(uuids3), len(uuids3_update)]
                self.cleaner.add_brush(uuids3 + uuids3_update, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)

        ## V4


        for each_sp1 in sp1s:
            for each_source in sources:
                uuids4 = []
                uuids4_update = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408):
                            uuids4_update.append(uuid2)
                            uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                        else:
                            continue
                    else:
                        uuids4.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.8')] = [len(uuids4), len(uuids4_update)]
                self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)

        sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_delete]))
        print(sql)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        for i in cc:
            print(i, cc[i])

        print(len(uuids_delete))
        # print(uuids_delete)

        print('检查是否有新增的pid，如有新增请确认--', pids)

        return True

    # def brush(self, smonth, emonth, logId=-1):
    def brush_05111(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\_\_\_%' and name like '%不建%' '''

        ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        pids = ','.join([str(v[0]) for v in ret_bujian])



        sales_by_uuid = {}
        cc = {}

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ["'Facial cream（乳液/面霜）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'"]

        uuids_delete = []




        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources:
                    uuids4 = []
                    uuids4_update = []
                    uuids_update = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source} and alias_all_bid='51297'
                    '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1, each_source=each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.95)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (0, 1, 2, 3, 4, 5, 6, 7):
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 2087):
                                uuids4_update.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 9, 0]
                            # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408):
                            #     uuids4_update.append(uuid2)
                            #     uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                            else:
                                continue
                        else:
                            uuids4.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                    cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.95')] = [len(uuids4), len(uuids4_update)]
                    # if len(uuids_update) > 0:
                    #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 9, ','.join([str(v) for v in uuids_update]))
                    #     print(sql)
                    #     self.cleaner.db26.execute(sql)
                    #     self.cleaner.db26.commit()
                    self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=8, sales_by_uuid=sales_by_uuid)



        print('检查是否有新增的pid，如有新增请确认--', pids)

        return True

    def brush_0228(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\___%' and name like '%不建%' '''

        ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        pids = ','.join([str(v[0]) for v in ret_bujian])



        sales_by_uuid = {}
        cc = {}

        uuids4_update = []
        uuids_delete = []
        uuids4 = []


        where = '''
        item_id in ('666758264546','666740116302','657131311785','667866141915')
        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408):
                    uuids4_update.append(uuid2)
                    uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                else:
                    continue
            else:
                uuids4.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]


        self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=8, sales_by_uuid=sales_by_uuid)

        return True


    def brush_0307(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} where pid in(3, 408)'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}
        sales_by_uuid = {}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        cc = {}

        bids = "'218469', '219391','106593', '218961', '246337','883874','218521','3756309','3132220', '218550','244645','2548829'"
        sp1s = ["'Essence（液态精华）','Ampoule（安瓶/原液）'", "'BodyCare（身体护理）'", "'Cleansing（洁面）'", "'Gel/toner（化妆水/爽肤水）'", "'Facial cream（乳液/面霜）'", "'防晒'", "'Set（面部护理套装）'", "'Mask（面膜）'", "'Eye cream（眼霜）'"]

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                uuids = []
                uuids_update = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where c_sp1 in({each_sp1}) and pkey>='{ssmonth}' and pkey<'{eemonth}') and alias_all_bid in ({bids}) and source =1 and shop_type>20
                '''.format(ctbl=ctbl, each_sp1=each_sp1, ssmonth=ssmonth, eemonth=eemonth, bids=bids)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=50)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408):
                            uuids_update.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                    else:
                        # continue
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                cc['{}~{}@{}@'.format(ssmonth, eemonth, each_sp1)] = [len(uuids), len(uuids_update)]
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=101, sales_by_uuid=sales_by_uuid1)
                self.cleaner.add_brush(uuids_update, clean_flag, visible_check=1, visible=102, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print (i, cc[i])

        return True

    def bids11(self):
        brands = [
            ['avene', 218469],
            ['Bioderma', 219391],
            ['Dr.Ci:Labo', 106593],
            ['Eucerin', 218961],
            ['Filorga', 246337],
            ['ISDIN', 883874],
            ['LRP', 218521],
            ['MartiDERM', 3756309],
            ['sesderma', 3132220],
            ['VICHY', 218550],
            ['WINONA', 244645],
            ['Biohyalux',2548829]
        ]
        bids = [str(v[1]) for v in brands]
        bids = ','.join(bids)
        return bids
        # sql = 'SELECT bid, alias_bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        # ret = self.cleaner.db26.query_all(sql)
        # bids = [str(v[0]) for v in ret]
        # bids = ','.join(bids)
        # bid_by_alias_bid = {}
        # for v in ret:
        #     bid, alias_bid = v
        #     if alias_bid not in bid_by_alias_bid:
        #         bid_by_alias_bid[alias_bid] = []
        #     bid_by_alias_bid[alias_bid].append(bid)
        # bids_origin = [v[1] for v in brands]
        # return bids, bid_by_alias_bid, bids_origin

    def bids13(self):
        brands = [
            ['avene', '218469'],
            ['Bioderma', '219391'],
            ['Dr.Ci:Labo', '106593'],
            ['Eucerin', '218961'],
            ['Filorga', '246337'],
            ['ISDIN', '883874'],
            ['LRP', '218521'],
            ['MartiDERM', '3756309'],
            ['sesderma', '3132220'],
            ['VICHY', '218550'],
            ['WINONA', '244645'],
            # ['Yuze', '1421233'],
            # ['Homefical pro', '3755964'],
            # ['Caudalie', '130781'],
            # ['Dr.Jart+', '218970'],
            # ['Kiehl＇s/科颜氏', '52239'],
            ['Biohyalux', '2548829']
        ]
        bids = [str(v[1]) for v in brands]
        bids = ','.join(bids)
        return bids
        # sql = 'SELECT bid, alias_bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=bids)
        # ret = self.cleaner.db26.query_all(sql)
        # bids = [str(v[0]) for v in ret]
        # bids = ','.join(bids)
        # bid_by_alias_bid = {}
        # for v in ret:
        #     bid, alias_bid = v
        #     if alias_bid not in bid_by_alias_bid:
        #         bid_by_alias_bid[alias_bid] = []
        #     bid_by_alias_bid[alias_bid].append(bid)
        # bids_origin = [v[1] for v in brands]
        # return bids, bid_by_alias_bid, bids_origin


    # def brush(self, smonth, emonth, logId=-1):
    def brush_p1andp3(self, smonth, emonth, logId=-1):
        # 默认规则

        sql = 'SELECT snum, tb_item_id, real_p1 FROM {} where pid not in (3,408) '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        # mantis 0044166
        # http://192.168.1.192:8001/view.php?id=44166
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        sales_by_uuid = {}

        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        # # 1.上月增量链接，9个子品类，各出50道source=tmall的top宝贝。实际有10个子品类，其中面部精华和安瓶原液这两个类，合并出题（两个类排一起出top50）
        # visible_check = 1
        # uuids1 = []
        # sql = 'select name from cleaner.clean_sub_batch where batch_id = {} and name != \'Make-up remover/micellar（卸妆）\''.format(self.cleaner.bid)
        # sub_batch = [v[0] for v in self.cleaner.db26.query_all(sql)]
        # special_sb = ['Ampoule（安瓶/原液）', 'Essence（液态精华）']
        # for sb in sub_batch:
        #     if sb in special_sb:
        #         continue
        #     where = '''
        #             uuid2 in (select uuid2 from {} where c_sp1 = '{}' and (alias_all_bid in ({})) )
        #             and source = 1 and (shop_type > 20 or shop_type < 10 )
        #             '''.format(ctbl, sb, bids)
        #     ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
        #     sales_by_uuid.update(sales_by_uuid_1)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if '{}-{}-{}'.format(source,tb_item_id,p1) in mpp2:
        #             continue
        #         # if self.skip_brush(source, tb_item_id, p1):
        #         #     continue
        #         mpp2['{}-{}-{}'.format(source,tb_item_id,p1)] = True
        #         uuids1.append(uuid2)
        # # 其中面部精华和安瓶原液这两个类，合并出题
        # where = '''
        #         uuid2 in (select uuid2 from {} where c_sp1 in ({}) and (alias_all_bid in ({})) )
        #         and source = 1 and (shop_type > 20 or shop_type < 10 )
        #         '''.format(ctbl, ','.join(['\'{}\''.format(s) for s in special_sb]), bids)
        # ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
        # sales_by_uuid.update(sales_by_uuid_1)
        # for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp2:
        #         continue
        #     # if self.skip_brush(source, tb_item_id, p1):
        #     #     continue
        #     mpp2['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        #     uuids1.append(uuid2)

        # 2.10个子品类里面以下这些店铺里的13个品牌的链接，限tmall，所有上月新冒出来的链接，出visible check3。
        bids = self.bids13()
        uuids2 = []
        sids = '8772717,191684443,183707489,171547304,189997011,194766658,177033651,192842574,183703205,193538597,193233473,167805789,188301666,170739197,193267678,188335691,193528534,191865936,187018262,194063219,193107896,189438175,188198733,64558533,186821601,176881288,195820479'
        # where_sql = '''
        #         (tb_item_id,p1) in (
        #         select distinct(tb_item_id,p1) as c from {tbl}_parts where month>= '{smonth}' and source = 'tmall' and sid in ({sids})
        #         and alias_all_bid in ({bids})
        #         and c not in (select distinct(tb_item_id,p1) as d from {tbl}_parts where month < '{smonth}' and source = 'tmall' and sid in ({sids}))
        #         )
        #         '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=self.get_entity_tbl(), sids=sids)

        where_sql = '''
                        uuid2 in (
                        select uuid2 as c from {tbl} where date>= '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 ) and sid in ({sids})
                        and alias_all_bid in ({bids})
                        and c not in (select uuid2 as d from {tbl} where date < '{smonth}' and source = 1 and (shop_type > 20 or shop_type < 10 )  and sid in ({sids}))
                        )
                        '''.format(bids=bids, smonth=smonth, emonth=emonth, tbl=atbl, sids=sids)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where_sql)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        # 3.时间限上月增量的上述11个品牌限tmall的各自top80％的链接（不限子品类），如果还有的话，出check 2。
        # bids, bid_by_alias_bid, bids_origin = self.bids11()
        bids = self.bids11()
        uuids3 = []
        for bid in bids.split(','):
            # 给出的原始bid, where条件用和原始bid关联的所有bid
            # alias = bid_by_alias_bid[bid] if bid in bid_by_alias_bid else []
            # bids = [bid] + alias
            # where = 'source=\'tmall\' and alias_all_bid in ({bids}) and month >= \'{}\' AND month < \'{}\''.format(smonth, emonth, bids=','.join([str(t) for t in bids]))
            where = 'source=1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid = {} '.format(bid)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids3.append(uuid2)



        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid)
        self.cleaner.add_brush(uuids2+uuids3, clean_flag, 2, sales_by_uuid)

        # print('add new brush {}, {}, {}'.format(len(uuids1), len(uuids2), len(uuids3)))
        # print(sql)
        # self.cleaner.db26.execute(sql)
        return True

    def brush_0511(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}  '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        sales_by_uuid = {}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ['Essence（液态精华）', 'Ampoule（安瓶/原液）']

        uuids = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources:
                    for each_bid  in [218961,106593]:

                        where = '''
                                uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in (\'{each_sp1}\') and  alias_all_bid in ({each_bid})) and {each_source}
                                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1, each_source=each_source, each_bid=each_bid)
                        ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000)
                        sales_by_uuid.update(sales_by_uuid1)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids.append(uuid2)


        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid)

        return True

    def brush_1(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {} '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        sales_by_uuid = {}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ['Essence（液态精华）', 'Ampoule（安瓶/原液）']

        uuids = []

        for each_sp1 in sp1s:
            for each_source in sources:
                uuids_u = []
                where = '''
                        uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in (\'{each_sp1}\') and  alias_all_bid in (218961,106593)) and {each_source}
                        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100000)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        uuids_u.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                print (len(uuids_u))


        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0720(self, smonth, emonth, logId=-1):
    # def brush_p2andp4(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\_\_\_%' and name like '%不建%' '''

        ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        pids = ','.join([str(v[0]) for v in ret_bujian])



        sales_by_uuid = {}
        cc = {}

        sources = ['source = 1 and shop_type>20', 'source = 2']
        sp1s = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'"]

        uuids_delete = []


        # V3


        for each_sp1 in sp1s:
            for each_source in sources:
                uuids3 = []
                uuids3_update = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                and alias_all_bid not in ('246499',	'3003575',	'52501',	'4814012',	'246081',	'10744',	'207',	'2663784',	'2663784',	'52238',	'3756242',	'273204',	'14362',	'54358',	'11092',	'244881',	'3756308',	'241024',	'4571664',	'207',	'2204',	'2503',	'3560',	'3681',	'3779',	'4147',	'4589',	'5621',	'5825',	'6404',	'6758',	'6805',	'7012',	'8271',	'10744',	'11092',	'14287',	'14362',	'16129',	'16504',	'20645',	'23253',	'32871',	'47858',	'48199',	'51962',	'52038',	'52120',	'52188',	'52238',	'52297',	'52458',	'52498',	'52501',	'52567',	'52711',	'53191',	'53309',	'53312',	'53946',	'54358',	'56119',	'59203',	'61251',	'68367',	'71334',	'75353',	'78270',	'97604',	'102094',	'105167',	'106548',	'106593',	'110979',	'113866',	'124790',	'130781',	'133600',	'156557',	'159027',	'180407',	'181468',	'182627',	'197324',	'202229',	'218448',	'218450',	'218458',	'218461',	'218491',	'218502',	'218518',	'218520',	'218526',	'218529',	'218549',	'218562',	'218566',	'218592',	'218724',	'218787',	'218789',	'218793',	'218827',	'218895',	'218961',	'218976',	'219214',	'219394',	'241024',	'244706',	'244881',	'244933',	'245049',	'245072',	'245075',	'245138',	'245140',	'245212',	'245278',	'245301',	'245679',	'245844',	'245916',	'245950',	'246081',	'246337',	'246466',	'246499',	'266860',	'273204',	'281315',	'306283',	'404633',	'447992',	'474059',	'487946',	'487962',	'493003',	'502692',	'594015',	'613606',	'677694',	'726408',	'1042268',	'1052052',	'1055126',	'1058246',	'1074459',	'1651498',	'1725953',	'1941326',	'2142302',	'2169320',	'2339131',	'2663784',	'3003575',	'3132220',	'3161611',	'3431026',	'3540923',	'3746246',	'3755998',	'3756242',	'3756308',	'4571664',	'4729752',	'4803456',	'4814012',	'5475636',	'5489424',	'5735191',	'5884682',	'6335702',	'6336578',	'6574304')
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.3)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408, 2087, 2573, 2574, 2575, 2576):
                            uuids3_update.append(uuid2)
                            uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                        else:
                            continue
                    else:
                        uuids3.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.3')] = [len(uuids3), len(uuids3_update)]
                self.cleaner.add_brush(uuids3 + uuids3_update, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)

        sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_delete]))
        print(sql)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        # V4

        # /*alias_bid_sql = '''
        #         select  distinct alias_all_bid from {} where created >=cast(now() as date) and visible=3
        #         '''.format(self.cleaner.get_tbl())
        # ret_bid = self.cleaner.db26.query_all(alias_bid_sql)
        # bid_list = {'{}'.format(v[0]): [1] for v in ret_bid}*/


        for each_sp1 in sp1s:
            for each_source in sources:
                uuids4 = []
                uuids4_update = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
                and alias_all_bid not in ('246499',	'3003575',	'52501',	'4814012',	'246081',	'10744',	'207',	'2663784',	'2663784',	'52238',	'3756242',	'273204',	'14362',	'54358',	'11092',	'244881',	'3756308',	'241024',	'4571664',	'207',	'2204',	'2503',	'3560',	'3681',	'3779',	'4147',	'4589',	'5621',	'5825',	'6404',	'6758',	'6805',	'7012',	'8271',	'10744',	'11092',	'14287',	'14362',	'16129',	'16504',	'20645',	'23253',	'32871',	'47858',	'48199',	'51962',	'52038',	'52120',	'52188',	'52238',	'52297',	'52458',	'52498',	'52501',	'52567',	'52711',	'53191',	'53309',	'53312',	'53946',	'54358',	'56119',	'59203',	'61251',	'68367',	'71334',	'75353',	'78270',	'97604',	'102094',	'105167',	'106548',	'106593',	'110979',	'113866',	'124790',	'130781',	'133600',	'156557',	'159027',	'180407',	'181468',	'182627',	'197324',	'202229',	'218448',	'218450',	'218458',	'218461',	'218491',	'218502',	'218518',	'218520',	'218526',	'218529',	'218549',	'218562',	'218566',	'218592',	'218724',	'218787',	'218789',	'218793',	'218827',	'218895',	'218961',	'218976',	'219214',	'219394',	'241024',	'244706',	'244881',	'244933',	'245049',	'245072',	'245075',	'245138',	'245140',	'245212',	'245278',	'245301',	'245679',	'245844',	'245916',	'245950',	'246081',	'246337',	'246466',	'246499',	'266860',	'273204',	'281315',	'306283',	'404633',	'447992',	'474059',	'487946',	'487962',	'493003',	'502692',	'594015',	'613606',	'677694',	'726408',	'1042268',	'1052052',	'1055126',	'1058246',	'1074459',	'1651498',	'1725953',	'1941326',	'2142302',	'2169320',	'2339131',	'2663784',	'3003575',	'3132220',	'3161611',	'3431026',	'3540923',	'3746246',	'3755998',	'3756242',	'3756308',	'4571664',	'4729752',	'4803456',	'4814012',	'5475636',	'5489424',	'5735191',	'5884682',	'6335702',	'6336578',	'6574304')
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                        # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408, 2087, 2573, 2574, 2575, 2576):
                        #     if alias_all_bid in bid_list:
                        #         uuids4_update.append(uuid2)
                        #         uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        #         mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                        #     else:
                        #         continue
                    else:
                        uuids4.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]

                cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.8')] = [len(uuids4), len(uuids4_update)]
                self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)



        for i in cc:
            print(i, cc[i])

        # print(len(uuids_delete))
        # print(uuids_delete)

        print('检查是否有新增的pid，如有新增请确认--', pids)

        return True

    def skip_helper_208(self, smonth, emonth, uuids):

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # 找出当期之前所有出过的题
        sql = '''select uuid2 from {tbl}  where (snum, tb_item_id, real_p1,month)
        in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}'  group by snum, tb_item_id, real_p1)
        '''.format(tbl=self.cleaner.get_tbl(), smonth=smonth)
        ret = self.cleaner.db26.query_all(sql)
        # map = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        map = [v[0] for v in ret]

        # 找出当期前出题的最近一条机洗结果
        sql_result_old_uuid = '''

        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2,sales/num vol, c_sp19 FROM {ctbl}
        WHERE pkey  < '{smonth}' and uuid2 in({uuids})
        GROUP BY source, item_id, p1,uuid2,sales,num, c_sp19
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, uuids=','.join(["'" + str(v) + "'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5], v[6]] for v in
                        db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, alias_all_bid,uuid2,sales/num vol, c_sp19 FROM {ctbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0 and uuid2 in ({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth,
                   uuids=','.join(["'" + str(v) + "'" for v in uuids]))

        map_help_new = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5], v[6], v[4]] for v in
                        db.query_all(sql_result_new_uuid)}

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
                    new_uuids.append(str(new_value[3]))
                else:
                    # 之前有出过题，但某些重要机洗值改变，重新出题
                    for old_key, old_value in map_help_old.items():
                        if new_key == old_key and (
                                new_value[2] != old_value[2] or float(new_value[1]) > 1.3 * float(old_value[1]) or float(new_value[1]) < 0.7 * float(old_value[1])):
                            # if new_key == old_key and (
                            #         new_value[2] != old_value[2] or new_value[3] != old_value[3] or new_value[4] !=
                            #         old_value[4] or float(new_value[1]) > 3 * float(old_value[1]) or float(
                            #         new_value[1]) < (1 / 3) * float(old_value[1])):
                            old_uuids_update.append(str(new_value[3]))
                        else:
                            continue

        return new_uuids, old_uuids_update, map_help_new

    def brush(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()


        sales_by_uuid = {}
        cc = {}

        ####### V8需求

        # sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}
        #
        # sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\_\_\_%' and name like '%不建%' '''
        #
        # ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        # pids = ','.join([str(v[0]) for v in ret_bujian])
        #
        # sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        # sp1s = ["'Facial cream（乳液/面霜）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'"]
        #
        # uuids_v8_ttl = []
        #
        #
        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        #     for each_sp1 in sp1s:
        #         for each_source in sources:
        #             uuids4 = []
        #             uuids4_update = []
        #             where = '''
        #             uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source} and alias_all_bid='51297'
        #             '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1, each_source=each_source)
        #             ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.95)
        #             sales_by_uuid.update(sales_by_uuid1)
        #             for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #                 uuids_v8_ttl.append(uuid2)
        #                 if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #                     if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 2087):
        #                         uuids4_update.append(uuid2)
        #                         mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 9, 0]
        #                     else:
        #                         continue
        #                 else:
        #                     uuids4.append(uuid2)
        #                     mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #
        #             self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=8, sales_by_uuid=sales_by_uuid)
        #
        # new_uuids, old_uuids_update, map_help_new = self.skip_helper_208(smonth, emonth, uuids_v8_ttl)
        # self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=8, sales_by_uuid=sales_by_uuid)
        #
        #
        # print('检查是否有新增的pid，如有新增请确认--', pids)

        ####### V11

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}  '.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}



        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sp1s = ['Essence（液态精华）', 'Ampoule（安瓶/原液）']
        uuids_v11_ttl = []

        uuids = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources:
                    for each_bid, each_rate in [[218961, 1], [106593, 1], [44785, 0.9], [53147, 0.9]]:
                        where = '''
                        uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in (\'{each_sp1}\') and  alias_all_bid in ({each_bid})) and {each_source}
                        '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1,
                                   each_source=each_source, each_bid=each_bid)
                        ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=each_rate)
                        sales_by_uuid.update(sales_by_uuid1)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            uuids_v11_ttl.append(uuid2)
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_208(smonth, emonth, uuids_v11_ttl)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid)

        sources_douyin = ['source = 11']
        sp1s = ['Essence（液态精华）', 'Ampoule（安瓶/原液）']
        uuids_v19_ttl = []

        uuids_douyin = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_sp1 in sp1s:
                for each_source in sources_douyin:
                    for each_bid, each_rate in [[218961, 1], [106593, 1], [44785, 0.9], [53147, 0.9]]:
                        where = '''
                                uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1 in (\'{each_sp1}\') and  alias_all_bid in ({each_bid})) and {each_source}
                                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1,
                                           each_source=each_source, each_bid=each_bid)
                        ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=each_rate)
                        sales_by_uuid.update(sales_by_uuid1)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            uuids_v19_ttl.append(uuid2)
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_douyin.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_douyin, clean_flag, visible_check=1, visible=19, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_208(smonth, emonth, uuids_v19_ttl)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=19, sales_by_uuid=sales_by_uuid)


        # ### V3&V4
        #
        # sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}
        #
        # cname, ctbl = self.get_all_tbl()
        # aname, atbl = self.get_a_tbl()
        #
        # sql_bujian = '''SELECT distinct pid FROM product_lib.product_91792 where name like  '%\_\_\_%' and name like '%不建%' '''
        #
        # ret_bujian = self.cleaner.db26.query_all(sql_bujian)
        # pids = ','.join([str(v[0]) for v in ret_bujian])
        #
        #
        #
        # sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        # sp1s_v3 = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'", "'Essence（液态精华）','Ampoule（安瓶/原液）'", "'Set（面部护理套装）'"]
        # sp1s_v4 = ["'Facial cream（乳液/面霜）'", "'Gel/toner（化妆水/爽肤水）'", "'Eye cream（眼霜）'","'Essence（液态精华）','Ampoule（安瓶/原液）'"]
        #
        # uuids_delete = []
        #
        # # V3
        #
        # uuids_v3_ttl = []
        #
        # for each_sp1 in sp1s_v3:
        #     for each_source in sources:
        #         uuids3 = []
        #         uuids3_update = []
        #         where = '''
        #                 uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
        #                 and alias_all_bid not in ('246499',	'3003575',	'245049',	'52501',	'4814012',	'246081',	'194248',	'10744',	'207',	'2663784',	'2663784',	'52238',	'3756242',	'273204',	'14362',	'54358',	'11092',	'244881',	'68367',	'487962',	'487962',	'3756308',	'241024',	'4571664',	'207',	'2204',	'2503',	'3560',	'3681',	'3779',	'4147',	'4589',	'5621',	'5825',	'6404',	'6758',	'6805',	'7012',	'8271',	'10744',	'11092',	'14287',	'14362',	'16129',	'16504',	'20645',	'23253',	'32871',	'47858',	'48199',	'51962',	'52038',	'52120',	'52188',	'52238',	'52297',	'52458',	'52498',	'52501',	'52567',	'52711',	'53191',	'53309',	'53312',	'53946',	'54358',	'56119',	'59203',	'61251',	'68367',	'71334',	'75353',	'78270',	'97604',	'102094',	'105167',	'106548',	'106593',	'110979',	'113866',	'124790',	'130781',	'133600',	'156557',	'159027',	'180407',	'181468',	'182627',	'194248',	'197324',	'202229',	'218448',	'218450',	'218458',	'218461',	'218491',	'218502',	'218518',	'218520',	'218526',	'218529',	'218549',	'218562',	'218566',	'218592',	'218724',	'218787',	'218789',	'218793',	'218827',	'218961',	'218976',	'219214',	'219394',	'241024',	'244706',	'244881',	'244933',	'245049',	'245072',	'245075',	'245138',	'245140',	'245212',	'245278',	'245301',	'245679',	'245844',	'245916',	'245950',	'246081',	'246337',	'246466',	'246499',	'266860',	'273204',	'281315',	'306283',	'404633',	'447992',	'474059',	'487946',	'487962',	'493003',	'502692',	'594015',	'613606',	'677694',	'726408',	'1042268',	'1052052',	'1055126',	'1058246',	'1074459',	'1651498',	'1725953',	'1941326',	'2142302',	'2169320',	'2339131',	'2663784',	'3003575',	'3132220',	'3161611',	'3431026',	'3540923',	'3746246',	'3755998',	'3756242',	'3756308',	'4571664',	'4729752',	'4803456',	'4814012',	'5475636',	'5489424',	'5735191',	'5884682',	'6335702',	'6336578',	'6574304')
        #                 '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
        #         ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.3)
        #         sales_by_uuid.update(sales_by_uuid1)
        #         for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #             uuids_v3_ttl.append(uuid2)
        #             if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #                 # continue
        #                 if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408, 2087, 2573, 2574, 2575, 2576):
        #                     uuids3_update.append(uuid2)
        #                     # uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
        #                     mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #                 else:
        #                     continue
        #             else:
        #                 uuids3.append(uuid2)
        #                 mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #
        #         cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.3')] = [len(uuids3),
        #                                                                                           len(uuids3_update)]
        #         self.cleaner.add_brush(uuids3 + uuids3_update, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)
        #
        # new_uuids, old_uuids_update, map_help_new = self.skip_helper_208(smonth, emonth, uuids_v3_ttl)
        # self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)
        #
        # # sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_delete]))
        # # print(sql)
        # # self.cleaner.db26.execute(sql)
        # # self.cleaner.db26.commit()
        #
        # # V4
        #
        # uuids_v4_ttl = []
        #
        #
        # for each_sp1 in sp1s_v4:
        #     for each_source in sources:
        #         uuids4 = []
        #         uuids4_update = []
        #         where = '''
        #                 uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1 in ({each_sp1}) and c_sp12 = '女' and c_sp13='Mass') and {each_source}
        #                 and alias_all_bid not in ('246499',	'3003575',	'245049',	'52501',	'4814012',	'246081',	'194248',	'10744',	'207',	'2663784',	'2663784',	'52238',	'3756242',	'273204',	'14362',	'54358',	'11092',	'244881',	'68367',	'487962',	'487962',	'3756308',	'241024',	'4571664',	'207',	'2204',	'2503',	'3560',	'3681',	'3779',	'4147',	'4589',	'5621',	'5825',	'6404',	'6758',	'6805',	'7012',	'8271',	'10744',	'11092',	'14287',	'14362',	'16129',	'16504',	'20645',	'23253',	'32871',	'47858',	'48199',	'51962',	'52038',	'52120',	'52188',	'52238',	'52297',	'52458',	'52498',	'52501',	'52567',	'52711',	'53191',	'53309',	'53312',	'53946',	'54358',	'56119',	'59203',	'61251',	'68367',	'71334',	'75353',	'78270',	'97604',	'102094',	'105167',	'106548',	'106593',	'110979',	'113866',	'124790',	'130781',	'133600',	'156557',	'159027',	'180407',	'181468',	'182627',	'194248',	'197324',	'202229',	'218448',	'218450',	'218458',	'218461',	'218491',	'218502',	'218518',	'218520',	'218526',	'218529',	'218549',	'218562',	'218566',	'218592',	'218724',	'218787',	'218789',	'218793',	'218827',	'218961',	'218976',	'219214',	'219394',	'241024',	'244706',	'244881',	'244933',	'245049',	'245072',	'245075',	'245138',	'245140',	'245212',	'245278',	'245301',	'245679',	'245844',	'245916',	'245950',	'246081',	'246337',	'246466',	'246499',	'266860',	'273204',	'281315',	'306283',	'404633',	'447992',	'474059',	'487946',	'487962',	'493003',	'502692',	'594015',	'613606',	'677694',	'726408',	'1042268',	'1052052',	'1055126',	'1058246',	'1074459',	'1651498',	'1725953',	'1941326',	'2142302',	'2169320',	'2339131',	'2663784',	'3003575',	'3132220',	'3161611',	'3431026',	'3540923',	'3746246',	'3755998',	'3756242',	'3756308',	'4571664',	'4729752',	'4803456',	'4814012',	'5475636',	'5489424',	'5735191',	'5884682',	'6335702',	'6336578',	'6574304')
        #                 '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
        #         ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
        #         sales_by_uuid.update(sales_by_uuid1)
        #         for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #             uuids_v4_ttl.append(uuid2)
        #             if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #                 continue
        #                 # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][2] in (3, 408, 2087, 2573, 2574, 2575, 2576):
        #                 #     if alias_all_bid in bid_list:
        #                 #         uuids4_update.append(uuid2)
        #                 #         uuids_delete.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
        #                 #         mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #                 #     else:
        #                 #         continue
        #             else:
        #                 uuids4.append(uuid2)
        #                 mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
        #
        #         cc['{}~{}@{}@{}@{}'.format(smonth, emonth, each_sp1, str(each_source), '0.8')] = [len(uuids4),
        #                                                                                           len(uuids4_update)]
        #         self.cleaner.add_brush(uuids4 + uuids4_update, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)
        #
        # new_uuids, old_uuids_update, map_help_new = self.skip_helper_208(smonth, emonth, uuids_v4_ttl)
        # self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)

        return True




    def hotfix_new(self, tbl, dba, prefix):
        _, atbl = self.get_a_tbl()
        db26 = self.cleaner.get_db('26_apollo')

        self.hotfix_items(tbl, dba, prefix)

        sql = 'SELECT toYYYYMM(pkey) d FROM {} GROUP BY d'.format(tbl)
        ret = dba.query_all(sql)

        trade = '''arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value))))'''
        for d, in ret:
            items = []

            sql = '''
                SELECT item_id, {} p1 FROM {} WHERE (pkey, item_id, trade_props.value) IN (
                    SELECT pkey, item_id, trade_props.value FROM {} WHERE clean_alias_all_bid IN (52238,218970,1421233,3755964,130781) AND toYYYYMM(pkey) = {d}
                )
                GROUP BY item_id, p1 HAVING toYYYYMM(MIN(pkey)) = {d}
            '''.format(trade, atbl, tbl, d=d)
            rrr = dba.query_all(sql)

            for v in rrr:
                items.append('(\'{}\', {})'.format(v[0], v[1]))

            sql = 'SELECT pid FROM product_lib.product_{} WHERE name LIKE \'%不建%\''.format(self.cleaner.eid)
            rrr = db26.query_all(sql)
            pids = ','.join([str(i) for i, in rrr])

            for bid in [218469,219391,106593,218961,246337,883874,218521,3756309,3132220,218550,244645,2548829]:
                sql = '''
                    SELECT item_id, {} p1, sum(sales*sign) s FROM {} WHERE (pkey, item_id, trade_props.value) IN (
                        SELECT pkey, item_id, trade_props.value FROM {} WHERE clean_alias_all_bid = {} AND clean_sku_id IN ({}) AND toYYYYMM(pkey) = {d}
                    )
                    GROUP BY item_id, p1 HAVING toYYYYMM(MIN(pkey)) = {d} ORDER BY s DESC
                '''.format(trade, atbl, tbl, bid, pids, d=d)
                rrr = dba.query_all(sql)

                total = sum([v[-1] for v in rrr]) * 0.8
                for v in rrr:
                    items.append('(\'{}\', {})'.format(v[0], v[1]))
                    total -= v[-1]
                    if total < 0:
                        break

            if len(items) > 0:
                sql = '''
                    ALTER TABLE {} UPDATE `sp疑似新品` = '' WHERE toYYYYMM(pkey) = {}
                '''.format(tbl, d)
                dba.execute(sql)

                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)

                sql = '''
                    ALTER TABLE {} UPDATE `sp疑似新品` = '{}年“{}”新品'
                    WHERE (item_id, {}) IN ({}) AND toYYYYMM(pkey) = {}
                '''.format(tbl, int(d/100), int(d%100), trade, ','.join(items), d)
                dba.execute(sql)

                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)

        # sql = '''
        #     SELECT a.tb_item_id, b.alias_all_bid
        #     FROM product_lib.entity_91783_item a JOIN product_lib.product_91783 b USING (pid)
        #     WHERE a.flag = 2 AND b.alias_all_bid > 0
        # '''
        # ret = db26.query_all(sql)
        # aaa, bbb = ['\'{}\''.format(v[0]) for v in ret], [str(v[1]) for v in ret]

        # sql = '''
        #     ALTER TABLE {} UPDATE
        #         clean_all_bid = transform(item_id,[{a}],[{b}],clean_all_bid),
        #         clean_alias_all_bid = transform(item_id,[{a}],[{b}],clean_alias_all_bid)
        #     WHERE source = 11 AND clean_alias_all_bid = 0
        # '''.format(tbl, a=','.join(aaa), b=','.join(bbb))
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

    def hotfix_items(self, tbl, dba, prefix):
        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202201'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2022,1,1),27295,'3511439169585083210','OLAY抗糖小白瓶精华30ml/75ml 烟酰胺美白减黄',16641919,11,'438737',105174,53147,53147,53147,'',22900,22900,0,'',83791100,3659,83791100,3659,11,827,'Olay/玉兰油','否','否','','','Whitening&Brightening','Essence（液态精华）','女','Anti-Aging','Mass','Mass','1','Whitening&Brightening','否','WHITE RADIANCE ADVANCED 水感透白光曜精华露(抗糖小白瓶)[30ml]','','']
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_alias_all_bid`,`clean_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_pid`,`sp品牌`,`sp是否人工答题`,`sp是否买送`,`sp面膜片数`,`sp面膜系列`,`sp优色林-Funciton`,`sp子品类`,`sp妮维雅女士-是否男女`,`sp妮维雅女士-Funciton`,`sp妮维雅女士-品牌定位`,`sp2022品牌定位`,`spSKU件数`,`sp优色林-功效`,`sp优色林-是否喷雾`,`spSKU名`,`sp疑似新品标签`,`sp疑似新品`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202204'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2022,4,1),27295,'3511439169585083210','OLAY抗糖小白瓶精华30ml/75ml 烟酰胺美白减黄6903148239223ZB',16641919,11,'438737',105174,53147,53147,53147,'',22900,22900,0,'',80172900,3501,80172900,3501,11,827,'Olay/玉兰油','否','否','','','Whitening&Brightening','Essence（液态精华）','女','Anti-Aging','Mass','Mass','1','Whitening&Brightening','否','WHITE RADIANCE ADVANCED 水感透白光曜精华露(抗糖小白瓶)[30ml]','','']
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_alias_all_bid`,`clean_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_pid`,`sp品牌`,`sp是否人工答题`,`sp是否买送`,`sp面膜片数`,`sp面膜系列`,`sp优色林-Funciton`,`sp子品类`,`sp妮维雅女士-是否男女`,`sp妮维雅女士-Funciton`,`sp妮维雅女士-品牌定位`,`sp2022品牌定位`,`spSKU件数`,`sp优色林-功效`,`sp优色林-是否喷雾`,`spSKU名`,`sp疑似新品标签`,`sp疑似新品`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202207'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2022,7,1),27295,'3511439169585083210','OLAY抗糖小白瓶精华30ml/75ml 烟酰胺美白减黄6903148239223ZB',16641919,11,'438737',105174,53147,53147,53147,'',22900,22900,0,'',46349600,2024,46349600,2024,11,827,'Olay/玉兰油','否','否','','','Whitening&Brightening','Essence（液态精华）','女','Anti-Aging','Mass','Mass','1','Whitening&Brightening','否','WHITE RADIANCE ADVANCED 水感透白光曜精华露(抗糖小白瓶)[30ml]','','']
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_alias_all_bid`,`clean_all_bid`,`region_str`,`price`,`org_price`,`promo_price`,`img`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_pid`,`sp品牌`,`sp是否人工答题`,`sp是否买送`,`sp面膜片数`,`sp面膜系列`,`sp优色林-Funciton`,`sp子品类`,`sp妮维雅女士-是否男女`,`sp妮维雅女士-Funciton`,`sp妮维雅女士-品牌定位`,`sp2022品牌定位`,`spSKU件数`,`sp优色林-功效`,`sp优色林-是否喷雾`,`spSKU名`,`sp疑似新品标签`,`sp疑似新品`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)


    # def process_exx(self, tbl, prefix, logId=0):
    #     self.cleaner.update_aliasbid(tbl, self.cleaner.get_db('chsop'))