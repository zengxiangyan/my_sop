import sys
import time
import json
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    # 每月11号跑 p3 scripts/clean_import_brush_new.py -b 60 --process --start_month='2020-04-01' --end_month='2020-05-01'
    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=20000, rate=0.8)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        bids = [137230,484006,5874699,363187,636571,499976,139065,9247,139067,385372,137280,421491,176899,394875,139066,420546,132372,380545,421497,30572]
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=100000, where='alias_all_bid IN ({})'.format(','.join([str(bid) for bid in bids])))
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        print('add new brush {}'.format(len(uuids)))

        return True


    def brush_3(self, smonth, emonth):
        # 当前版本 2020 07 22
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=20000, rate=0.8)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        # bids = [137230,484006,5874699,363187,636571,499976,139065,9247,139067,385372,137280,421491,176899,394875,139066,420546,132372,380545,421497,30572]
        # ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=100000, where='alias_all_bid IN ({})'.format(','.join([str(bid) for bid in bids])))
        # for uuid2, source, tb_item_id, p1, in ret:
        #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #         continue
        #     mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        #     uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        print('add new brush {}'.format(len(uuids)))

        return True


    def brush_4(self, smonth, emonth):
        # 测试用 调查 从2019年10月开始到2020年6月，如果这些店铺截至6月每一条宝贝都要答题，要答多少题，排除人工已经答过的，分每个店铺统计题数
        sid_list = [
            ['jd', '芝华士自营旗舰店', 1000004382],
            ['jd', '马爹利京东自营专卖店', 1000004365],
            ['jd', '绝对伏特加京东自营专卖店', 1000004383],
            ['jd', '杰卡斯葡萄酒京东自营专区', 1000007181],
            ['jd', '保乐力加洋酒京东自营专区', 1000097462],
            ['jd', '百龄坛京东自营官方旗舰店', 1000004357],
            ['tmall', '保乐力加官方旗舰店', 9730213],
            ['tmall', 'martell马爹利旗舰店', 192723120],
            ['tmall', 'martell马爹利官方旗舰店', 192723120],
            ['tmall', 'chivas芝华士官方旗舰店', 181936762],
            ['tmall', 'absolut官方旗舰店', 61276625],
            ['jx', '酒库精品店', 1227],
            ['jx', '华溪国际官方旗舰店', 1134]
        ]

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sales_by_uuid = {}
        uuids = []
        count = []
        for source, shopname, sid in sid_list:
            uuid_tmp = []
            where = '''
            sid = {} and source = \'{}\'
            '''.format(sid, source)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                uuids.append(uuid2)
                uuid_tmp.append(uuid2)
            count.append([sid, source, len(uuid_tmp)])
        for i in count:
            print(i)
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)


    def brush_0915(self, smonth, emonth):
        # 出题规则0043968 20.08.10
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        uuids = []

        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=20000, rate=0.8)
        for uuid2, source, tb_item_id, p1,bid in ret:
            # if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
            #     continue
            # mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            if self.skip_brush(source, tb_item_id, p1,remove=True):
                continue
            uuids.append(uuid2)
        sid_list = [
            ['jd', '芝华士自营旗舰店', 1000004382],
            ['jd', '马爹利京东自营专卖店', 1000004365],
            ['jd', '绝对伏特加京东自营专卖店', 1000004383],
            ['jd', '杰卡斯葡萄酒京东自营专区', 1000007181],
            ['jd', '保乐力加洋酒京东自营专区', 1000097462],
            ['jd', '百龄坛京东自营官方旗舰店', 1000004357],
            ['tmall', '保乐力加官方旗舰店', 9730213],
            ['tmall', 'martell马爹利旗舰店', 192723120],
            ['tmall', 'martell马爹利官方旗舰店', 192723120],
            ['tmall', 'chivas芝华士官方旗舰店', 181936762],
            ['tmall', 'absolut官方旗舰店', 61276625],
            ['jx', '酒库精品店', 1227],
            ['jx', '华溪国际官方旗舰店', 1134]
        ]
        for source, shopname, sid in sid_list:
            uuid_tmp = []
            where = '''
                   sid = {} and source = \'{}\'
                   '''.format(sid, source)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1,bid in ret:
                # if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                #     continue
                # mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                if self.skip_brush(source, tb_item_id, p1,remove=True):
                    continue
                uuids.append(uuid2)
                uuid_tmp.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        return True


    def brush_test(self, smonth, emonth):
        # 测试用 调查  这些id 历史上没出过题的条数
        item_list = self.item_list()

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sales_by_uuid = {}
        uuids = []
        count = []

        for source, tb_item_id in item_list:
            uuid_tmp = []
            where = '''
            tb_item_id = '{}' and source = \'{}\'
            '''.format(tb_item_id, source)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, where=where)
            if len(ret) > 0:
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                    uuids.append(uuid2)
                    uuid_tmp.append(uuid2)
            # count.append([tb, source, len(uuid_tmp)])
        # for i in count:
        #     print(i)
        print('add brush', len(uuids))
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

    def brush_AAAA(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}
        sql_source = '''
        select distinct case when source=1 and (shop_type<20 and shop_type>10) then 'source=1 and (shop_type<20 and shop_type>10)' when source=1 and shop_type>20 then 'source=1 and shop_type>20' else 'source= '||toString(source) end as sources from {atbl} where pkey>='{smonth}' and pkey<'{emonth}'
        and uuid2 not in (select uuid2 from {atbl} where source=1 and (shop_type<20 and shop_type>10))
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        sources = [v[0] for v in db.query_all(sql_source)]

        for each_source in sources:
            uuids = []
            uuids_update = []
            uuids_update_uuid=[]
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1= \'干邑白兰地\' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000, rate=0.8)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, ):
                        uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            cc['{}+{}~{}+{}'.format(1,smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 2, ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()

        sql2 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret2 = self.cleaner.db26.query_all(sql2)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret2}
        lists = [['source = 2', 1000016184, '人头马君度京东自营专区'],
                 ['source = 1 and shop_type>20', 60553350, '人头马官方旗舰店'],
                 ['source = 2', 46762, '人头马官方旗舰店'],
                 ['source = 9', 1597, '人头马 酒仙自营'],
                 ['source = 2', 1000363981, '人头马海外自营旗舰店'],
                 ['source = 11', 8163226, '人头马旗舰店']]
        for each_source, each_sid, each_name in lists:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}') and {each_source} and sid=\'{each_sid}\'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp2:
                    if mpp2['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                        uuids_update.append(mpp2['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp2['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            cc['{}+{}+{}~{}+{}'.format(2, each_sid, smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 2, ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()





        for i in cc:
            print(i, cc[i])
        # exit()

        return True

    # def brush_fuzhu(self, smonth):



    def brush_1019(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        lists = [['source = 2', 1000016184, '人头马君度京东自营专区'],
                 ['source = 1 and shop_type>20', 60553350, '人头马官方旗舰店'],
                 ['source = 2', 46762, '人头马官方旗舰店'],
                 ['source = 9', 1597, '人头马 酒仙自营'],
                 ['source = 2', 1000363981, '人头马海外自营旗舰店'],
                 ['source = 11', 8163226, '人头马旗舰店']]
        for each_source, each_sid, each_name in lists:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
                    uuid2 in (select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}') and {each_source} and sid=\'{each_sid}\'
                    '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            cc['{}+{}+{}~{}+{}'.format(2, each_sid, smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 3, ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()

        return True

    def brush_0923(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}
        sql_source = '''
        select distinct case when source=1 and (shop_type<20 and shop_type>10) then 'source=1 and (shop_type<20 and shop_type>10)' when source=1 and shop_type>20 then 'source=1 and shop_type>20' else 'source= '||toString(source) end as sources from {atbl} where pkey>='{smonth}' and pkey<'{emonth}'
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        # sources = [v[0] for v in db.query_all(sql_source)]
        sources = ['source = 11']

        for each_source in sources:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1= \'干邑白兰地\' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000, rate=0.8)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3 ):
                        uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            cc['{}+{}~{}+{}'.format(1,smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid1)
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 4, ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()

        sql2 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret2 = self.cleaner.db26.query_all(sql2)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret2}
        lists = [['source = 11', 8163226, '人头马旗舰店']]
        for each_source, each_sid, each_name in lists:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}') and {each_source} and sid=\'{each_sid}\'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp2:
                    if mpp2['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,2,3):
                        uuids_update.append(mpp2['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp2['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            cc['{}+{}+{}~{}+{}'.format(2, each_sid, smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid1)
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 4, ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()


        for i in cc:
            print(i, cc[i])


        return True

    def brush_111(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}
        sql_source = '''
        select distinct case when source=1 and (shop_type<20 and shop_type>10) then 'source=1 and (shop_type<20 and shop_type>10)' when source=1 and shop_type>20 then 'source=1 and shop_type>20' else 'source= '||toString(source) end as sources from {atbl} where pkey>='{smonth}' and pkey<'{emonth}'
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        sources = [v[0] for v in db.query_all(sql_source)]

        for each_source in sources:
            uuids = []
            uuids_update = []
            uuids_update_uuid=[]
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1= \'干邑白兰地\' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000, rate=0.8)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, ):
                        uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)
            print(len(uuids_update), len(uuids))
            # cc['{}+{}~{}+{}'.format(1,smonth, emonth, str(each_source))] = [len(uuids_update), len(uuids)]
            cc['{}'.format(str(each_source))] = [len(uuids)]
            self.cleaner.add_brush(uuids+uuids_update_uuid, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print(i, cc[i])
        # exit()

        return True

    def brush_1008(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        self.mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}
        sql_source = '''
        select distinct case when source=1 and (shop_type<20 and shop_type>10) then 'source=1 and (shop_type<20 and shop_type>10)' when source=1 and shop_type>20 then 'source=1 and shop_type>20' else 'source= '||toString(source) end as sources from {atbl} where pkey>='{smonth}' and pkey<'{emonth}'
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        sources = [v[0] for v in db.query_all(sql_source)]



        for each_source in sources:
            uuids = []
            uuids_update = []
            uuids_total = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1= \'干邑白兰地\' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000, rate=0.9)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_total.append(uuid2)

        return True

    def skip_helper_60(self, smonth, emonth, uuids):

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
        select a.*,b.sp23 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2 FROM {atbl}
        WHERE pkey  < '{smonth}'
        GROUP BY source, item_id, p1,uuid2) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where a.uuid2 in({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, uuids=','.join(["'"+str(v)+"'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5]] for v in db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
         select a.*,b.sp23 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, alias_all_bid,uuid2 FROM {atbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0
        AND uuid2 NOT IN (SELECT uuid2 FROM {atbl}  WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sign = -1)
        ) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where a.uuid2 in ({uuids})
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

    ##月度默认规则废弃
    def brush_moren_old(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}

        ###V2部分

        sql_source = '''
        select distinct case when source=1 and (shop_type<20 and shop_type>10) then 'source=1 and (shop_type<20 and shop_type>10)' when source=1 and shop_type>20 then 'source=1 and shop_type>20' else 'source= '||toString(source) end as sources from {atbl} where pkey>='{smonth}' and pkey<'{emonth}'
        and uuid2 not in (select uuid2 from {atbl} where source=1 and (shop_type<20 and shop_type>10))
        '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        sources = [v[0] for v in db.query_all(sql_source)]

        for each_source in sources:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1= \'干邑白兰地\' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000, rate=0.8)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids.append(uuid2)
                # if len(uuids) > 0:
                #     new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1, ):
                        uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids_update_uuid.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)

            new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
            self.cleaner.add_brush(new_uuids + old_uuids_update, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)

        lists = [['source = 2', 1000016184, '人头马君度京东自营专区'],
                 ['source = 1 and shop_type>20', 60553350, '人头马官方旗舰店'],
                 ['source = 2', 46762, '人头马官方旗舰店'],
                 ['source = 9', 1597, '人头马 酒仙自营'],
                 ['source = 2', 1000363981, '人头马海外自营旗舰店'],
                 ['source = 11', 8163226, '人头马旗舰店']]
        for each_source, each_sid, each_name in lists:
            uuids = []
            uuids_update = []
            uuids_update_uuid = []
            where = '''
                            uuid2 in (select uuid2 from {ctbl} where  pkey>='{smonth}' and pkey<'{emonth}') and {each_source} and sid=\'{each_sid}\'
                            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source,
                                       each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids.append(uuid2)
                # if len(uuids) > 0:
                #     new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                        uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        uuids_update_uuid.append(uuid2)
                else:
                    uuids_update_uuid.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    k = '{}-{}'.format(each_source, alias_all_bid)

            new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
            self.cleaner.add_brush(new_uuids + old_uuids_update, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid1)

        uuids = []
        uuids_update = []
        uuids_update_uuid = []
        where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 ='干邑白兰地' and pkey>='{smonth}' and pkey<'{emonth}') and alias_all_bid in (3159319,5433442,363187,1787333,2384184,5433442,372528,4773316,4612208,1829870,4884370)
                and uuid2 not in (select uuid2 from {atbl} where source=1 and (shop_type<20 and shop_type>10) and pkey>='{smonth}' and pkey<'{emonth}')
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, atbl=atbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                    uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    uuids_update_uuid.append(uuid2)
            else:
                uuids_update_uuid.append(uuid2)
                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                k = '{}-{}'.format(each_source, alias_all_bid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
        # print(len(uuids_update), len(uuids))
        self.cleaner.add_brush(new_uuids + old_uuids_update, clean_flag, visible_check=1, visible=2, sales_by_uuid = sales_by_uuid1)

        sales_by_uuid = {}

        ###V41
        uuids_v41 = []
        uuids_update_v41 = []
        where = '''
                alias_all_bid in ('3159319',	'5433442',	'363187',	'530596',	'3621222',	'5433442',	'84360',	'372528',	'3931622',	'2492859',	'370717',	'4884370',	'1911710')
                and uuid2 not in (select uuid2 from {atbl} where source=1 and (shop_type<20 and shop_type>10) and pkey>='{smonth}' and pkey<'{emonth}')
                '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=400000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids_update_v41.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                continue
            else:
                uuids_v41.append(uuid2)
                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp13,each_limit)] = len(uuids)
        self.cleaner.add_brush(uuids_v41, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)

        new_uuids_v41, old_uuids_update_v41, map_help_new = self.skip_helper_60(smonth, emonth, uuids_update_v41)
        self.cleaner.add_brush(old_uuids_update_v41, clean_flag, visible_check=1, visible=4, sales_by_uuid=sales_by_uuid)

        ###V42
        uuids_update_v42 = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in ['source = 11', 'source = 9', 'source=2', 'source=1 and shop_type>20']:
                uuids = []
                where = '''
                (name like '%圣雷米%'
                or name like '%迈夏尔%'
                or name like '%人头马%'
                or name like '%路易十三%'
                or name like '%植物学家%'
                or name like '%君度%'
                or name like '%波夏%'
                or name like '%布赫拉迪%'
                or name like '%泥煤怪兽%'
                or name like '%星图%'
                or name like '%玳慕%'
                or name like '%凯珊%'
                or upper(name) like upper('%ST%REMY%')
                or upper(name) like upper('%METAXA%')
                or upper(name) like upper('%REMY MARTIN%')
                or upper(name) like upper('%LOUIS XIII%')
                or upper(name) like upper('%THE BOTANIST%')
                or upper(name) like upper('%COINTREAU%')
                or upper(name) like upper('%PORT CHARLOTTE%')
                or upper(name) like upper('%BRUICHLADDICH%')
                or upper(name) like upper('%OCTOMORE%')
                or upper(name) like upper('%BLACK ART%')
                or upper(name) like upper('%TELMONT%')
                or upper(name) like upper('%Mount Gay%')
                ) and {each_source}
                '''.format(each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids_update_v42.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp13,each_limit)] = len(uuids)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)

        new_uuids_v42, old_uuids_update_v42, map_help_new = self.skip_helper_60(smonth, emonth, uuids_update_v42)
        self.cleaner.add_brush(old_uuids_update_v42, clean_flag, visible_check=1, visible=5,sales_by_uuid=sales_by_uuid)
        # ##V3部分
        # for each_source in ['source=1 and shop_type>20', 'source =2']:
        #     uuids = []
        #     uuids_update = []
        #     uuids_update_uuid = []
        #     where = '''
        #     uuid2 in (select uuid2 from {ctbl} where sp1 in('调和威士忌','调和麦芽威士忌','单一麦芽威士忌','其他威士忌') and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
        #     '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000, rate=0.8)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         uuids.append(uuid2)
        #         if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
        #             if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
        #                 uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
        #                 uuids_update_uuid.append(uuid2)
        #         else:
        #             uuids_update_uuid.append(uuid2)
        #             mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        #
        #     new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids)
        #     self.cleaner.add_brush(new_uuids + old_uuids_update, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid1)

        # ##刷V3部分属性
        # sql = "update product_lib.entity_90526_item set spid30='单品',spid17='瓶装'  where visible=3 and flag=0"
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit()
        #
        # sql = "update product_lib.entity_90526_item set spid1='调和威士忌'  where visible=3 and flag=0  and spid1='其他威士忌'"
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit()

        return True

    def brush_0711(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}


        sales_by_uuid = {}
        cc = {}

        sources = ['source=1 and shop_type>20', 'source=2']

        uuids6 = []
        for ssmonth, eemonth in [['2021-04-01', '2021-07-01'],
                                 ['2021-07-01', '2021-10-01'],
                                 ['2021-10-01', '2022-01-01']]:
            for each_source in sources:
                for c_name, e_name in [['裸雀', 'Naked malt'], ['三只猴子', 'Monkey shoulder'], ['奥克尼高原骑士', 'Highland Park'], ['尊尼获加绿牌绿方', 'Johnnie walker green label'], ['爱丁顿', 'Eddington']]:
                    uuid_re = []
                    where = '''
                    (name like '%{c_name}%'
                    or upper(name) like upper('%{e_name}%')
                    ) and {each_source}
                    '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl, c_name=c_name, e_name=e_name)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                            continue
                        else:
                            uuids6.append(uuid2)
                            uuid_re.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_source, 'T3')] = len(uuid_re)
        self.cleaner.add_brush(uuids6, clean_flag, visible_check=1, visible=10, sales_by_uuid=sales_by_uuid)

        uuids7 = []
        for ssmonth, eemonth in [['2021-04-01', '2021-07-01'],
                                 ['2021-07-01', '2021-10-01'],
                                 ['2021-10-01', '2022-01-01']]:
            for each_source in sources:
                uuid_re = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 in ('调和威士忌','调和麦芽威士忌','其他威士忌') and pkey>='{ssmonth}' and pkey<'{eemonth}') and {each_source}
                '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids7.append(uuid2)
                        uuid_re.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_source, 'T1')] = len(uuid_re)
        self.cleaner.add_brush(uuids7, clean_flag, visible_check=1, visible=11, sales_by_uuid=sales_by_uuid)

        uuids8 = []
        for ssmonth, eemonth in [['2021-04-01', '2021-07-01'],
                                 ['2021-07-01', '2021-10-01'],
                                 ['2021-10-01', '2022-01-01']]:
            for each_source in sources:
                uuid_re = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 in ('调和威士忌','单一麦芽威士忌','调和麦芽威士忌','其他威士忌') and pkey>='{ssmonth}' and pkey<'{eemonth}') and {each_source}
                '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids8.append(uuid2)
                        uuid_re.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_source, 'T2')] = len(uuid_re)
        self.cleaner.add_brush(uuids8, clean_flag, visible_check=1, visible=12, sales_by_uuid=sales_by_uuid)

        uuids9 = []
        for ssmonth, eemonth in [['2021-04-01', '2021-07-01'],
                                 ['2021-07-01', '2021-10-01'],
                                 ['2021-10-01', '2022-01-01']]:
            for each_source in sources:
                for each_sid in [195852076, 193708894]:
                    uuid_re = []
                    where = '''
                    sid = {each_sid}
                    '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl, each_sid=each_sid)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=100000, rate=0.8)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                            continue
                        else:
                            uuids9.append(uuid2)
                            uuid_re.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_source, 'T4')] = len(uuid_re)
        self.cleaner.add_brush(uuids9, clean_flag, visible_check=1, visible=13,sales_by_uuid=sales_by_uuid)

        # for i in cc:
        #     print(i, cc[i])

        return True



    def brush_1115B(self, smonth, emonth, logId=-1):

        # a = self.skip_helper_60(smonth)
        # print(a)
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        uuids_update =[]
        uuids_update_uuid = []

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}

        where = '''
        uuid2 in (select uuid2 from {ctbl} where sp1 in('调和威士忌','调和麦芽威士忌','单一麦芽威士忌','其他威士忌') and pkey>='{smonth}' and pkey<'{emonth}') and source =2
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000, rate=0.8)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                    uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    uuids_update_uuid.append(uuid2)
            else:
                uuids.append(uuid2)
                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        print(len(uuids_update), len(uuids))
        self.cleaner.add_brush(uuids + uuids_update_uuid, clean_flag, visible_check=1, visible=4,
                               sales_by_uuid=sales_by_uuid1)

        exit()

        return True

    def brush_1116(self, smonth, emonth, logId=-1):

        # a = self.skip_helper_60(smonth)
        # print(a)
        sales_by_uuid = {}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cc = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        uuids = []
        uuids_update =[]
        uuids_update_uuid = []

        sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {} '.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}

        where = '''
        uuid2 in (select uuid2 from {ctbl} where sp1 ='干邑白兰地' and pkey>='{smonth}' and pkey<'{emonth}') and alias_all_bid in (3159319,5433442,363187,1787333,2384184,5433442,372528,4773316,4612208,1829870,4884370)
        and uuid2 not in (select uuid2 from {atbl} where source=1 and (shop_type<20 and shop_type>10) and pkey>='{smonth}' and pkey<'{emonth}')
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, atbl=atbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1000000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                if mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                    uuids_update.append(mpp1['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    uuids_update_uuid.append(uuid2)
            else:
                uuids.append(uuid2)
                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        print(len(uuids_update), len(uuids))
        self.cleaner.add_brush(uuids + uuids_update_uuid, clean_flag, visible_check=1, visible=5,
                               sales_by_uuid=sales_by_uuid1)


        return True

    ##月度默认规则_最新221026
    def brush(self, smonth, emonth, logId=-1):

        sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        clean_flag = self.cleaner.last_clean_flag() + 1
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        sales_by_uuid = {}

        # sql1 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        # ret1 = self.cleaner.db26.query_all(sql1)
        # mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret1}

        sql1 = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret1 = self.cleaner.db26.query_all(sql1)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret1:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp1 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        # sql_v3_flag_0 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {} where visible = 3 and flag = 0'.format(
        #     self.cleaner.get_tbl())
        # ret_v3_flag_0 = self.cleaner.db26.query_all(sql_v3_flag_0)
        # mpp_v3_flag_0 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret_v3_flag_0}

        sql_v3_flag_0 = 'SELECT snum, tb_item_id, p1, id, visible FROM {} where visible = 3 and flag = 0'.format(self.cleaner.get_tbl())
        ret_v3_flag_0 = self.cleaner.db26.query_all(sql_v3_flag_0)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data_v3 = []
        for snum, tb_item_id, p1, id, visible in ret_v3_flag_0:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])
        mpp_v3_flag_0 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data_v3}

        uuids_delete = []

        ###V1部分----人头马项目
        uuids1 = []

        ##### 子品类=干邑白兰地，分平台top80%出题

        sources = ['source = 11', 'source = 9', 'source=2', 'source = 1 and (shop_type > 20 or shop_type < 10 )']

        for each_source in sources:
            uuids_update_uuid = []
            where = '''
            c_sp1= \'干邑白兰地\' and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=10000, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids1.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                    uuids_update_uuid.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                else:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids_update_uuid.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            self.cleaner.add_brush(uuids_update_uuid, clean_flag, visible_check=1, visible=1,
                                   sales_by_uuid=sales_by_uuid1)

        ##### 不限子品类，人头马旗舰店100%宝贝出题

        lists = [['source = 2', 1000016184, '人头马君度京东自营专区'],
                 ['source = 1 and (shop_type > 20 or shop_type < 10 )', 60553350, '人头马官方旗舰店'],
                 ['source = 2', 46762, '人头马官方旗舰店'],
                 ['source = 9', 1597, '人头马 酒仙自营'],
                 ['source = 2', 1000363981, '人头马海外自营旗舰店'],
                 ['source = 11', 8163226, '人头马旗舰店'],
                 ['source = 2', 12052289, '布赫拉迪旗舰店'],
                 ['source = 1 and (shop_type > 20 or shop_type < 10 )', 236141972, '布赫拉迪旗舰店']]
        for each_source, each_sid, each_name in lists:
            uuids_update_uuid = []
            where = '''
            {each_source} and sid=\'{each_sid}\'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source,
                       each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids1.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                    uuids_update_uuid.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                else:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids_update_uuid.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            self.cleaner.add_brush(uuids_update_uuid, clean_flag, visible_check=1, visible=1,
                                   sales_by_uuid=sales_by_uuid1)

        #### 厂商=人头马，B2C平台（不含tb）的100%宝贝出题
        uuids_v41 = []
        where = '''
                alias_all_bid in ('3159319',	'5433442',	'363187',	'530596',	'3621222',	'5433442',	'84360',	'372528',	'3931622',	'2492859',	'370717',	'4884370',	'1911710')
                and uuid2 not in (select uuid2 from {atbl} where source = 1 and (shop_type < 20 and shop_type > 10 )and pkey>='{smonth}' and pkey<'{emonth}')
                '''.format(atbl=atbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=10000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids1.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                uuids_v41.append(uuid2)
                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
            else:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    continue
                else:
                    uuids_v41.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids_v41, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        #### 人头马重点品牌的中英文关键词出100%题
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in ['source = 11', 'source = 9', 'source=2',
                                'source = 1 and (shop_type > 20 or shop_type < 10 )']:
                uuids_v42 = []
                where = '''
                (name like '%圣雷米%'
                or name like '%迈夏尔%'
                or name like '%人头马%'
                or name like '%路易十三%'
                or name like '%植物学家%'
                or name like '%君度%'
                or name like '%波夏%'
                or name like '%布赫拉迪%'
                or name like '%泥煤怪兽%'
                or name like '%星图%'
                or name like '%玳慕%'
                or name like '%凯珊%'
                or upper(name) like upper('%ST%REMY%')
                or upper(name) like upper('%METAXA%')
                or upper(name) like upper('%REMY MARTIN%')
                or upper(name) like upper('%LOUIS XIII%')
                or upper(name) like upper('%THE BOTANIST%')
                or upper(name) like upper('%COINTREAU%')
                or upper(name) like upper('%PORT CHARLOTTE%')
                or upper(name) like upper('%BRUICHLADDICH%')
                or upper(name) like upper('%OCTOMORE%')
                or upper(name) like upper('%BLACK ART%')
                or upper(name) like upper('%TELMONT%')
                or upper(name) like upper('%Mount Gay%')
                ) and {each_source}
                '''.format(each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=10000)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids1.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                        uuids_v42.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    else:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                            continue
                        else:
                            uuids_v42.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                self.cleaner.add_brush(uuids_v42, clean_flag, visible_check=1, visible=1,
                                       sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids1)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=1,
                               sales_by_uuid=sales_by_uuid)

        ###-------------------------------------------------------------------分割线-----------------------------------------------------------------------------
        ###### V2部分---爱丁顿项目
        uuids2 = []

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source=2']

        #### 爱丁顿关注品牌的中英文关键词，平台=tmall、jd，分平台top80%出题
        uuids6 = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in sources:
                for c_name, e_name in [['裸雀', 'Naked malt'], ['三只猴子', 'Monkey shoulder'],
                                       ['奥克尼高原骑士', 'Highland Park'],
                                       ['尊尼获加绿牌绿方', 'Johnnie walker green label'], ['爱丁顿', 'Edrington']]:
                    where = '''
                            (name like '%{c_name}%'
                            or upper(name) like upper('%{e_name}%')
                            ) and {each_source}
                            '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl,
                                       c_name=c_name, e_name=e_name)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100000,
                                                                        rate=0.8)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids2.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                            uuids6.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                                continue
                            else:
                                uuids6.append(uuid2)
                                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids6, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        ##### 子品类 = 调和威士忌、调和麦芽威士忌、其他威士忌，平台 = tmall、jd，分平台top80 % 出题
        uuids7 = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in sources:
                where = '''
                c_sp1 in ('调和威士忌','调和麦芽威士忌','其他威士忌') and {each_source}
                '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl)
                ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100000,
                                                                    rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids2.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                        uuids7.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    else:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                            continue
                        else:
                            uuids7.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids7, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        ##### 子品类 = 调和威士忌、单一麦芽威士忌、调和麦芽威士忌、其他威士忌，平台 = tmall、jd，分平台top80 % 出题
        uuids8 = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in sources:
                where = '''
                c_sp1 in ('调和威士忌','单一麦芽威士忌','调和麦芽威士忌','其他威士忌') and {each_source}
                '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl)
                ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100000,
                                                                    rate=0.8)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuids2.append(uuid2)
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                        uuids8.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    else:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                            continue
                        else:
                            uuids8.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids8, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        #### sid=195852076、193708894(EDRINGTON旗舰店、MACALLAN官方旗舰店），分店铺top80%出题
        uuids9 = []
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_source in sources:
                for each_sid in [195852076, 193708894]:
                    where = '''
                    sid = {each_sid}
                    '''.format(ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, ctbl=ctbl,
                               each_sid=each_sid)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=100000,
                                                                        rate=0.8)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids2.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                            uuids9.append(uuid2)
                            mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                            uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                                continue
                            else:
                                uuids9.append(uuid2)
                                mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids9, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        #### 品类=龙舌兰酒，alias_all_bid=137230(Chivas Regal/芝华士),全平台100%出题
        uuids16 = []
        for each_bid in ['137230']:
            where = '''
            alias_all_bid = '{each_bid}' and uuid2 not in (select uuid2 from {ctbl} where source = 1 and (shop_type < 20 and shop_type > 10 )and pkey>='{smonth}' and pkey<'{emonth}')
            and c_sp1='龙舌兰酒'
            '''.format(each_bid=each_bid, atbl=atbl, smonth=smonth, emonth=emonth, ctbl=ctbl)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=100000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids2.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp_v3_flag_0:
                    uuids16.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                    uuids_delete.append(mpp_v3_flag_0['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                else:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                        continue
                    else:
                        uuids16.append(uuid2)
                        mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids16, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_60(smonth, emonth, uuids2)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=2,
                               sales_by_uuid=sales_by_uuid)

        ###-------------------------------------------------------------------------------分割线-----------------------------------------------------------

        ####（all_bid_sp优先)alias_all_bid=500144、487212(帝亚吉欧 、保乐力加)，子品类=调和威士忌、单一麦芽威士忌、调和麦芽威士忌、其他威士忌，平台=抖音，top80%出题

        uuids_brand = []
        for each_bid in ['500144', '487212']:
            where = '''
                    alias_all_bid = '{each_bid}' and source=11 and c_sp1 in ('调和威士忌','调和麦芽威士忌','其他威士忌', '单一麦芽威士忌') and c_all_bid = 0
                    '''.format(each_bid=each_bid, ctbl=ctbl, smonth=smonth, emonth=emonth)
            ret, sales_by_uuid1 = self.cleaner.process_top_aft200(smonth, emonth, where=where, limit=100000, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp1:
                    continue
                else:
                    uuids_brand.append(uuid2)
                    mpp1['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids_brand, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)



        self.cleaner.set_default_val_special(visible='2,3')

        # self.cleaner.set_default_val(visible='2,3')

        sql_update1 = '''
                        drop table if EXISTS product_lib.entity_90526_item_temp;
                        '''
        print(sql_update1)
        self.cleaner.db26.execute(sql_update1)
        self.cleaner.db26.commit()

        sql_update2 = '''
                create table product_lib.entity_90526_item_temp as select snum,tb_item_id,real_p1 from product_lib.entity_90526_item a
                where  cast(created as date) < cast(now() as date);
                '''
        print(sql_update2)
        self.cleaner.db26.execute(sql_update2)
        self.cleaner.db26.commit()

        sql_update3 = '''
                update product_lib.entity_90526_item set clean_spid1='调和威士忌'  where  cast(created as date) = cast(now() as date) and  flag=0  and clean_spid1='其他威士忌'
                and (snum,tb_item_id,real_p1) not in (select snum,tb_item_id,real_p1 from product_lib.entity_90526_item_temp)
                '''
        print(sql_update3)
        self.cleaner.db26.execute(sql_update3)
        self.cleaner.db26.commit()

        print(uuids_delete)

        return True

    ## 刷新sp1,sp17(包装),sp30(套包类型)
    def brush_fix_sp(self, smonth, emonth, logId=-1):
        sql = "update product_lib.entity_90526_item set spid30='单品',spid17='瓶装'  where visible=3 and flag=0"
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        sql = "update product_lib.entity_90526_item set spid1='调和威士忌'  where visible=3 and flag=0  and spid1='其他威士忌'"
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        exit()

        return True





    def fix_jx_item():
        # SELECT * FROM sop.entity_prod_90526_A epa WHERE `source` = 9 AND pkey >= '2020-09-01' AND pkey < '2020-10-01'
        # AND (item_id, date) IN (SELECT toString(item_id), date FROM mysql('192.168.10.187:13306', 'jx', 'sales_estimate_2020', 'cleanAdmin', '6DiloKlm') WHERE flag = 1);
        pass


    # 拆套包 默认按照number均分 返回 [[rate, id], ...]
    def calc_splitrate(self, item, data):
        total_brand = len(list(set([v['alias_all_bid'] for v in data])))
        total_sku = len(list(set([v['pid'] for v in data])))

        if len(data) > 1:
            for v in data:
                sp6 = v['sp6'].lower().replace('ml', '')
                sp6 = 0 if sp6 == '' else float(sp6)
                sp6 = 0 if sp6  < 50 else sp6
                v['split_rate'] = sp6 * v['number']

        total = sum([v['split_rate'] for v in data])
        for v in data:
            split_rate = 0 if total == 0 else v['split_rate'] / total
            v['split_rate'] = split_rate
            v['sp15'] = 'Multi Cross Brand' if total_brand > 1 else ('Multi Same Brand' if total_sku > 1 else 'Multi Same Sku')
        return data


    def prc_map(sku=None):
        data = {
            'AK-47151朗姆酒700ml':'其他朗姆 Rum Others',
            'AK-47AKBAR伏特加酒700ml':'其他伏特加 VodkaOthers',
            'AK-47伏特加酒40度原味150ml':'其他伏特加 VodkaOthers',
            'AK-47伏特加酒40度原味700ml':'其他伏特加 VodkaOthers',
            'AK-47男人鸡尾酒275ml':'RTD Others',
            'AK-47男人鸡尾酒275ml':'RTD Others',
            'AK-47男人鸡尾酒275ml':'RTD Others',
            'AK-47男人鸡尾酒其他产品':'RTD Others',
            'Chivas Regal/芝华士12年威士忌酒1000ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒200ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒350ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒375ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒4500ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒500ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒50ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒700ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士12年威士忌酒750ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士18年威士忌酒500ml':'芝华士18年 CR18YO',
            'Chivas Regal/芝华士18年威士忌酒700ml':'芝华士18年 CR18YO',
            'Chivas Regal/芝华士25年威士忌酒700ml':'芝华士25年 CR25YO',
            'Chivas Regal/芝华士J&J创始纪念版威士忌酒700ml':'芝华士J&J CRJ&J',
            'Chivas Regal/芝华士XV15年威士忌酒700ml':'芝华士15年 CR15YO',
            'Chivas Regal/芝华士水楢限定版威士忌酒700ml':'芝华士水楢 CR Mizunara',
            'Chivas Regal/芝华士吴亦凡威士忌酒700ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士新境12年威士忌酒700ml':'芝华士12年 CR12YO',
            'Chivas Regal/芝华士耀威士忌酒700ml':'芝华士耀 CR Ultis',
            'Craigellachie/克莱嘉赫斯贝塞1994威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            'four loko冰火鸡尾酒695ml':'RTD Others',
            'four loko粉蝎鸡尾酒473ml':'RTD Others',
            'Four loko紫罐鸡尾酒473ml':'RTD Others',
            'goddess/高迪诗XO白兰地500ml':'NA',
            'Great Wall/长城窖藏白兰地700ml':'NA',
            'Great Wall/长城三星白兰地700ml':'NA',
            'GREY GOOSE/灰雁橙味伏特加酒750ml':'灰雁 Grey Goose',
            'GREY GOOSE/灰雁伏特加酒375ml':'灰雁 Grey Goose',
            'GREY GOOSE/灰雁伏特加酒750ml':'灰雁 Grey Goose',
            'KANERUT/卡尔威金樽XO白兰地700ml':'NA',
            'Lafite/拉菲传世干邑白兰地700ml':'NA',
            'laphroaig2005年单一麦芽威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig24年单一麦芽威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig27年单一麦芽威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig28年单一麦芽威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig30年单一麦芽威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig30年威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig纯麦10年威士忌酒700ml':'拉弗格 Laphroaig',
            'laphroaig精锐威士忌酒700ml':'拉弗格 Laphroaig',
            'LOUIS MARSYNER/路易马西尼XO白兰地375ml':'NA',
            'LOUIS MARSYNER/路易马西尼XO白兰地50ml':'NA',
            'LOUIS MARSYNER/路易马西尼XO白兰地700ml':'NA',
            'LOUIS MARSYNER/路易马西尼金装XO白兰地700ml':'NA',
            'LOUIS MARSYNER/路易马西尼凯旋XO白兰地1000ml':'NA',
            'LOUIS MARSYNER/路易马西尼凯旋XO白兰地700ml':'NA',
            'LOUIS MARSYNER/路易马西尼特醇XO白兰地700ml':'NA',
            'Nikka单一麦芽威士忌宫城峡700ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌余市本尼维斯10年700ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌余市日本武士限量版750ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌余市原桶700ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌竹鹤17年700ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌竹鹤21年700ml':'其他单一麦芽 SinglemaltOthers',
            'Nikka单一麦芽威士忌竹鹤700ml':'其他单一麦芽 SinglemaltOthers',
            'RED MAGIC/红魔伏特加酒270ml':'其他伏特加 VodkaOthers',
            'WELTERBO/威迪宝XO白兰地700ml':'NA',
            '阿卡塔（AKATA）微醺鸡尾酒275ml':'RTD Others',
            '爱丽莎女爵（ELISA）VSOP白兰地700ml':'NA',
            '爱之湾桑格利亚鸡尾酒330ml':'RTD Others',
            '白鹤淡丽纯米清酒1800ml':'NA',
            '白鹤金冠上选清酒1800ml':'NA',
            '白鹤梅子利口酒720ml':'NA',
            '白鹤山田穗清酒720ml':'NA',
            '白鹤特选大吟酿清酒1800ml':'NA',
            '白雪超特选大吟酿万岁纹清酒1800ml':'NA',
            '白雪丹波清酒1800ml':'NA',
            '白雪滴水藏海吟酿清酒300ml':'NA',
            '白洋河VO白兰地500ml':'NA',
            '白洋河三星白兰地700ml':'NA',
            '白州12年威士忌酒700ml':'三得利白州 Hakushu',
            '白州1973威士忌酒700ml':'三得利白州 Hakushu',
            '白州白州18年威士忌酒700ml':'三得利白州 Hakushu',
            '百富12年单桶初装陈酿威士忌酒700ml':'百富 Balvenie',
            '百富12年双桶陈酿威士忌酒700ml':'百富 Balvenie',
            '百富14年加勒比桶陈酿威士忌酒700ml':'百富 Balvenie',
            '百富15年单桶陈酿威士忌酒700ml':'百富 Balvenie',
            '百富17年双桶陈酿威士忌酒700ml':'百富 Balvenie',
            '百富21年波特桶陈酿威士忌酒700ml':'百富 Balvenie',
            '百富25年单桶陈酿单一纯麦苏格兰威士忌700ml':'百富 Balvenie',
            '百富30年陈酿威士忌酒700ml':'百富 Balvenie',
            '百加得151朗姆酒750ml':'百加得 Bacardi',
            '百加得8年朗姆酒1000ml':'百加得 Bacardi',
            '百加得8年朗姆酒750ml':'百加得 Bacardi',
            '百加得白朗姆酒200ml':'百加得 Bacardi',
            '百加得白朗姆酒500ml':'百加得 Bacardi',
            '百加得白朗姆酒50ml':'百加得 Bacardi',
            '百加得白朗姆酒750ml':'百加得 Bacardi',
            '百加得黑朗姆酒500ml':'百加得 Bacardi',
            '百加得黑朗姆酒750ml':'百加得 Bacardi',
            '百加得金朗姆酒750ml':'百加得 Bacardi',
            '百加得铭轩-限量珍藏版朗姆酒700ml':'百加得 Bacardi',
            '百加得橡木心辛香味朗姆酒700ml':'百加得 Bacardi',
            '百加得橡木心辛香味朗姆酒750ml':'百加得 Bacardi',
            '百利咖啡味甜酒700ml':'百利甜酒 Bailey\'s',
            '百利原味甜酒1000ml':'百利甜酒 Bailey\'s',
            '百利原味甜酒200ml':'百利甜酒 Bailey\'s',
            '百利原味甜酒375ml':'百利甜酒 Bailey\'s',
            '百利原味甜酒50ml':'百利甜酒 Bailey\'s',
            '百利原味甜酒750ml':'百利甜酒 Bailey\'s',
            '百龄坛12年威士忌酒500ml':'百龄坛12年 BAL12YO',
            '百龄坛12年威士忌酒50ml':'百龄坛12年 BAL12YO',
            '百龄坛12年威士忌酒700ml':'百龄坛12年 BAL12YO',
            '百龄坛15年威士忌酒700ml':'百龄坛15年 BAL15YO',
            '百龄坛17年威士忌酒500ml':'百龄坛17年 BAL18YO',
            '百龄坛17年威士忌酒700ml':'百龄坛17年 BAL17YO',
            '百龄坛21年威士忌酒700ml':'百龄坛21年 BAL21YO',
            '百龄坛30年威士忌酒700ml':'百龄坛30年 BAL30YO',
            '百龄坛巴西青柠威士忌酒700ml':'百龄坛巴西 BALBrazil',
            '百龄坛特醇威士忌酒1000ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒200ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒350ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒500ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒50ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒700ml':'百龄坛特醇 BALFinest',
            '百龄坛特醇威士忌酒750ml':'百龄坛特醇 BALFinest',
            '贝利尼桃子味果酒750ml':'NA',
            '冰锐鸡尾酒275ml':'冰锐 Breezer',
            '冰锐鸡尾酒330ml':'冰锐 Breezer',
            '布赫拉迪（Bruichladdich）艾雷岛威士忌200ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）艾雷岛威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）波夏艾雷岛威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）波夏威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）波夏擢跃十年威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）经典威士忌200ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）经典威士忌4500ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）经典威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）泥煤怪兽限量版7.1号威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）泥煤怪兽限量版7.3号威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）泥煤怪兽限量版8.1号威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）泥煤怪兽限量版9.1号威士忌700ml':'布赫拉迪 Bruichladdich',
            '布赫拉迪（Bruichladdich）星图限量版威士忌750ml':'布赫拉迪 Bruichladdich',
            '查尔斯二世（CHARLES）XO白兰地700ml':'NA',
            '查尔斯二世（CHARLES）嘉尼特XO白兰地700ml':'NA',
            '查尔斯二世（CHARLES）洛爵万事好VSOP白兰地700ml':'NA',
            '大关大坂屋长兵卫清酒1800ml':'NA',
            '大关大坂屋长兵卫清酒720ml':'NA',
            '大关纪州完熟梅酒700ml':'NA',
            '大关纪州完熟梅酒720ml':'NA',
            '大关金冠清酒1800ml':'NA',
            '大关山田锦清酒1800ml':'NA',
            '大关山田锦清酒720ml':'NA',
            '大关完熟梅酒720ml':'NA',
            '大关辛丹波清酒1800ml':'NA',
            '大关银冠清酒1800ml':'NA',
            '大关柚子梅酒500ml':'NA',
            '帝摩12年威士忌酒700ml':'达摩 Dalmore',
            '帝摩15年威士忌酒700ml':'达摩 Dalmore',
            '帝摩18年威士忌酒700ml':'达摩 Dalmore',
            '帝摩雪茄三桶威士忌酒700ml':'达摩 Dalmore',
            '帝摩亚历山大三世纪念款威士忌酒700ml':'达摩 Dalmore',
            '帝王12年调配苏格兰威士忌酒700ml':'帝王 Dewars',
            '蝶矢宇治茶梅酒720ml':'NA',
            '杜赛托XO干邑白兰地500ml':'NA',
            '铎世白兰地产品':'其他VSOP',
            '法蒂斯圣皇XO白兰地700ml':'NA',
            '梵仕堡（FANSHIBAO）XO白兰地700ml':'NA',
            '弗雷蒙（PHILEMON）XO白兰地700ml':'NA',
            '甘露咖啡力娇酒50ml':'甘露咖啡 Kahlua',
            '甘露咖啡力娇酒700ml':'甘露咖啡 Kahlua',
            '甘露咖啡抹茶风味力娇酒700ml':'甘露咖啡 Kahlua',
            '格兰菲迪12年单一纯麦芽威士忌500ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪12年单一纯麦芽威士忌700ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪15年单一纯麦芽威士忌700ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪18年单一纯麦芽威士忌700ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪21年威士忌酒700ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪23年璀璨珍藏威士忌酒700ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪26年No.7909威士忌酒ml':'格兰菲迪 Glenfiddich',
            '格兰菲迪30年单一纯麦芽威士忌700ml':'格兰菲迪 Glenfiddich',
            '格兰盖瑞（GLENGARIOCH）1979年威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '格兰杰18年威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰1974珍藏威士忌酒1000ml':'格兰杰 Glenmorangie',
            '格兰杰25年威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰波特酒桶窖藏陈酿12年威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰波特酒桶窖藏陈酿14年威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰经典威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰苏玳酒桶窖藏陈酿威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰稀印威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰香料集市威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰杰雪莉酒桶窖藏陈酿12年威士忌酒700ml':'格兰杰 Glenmorangie',
            '格兰天使威士忌700ml':'高端威士忌其他 SP+whiskyOthers',
            '格兰威特12年威士忌酒700ml':'格兰威特 TheGlenlivet',
            '格兰威特15年威士忌酒700ml':'格兰威特 TheGlenlivet',
            '格兰威特18年威士忌酒700ml':'格兰威特 TheGlenlivet',
            '格兰威特创始人甄选威士忌酒700ml':'格兰威特 TheGlenlivet',
            '格兰威特醇萃12年威士忌酒700ml':'格兰威特 TheGlenlivet',
            '格兰威特纳朵拉初桶威士忌酒700ml':'格兰威特 TheGlenlivet',
            '豪帅特醇金标龙舌兰酒750ml':'金快活 Cuervo',
            '豪帅特醇银标龙舌兰酒750ml':'金快活 Cuervo',
            '好天好饮蓝莓味烧酒360ml':'NA',
            '好天好饮蜜桃味烧酒360ml':'NA',
            '好天好饮青葡萄味烧酒360ml':'NA',
            '好天好饮原味烧酒360ml':'NA',
            '黑雾岛烧酒720ml':'NA',
            '红魔鸡尾酒275ml':'RTD Others',
            '红魔微醺鸡尾酒275ml':'RTD Others',
            '花园谷拿破仑XO白兰地700ml':'NA',
            '皇家礼炮21年沙滩马球限量版威士忌700ml':'皇家礼炮21年 RS22YO',
            '皇家礼炮21年威士忌3L':'皇家礼炮21年 RS22YO',
            '皇家礼炮21年威士忌500ml':'皇家礼炮21年 RS21YO',
            '皇家礼炮21年威士忌50ml':'皇家礼炮21年 RS22YO',
            '皇家礼炮21年威士忌700ml':'皇家礼炮21年 RS21YO',
            '皇家礼炮38年威士忌700ml':'皇家礼炮其他 RS Others',
            '皇家礼炮62响威士忌1000ml':'皇家礼炮其他 RS Others',
            '皇家礼炮女王加冕60周年典藏版威士忌700ml':'皇家礼炮其他 RS Others',
            '皇家礼炮钻石之樽威士忌700ml':'皇家礼炮其他 RS Others',
            '灰雁伏特加750ml':'灰雁 Grey Goose',
            '贾斯汀圣利拉郎纪念版XO白兰地700ml':'NA',
            '贾斯汀圣利拉郎金钻XO白兰地700ml':'NA',
            '贾斯汀圣利拉郎金尊XO白兰地700ml':'NA',
            '杰克丹尼150周年纪念版威士忌700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼NO.27金标威士忌酒700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼传承限量版威士忌700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼单桶威士忌酒700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼蜂蜜味力娇酒350ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼蜂蜜味力娇酒700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼红狗沙龙125周年纪念版威士忌700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼火焰杰克力娇酒700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼可乐威士忌味预调酒340ml':'杰克丹尼预调 JackDaniel\'s RTD',
            '杰克丹尼可乐味/柠檬味/苹果味预调酒330ml':'杰克丹尼预调 JackDaniel\'s RTD',
            '杰克丹尼绅士杰克威士忌酒750ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼威士忌1750ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼威士忌375ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼威士忌50ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼威士忌700ml':'杰克丹尼 JackDaniel\'s',
            '杰克丹尼音箱礼盒威士忌700ml':'杰克丹尼 JackDaniel\'s',
            '菊正宗纯米清酒300ml':'NA',
            '菊正宗纯米清酒720ml':'NA',
            '菊正宗嘉宝藏纯米大吟酿清酒1800ml':'NA',
            '菊正宗嘉宝藏纯米大吟酿清酒720ml':'NA',
            '菊正宗庆祝本酿造清酒1800ml':'NA',
            '菊正宗上选本酿造清酒1800ml':'NA',
            '菊正宗上选清酒1800ml':'NA',
            '菊正宗上选清酒720ml':'NA',
            '绝对X冰萃伏特加700ml':'绝对伏特加 Absolut',
            '绝对彩虹限量版700ml':'绝对伏特加 Absolut',
            '绝对覆盆莓味伏特加700ml':'绝对伏特加 Absolut',
            '绝对柑橘味伏特加700ml':'绝对伏特加 Absolut',
            '绝对歌词瓶伏加特酒500ml':'绝对伏特加 Absolut',
            '绝对混调限量版伏加特酒700ml':'绝对伏特加 Absolut',
            '绝对棱境限量版伏加特酒700ml':'绝对伏特加 Absolut',
            '绝对柠檬味伏特加700ml':'绝对伏特加 Absolut',
            '绝对苹果梨味伏特加700ml':'绝对伏特加 Absolut',
            '绝对苹果梨味伏特加750ml':'绝对伏特加 Absolut',
            '绝对青柠味伏特加700ml':'绝对伏特加 Absolut',
            '绝对夜装限量版隐夜装700ml':'绝对伏特加 Absolut',
            '绝对因为爱伏特加700ml':'绝对伏特加 Absolut',
            '绝对原创限量版700ml':'绝对伏特加 Absolut',
            '绝对原味伏加特酒1500ml':'绝对伏特加 Absolut',
            '绝对原味伏特加1000ml':'绝对伏特加 Absolut',
            '绝对原味伏特加200ml':'绝对伏特加 Absolut',
            '绝对原味伏特加350ml':'绝对伏特加 Absolut',
            '绝对原味伏特加500ml':'绝对伏特加 Absolut',
            '绝对原味伏特加50ml':'绝对伏特加 Absolut',
            '绝对原味伏特加700ml':'绝对伏特加 Absolut',
            '绝对原味伏特加750ml':'绝对伏特加 Absolut',
            '绝对重造伏加特酒700ml':'绝对伏特加 Absolut',
            '君度橙味力娇酒1000ml':'君度 Cointreau',
            '君度橙味力娇酒500ml':'君度 Cointreau',
            '君度橙味力娇酒50ml':'君度 Cointreau',
            '君度橙味力娇酒700ml':'君度 Cointreau',
            '君度咖啡力娇酒700ml':'君度 Cointreau',
            '卡慕1973年份干邑白兰地700ml':'卡慕XO',
            '卡慕1975年份干邑白兰地700ml':'卡慕XO',
            '卡慕CUVEE4.160干邑白兰地700ml':'卡慕VSOP',
            '卡慕VSOP醇酿升级版干邑白兰地700ml':'卡慕VSOP',
            '卡慕波特桶干邑白兰地700ml':'卡慕VSOP',
            '卡慕布特妮VSOP干邑白兰地700ml':'卡慕VSOP',
            '卡慕布特妮XO干邑白兰地700ml':'卡慕XO',
            '卡慕瓷书艺术大师收藏美人花园1886干邑白兰地700ml':'卡慕XO',
            '卡慕瓷书艺术大师收藏系列浴后美人1888干邑白兰地700ml':'卡慕XO',
            '卡慕瓷书艺术大师收藏星夜1882干邑白兰地700ml':'卡慕XO',
            '卡慕皇冠GMC干邑白兰地1000ml':'卡慕VSOP',
            '卡慕皇冠GMC干邑白兰地700ml':'卡慕VSOP',
            '卡慕皇冠XO干邑白兰地700ml':'卡慕XO',
            '卡慕家族珍藏布特妮XO干邑白兰地700ml':'卡慕XO',
            '卡慕经典VSOP干邑白兰地1000ml':'卡慕VSOP',
            '卡慕经典VSOP干邑白兰地700ml':'卡慕VSOP',
            '卡慕经典XO干邑白兰地500ml':'卡慕XO',
            '卡慕经典XO干邑白兰地700ml':'卡慕XO',
            '卡慕经典特醇干邑白兰地1750ml':'卡慕XO',
            '卡慕经典特醇干邑白兰地350ml':'卡慕XO',
            '卡慕经典特醇干邑白兰地700ml':'卡慕XO',
            '卡慕雷岛优质XO干邑白兰地700ml':'卡慕XO',
            '卡慕雷岛优质干邑白兰地700ml':'卡慕VSOP',
            '卡慕雷岛优质双桶陈酿干邑白兰地700ml':'卡慕VSOP',
            '卡慕特醇天使之享48.8干邑白兰地700ml':'卡慕XO',
            '卡赛欧八度西柚味力娇酒750ml':'其他力娇 Liqueur Others',
            '卡赛欧莫奈vsop干邑白兰地700ml':'NA',
            '卡赛欧莫奈XO干邑白兰地1000ml':'NA',
            '克维克斯KEVICKS（KEVICKS）XO干邑白兰地700ml':'NA',
            '拉斐VSOP白兰地700ml':'NA',
            '拉弗格纯麦威士忌酒700ml':'拉弗格 Laphroaig',
            '拉图金萨仑根雷斯XO白兰地700ml':'NA',
            '莱昂金标XO白兰地700ml':'NA',
            '蓝天90伏特加酒700ml':'蓝天 Skyy',
            '蓝天菠萝味伏特加酒750ml':'蓝天 Skyy',
            '蓝天柑橘味伏特加酒750ml':'蓝天 Skyy',
            '蓝天莓子口味伏特加酒750ml':'蓝天 Skyy',
            '蓝天柠檬味伏加特酒275ml':'蓝天 Skyy',
            '蓝天乔治亚蜜桃口味伏特加酒750ml':'蓝天 Skyy',
            '蓝天香草口味伏特加酒750ml':'蓝天 Skyy',
            '蓝天星空伏加特酒750ml':'蓝天 Skyy',
            '蓝天樱桃味伏特加酒750ml':'蓝天 Skyy',
            '蓝天原味伏特加酒375ml':'蓝天 Skyy',
            '蓝天原味伏特加酒750ml':'蓝天 Skyy',
            '乐加维林12年威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '乐加维林16年威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '乐加维林37年威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '乐加维林限定款威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '龙力特别纯米清酒720ml':'NA',
            '龙泽清酒750ml':'NA',
            '泸州超体鸡尾酒275ml':'RTD Others',
            '泸州桃花醉青果酒500ml':'NA',
            '泸州桃花醉时尚果酒500ml':'NA',
            '泸州桃花醉仙侠果酒500ml':'NA',
            '罗慕路斯珍藏VSOP白兰地700ml':'NA',
            '马爹利1858远航限量珍藏版特享干邑白兰地700ml':'马爹利凯旋 MartellCreation',
            '马爹利X.O干邑白兰地1000ml':'马爹利XO MartellXO',
            '马爹利X.O干邑白兰地1500ml':'马爹利XO MartellXO',
            '马爹利X.O干邑白兰地3000ml':'马爹利XO MartellXO',
            '马爹利X.O干邑白兰地350ml':'马爹利XO MartellXO',
            '马爹利X.O干邑白兰地50ml':'马爹利XO MartellXO',
            '马爹利X.O干邑白兰地700ml':'马爹利XO MartellXO',
            '马爹利XO300周年纪念版干邑白兰地700ml':'马爹利XO MartellXO',
            '马爹利XO建筑灵感限量版干邑白兰地700ml':'马爹利XO MartellXO',
            '马爹利鼎盛干邑白兰地1000ml':'马爹利鼎盛 MartellDistinction',
            '马爹利鼎盛干邑白兰地500ml':'马爹利鼎盛 MartellDistinction',
            '马爹利鼎盛干邑白兰地700ml':'马爹利鼎盛 MartellDistinction',
            '马爹利高希霸干邑白兰地700ml':'马爹利高希巴 MartellCohiba',
            '马爹利金皇限量版干邑白兰地700ml':'马爹利至尊 MartellL\'Or',
            '马爹利凯旋珍享干邑白兰地700ml':'马爹利凯旋 MartellCreation',
            '马爹利蓝带2018限量版干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带2019限量版干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带300周年限量版干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带傲创干邑白兰地1000ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带傲创干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地1000ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地1500ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地3000ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地350ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地4500ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地500ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带盛颂100年限量版干邑白兰地3000ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利蓝带盛颂100年限量版干邑白兰地700ml':'马爹利蓝带 MartellCordonBleu',
            '马爹利名士法式派对锋潮限量版干邑白兰地700ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地1000ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地1500ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地3000ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地350ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地50ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地6000ml':'马爹利名士 MartellNoblige',
            '马爹利名士干邑白兰地700ml':'马爹利名士 MartellNoblige',
            '马爹利尚选干邑白兰地700ml':'马爹利尚选 MartellChanteloup',
            '马爹利至尊干邑白兰地700ml':'马爹利至尊 MartellL\'Or',
            '马尔堡XO白兰地700ml':'NA',
            '马天尼白威末酒1000ml':'马天尼 Martini',
            '马天尼干威末酒1000ml':'马天尼 Martini',
            '马天尼红威末酒1000ml':'马天尼 Martini',
            '麦卡伦12年黄金三桶威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦12年双雪莉桶威士忌酒700ml':'麦卡伦 Macallan',
            '麦卡伦12年威士忌350ml':'麦卡伦 Macallan',
            '麦卡伦12年威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦12年威士忌酒500ml':'麦卡伦 Macallan',
            '麦卡伦15年黄金三桶威士忌酒700ml':'麦卡伦 Macallan',
            '麦卡伦17年黄金三桶威士忌酒700ml':'麦卡伦 Macallan',
            '麦卡伦1824璀璨威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦1824皓钻威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦1824黑钻威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦1824晖钻威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦1824绿标威士忌1000ml':'麦卡伦 Macallan',
            '麦卡伦1824耀钻威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦1824赭石威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦18年黄金三桶威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦18年威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦25年威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦30年独角兽威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦璀璨黑2018威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦金钻1700威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦精萃2017年限量版威士忌酒700ml':'麦卡伦 Macallan',
            '麦卡伦精萃2019年限量版威士忌酒700ml':'麦卡伦 Macallan',
            '麦卡伦双雪莉桶灿金威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦银钻1700威士忌700ml':'麦卡伦 Macallan',
            '麦卡伦紫钻1700威士忌酒700ml':'麦卡伦 Macallan',
            '曼洛克摩尔25年威士忌酒700ml':'其他单一麦芽 SinglemaltOthers',
            '梅乃宿葛城纯米大吟酿清酒720ml':'NA',
            '梅乃宿果肉梅酒720ml':'NA',
            '梅乃宿果肉桃果酒720ml':'NA',
            '梅乃宿梅酒1800ml':'NA',
            '梅乃宿梅酒300ml':'NA',
            '梅乃宿梅酒720ml':'NA',
            '梅乃宿蜜柑果酒720ml':'NA',
            '梅乃宿苹果果酒720ml':'NA',
            '梅乃宿桃子果酒720ml':'NA',
            '梅乃宿柚子果酒720ml':'NA',
            '美尼尔城堡黑甲骑士XO干邑白兰地700ml':'NA',
            '美尼尔城堡圣利拉郎金钻XO干邑白兰地700ml':'NA',
            '美尼尔城堡图朗穆休斯顿XO干邑白兰地700ml':'NA',
            '孟买蓝宝石金酒750ml':'孟买金酒 Bombay',
            '名仕罗纳德春夏秋冬果酒500ml':'NA',
            '莫罗3号力娇酒500ml':'其他力娇 Liqueur Others',
            '慕拉XO干邑白兰地700ml':'NA',
            '慕拉蓝莓果酒187ml':'NA',
            '慕拉桑葚果酒187ml':'NA',
            '慕拉山楂果酒187ml':'NA',
            '慕拉桃子果酒187ml':'NA',
            '慕拉桃子果酒750ml':'NA',
            '慕拉樱桃果酒187ml':'NA',
            '拿破仑XO干邑白兰地1000ml':'拿破仑XO',
            '拿破仑XO干邑白兰地700ml':'拿破仑XO',
            '拿破仑大帝（NOPALEON）VSOP白兰地700ml':'NA',
            '拿破仑皇廷干邑白兰地1000ml':'拿破仑XO',
            '拿破仑金尊vsop干邑白兰地1000ml':'拿破仑VSOP',
            '拿破仑金尊vsop干邑白兰地700ml':'拿破仑VSOP',
            '拿破仑一号珍藏XO干邑白兰地700ml':'拿破仑XO',
            '拿破仑一世干邑白兰地700ml':'拿破仑VSOP',
            '拿破仑至尊干邑白兰地700ml':'拿破仑XO',
            '欧斯特炫吧力娇酒酒700ml':'NA',
            '帕瑞罗XO白兰地700ml':'NA',
            '派斯顿XO白兰地1000ml':'NA',
            '派斯顿XO白兰地18000ml':'NA',
            '派斯顿XO白兰地32800ml':'NA',
            '派斯顿XO白兰地50ml':'NA',
            '派斯顿百事乐威士忌700ml':'NA',
            '派斯顿方樽XO白兰地700ml':'NA',
            '派斯顿贵族豪门XO白兰地700ml':'NA',
            '派斯顿贵族金皇XO白兰地700ml':'NA',
            '派斯顿金葫芦XO白兰地700ml':'NA',
            '派斯顿金钻XO白兰地1000ml':'NA',
            '派斯顿金钻XO白兰地700ml':'NA',
            '派斯顿金尊XO白兰地700ml':'NA',
            '派斯顿爵士XO白兰地700ml':'NA',
            '派斯顿蓝樽陶瓷瓶XO白兰地1500ml':'NA',
            '派斯顿希尔XO白兰地700ml':'NA',
            '派斯顿银樽XO白兰地700ml':'NA',
            '派斯顿钻石XO白兰地700ml':'NA',
            '彭索酒庄圣地拉郎纪念版XO干邑白兰地700ml':'NA',
            '彭索酒庄圣地拉郎金钻XO干邑白兰地700ml':'NA',
            '彭索酒庄圣地拉郎金尊XO干邑白兰地700ml':'NA',
            '俏雅黑糖梅酒720ml':'NA',
            '俏雅黑糖梅酒750ml':'NA',
            '俏雅梅酒160ml':'NA',
            '俏雅梅酒1800ml':'NA',
            '俏雅梅酒350ml':'NA',
            '俏雅梅酒750ml':'NA',
            '俏雅熟成梅酒720ml':'NA',
            '俏雅宇治茶梅酒720ml':'NA',
            '人头马1898干邑白兰地700ml':'人头马1898 Remy1898',
            '人头马CLUB处女座限量版干邑白兰地350ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地1000ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地1500ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地3000ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地30ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地350ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地500ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地6000ml':'人头马特级 RemyClub',
            '人头马CLUB干邑白兰地700ml':'人头马特级 RemyClub',
            '人头马CLUB巨蟹座限量版干邑白兰地350ml':'人头马特级 RemyClub',
            '人头马CLUB射手座限量版干邑白兰地350ml':'人头马特级 RemyClub',
            '人头马CLUB狮子座限量版干邑白兰地350ml':'人头马特级 RemyClub',
            '人头马CLUB特级炫银限量版干邑白兰地700ml':'人头马特级 RemyClub',
            '人头马CLUB耀黑珍藏版干邑白兰地1000ml':'人头马特级 RemyClub',
            '人头马VSOPl马特·穆尔限量版干邑白兰地700ml':'人头马VSOP RemyVSOP',
            '人头马VSOP蔡依林舞动地心版干邑白兰地700ml':'人头马VSOP RemyVSOP',
            '人头马VSOP蔡依林限量版干邑白兰地1500m':'人头马VSOP RemyVSOP',
            '人头马VSOP蔡依林限量版干邑白兰地3000ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地1000ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地200ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地3000ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地350ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地375ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地50ml':'人头马VSOP RemyVSOP',
            '人头马VSOP干邑白兰地700ml':'人头马VSOP RemyVSOP',
            '人头马VSOP马特·穆尔限量版干邑白兰地375ml':'人头马VSOP RemyVSOP',
            '人头马VSO干邑白兰地1500ml':'人头马VSOP RemyVSOP',
            '人头马XO干邑白兰地1500ml':'人头马XO RemyXO',
            '人头马XO干邑白兰地3000ml':'人头马XO RemyXO',
            '人头马XO干邑白兰地350ml':'人头马XO RemyXO',
            '人头马XO干邑白兰地50ml':'人头马XO RemyXO',
            '人头马XO干邑白兰地700ml':'人头马XO RemyXO',
            '人头马XO黑金珍藏版干邑白兰地350ml':'人头马XO RemyXO',
            '人头马XO戛纳限量版干邑白兰地3000ml':'人头马XO RemyXO',
            '人头马XO戛纳限量版干邑白兰地700ml':'人头马XO RemyXO',
            '人头马XO史蒂芬·理查德匠心典藏版干邑白兰地700ml':'人头马XO RemyXO',
            '人头马XO新年礼盒干邑白兰地700ml':'人头马XO RemyXO',
            '人头马诚印干邑白兰地1000ml':'人头马诚印 RemyCentaure',
            '人头马诚印干邑白兰地1500ml':'人头马诚印 RemyCentaure',
            '人头马诚印干邑白兰地3000ml':'人头马诚印 RemyCentaure',
            '人头马诚印干邑白兰地700ml':'人头马诚印 RemyCentaure',
            '人头马酒窖精选珍藏28号白兰地700ml':'人头马其他 RemyOthers',
            '人头马酒窖特选珍藏梅尔滨白兰地700ml':'人头马其他 RemyOthers',
            '人头马路易十三白兰地700ml':'人头马路易十三 RemyLouisXIII',
            '人头马上海1903盛世珍藏干邑白兰地700ml':'人头马其他 RemyOthers',
            '人头马天醇XO干邑白兰地700ml':'人头马XO RemyXO',
            '人头马天醇XO特优香槟干邑白兰地700ml':'人头马XO RemyXO',
            '人头马文森特.勒鲁瓦限量版干邑白兰地700ml':'人头马其他 RemyOthers',
            '人头马禧钻干邑白兰地700ml':'人头马1898 Remy1898',
            '日本盛大吟酿清酒1800ml':'NA',
            '日本盛上撰清酒1800ml':'NA',
            '日本盛特选本酿清酒1800ml':'NA',
            '锐澳（Riō）本味5度鸡尾酒355ml':'锐澳 Rio',
            '锐澳3.8%vol鸡尾酒275ml':'锐澳 Rio',
            '锐澳LINE FRIENDS鸡尾酒330ml':'锐澳 Rio',
            '锐澳Miss Candy联名款马尔斯绿鸡尾酒275ml':'锐澳 Rio',
            '锐澳本味5度鸡尾酒275ml':'锐澳 Rio',
            '锐澳本榨鸡尾酒330ml':'锐澳 Rio',
            '锐澳春风十里限定瓶鸡尾酒275ml':'锐澳 Rio',
            '锐澳红标佐餐鸡尾酒460ml':'锐澳 Rio',
            '锐澳鸡尾酒330ml':'锐澳 Rio',
            '锐澳金标佐餐鸡尾酒460ml':'锐澳 Rio',
            '锐澳六神特调鸡尾酒275ml':'锐澳 Rio',
            '锐澳强爽8度鸡尾酒330ml':'锐澳 Rio',
            '锐澳强爽8度鸡尾酒355ml':'锐澳 Rio',
            '锐澳强爽8度鸡尾酒500ml':'锐澳 Rio',
            '锐澳秋季限定鸡尾酒330ml':'锐澳 Rio',
            '锐澳微醺鸡尾酒330ml':'锐澳 Rio',
            '锐澳微醺鸡尾酒330ml':'锐澳 Rio',
            '锐澳微醺鸡尾酒355ml':'锐澳 Rio',
            '锐澳微醺鸡尾酒500ml':'锐澳 Rio',
            '锐澳英雄蓝黑色墨水联名礼盒鸡尾酒275ml':'锐澳 Rio',
            '锐澳原果浸渍果酒252ml':'锐澳 Rio',
            '三得利VSOP白兰地700ml':'NA',
            '三得利白州12年单一麦芽威士忌700ml':'三得利白州 Hakushu',
            '三得利白州18年单一麦芽威士忌700ml':'三得利白州 Hakushu',
            '三得利白州1973单一麦芽威士忌700ml':'三得利白州 Hakushu',
            '三得利角瓶威士忌酒1920ml':'三得利角瓶 SuntoryKakubin',
            '三得利角瓶威士忌酒700ml':'三得利角瓶 SuntoryKakubin',
            '三得利老牌威士忌700ml':'高端威士忌其他 SP+whiskyOthers',
            '三得利六精酿金酒700ml':'其他金酒 GinOthers',
            '三得利梅酒2000ml':'NA',
            '三得利梅酒660ml':'NA',
            '三得利梅酒720ml':'NA',
            '三得利梅酒750ml':'NA',
            '三得利旺果烧酒720ml':'NA',
            '三得利响21年花鸟限量版威士忌700ml':'三得利响 Hibiki',
            '三得利响30年威士忌700ml':'三得利响 Hibiki',
            '三得利响大师樱花限量版威士忌700ml':'三得利响 Hibiki',
            '三毛水蜜桃味果酒750ml':'NA',
            '山崎12年威士忌酒700ml':'三得利山崎 Yamazaki',
            '山崎18年威士忌酒700ml':'三得利山崎 Yamazaki',
            '山崎1923威士忌酒700ml':'三得利山崎 Yamazaki',
            '山崎25年威士忌700ml':'三得利山崎 Yamazaki',
            '山崎波本橡木桶威士忌酒700mL':'三得利山崎 Yamazaki',
            '升禧桂花香果酒500ml':'NA',
            '生命之水96度伏特加酒500ml':'其他伏特加 VodkaOthers',
            '圣雷米vsop白兰地700ml':'NA',
            '苏格登18年威士忌酒700ml':'苏格登 Singleton',
            '苏格登25年威士忌700ml':'苏格登 Singleton',
            '苏格登43年时光窖藏威士忌酒700ml':'苏格登 Singleton',
            '苏格登登达夫镇12年威士忌700ml':'苏格登 Singleton',
            '苏格登格兰欧德12年威士忌酒700ml':'苏格登 Singleton',
            '苏格登格兰欧德14年威士忌酒700ml':'苏格登 Singleton',
            '獭祭45清酒1800ml':'NA',
            '獭祭45清酒300ml':'NA',
            '獭祭45清酒720ml':'NA',
            '獭祭50清酒1800ml':'NA',
            '獭祭50清酒300ml':'NA',
            '獭祭50清酒720ml':'NA',
            '獭祭岛耕作清酒720ml':'NA',
            '獭祭二割三分清酒1800ml':'NA',
            '獭祭二割三分清酒300ml':'NA',
            '獭祭二割三分清酒720ml':'NA',
            '獭祭二割三分远心分离清酒1800ml':'NA',
            '獭祭二割三分远心分离清酒720ml':'NA',
            '獭祭磨之先及清酒720ml':'NA',
            '獭祭三割九分清酒1800ml':'NA',
            '獭祭三割九分清酒300ml':'NA',
            '獭祭三割九分清酒720ml':'NA',
            '獭祭三割九分远心分离清酒1800ml':'NA',
            '獭祭三割九分远心分离清酒720ml':'NA',
            '獭祭烧酒720ml':'NA',
            '泰斯卡10年威士忌200ml':'泰斯卡 Talisker',
            '泰斯卡10年威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡18年威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡30年威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡8年威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡北纬57度威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡风暴威士忌700ml':'泰斯卡 Talisker',
            '泰斯卡酒厂限量版威士忌700ml':'泰斯卡 Talisker',
            '天空之月梅子酒720ml':'NA',
            '沃德雷（VODREY XO）XO白兰地700ml':'NA',
            '沃德雷（VODREY XO）昂伯特XO干邑白兰地700ml':'NA',
            '沃德雷（VODREY XO）高地老友记威士忌酒1000ml':'Prestige whisky Others',
            '沃德雷（VODREY XO）雷斯慕XO干邑白兰地700ml':'NA',
            '沃德雷（VODREY XO）玛君VSOP干邑白兰地700ml':'NA',
            '沃德雷（VODREY XO）肖维XO干邑白兰地700ml':'NA',
            '沃德雷（VODREY XO）野牛力娇酒500ml':'NA',
            '沃德雷（VODREY XO）游艇会威士忌酒700ml':'Prestige whisky Others',
            '西夫拉姆特级XO白兰地700ml':'NA',
            '香奈XO白兰地700ml':'NA',
            '响17年威士忌酒700ml':'三得利响 Hibiki',
            '响21年威士忌酒700ml':'三得利响 Hibiki',
            '响和风醇韵威士忌酒700ml':'三得利响 Hibiki',
            '轩尼诗250周年珍藏干邑白兰地1000ml':'轩尼诗百乐廷 HennessyParadis',
            '轩尼诗Marc Newson特别珍藏版XO干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗V.S.O.P干邑白兰地1000ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地1500ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地200ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地3000ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地350ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地500ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地50ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗V.S.O.P干邑白兰地700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP200周年珍藏干邑白兰地700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP2019猪年新春特别版礼盒700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP250周年珍藏干邑白兰地700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第六代珍藏版限量版礼盒700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第七代珍藏版限量版礼盒700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第三代珍藏版限量版礼盒3000ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第四代珍藏版限量版礼盒1500ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第五代珍藏版限量版礼盒1500ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗VSOP第五代珍藏版限量版礼盒700ml':'轩尼诗VSOP HennessyVSOP',
            '轩尼诗XO冰享探索装干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO冰雪世界干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第八代珍藏版干邑白兰地1500ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第八代珍藏版干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第九代珍藏版干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第六代珍藏版干邑白兰地1500ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第六代珍藏版干邑白兰地3000ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第六代珍藏版干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第七代珍藏版干邑白兰地1500ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第七代珍藏版干邑白兰地礼盒700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第十代珍藏版干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO第十一代珍藏版干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地1000ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地1500ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地3000ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地350ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地50ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO加冰限量版干邑白兰地礼盒700ml':'轩尼诗XO HennessyXO',
            '轩尼诗XO猪年新春装干邑白兰地700ml':'轩尼诗XO HennessyXO',
            '轩尼诗百乐廷皇禧干邑白兰地700ml':'轩尼诗百乐廷 HennessyParadis',
            '轩尼诗百乐廷珍享干邑白兰地1500ml':'轩尼诗百乐廷 HennessyParadis',
            '轩尼诗百乐廷珍享干邑白兰地700ml':'轩尼诗百乐廷 HennessyParadis',
            '轩尼诗李察干邑白兰地700ml':'轩尼诗李察 HennessyRichard',
            '轩尼诗新点干邑白兰地1500ml':'轩尼诗新点 HennessyClassivm',
            '轩尼诗新点干邑白兰地200ml':'轩尼诗新点 HennessyClassivm',
            '轩尼诗新点干邑白兰地700ml':'轩尼诗新点 HennessyClassivm',
            '轩尼诗新点猪年新春装干邑白兰地700ml':'轩尼诗新点 HennessyClassivm',
            '轩尼诗詹姆斯干邑白兰地700ml':'轩尼诗VSOP HennessyVSOP',
            '雪姬雪姬梅酒500ml':'NA',
            '野格力娇酒1000ml':'野格 Jagermeister',
            '野格力娇酒1750ml':'野格 Jagermeister',
            '野格力娇酒200ml':'野格 Jagermeister',
            '野格力娇酒20ml':'野格 Jagermeister',
            '野格力娇酒40ml':'野格 Jagermeister',
            '野格力娇酒700ml':'野格 Jagermeister',
            '鹰勇12年威士忌700ml':'高端威士忌其他 SP+whiskyOthers',
            '誉宾老山楂果酒500ml':'NA',
            '月桂冠纯粹清酒1800ml':'NA',
            '月桂冠纯米大吟酿清酒720ml':'NA',
            '月桂冠纯米清酒300ml':'NA',
            '月桂冠大吟酿清酒720ml':'NA',
            '月桂冠清酒2000ml':'NA',
            '月桂冠清爽清酒1800ml':'NA',
            '月桂冠上选辛口清酒1800ml':'NA',
            '月桂冠特别本酿造清酒720ml':'NA',
            '张裕12年白兰地700ml':'NA',
            '张裕VO金奖白兰地750ml':'NA',
            '张裕金奖白兰地100ml':'NA',
            '张裕金奖白兰地200ml':'NA',
            '张裕金奖白兰地700ml':'NA',
            '张裕金奖酝酿芬芳白兰地700ml':'NA',
            '张裕可雅桶藏10年XO白兰地700ml':'NA',
            '张裕可雅桶藏15年XO白兰地650ml':'NA',
            '张裕可雅桶藏6年vsop白兰地700ml':'NA',
            '张裕老桶白兰地700ml':'NA',
            '张裕迷霓白兰地188ml':'NA',
            '张裕其它酒类':'NA',
            '张裕三星金奖白兰地700ml':'NA',
            '张裕四星金奖白兰地700ml':'NA',
            '张裕五星金奖白兰地500ml':'NA',
            '张裕五星金奖白兰地700ml':'NA',
            '张裕五星珍藏版金奖白兰地700ml':'NA',
            '张裕星愿365加气苹果酒750ml':'NA',
            '长白山蓝莓果酒500ml':'NA',
            '真露李子味烧酒360ml':'NA',
            '真露梅花秀果酒300ml':'NA',
            '真露青葡萄烧酒360ml':'NA',
            '真露西柚烧酒360ml':'NA',
            '真露竹炭烧酒360ml':'NA',
            '知多威士忌700ml':'NA',
            '植物学家其他洋酒产品':'植物学家 TheBotanistGin',
            '醉鹅娘狮子歌歌果酒375ml':'NA',
            '尊尼获加15年雪莉版威士忌700ml':'尊尼获加其他 JWOthers',
            '尊尼获加Blenders\'Batch威士忌700ml':'尊尼获加其他 JWOthers',
            '尊尼获加XR21年苏格兰威士忌750ml':'尊尼获加XR21年 JWXR21',
            '尊尼获加铂金18年威士忌酒750ml':'尊尼获加铂金18年 JWPlatinum',
            '尊尼获加东风西蕴威士忌750ml':'兰方 JWBlue',
            '尊尼获加狗年限量版威士忌750ml':'兰方 JWBlue',
            '尊尼获加黑牌城市限量版苏格兰威士忌700ml':'黑方 JWBlack',
            '尊尼获加黑牌醇黑威士忌700ml':'尊尼获加醇黑/劲黑 Johnnie Walker Double Black',
            '尊尼获加黑牌定制加油瓶威士忌酒700ml':'黑方 JWBlack',
            '尊尼获加黑牌定制瓶Your Way威士忌酒700ml':'黑方 JWBlack',
            '尊尼获加黑牌火焰限量版威士忌700ml':'黑方 JWBlack',
            '尊尼获加黑牌尚雯婕明星定制限量版威士忌700ml':'黑方 JWBlack',
            '尊尼获加黑牌调配型苏格兰威士忌700ml':'黑方 JWBlack',
            '尊尼获加黑牌威士忌200ml':'黑方 JWBlack',
            '尊尼获加黑牌威士忌375ml':'黑方 JWBlack',
            '尊尼获加黑牌威士忌500ml':'黑方 JWBlack',
            '尊尼获加黑牌威士忌50ml':'黑方 JWBlack',
            '尊尼获加黑牌威士忌750ml':'黑方 JWBlack',
            '尊尼获加红牌威士忌200ml':'红方 JWRed',
            '尊尼获加红牌威士忌50ml':'红方 JWRed',
            '尊尼获加红牌威士忌700ml':'红方 JWRed',
            '尊尼获加红牌威士忌750ml':'红方 JWRed',
            '尊尼获加金牌威士忌酒750ml':'金方 JWGold',
            '尊尼获加蓝牌12星座限量款威士忌200ml':'兰方 JWBlue',
            '尊尼获加蓝牌布朗拉限量版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌创始纪念版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌福禄寿限量版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌古都四季威士忌酒200ml':'兰方 JWBlue',
            '尊尼获加蓝牌婚礼定制瓶威士忌酒200ml':'兰方 JWBlue',
            '尊尼获加蓝牌金猪聚财特别版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌三羊开泰威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌十二生肖限量版威士忌200ml':'兰方 JWBlue',
            '尊尼获加蓝牌丝路限量版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌四大发明威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌祥瑞麒麟特别版威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌消逝的酒厂威士忌750ml':'兰方 JWBlue',
            '尊尼获加蓝牌艺术家限量版威士忌200ml':'兰方 JWBlue',
            '尊尼获加蓝牌鱼跃龙门限量版威士忌750ml':'兰方 JWBlue',
            '尊尼获加绿牌15年威士忌750ml':'绿方 JWGreen',
            '尊尼获加绿牌威士忌200ml':'绿方 JWGreen',
            '尊尼获加权力的游戏威士忌700ml':'黑方 JWBlack',
            '尊尼获加天马行空版威士忌酒750ml':'尊尼获加其他 JWOthers',
            '尊尼获加调配大师限量系列-醇香新酿威士忌酒700ml':'尊尼获加其他 JWOthers',
            '尊尼获加调配大师限量系列-甘熏新酿威士忌500ml':'尊尼获加其他 JWOthers',
            '尊尼获加英皇乔治五世威士忌750ml':'尊尼获加其他 JWOthers',
            '尊尼获加尊酩威士忌700ml':'尊尼获加其他 JWOthers'
        }
        if sku is None:
            return data
        return data[sku] if sku in data else ''


    def type_map(source, sid):
        data = [
            ['jd', 1000097462, 'JD SELF RUN', '保乐力加洋酒京东自营专区'],
            ['jd', 0, 'JD SELF RUN', '京东自营'],
            ['jd', 75371, 'JD PRC POP', '宝树行官方旗舰店'],
            ['jd', 1000016184, 'JD SELF RUN', '人头马君度京东自营专区'],
            ['jd', 1000082661, 'JD SELF RUN', '百加得京东自营旗舰店'],
            ['jd', 1000003530, 'JD SELF RUN', '杰克丹尼京东自营旗舰店'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京东自营旗舰店'],
            ['jd', 1000006863, 'JD SELF RUN', '帝亚吉欧京东自营专区'],
            ['jd', 1000074742, 'JD SELF RUN', '格兰父子洋酒京东自营专区'],
            ['jd', 1000101704, 'JD SELF RUN', '日韩小美酒京东自营专区'],
            ['jd', 46762, 'JD OTHER FSS', '人头马旗舰店'],
            ['jd', 110303, 'JD OTHER POP', '加枫红酒类官方旗舰店'],
            ['jd', 1000097502, 'JD SELF RUN', '好来喜洋酒京东自营专区'],
            ['jd', 1000114166, 'JD SELF RUN', '尊尼获加自营旗舰店'],
            ['jd', 663138, 'JD PRC POP', '宝树行洋酒旗舰店'],
            ['jd', 628046, 'JD PRC POP', '高华仕酒类专营店'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗重新发现中国味'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营店'],
            ['jd', 140747, 'JD OTHER POP', '智恒达商食品专营店'],
            ['jd', 173122, 'JD OTHER POP', '优红酒类官方旗舰店'],
            ['jd', 188438, 'JD OTHER FSS', '酩悦轩尼诗官方旗舰店'],
            ['jd', 208868, 'JD OTHER POP', '酒仙网红酒官方旗舰店'],
            ['jd', 1000102032, 'JD SELF RUN', '菊正宗京东自营旗舰店'],
            ['jd', 1000100862, 'JD SELF RUN', '汇泉洋酒京东自营专区'],
            ['jd', 1000102030, 'JD SELF RUN', '梅乃宿京东自营旗舰店'],
            ['jd', 598847, 'JD OTHER POP', '山姆会员商店官方旗舰店'],
            ['jd', 77552, 'JD PRC POP', '御玖轩酒类专营店'],
            ['jd', 1000097501, 'JD SELF RUN', '琥珀洋酒京东自营专区'],
            ['jd', 196822, 'JD OTHER POP', '智恒达商冲调专营店'],
            ['jd', 628019, 'JD OTHER POP', '酒号魔方官方旗舰店'],
            ['jd', 745347, 'JD OTHER POP', '海荟码头酒类旗舰店'],
            ['jd', 'not found', 'JD OTHER POP', '伊樽酒水专营店'],
            ['jd', 721466, 'JD OTHER FSS', '帝亚吉欧洋酒官方旗舰店'],
            ['jd', 151121, 'JD OTHER POP', '梦扬酒类专营店'],
            ['jd', 79797, 'JD OTHER POP', '江秀旗舰店'],
            ['jd', 630047, 'JD PRC POP', '郎家园官方旗舰店'],
            ['jd', 621222, 'JD OTHER POP', '侠风官方旗舰店'],
            ['jd', 668773, 'JD PRC POP', '酒牧官方旗舰店'],
            ['jd', 81840, 'JD PRC POP', '宏放酒业专营店'],
            ['jd', 883600, 'JD PRC FSS', '保乐力加官方旗舰店'],
            ['jd', 1000130883, 'JD SELF RUN', '单一麦芽威士忌京东自营专区'],
            ['jd', 857001, 'JD PRC POP', '奢蕾汇官方旗舰店'],
            ['jd', 1000121041, 'JD SELF RUN', '铂珑（BOUTILLIER）洋酒京东自营专区'],
            ['jd', 10596, 'JD OTHER POP', 'BeWine酒类旗舰店'],
            ['jd', 784130, 'JD OTHER FSS', 'Camus官方旗舰店'],
            ['jd', 1000125574, 'JD SELF RUN', 'CAMUS卡慕京东自营官方旗舰店'],
            ['jd', 1000102998, 'JD SELF RUN', 'fourloko京东自营旗舰店'],
            ['jd', 739078, 'JD OTHER FSS', 'fourloko洋酒旗舰店'],
            ['jd', 644990, 'JD OTHER FSS', 'Lautergold酒类 官方旗舰店'],
            ['jd', 69687, 'JD OTHER FSS', 'rio锐澳旗舰店'],
            ['jd', 1000139741, 'JD OTHER POP', 'TOPWINE'],
            ['jd', 54945, 'JD OTHER FSS', 'VC红酒官方旗舰店'],
            ['jd', 813144, 'JD OTHER POP', '艾芙罗蒂酒类专营店'],
            ['jd', 666331, 'JD OTHER POP', '艾森特酒类专营店'],
            ['jd', 796667, 'JD OTHER POP', '爱尔兰馆'],
            ['jd', 1000081125, 'JD SELF RUN', '爱之湾葡萄酒京东自营旗舰店'],
            ['jd', 1000124421, 'JD SELF RUN', '安努克（AnCnoc）京东自营专区'],
            ['jd', 141967, 'JD OTHER POP', '安 酌酒类专营店'],
            ['jd', 1000091165, 'JD SELF RUN', '澳洲葡萄酒京东自营专区'],
            ['jd', 839480, 'JD OTHER FSS', '百富门洋酒旗舰店'],
            ['jd', 719592, 'JD OTHER FSS', '百加得官方旗舰店'],
            ['jd', 1000004357, 'JD SELF RUN', '百龄坛京东自营官方旗舰店'],
            ['jd', 652423, 'JD OTHER POP', '奔弛酒类专营店'],
            ['jd', 206443, 'JD OTHER POP', '奔富红酒品尚专卖店'],
            ['jd', 1000085511, 'JD SELF RUN', '卞氏米酒京东自营旗舰店'],
            ['jd', 1000086792, 'JD SELF RUN', '冰青青梅酒自营旗舰店'],
            ['jd', 730875, 'JD OTHER POP', '布多格酒类旗舰店'],
            ['jd', 166256, 'JD OTHER POP', '畅健酒类专营店'],
            ['jd', 101847, 'JD OTHER POP', '澄意生活旗舰店'],
            ['jd', 109668, 'JD PRC POP', '池陈辉酒类专营店'],
            ['jd', 667023, 'JD OTHER POP', '础石酒类专营店'],
            ['jd', 697745, 'JD OTHER POP', '春融食品专营店'],
            ['jd', 1000090402, 'JD SELF RUN', '大关清酒京东自营专区'],
            ['jd', 177770, 'JD OTHER POP', '德万利酒类官方旗舰店'],
            ['jd', 138519, 'JD OTHER POP', '登辉酒类专营店'],
            ['jd', 708739, 'JD OTHER POP', '迪欧葡萄酒专营店'],
            ['jd', 818570, 'JD OTHER POP', '蒂优合酒类官方旗舰店'],
            ['jd', 29380, 'JD OTHER POP', '东方聚仙酒类专营店'],
            ['jd', 198485, 'JD OTHER POP', '发九国际酒类专营店'],
            ['jd', 1000085804, 'JD SELF RUN', '法国红酒京东自营专区'],
            ['jd', 1000090564, 'JD SELF RUN', '翡马葡萄酒京东自营旗舰店'],
            ['jd', 652268, 'JD OTHER POP', '枫羿酒类专营店'],
            ['jd', 54739, 'JD OTHER POP', '福乐盟官方旗舰店'],
            ['jd', 1000086342, 'JD SELF RUN', '富邑京东自营专区'],
            ['jd', 659245, 'JD OTHER FSS', '富邑葡萄酒集团旗舰店'],
            ['jd', 722661, 'JD OTHER POP', '噶瑪蘭威士忌官方旗舰店'],
            ['jd', 1000112681, 'JD SELF RUN', '干露酒厂京东 自营旗舰店'],
            ['jd', 114398, 'JD OTHER POP', '给力酒类专营店'],
            ['jd', 197583, 'JD OTHER POP', '购酒网官方旗舰店'],
            ['jd', 785856, 'JD OTHER POP', '好来喜酒类 专营店'],
            ['jd', 1000102041, 'JD SELF RUN', '好天好饮京东自营旗舰店'],
            ['jd', 52340, 'JD OTHER POP', '和道酒坊官方旗舰店'],
            ['jd', 893241, 'JD OTHER POP', '红 冠雄鸡旗舰店'],
            ['jd', 786458, 'JD OTHER POP', '红魔鬼官方旗舰店'],
            ['jd', 602015, 'JD OTHER POP', '浣熙酒类专营店'],
            ['jd', 821932, 'JD OTHER FSS', '黄尾袋鼠 葡萄酒官方旗舰店'],
            ['jd', 1000080704, 'JD SELF RUN', '黄尾袋鼠葡萄酒京东自营旗舰店'],
            ['jd', 1000095543, 'JD SELF RUN', '加州乐事葡萄酒京东自营旗舰店'],
            ['jd', 757577, 'JD OTHER POP', '嘉食和食品旗舰店'],
            ['jd', 789177, 'JD OTHER POP', '贾斯汀旗舰店'],
            ['jd', 891054, 'JD OTHER POP', '建清（建昌）酒类专营店'],
            ['jd', 780857, 'JD OTHER POP', '建清葡萄酒专营店'],
            ['jd', 1000007181, 'JD SELF RUN', '杰卡斯葡萄酒京东自营专区'],
            ['jd', 678436, 'JD OTHER POP', '金采龙凤官方旗舰店'],
            ['jd', 667985, 'JD OTHER POP', '金都恒泰官方旗舰店'],
            ['jd', 27999, 'JD OTHER POP', '金都恒泰酒类旗舰店'],
            ['jd', 180040, 'JD OTHER POP', '金豪酒类专营店'],
            ['jd', 664135, 'JD OTHER POP', '金豪葡萄酒专营店'],
            ['jd', 1000076153, 'JD SELF RUN', '京东1小时达'],
            ['jd', 1000097094, 'JD SELF RUN', '京东海外直采官方自营店'],
            ['jd', 1000096602, 'JD OTHER POP', '京东京造官方旗舰店'],
            ['jd', 671105, 'JD OTHER POP', '九珑因特酒类专营店'],
            ['jd', 139272, 'JD OTHER POP', '久赞四海 官方旗舰店'],
            ['jd', 85561, 'JD OTHER POP', '久之盈酒类专营店'],
            ['jd', 158631, 'JD OTHER POP', '酒多多酒类专营店'],
            ['jd', 901096, 'JD OTHER POP', '酒多多酒水专营店'],
            ['jd', 623605, 'JD OTHER POP', '酒富盛酩官方旗舰店'],
            ['jd', 156927, 'JD OTHER POP', '酒立有旗舰店'],
            ['jd', 196146, 'JD OTHER POP', '酒廷1990旗舰店'],
            ['jd', 653268, 'JD OTHER POP', '酒玩家酒类旗舰店'],
            ['jd', 601324, 'JD OTHER POP', '酒玩家旗舰店'],
            ['jd', 14796, 'JD PRC POP', '酒仙网官方旗舰店'],
            ['jd', 31348, 'JD PRC POP', '酒仙网世界名酒旗舰店'],
            ['jd', 781742, 'JD PRC POP', '酒宿旗舰店'],
            ['jd', 623621, 'JD OTHER POP', '酒庄直通官方旗舰店'],
            ['jd', 691398, 'JD OTHER POP', '酒庄直通精品酒旗舰店'],
            ['jd', 784773, 'JD OTHER POP', '酒庄直通洋酒旗舰店'],
            ['jd', 1000004383, 'JD SELF RUN', '绝对伏特加京东自营专卖店'],
            ['jd', 149421, 'JD OTHER POP', '俊阁酒类专营店'],
            ['jd', 202038, 'JD OTHER POP', '卡巴酒类专营店'],
            ['jd', 202038, 'JD OTHER POP', '卡巴商贸酒类专营店'],
            ['jd', 1000128562, 'JD SELF RUN', '卡伯纳葡萄酒京东自营旗舰店'],
            ['jd', 28244, 'JD OTHER POP', '开为进口食品专营店'],
            ['jd', 714754, 'JD OTHER POP', '跨理洋酒专营店'],
            ['jd', 64501, 'JD OTHER POP', '拉菲红酒VC专卖店'],
            ['jd', 1000084051, 'JD SELF RUN', '拉菲葡萄酒京东自营专卖店'],
            ['jd', 610735, 'JD OTHER POP', '拉斐红酒 官方旗舰店'],
            ['jd', 28717, 'JD PRC POP', '郎家园酒类旗舰店'],
            ['jd', 28717, 'JD PRC POP', '郎家园酒类专营店'],
            ['jd', 787326, 'JD OTHER POP', '雷诺官方旗舰店'],
            ['jd', 762801, 'JD OTHER POP', '雷司酒类官方旗舰店'],
            ['jd', 761565, 'JD OTHER POP', '领亿来旗舰店'],
            ['jd', 149158, 'JD OTHER POP', '泸州老窖官方旗舰店'],
            ['jd', 889770, 'JD OTHER POP', '路易卡丹旗舰店'],
            ['jd', 1000102749, 'JD SELF RUN', '路易老爷京东自营旗舰店'],
            ['jd', 1000101425, 'JD SELF RUN', '罗莎庄园葡萄 酒京东自营旗舰店'],
            ['jd', 1000004365, 'JD SELF RUN', '马爹利京东自营专卖店'],
            ['jd', 606881, 'JD OTHER POP', '玛吉欧酒类专营店'],
            ['jd', 686018, 'JD OTHER POP', '麦吉欧克酒类专营店'],
            ['jd', 109668, 'JD OTHER POP', '麦摇吧酒类专营店'],
            ['jd', 1000097097, 'JD SELF RUN', '曼戈洋酒京东自营旗舰店'],
            ['jd', 136746, 'JD OTHER POP', '曼拉维酒类旗舰店'],
            ['jd', 672469, 'JD OTHER POP', '猫呗旗舰店'],
            ['jd', 711165, 'JD OTHER POP', '每日壹品葡萄酒专营店'],
            ['jd', 708953, 'JD OTHER POP', '美酒汇酒类专营店'],
            ['jd', 785821, 'JD OTHER POP', '美尼尔城堡旗舰店'],
            ['jd', 723940, 'JD OTHER POP', '蒙特斯官方旗舰店'],
            ['jd', 1000083361, 'JD SELF RUN', '蒙特斯葡萄酒京东自营旗舰店'],
            ['jd', 1000088001, 'JD SELF RUN', '米客米酒京东自营旗舰店'],
            ['jd', 653667, 'JD OTHER POP', '米赞国际中外名酒专营店'],
            ['jd', 12961, 'JD OTHER POP', '民酒网官方旗舰店'],
            ['jd', 672082, 'JD OTHER POP', '民酒网红酒旗舰店'],
            ['jd', 51819, 'JD OTHER FSS', '莫高官方旗舰店'],
            ['jd', 1000089564, 'JD SELF RUN', '莫高葡萄酒京东自营旗舰店'],
            ['jd', 804272, 'JD OTHER POP', '慕拉酒类旗舰店'],
            ['jd', 743540, 'JD OTHER POP', '南非国家酒馆'],
            ['jd', 712497, 'JD OTHER POP', '偶醉酒类专营店'],
            ['jd', 48459, 'JD OTHER POP', '派斯顿酒类专卖店'],
            ['jd', 896815, 'JD OTHER POP', '派斯顿旗舰店'],
            ['jd', 847606, 'JD OTHER POP', '派斯顿乔森专卖店'],
            ['jd', 942341, 'JD SELF RUN', '派斯顿洋酒京东自营旗舰店'],
            ['jd', 828450, 'JD OTHER POP', '彭索酒庄旗舰店'],
            ['jd', 850893, 'JD OTHER POP', '品鉴洋酒旗舰店'],
            ['jd', 693096, 'JD OTHER POP', '品茗酒类专营店'],
            ['jd', 51360, 'JD OTHER POP', '品牌贸易直营店'],
            ['jd', 35773, 'JD OTHER POP', '品尚汇旗舰店'],
            ['jd', 622463, 'JD OTHER POP', '品尚优选官方旗舰店'],
            ['jd', 618618, 'JD OTHER POP', '品正源清酒类旗舰店'],
            ['jd', 584089, 'JD OTHER POP', '榀酒酒类专营店'],
            ['jd', 1000086792, 'JD SELF RUN', '圃田冰青青梅酒京东自营旗舰店'],
            ['jd', 814913, 'JD OTHER POP', '七酒酒类专营店'],
            ['jd', 652491, 'JD OTHER POP', '千叶酒类专营店'],
            ['jd', 135933, 'JD OTHER POP', '谦法酒类专营店'],
            ['jd', 629185, 'JD OTHER POP', '谦法中外名酒专营店'],
            ['jd', 690222, 'JD OTHER POP', '乔森酒类专营店'],
            ['jd', 627258, 'JD OTHER POP', '青出于蓝酒类专营店'],
            ['jd', 729877, 'JD OTHER POP', '青出于蓝中外名酒专营店'],
            ['jd', 662066, 'JD OTHER POP', '融众恒泰酒类专营店'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京���自营官方旗舰店'],
            ['jd', 784180, 'JD OTHER POP', '锐达酒类专营店'],
            ['jd', 690779, 'JD OTHER POP', '瑞丰壹品官方旗舰店'],
            ['jd', 182149, 'JD OTHER POP', '萨拉维官方旗舰店'],
            ['jd', 27064, 'JD OTHER POP', '萨拉维酒业旗舰店'],
            ['jd', 617595, 'JD OTHER POP', '塞德菲贸易官方旗舰店'],
            ['jd', 1000100881, 'JD SELF RUN', '三得利洋酒自营店'],
            ['jd', 598847, 'JD SELF RUN', '山姆会员商店京东自营官方旗舰店'],
            ['jd', 1000097196, 'JD SELF RUN', '山图葡萄酒京东自营旗舰店'],
            ['jd', 1000091648, 'JD SELF RUN', '圣丽塔葡萄酒京东自营旗舰店'],
            ['jd', 64881, 'JD OTHER POP', '圣品食品专营店'],
            ['jd', 1000114761, 'JD SELF RUN', '圣芝葡萄酒京东自营旗舰店'],
            ['jd', 619819, 'JD OTHER POP', '盛世普达酒类专营店'],
            ['jd', 716773, 'JD OTHER POP', '师捷酒水专营店'],
            ['jd', 876785, 'JD OTHER POP', '十里九品旗舰店'],
            ['jd', 37889, 'JD OTHER POP', '拾全酒美官方旗舰店'],
            ['jd', 598669, 'JD OTHER POP', '世界名酒专营店'],
            ['jd', 123515, 'JD OTHER POP', '双宏天乐酒类旗舰店'],
            ['jd', 1000154003, 'JD SELF RUN', '顺昌源京东自营旗舰店'],
            ['jd', 601663, 'JD OTHER POP', '思萨美康酒类专营店'],
            ['jd', 816861, 'JD OTHER POP', '苏麦威官方旗舰店'],
            ['jd', 1000105659, 'JD SELF RUN', '苏州桥京东自营旗舰店'],
            ['jd', 751994, 'JD OTHER POP', '苏州桥旗舰店'],
            ['jd', 609057, 'JD OTHER POP', '随猴酒类专营店'],
            ['jd', 1000225181, 'JD SELF RUN', '獭祭京东自营旗舰店'],
            ['jd', 178142, 'JD OTHER POP', '泰和酩庄官方旗舰店'],
            ['jd', 1000121721, 'JD SELF RUN', '桃乐丝葡萄酒京东自营旗舰店'],
            ['jd', 741352, 'JD OTHER POP', '特马桐酒类专营店'],
            ['jd', 79194, 'JD OTHER POP', '腾易红酒官方旗舰店'],
            ['jd', 645612, 'JD OTHER POP', '天阶官方旗舰店'],
            ['jd', 726062, 'JD OTHER POP', '听花语旗舰店'],
            ['jd', 810137, 'JD OTHER POP', '同福永官方旗舰店'],
            ['jd', 776922, 'JD OTHER POP', '兔兔冷泉酒官方旗舰店'],
            ['jd', 1000089230, 'JD SELF RUN', '王朝葡萄酒京东自营旗舰店'],
            ['jd', 685997, 'JD OTHER POP', '威爵酒类专营店'],
            ['jd', 1000080006, 'JD SELF RUN', '威龙葡萄酒京东自营旗舰店'],
            ['jd', 23686, 'JD OTHER POP', '威赛帝斯官方旗舰店'],
            ['jd', 920233, 'JD SELF RUN', '威赛帝斯酒水京东自营旗舰店'],
            ['jd', 653155, 'JD OTHER POP', '威赛帝斯葡萄酒精品店'],
            ['jd', 1000130161, 'JD SELF RUN', '微醺洋酒京东自营专区'],
            ['jd', 659016, 'JD OTHER POP', '沃尔玛官方旗舰店'],
            ['jd', 659016, 'JD SELF RUN', '沃尔玛京东自营官方旗舰店'],
            ['jd', 790230, 'JD OTHER POP', '五福源酒类专营店'],
            ['jd', 778289, 'JD OTHER POP', '五洲海购酒类旗舰店'],
            ['jd', 192182, 'JD OTHER POP', '伍壹玖酒类专营店'],
            ['jd', 1000090387, 'JD SELF RUN', '西班牙葡萄酒京东自营专区'],
            ['jd', 1000078272, 'JD SELF RUN', '西夫拉姆葡萄酒京东自营旗舰店'],
            ['jd', 806900, 'JD OTHER POP', '西域烈焰旗舰店'],
            ['jd', 179651, 'JD OTHER POP', '昔澜酒类专营店'],
            ['jd', 1000112381, 'JD SELF RUN', '香奈葡萄酒京东自营旗舰店'],
            ['jd', 40837, 'JD OTHER POP', '橡树之约葡萄酒优选店'],
            ['jd', 135482, 'JD OTHER POP', '学权酒类专营店'],
            ['jd', 17784, 'JD PRC POP', '也买酒官方旗舰店'],
            ['jd', 31942, 'JD PRC POP', '也买酒旗舰店'],
            ['jd', 47992, 'JD OTHER POP', '伊樽月亮谷酒类专营店'],
            ['jd', 144102, 'JD OTHER POP', '壹捌捌久官方旗舰店'],
            ['jd', 174090, 'JD OTHER POP', '宜信酒类旗舰店'],
            ['jd', 774766, 'JD OTHER POP', '邑品酒类专营店'],
            ['jd', 204363, 'JD OTHER POP', '崟泰酒类专营店'],
            ['jd', 943285, 'JD SELF RUN', '优红国际京东自营旗舰店'],
            ['jd', 14838, 'JD OTHER POP', '优红酒'],
            ['jd', 685911, 'JD OTHER POP', '优葡荟官方旗舰店'],
            ['jd', 12193, 'JD OTHER POP', '有家红酒官方旗舰店'],
            ['jd', 790107, 'JD PRC POP', '御玖 轩官方旗舰店'],
            ['jd', 128287, 'JD OTHER POP', '御隆轩红酒专营店'],
            ['jd', 617388, 'JD OTHER POP', '元合陶艺红酒旗舰店'],
            ['jd', 54761, 'JD OTHER POP', '原产团旗舰店'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷酒类专营店'],
            ['jd', 47992, 'JD OTHER POP', '月氏良园'],
            ['jd', 650742, 'JD OTHER POP', '月氏人酒类专营店'],
            ['jd', 751364, 'JD OTHER POP', '月氏人洋酒专营店'],
            ['jd', 659112, 'JD OTHER POP', '云的酒类专营店'],
            ['jd', 58502, 'JD OTHER FSS', '张裕官方旗舰店'],
            ['jd', 1000004718, 'JD SELF RUN', '张裕葡萄酒京东自营旗舰店'],
            ['jd', 1000090684, 'JD SELF RUN', '长白山京东自营旗舰店'],
            ['jd', 50080, 'JD OTHER POP', '长白山特产专营店'],
            ['jd', 757260, 'JD OTHER POP', '长城葡萄酒官方旗舰店'],
            ['jd', 1000004719, 'JD SELF RUN', '长城葡萄酒京东自营旗舰店'],
            ['jd', 38608, 'JD OTHER POP', '真露旗舰店'],
            ['jd', 1000090830, 'JD SELF RUN', '智利葡萄酒京东自营专区'],
            ['jd', 1000080527, 'JD SELF RUN', '智象葡萄酒京东自营旗舰店'],
            ['jd', 547312, 'JD OTHER POP', '中国特产·泸州馆'],
            ['jd', 704519, 'JD OTHER POP', '中粮名庄荟酒类旗舰店'],
            ['jd', 582537, 'JD OTHER POP', '中粮名庄荟旗舰店'],
            ['jd', 765450, 'JD OTHER POP', '中葡汇官方旗舰店'],
            ['jd', 199202, 'JD OTHER POP', '中浦耐杯酒类专营店'],
            ['jd', 34451, 'JD OTHER POP', '醉鹅娘葡萄酒旗舰店'],
            ['jd', 793598, 'JD OTHER POP', '醉美酒酒类专营店'],
            ['jd', 846070, 'JD OTHER POP', '醉梦红酒旗舰店'],
            ['tmall', 181936762, 'TMALL PRC FSS', 'chivas芝华士官方旗舰店'],
            ['tmall', 67726749, 'TMALL PRC POP', '宝树行酒类旗舰店'],
            ['tmall', 9730213, 'TMALL PRC FSS', '保乐力加官方旗舰店'],
            ['tmall', 181925703, 'TMALL PRC POP', '池陈辉酒类专营店'],
            ['tmall', 945237, 'TMALL PRC POP', '登岑酒水专营店'],
            ['tmall', 182340153, 'TMALL PRC POP', '高华仕酒类专营店'],
            ['tmall', 64070709, 'TMALL OTHER FSS', '杰克丹尼官方旗舰店'],
            ['tmall', 61276625, 'TMALL PRC FSS', 'absolut官方旗舰店'],
            ['tmall', 2561079, 'TMALL PRC POP', '郎家园酒类专营店'],
            ['tmall', 189367760, 'TMALL OTHER FSS', '酩悦轩尼诗官方旗舰店'],
            ['tmall', 60553350, 'TMALL OTHER FSS', '人头马官方旗舰店'],
            ['tmall', 9281929, 'TMALL SUPER', '天猫超市'],
            ['tmall', 189673674, 'TMALL OTHER POP', '侠风酒类专营店'],
            ['tmall', 59180296, 'TMALL PRC POP', '旭隆久合酒类专营店'],
            ['tmall', 190864630, 'TMALL PRC POP', '寅瑜酒类专营店'],
            ['tmall', 16994272, 'TMALL OTHER FSS', '尊尼获加官方旗舰店'],
            ['tmall', 192635616, 'TMALL PRC POP', '奈思酒类专营店'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利官方旗舰店'],
            ['tmall', 60309695, 'TMALL PRC POP', '御玖轩酒类专营店'],
            ['tmall', 168343869, 'TMALL OTHER FSS', 'macallan旗舰店'],
            ['tmall', 61123847, 'TMALL OTHER FSS', 'rio锐澳旗舰店'],
            ['tmall', 63282256, 'TMALL OTHER FSS', '百利官方旗舰店'],
            ['tmall', 67937129, 'TMALL OTHER FSS', '帝亚吉欧洋酒官方旗舰店'],
            ['tmall', 163407529, 'TMALL OTHER FSS', '翰格蓝爵官方旗舰店'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利旗舰店'],
            ['tmall', 16433652, 'TMALL OTHER FSS', 'asc官方旗舰店'],
            ['tmall', 186938208, 'TMALL OTHER POP', '云的食品专营店'],
            ['tmall', 10795084, 'TMALL OTHER POP', '艾森特酒类专营店'],
            ['tmall', 174541689, 'TMALL OTHER POP', '谦法酒类专营店'],
            ['tmall', 179501854, 'TMALL OTHER POP', '梦扬酒类专营店'],
            ['tmall', 945362, 'TMALL OTHER POP', '月亮谷酒类专营店'],
            ['tmall', 167186023, 'TMALL OTHER POP', '中浦耐杯酒类专营店'],
            ['tmall', 64886808, 'TMALL OTHER POP', '富水食品专营店'],
            ['tmall', 9282437, 'TMALL OTHER POP', '江秀酒类专营店'],
            ['tmall', 189890305, 'TMALL OTHER POP', '泰圣原酒类专营店'],
            ['tmall', 66986595, 'TMALL OTHER POP', '广存禄酒类专营店'],
            ['tmall', 190184104, 'TMALL OTHER POP', 'ak47酒类旗舰店'],
            ['tmall', 190859232, 'TMALL OTHER FSS', 'aldi旗舰店'],
            ['tmall', 167940348, 'TMALL OTHER FSS', 'asahi朝日旗舰店'],
            ['tmall', 186802661, 'TMALL OTHER FSS', 'baraka酒类旗舰店'],
            ['tmall', 190833445, 'TMALL OTHER FSS', 'beamsuntory宾三得利旗舰'],
            ['tmall', 181931354, 'TMALL OTHER FSS', 'bols波士力娇酒旗舰店'],
            ['tmall', 190759564, 'TMALL OTHER FSS', 'camus卡慕官方旗舰店'],
            ['tmall', 192980474, 'TMALL OTHER FSS', 'choya俏雅旗舰店'],
            ['tmall', 9338011, 'TMALL OTHER FSS', 'cmp旗舰店'],
            ['tmall', 189712524, 'TMALL OTHER FSS', 'costco官方旗舰店'],
            ['tmall', 189222031, 'TMALL OTHER FSS', 'dekuyper海外旗舰店'],
            ['tmall', 187805167, 'TMALL OTHER FSS', 'emp海外旗舰店'],
            ['tmall', 192599080, 'TMALL OTHER FSS', 'fastking莎堡皇旗舰店'],
            ['tmall', 186605891, 'TMALL OTHER FSS', 'fourloko旗舰店'],
            ['tmall', 186902811, 'TMALL OTHER FSS', 'giv旗舰店'],
            ['tmall', 188755410, 'TMALL OTHER FSS', 'hilbaton希伯顿旗舰店'],
            ['tmall', 192127385, 'TMALL OTHER FSS', 'ibanesas伊柏妮莎旗舰店'],
            ['tmall', 188185494, 'TMALL OTHER FSS', 'lffo旗 舰店'],
            ['tmall', 188150563, 'TMALL OTHER FSS', 'meukow墨高旗舰店'],
            ['tmall', 186963464, 'TMALL OTHER FSS', 'montes旗舰店'],
            ['tmall', 181936150, 'TMALL OTHER FSS', 'rekorderlig旗舰店'],
            ['tmall', 189285528, 'TMALL OTHER FSS', 'sinodrink华饮旗舰店'],
            ['tmall', 68372177, 'TMALL OTHER FSS', 'suamgy圣芝官方旗舰店'],
            ['tmall', 192825647, 'TMALL OTHER FSS', 'wemyss旗舰店'],
            ['tmall', 187740248, 'TMALL OTHER FSS', '阿道克旗舰店'],
            ['tmall', 60268731, 'TMALL OTHER POP', '爱屋食 品专营店'],
            ['tmall', 187217166, 'TMALL OTHER FSS', '安特旗舰店'],
            ['tmall', 67036345, 'TMALL OTHER POP', '安酌酒类专营店'],
            ['tmall', 186905859, 'TMALL OTHER POP', '奥蒂兰丝旗舰店'],
            ['tmall', 63676728, 'TMALL OTHER POP', '奥美圣酒类专营店'],
            ['tmall', 190745141, 'TMALL OTHER POP', '奥图纳旗舰店'],
            ['tmall', 190152783, 'TMALL OTHER POP', '奥雪谷酒类专营店'],
            ['tmall', 169343891, 'TMALL OTHER POP', '澳迪尼旗舰店'],
            ['tmall', 185708057, 'TMALL OTHER POP', '芭诺斯酒类专营店'],
            ['tmall', 181935488, 'TMALL OTHER POP', '白洋河酒类旗舰店'],
            ['tmall', 189458091, 'TMALL OTHER POP', '百醇品逸酒类专营店'],
            ['tmall', 192831218, 'TMALL OTHER POP', '百富门酒类旗舰店'],
            ['tmall', 5666084, 'TMALL OTHER POP', '百正利酒类专营店'],
            ['tmall', 3743705, 'TMALL OTHER POP', '柏特酒类专营店'],
            ['tmall', 10794997, 'TMALL OTHER POP', '杯中时光酒类专营店'],
            ['tmall', 191604668, 'TMALL OTHER POP', '北京榜样酒类专营店'],
            ['tmall', 64030717, 'TMALL OTHER POP', '北京酒葫芦酒类专营店'],
            ['tmall', 60006673, 'TMALL OTHER POP', '北京亿万典藏酒类专营店'],
            ['tmall', 69685590, 'TMALL OTHER POP', '北京智恒达商食品专营店'],
            ['tmall', 7602595, 'TMALL OTHER POP', '奔弛酒类专营店'],
            ['tmall', 192617571, 'TMALL OTHER POP', '奔富极昼专卖店'],
            ['tmall', 69111791, 'TMALL OTHER POP', '本初酒类专营店'],
            ['tmall', 187739761, 'TMALL OTHER POP', '必得利旗舰店'],
            ['tmall', 189534060, 'TMALL OTHER POP', '碧波林海酒类专营店'],
            ['tmall', 192127388, 'TMALL OTHER POP', '镖士旗舰店'],
            ['tmall', 8121431, 'TMALL OTHER POP', '槟悦酒类专营店'],
            ['tmall', 189239808, 'TMALL OTHER POP', '波尔亚葡萄酒旗舰店'],
            ['tmall', 163744402, 'TMALL OTHER POP', '波菲摩根酒类专营店'],
            ['tmall', 191517232, 'TMALL OTHER POP', '伯爵酒类专营店'],
            ['tmall', 191976497, 'TMALL OTHER POP', '布多格旗舰店'],
            ['tmall', 64528280, 'TMALL OTHER POP', '昌盛德酒类专营店'],
            ['tmall', 945418, 'TMALL OTHER POP', '畅健酒类专营店'],
            ['tmall', 189151241, 'TMALL OTHER POP', ' 诚善堂酒类专营'],
            ['tmall', 181936162, 'TMALL OTHER POP', '初生活酒类企业店'],
            ['tmall', 192103483, 'TMALL OTHER POP', '触角食品专营店'],
            ['tmall', 188943083, 'TMALL OTHER POP', '春融食品专营店'],
            ['tmall', 163796968, 'TMALL OTHER POP', '春彦佳业酒类专营店'],
            ['tmall', 181936050, 'TMALL OTHER POP', '第7元素旗舰店'],
            ['tmall', 945291, 'TMALL OTHER POP', '东晟酒类专营店'],
            ['tmall', 190610751, 'TMALL OTHER POP', '法多克旗舰店'],
            ['tmall', 188811200, 'TMALL OTHER POP', '法尔 城堡旗舰店'],
            ['tmall', 186503318, 'TMALL OTHER POP', '法夫尔堡旗舰店'],
            ['tmall', 7665680, 'TMALL OTHER POP', '菲利特酒类专营店'],
            ['tmall', 165603201, 'TMALL OTHER POP', '菲尼雅酒类专营店'],
            ['tmall', 187418569, 'TMALL OTHER POP', '枫羿酒类专营店'],
            ['tmall', 945320, 'TMALL OTHER POP', '福客仕酒类专营店'],
            ['tmall', 190892990, 'TMALL OTHER POP', '福洮食品专营店'],
            ['tmall', 187236984, 'TMALL OTHER POP', '富瑞斯旗舰店'],
            ['tmall', 192827257, 'TMALL PRC POP', '富以邻酒类专营店'],
            ['tmall', 181903836, 'TMALL OTHER POP', '富邑葡萄酒集团旗舰店'],
            ['tmall', 191834815, 'TMALL OTHER POP', '高地旗舰店'],
            ['tmall', 191669504, 'TMALL OTHER POP', '歌然酒类专营店'],
            ['tmall', 184272306, 'TMALL OTHER FSS', '格拉洛旗舰店'],
            ['tmall', 192175320, 'TMALL OTHER FSS', '格兰父子旗舰店'],
            ['tmall', 11712171, 'TMALL OTHER POP', '购酒网官方旗舰店'],
            ['tmall', 188192538, 'TMALL OTHER POP', '购酒网葡萄酒旗舰店'],
            ['tmall', 62171152, 'TMALL OTHER POP', '古仓酒类专 营店'],
            ['tmall', 189705131, 'TMALL OTHER POP', '顾悦酒类专营店'],
            ['tmall', 188405924, 'TMALL OTHER POP', '冠振酒类专营店'],
            ['tmall', 191234834, 'TMALL OTHER POP', '广州美助达酒类专营店'],
            ['tmall', 190785471, 'TMALL OTHER POP', '哈力高旗舰店'],
            ['tmall', 190145492, 'TMALL OTHER POP', '海优食品专营店'],
            ['tmall', 191900950, 'TMALL OTHER POP', '浩丰隆睿酒类专营店'],
            ['tmall', 8437261, 'TMALL OTHER POP', '和道酒类专营店'],
            ['tmall', 192144831, 'TMALL PRC POP', '宏放酒类专 营店'],
            ['tmall', 9282647, 'TMALL OTHER POP', '泓利酒类专营店'],
            ['tmall', 59504827, 'TMALL OTHER POP', '洪英华泰酒类专营店'],
            ['tmall', 190174879, 'TMALL OTHER POP', '浣熙食品专营店'],
            ['tmall', 191166470, 'TMALL OTHER FSS', '黄尾袋鼠官方旗舰店'],
            ['tmall', 189224565, 'TMALL OTHER POP', '汇泉酒类专营店'],
            ['tmall', 10182220, 'TMALL OTHER POP', '吉量食品专营店'],
            ['tmall', 945372, 'TMALL OTHER POP', '加枫红进口红酒专营'],
            ['tmall', 191717130, 'TMALL OTHER POP', '加州乐事旗舰店'],
            ['tmall', 945585, 'TMALL OTHER POP', '嘉美醇酒类专营店'],
            ['tmall', 70102414, 'TMALL OTHER POP', '简拙酒类专营店'],
            ['tmall', 190802928, 'TMALL OTHER POP', '将军井山东专卖店'],
            ['tmall', 70420850, 'TMALL OTHER POP', '将军井梓轩专卖店'],
            ['tmall', 163055240, 'TMALL OTHER POP', '杰奥森酒类专营店'],
            ['tmall', 191071067, 'TMALL OTHER POP', '今朝美酒类专营店'],
            ['tmall', 192027065, 'TMALL OTHER POP', '金巴厘旗舰店'],
            ['tmall', 19694041, 'TMALL OTHER POP', '金车食品专营 店'],
            ['tmall', 945227, 'TMALL OTHER POP', '金都恒泰酒类专营'],
            ['tmall', 192345241, 'TMALL OTHER POP', '金利莎旗舰店'],
            ['tmall', 12346794, 'TMALL OTHER POP', '金源鹏祥酒类专营店'],
            ['tmall', 189584370, 'TMALL OTHER POP', '金泽恒酒类专营'],
            ['tmall', 19869513, 'TMALL OTHER POP', '金樽格兰酒类专营店'],
            ['tmall', 68606331, 'TMALL OTHER POP', '京鲁信达酒类专营店'],
            ['tmall', 192847223, 'TMALL OTHER POP', '京瑞酒类专营店'],
            ['tmall', 190603106, 'TMALL OTHER POP', '精酉酒类专 营店'],
            ['tmall', 67721348, 'TMALL OTHER POP', '景泰蓝酒类专营店'],
            ['tmall', 3928664, 'TMALL OTHER POP', '久加久酒博汇旗舰店'],
            ['tmall', 70101091, 'TMALL OTHER POP', '玖品酒类专营店'],
            ['tmall', 188099938, 'TMALL OTHER POP', '酒富盛酩酒类专营店'],
            ['tmall', 191944097, 'TMALL OTHER POP', '酒购乐酒类专营店'],
            ['tmall', 163340849, 'TMALL OTHER POP', '酒嗨酒酒类专营店'],
            ['tmall', 188435285, 'TMALL OTHER POP', '酒玩家酒类专营店'],
            ['tmall', 6112291, 'TMALL OTHER POP', '酒仙 网官方旗舰店'],
            ['tmall', 186803036, 'TMALL OTHER POP', '酒仙网葡萄酒旗舰店'],
            ['tmall', 166872422, 'TMALL OTHER POP', '酒怡酒类专营店'],
            ['tmall', 192837172, 'TMALL OTHER POP', '卡爹拉酒类旗舰店'],
            ['tmall', 191669304, 'TMALL OTHER POP', '卡普酒类专营店'],
            ['tmall', 188297880, 'TMALL OTHER POP', '卡诗图旗舰店'],
            ['tmall', 192066196, 'TMALL OTHER POP', '卡图酒类专营店'],
            ['tmall', 8440231, 'TMALL OTHER POP', '开为酒类专营店'],
            ['tmall', 169821549, 'TMALL OTHER POP', '蓝利 酒类专营店'],
            ['tmall', 190651317, 'TMALL OTHER POP', '乐喝酒类专营店'],
            ['tmall', 187445065, 'TMALL OTHER POP', '乐斯卡酒类专营店'],
            ['tmall', 163237776, 'TMALL OTHER POP', '雷辉酒类专营店'],
            ['tmall', 65759084, 'TMALL OTHER POP', '类人首旗舰店'],
            ['tmall', 69239662, 'TMALL OTHER POP', '李氏兄弟酒类专营店'],
            ['tmall', 189528060, 'TMALL OTHER POP', '力泉酒类专营店'],
            ['tmall', 189179357, 'TMALL OTHER POP', '丽乐威酒类专营店'],
            ['tmall', 69665044, 'TMALL OTHER POP', '丽日蓝天酒类专营店'],
            ['tmall', 189131471, 'TMALL OTHER POP', '刘嘉玲生活品味海外旗舰店'],
            ['tmall', 168772523, 'TMALL OTHER POP', '龙马酒类专营店'],
            ['tmall', 64526011, 'TMALL OTHER POP', '隆斐堡旗舰店'],
            ['tmall', 185245516, 'TMALL OTHER POP', '路易马西尼旗舰店'],
            ['tmall', 190554679, 'TMALL OTHER POP', '路易十三官方旗舰店'],
            ['tmall', 181622804, 'TMALL OTHER POP', '罗莎酒类旗舰店'],
            ['tmall', 190826300, 'TMALL OTHER POP', '罗提斯酒类旗舰店'],
            ['tmall', 9666574, 'TMALL OTHER POP', '罗提斯酒类专营店'],
            ['tmall', 192857829, 'TMALL OTHER POP', '洛克之羽旗舰店'],
            ['tmall', 172944703, 'TMALL OTHER POP', '麦德龙官方海外旗舰店'],
            ['tmall', 170355320, 'TMALL OTHER POP', '麦德龙官方旗舰店'],
            ['tmall', 162888716, 'TMALL OTHER POP', '美圣酒类专营店'],
            ['tmall', 181928781, 'TMALL OTHER POP', '美助达 酒类专营店'],
            ['tmall', 173542702, 'TMALL OTHER POP', '蒙大菲旗舰店'],
            ['tmall', 20101735, 'TMALL OTHER POP', '孟特罗斯庄园酒类专营店'],
            ['tmall', 190406543, 'TMALL OTHER POP', '米宝拓酒类专营店'],
            ['tmall', 192107315, 'TMALL OTHER POP', '名仕罗纳德酒类旗舰店'],
            ['tmall', 10177878, 'TMALL OTHER POP', '莫高官方旗舰店'],
            ['tmall', 185163884, 'TMALL OTHER POP', '慕拉旗舰店'],
            ['tmall', 167698271, 'TMALL OTHER POP', '尼雅酒类旗舰店'],
            ['tmall', 186920958, 'TMALL OTHER POP', ' 宁夏葡萄酒官方旗舰店'],
            ['tmall', 190733669, 'TMALL OTHER POP', '欧利千隆酒类旗舰店'],
            ['tmall', 190743351, 'TMALL OTHER POP', '欧绅酒类旗舰店'],
            ['tmall', 17437054, 'TMALL OTHER POP', '派斯顿酒类专营店'],
            ['tmall', 189816418, 'TMALL OTHER POP', '潘果旗舰店'],
            ['tmall', 185200804, 'TMALL OTHER POP', '朋珠酒类旗舰店'],
            ['tmall', 68315639, 'TMALL OTHER POP', '品厨食品专营店'],
            ['tmall', 4200399, 'TMALL OTHER POP', '品尚红酒官方旗舰店'],
            ['tmall', 165081244, 'TMALL OTHER POP', '品天下酒类专营店'],
            ['tmall', 191885632, 'TMALL OTHER POP', '葡萄酒小皮旗舰店'],
            ['tmall', 192040162, 'TMALL OTHER POP', '葡萄酿酒类专营店'],
            ['tmall', 187357317, 'TMALL OTHER POP', '祺彬酒类专营店'],
            ['tmall', 20619935, 'TMALL OTHER POP', '巧厨食品专营店'],
            ['tmall', 64545265, 'TMALL OTHER POP', '清之光食品专营店'],
            ['tmall', 190745134, 'TMALL OTHER POP', '锐澳师捷专卖店'],
            ['tmall', 189528055, 'TMALL OTHER POP', '锐澳硕悦专卖店'],
            ['tmall', 192898261, 'TMALL OTHER POP', '上海酒玩家酒类专营店'],
            ['tmall', 182339809, 'TMALL OTHER POP', '上海学权酒类专营店'],
            ['tmall', 191407553, 'TMALL OTHER POP', '舍岭酒类专营店'],
            ['tmall', 59208730, 'TMALL OTHER POP', '升升食品专营店'],
            ['tmall', 192324546, 'TMALL OTHER POP', '晟阳酒类专营店'],
            ['tmall', 167840211, 'TMALL OTHER POP', '盛世普达酒类专营店'],
            ['tmall', 191205218, 'TMALL OTHER POP', '盛卫酒类专营店'],
            ['tmall', 192830888, 'TMALL OTHER POP', '守望之鹰葡萄酒旗舰店'],
            ['tmall', 192855845, 'TMALL OTHER POP', '曙云食品专营店'],
            ['tmall', 189118013, 'TMALL OTHER POP', '苏佳利旗舰店'],
            ['tmall', 172696000, 'TMALL OTHER POP', '苏宁易购官方旗舰店'],
            ['tmall', 190709131, 'TMALL OTHER POP', '塔希葡萄酒旗舰店'],
            ['tmall', 192848674, 'TMALL OTHER POP', '獭祭旗舰店'],
            ['tmall', 170846288, 'TMALL OTHER POP', '特其拉之家'],
            ['tmall', 16476202, 'TMALL OTHER POP', '腾易永立酒类专营店'],
            ['tmall', 61304831, 'TMALL OTHER POP', '天尝地酒酒类专营店'],
            ['tmall', 170685533, 'TMALL OTHER POP', '天猫国际官方直营'],
            ['tmall', 181871121, 'TMALL OTHER POP', '天猫国际官方直营国内现货'],
            ['tmall', 61531947, 'TMALL OTHER POP', '天添乐酒类专营店'],
            ['tmall', 188453101, 'TMALL OTHER POP', '天源食品专营店'],
            ['tmall', 186990195, 'TMALL OTHER POP', '通化旗舰店'],
            ['tmall', 191715391, 'TMALL OTHER POP', '兔兔冷泉酒旗舰店'],
            ['tmall', 181932183, 'TMALL OTHER POP', '万酒酒类专营'],
            ['tmall', 9327449, 'TMALL OTHER POP', '王松酒类专营店'],
            ['tmall', 945416, 'TMALL OTHER POP', '威赛帝斯酒类专营'],
            ['tmall', 191900921, 'TMALL OTHER POP', '薇林酒类旗舰店'],
            ['tmall', 190654972, 'TMALL OTHER POP', '维定酒类专营店'],
            ['tmall', 190338990, 'TMALL OTHER POP', '伟贞酒类专营店'],
            ['tmall', 190760637, 'TMALL OTHER POP', '卫宾酒类旗舰店'],
            ['tmall', 169359891, 'TMALL OTHER POP', '武若酒类专营店'],
            ['tmall', 60318412, 'TMALL OTHER POP', '西夫拉姆旗舰店'],
            ['tmall', 190597213, 'TMALL OTHER POP', '夏多纳酒类专营店'],
            ['tmall', 17374987, 'TMALL OTHER POP', '相拓酒类专营店'],
            ['tmall', 181931513, 'TMALL OTHER POP', '祥栈酒类专营店'],
            ['tmall', 189345451, 'TMALL OTHER POP', '小冰酒类专营店'],
            ['tmall', 169403866, 'TMALL OTHER POP', '小飞象酒类专营店'],
            ['tmall', 12837183, 'TMALL OTHER POP', '新发食品专营店'],
            ['tmall', 188471711, 'TMALL OTHER POP', '新启达酒类专营店'],
            ['tmall', 163519396, 'TMALL OTHER POP', '星韩亚食品专营店'],
            ['tmall', 184984900, 'TMALL OTHER POP', '炫品酒类专营店'],
            ['tmall', 59504847, 'TMALL OTHER POP', '雅醇酒类专营店'],
            ['tmall', 67643980, 'TMALL OTHER POP', '阳光万国酒类专营店'],
            ['tmall', 945356, 'TMALL OTHER POP', '也买酒官方旗舰店'],
            ['tmall', 181939573, 'TMALL OTHER POP', '依仕迪旗舰店'],
            ['tmall', 9282683, 'TMALL OTHER POP', '壹玖壹玖官方旗舰店'],
            ['tmall', 188113645, 'TMALL OTHER POP', '艺术玻璃旗舰店'],
            ['tmall', 192626419, 'TMALL OTHER POP', '邑品酒类专营店'],
            ['tmall', 187259490, 'TMALL OTHER POP', '逸香酒类旗舰店'],
            ['tmall', 182339346, 'TMALL OTHER POP', '崟泰食品专营店'],
            ['tmall', 61754686, 'TMALL OTHER POP', '雍天酒类专营店'],
            ['tmall', 64482742, 'TMALL OTHER POP', '裕诚酒类旗舰店'],
            ['tmall', 165334654, 'TMALL OTHER POP', '裕泉酒类专营店'],
            ['tmall', 70606664, 'TMALL OTHER POP', '誉佳顺酒类专营店'],
            ['tmall', 6697591, 'TMALL OTHER POP', '源宏酒类折扣店'],
            ['tmall', 945015, 'TMALL OTHER FSS', '张裕官方旗舰店'],
            ['tmall', 188057778, 'TMALL OTHER POP', '张裕长庆和专卖店'],
            ['tmall', 5400262, 'TMALL OTHER POP', ' 长城葡萄酒官方旗舰店'],
            ['tmall', 174412493, 'TMALL OTHER POP', '赵薇梦陇酒庄旗舰店'],
            ['tmall', 69208154, 'TMALL OTHER POP', '哲畅酒类专营店'],
            ['tmall', 186600047, 'TMALL OTHER POP', '喆园酒类专营店'],
            ['tmall', 185354125, 'TMALL OTHER POP', '真韩食品专营店'],
            ['tmall', 190319525, 'TMALL OTHER POP', '真露酒类旗舰店'],
            ['tmall', 187567202, 'TMALL OTHER POP', '正鸿食品专营店'],
            ['tmall', 192876165, 'TMALL OTHER POP', '正善酒类专营店'],
            ['tmall', 60948875, 'TMALL OTHER POP', '中酒网官方旗舰店'],
            ['tmall', 186903121, 'TMALL OTHER POP', '中粮名庄荟酒类旗舰店'],
            ['tmall', 189685253, 'TMALL OTHER POP', '众思创酒类专营店'],
            ['tmall', 192017787, 'TMALL OTHER POP', '众酉酒类专营店'],
            ['tmall', 186342503, 'TMALL OTHER POP', '卓羿酒类专营店'],
            ['tmall', 192075749, 'TMALL OTHER POP', '纵答食品专营店'],
            ['tmall', 190318752, 'TMALL OTHER POP', '醉鹅娘葡萄酒旗舰店'],
            ['tmall', 13262082, 'TMALL OTHER POP', '醉梦酒类专营店'],
            ['jd', 783024, 'JD OTHER POP', '京糖我要酒官方旗舰店'],
            ['jd', 778221, 'JD OTHER POP', '醴鱼酒类专营店'],
            ['jd', 35946, 'JD OTHER POP', 'ASC官方旗舰店'],
            ['jd', 117815, 'JD OTHER POP', '京 糖我要酒旗舰店'],
            ['jd', 723860, 'JD OTHER POP', '旻彤精品酒业旗舰店'],
            ['tmall', 189890305, 'TMALL OTHER POP', 'whiskyl旗舰店'],
            ['tmall', 170685533, 'TMALL INTERNATIONAL', '天猫国际进口超市'],
            ['tmall', 181871121, 'TMALL INTERNATIONAL', '天猫国际进口超市国内现货'],
            ['jd', 670462, 'JD OTHER POP', 'Bannychoice海外旗 舰店'],
            ['jd', 109604, 'JD OTHER POP', 'CMP巴黎庄园葡萄酒官方旗舰店'],
            ['jd', 1000084051, 'JD SELF RUN', 'DBR拉菲葡萄酒京东自营专卖店'],
            ['jd', 206860, 'JD OTHER POP', 'JOYVIO佳沃葡萄酒旗舰店'],
            ['jd', 1000189381, 'JD SELF RUN', 'MO�0�9T酩悦香槟自营官方旗舰店'],
            ['jd', 888220, 'JD OTHER FSS', 'POWER STATION动力火车旗舰店'],
            ['jd', 213398, 'JD OTHER FSS', '奥贝尔庄园红酒旗舰店'],
            ['jd', 692398, 'JD OTHER POP', '澳享葡萄酒专营店'],
            ['jd', 1000165881, 'JD SELF RUN', '八角星葡萄酒自营旗舰店'],
            ['jd', 880552, 'JD OTHER FSS', '八芒星酒类旗舰店'],
            ['jd', 777644, 'JD OTHER POP', '百醇品逸酒类专营店'],
            ['jd', 626202, 'JD OTHER FSS', '奔 富麦克斯旗舰店'],
            ['jd', 1000100881, 'JD SELF RUN', '宾三得利洋酒自营店'],
            ['jd', 734930, 'JD OTHER FSS', '伯克英达官方旗舰店'],
            ['jd', 1000120985, 'JD SELF RUN', '布多格酒类京东自营专区'],
            ['jd', 588286, 'JD OTHER FSS', '超级波精品旗舰店'],
            ['jd', 140755, 'JD OTHER POP', '当歌国际酒类专营店'],
            ['jd', 1000090349, 'JD SELF RUN', '法国列级名庄酒京东自营专区'],
            ['jd', 863174, 'JD OTHER POP', '法国美酒精品店'],
            ['jd', 949978, 'JD SELF RUN', '菲特瓦葡萄酒京东自营官方旗舰店'],
            ['jd', 691720, 'JD OTHER POP', '福易浩酒类专营店'],
            ['jd', 720539, 'JD OTHER POP', '富德酒类专营店'],
            ['jd', 953900, 'JD OTHER FSS', '富瑞拉酒类官方旗舰店'],
            ['jd', 777602, 'JD OTHER FSS', '富森丽安葡萄酒官方旗舰店'],
            ['jd', 686900, 'JD OTHER FSS', '富邑葡萄酒海外旗舰店'],
            ['jd', 1000091188, 'JD SELF RUN', '贺兰山葡萄酒京东自营旗舰店'],
            ['jd', 801770, 'JD OTHER POP', '恒众伟业葡萄酒专营店'],
            ['jd', 1000076993, 'JD SELF RUN', '红酒海外自营旗舰店'],
            ['jd', 709725, 'JD OTHER POP', '鸿大葡萄酒专营店'],
            ['jd', 183917, 'JD OTHER POP', '环球时代酒类专营店'],
            ['jd', 885816, 'JD OTHER POP', '汇泉洋酒专营店'],
            ['jd', 602024, 'JD OTHER FSS', '吉卖汇官方旗舰店'],
            ['jd', 98982, 'JD OTHER FSS', '集美旗舰店'],
            ['jd', 807471, 'JD OTHER FSS', '集颜旗舰店'],
            ['jd', 182514, 'JD OTHER POP', '加枫国际进口 食品专营店'],
            ['jd', 688680, 'JD OTHER FSS', '加州乐事官方旗舰店'],
            ['jd', 925445, 'JD OTHER POP', '贾真酒类专营店'],
            ['jd', 665479, 'JD OTHER POP', '杰奥森红 酒专营店'],
            ['jd', 205310, 'JD OTHER POP', '京方丹酒类专营店'],
            ['jd', 106117, 'JD OTHER FSS', '京酒汇官方旗舰店'],
            ['jd', 583357, 'JD OTHER POP', '九荣图酒类 专营店'],
            ['jd', 783878, 'JD OTHER POP', '久悦久酒类专营店'],
            ['jd', 53424, 'JD OTHER FSS', '酒葫芦官方旗舰店'],
            ['jd', 201977, 'JD OTHER FSS', '酒惠网精品旗舰店'],
            ['jd', 106116, 'JD OTHER FSS', '酒联聚旗舰店'],
            ['jd', 606501, 'JD OTHER FSS', '酒品惠官方旗舰店'],
            ['jd', 868288, 'JD OTHER FSS', '酒速汇旗舰店'],
            ['jd', 32252, 'JD OTHER POP', '酒仙网精品旗舰店'],
            ['jd', 173196, 'JD OTHER FSS', '酒一站旗舰店'],
            ['jd', 724349, 'JD OTHER FSS', '聚藏旗舰店'],
            ['jd', 868417, 'JD OTHER FSS', '卡伯纳官方旗舰店'],
            ['jd', 802859, 'JD OTHER FSS', '卡聂高旗舰店'],
            ['jd', 992265, 'JD OTHER FSS', '凯特伊曼酒庄葡萄酒旗舰店'],
            ['jd', 1000091242, 'JD SELF RUN', '拉蒙葡萄酒京东自营旗舰店'],
            ['jd', 1000091203, 'JD SELF RUN', '乐朗1374葡萄酒京东自营旗舰店'],
            ['jd', 766970, 'JD OTHER FSS', '勒度官方旗舰店'],
            ['jd', 841646, 'JD OTHER FSS', '雷司葡萄酒旗舰店'],
            ['jd', 659031, 'JD OTHER FSS', '雷司旗舰店'],
            ['jd', 1000072603, 'JD SELF RUN', '类人首葡萄酒京东自营旗 舰店'],
            ['jd', 862679, 'JD OTHER POP', '利港葡萄酒专营店'],
            ['jd', 891054, 'JD OTHER FSS', '利藤葡萄酒官方旗舰店'],
            ['jd', 708133, 'JD OTHER POP', '利盈红酒专 营店'],
            ['jd', 853486, 'JD OTHER FSS', '琳赛葡萄酒旗舰店'],
            ['jd', 676859, 'JD OTHER FSS', '零利官方旗舰店'],
            ['jd', 870978, 'JD OTHER FSS', '泸州老窖进口葡萄 酒官方旗舰店'],
            ['jd', 717858, 'JD OTHER FSS', '吕森堡酒类旗舰店'],
            ['jd', 733753, 'JD OTHER FSS', '曼妥思酒类旗舰店'],
            ['jd', 740451, 'JD OTHER FSS', '玫嘉官 方旗舰店'],
            ['jd', 776736, 'JD OTHER FSS', '玫嘉旗舰店'],
            ['jd', 663701, 'JD OTHER POP', '美国国家酒馆'],
            ['jd', 728304, 'JD OTHER FSS', '美景庄园官方旗舰店'],
            ['jd', 685998, 'JD OTHER FSS', '美玫圣官方旗舰店'],
            ['jd', 622892, 'JD OTHER FSS', '酩品大师酒类旗舰店'],
            ['jd', 1000187770, 'JD SELF RUN', '酩悦轩尼诗帝亚吉 欧MHD自营旗舰店'],
            ['jd', 107067, 'JD OTHER FSS', '南航发现会葡萄酒官方旗舰店'],
            ['jd', 780774, 'JD OTHER FSS', '宁夏贺兰山东麓青铜峡产区官方旗舰店'],
            ['jd', 809546, 'JD OTHER FSS', '女爵酒庄官方旗舰店'],
            ['jd', 780857, 'JD OTHER FSS', '佩蒂克斯葡萄酒官方旗舰店'],
            ['jd', 965804, 'JD SELF RUN', '品尚汇京东自营旗舰店'],
            ['jd', 10803, 'JD OTHER FSS', '品尚汇酒类旗舰店'],
            ['jd', 912282, 'JD OTHER FSS', '青岛保税港区官方旗舰店'],
            ['jd', 1000135321, 'JD SELF RUN', '清酒屋京东自 营专区'],
            ['jd', 753952, 'JD OTHER FSS', '萨克森堡酒类旗舰店'],
            ['jd', 190315, 'JD OTHER POP', '赛普利斯红酒专营店'],
            ['jd', 1000103573, 'JD OTHER POP', '三全 测试店铺333'],
            ['jd', 804351, 'JD OTHER POP', '莎玛拉庄园酒类专营店'],
            ['jd', 1000281684, 'JD SELF RUN', '圣侯爵葡萄酒京东自营旗舰店'],
            ['jd', 583005, 'JD OTHER FSS', '十点品酒酒类旗舰店'],
            ['jd', 988739, 'JD OTHER FSS', '蜀兼香酒类旗舰店'],
            ['jd', 785877, 'JD OTHER POP', '蜀兼香酒类专营店'],
            ['jd', 1000159902, 'JD SELF RUN', '苏州桥酒京东自营旗舰店'],
            ['jd', 1000178125, 'JD SELF RUN', '獭祭（DASSAI）海外自营旗舰店'],
            ['jd', 779694, 'JD OTHER POP', '萄醇葡萄酒专营店'],
            ['jd', 1000078445, 'JD SELF RUN', '通化葡萄酒京东自营旗舰店'],
            ['jd', 654757, 'JD OTHER FSS', '万酒网红酒旗舰店'],
            ['jd', 626312, 'JD OTHER FSS', '王朝葡萄酒旗舰 店'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营专区'],
            ['jd', 1000091521, 'JD SELF RUN', '唯浓葡萄酒京东自营旗舰店'],
            ['jd', 1000156067, 'JD SELF RUN', '夏桐Chandon自营旗舰店'],
            ['jd', 184977, 'JD OTHER FSS', '橡树之约旗舰店'],
            ['jd', 777642, 'JD OTHER POP', '新华酒类专营店'],
            ['jd', 1000101205, 'JD SELF RUN', '星得斯葡萄酒京东自营旗舰店'],
            ['jd', 852797, 'JD OTHER FSS', '兴利臻酒汇旗舰店'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗Hennessy自营旗舰店'],
            ['jd', 956156, 'JD OTHER FSS', '一号城堡酒类官方旗舰店'],
            ['jd', 789982, 'JD OTHER POP', '壹扬扬酒类专营店'],
            ['jd', 583708, 'JD OTHER POP', '怡九福酒类专营店'],
            ['jd', 583268, 'JD OTHER POP', '怡九怡五酒类专营店'],
            ['jd', 1000094450, 'JD SELF RUN', '易指酒类京东自营专区'],
            ['jd', 618447, 'JD OTHER POP', '逸隆酒业专营店'],
            ['jd', 975050, 'JD OTHER POP', '银泰葡萄酒专营店'],
            ['jd', 210015, 'JD OTHER POP', '昱嵘酒类专营店'],
            ['jd', 790179, 'JD OTHER POP', '云朵葡萄酒专营店'],
            ['jd', 1000101743, 'JD SELF RUN', '张裕葡萄酒京东自营专区'],
            ['jd', 1000118610, 'JD SELF RUN', '张裕先锋葡萄酒京东自营旗舰店'],
            ['jd', 629979, 'JD OTHER POP', '臻品尚酒类专营店'],
            ['jd', 837457, 'JD OTHER FSS', '中酒网红酒官方旗舰店'],
            ['jd', 653600, 'JD OTHER FSS', '中淘网官方旗舰店'],
            ['tmall', 193018741, 'TMALL OTHER POP', '0葡萄酒旗舰店'],
            ['tmall', 188273793, 'TMALL OTHER POP', 'Aldi海外旗舰店'],
            ['tmall', 187646666, 'TMALL OTHER POP', 'delegat酒类旗舰店'],
            ['tmall', 170680601, 'TMALL OTHER POP', 'jessicassuitcase海外旗舰'],
            ['tmall', 190257181, 'TMALL OTHER POP', 'js90高分葡萄酒旗舰店'],
            ['tmall', 192904481, 'TMALL OTHER POP', 'mestia梅斯蒂亚酒类旗舰店'],
            ['tmall', 185263311, 'TMALL OTHER POP', 'paologuidi旗舰店'],
            ['tmall', 189744812, 'TMALL OTHER POP', 'vino75海外旗舰店'],
            ['tmall', 192926450, 'TMALL OTHER POP', 'woegobo沃歌堡旗舰店'],
            ['tmall', 191236011, 'TMALL OTHER POP', '爱龙堡酒类旗舰店'],
            ['tmall', 192882168, 'TMALL OTHER POP', '澳淘酒 类专营店'],
            ['tmall', 190558245, 'TMALL OTHER POP', '柏尔兰堡旗舰店'],
            ['tmall', 192486369, 'TMALL OTHER POP', '奔富品尚汇专卖店'],
            ['tmall', 193211468, 'TMALL OTHER POP', '川富旗舰店'],
            ['tmall', 164537968, 'TMALL OTHER POP', '创群酒类专营店'],
            ['tmall', 10674908, 'TMALL OTHER POP', '德龙宝真酒类旗舰店'],
            ['tmall', 193000541, 'TMALL OTHER POP', '丁戈树旗舰店'],
            ['tmall', 192144830, 'TMALL OTHER POP', '鼎沃酒类专营店'],
            ['tmall', 192915033, 'TMALL OTHER POP', '福事多酒类专营店'],
            ['tmall', 68643227, 'TMALL OTHER POP', '谷度酒类专营店'],
            ['tmall', 192906330, 'TMALL OTHER POP', '海日新酒类专营店'],
            ['tmall', 192011951, 'TMALL OTHER POP', '红魔鬼官方旗舰店'],
            ['tmall', 193020949, 'TMALL OTHER POP', '酒奢酒类专营店'],
            ['tmall', 190446898, 'TMALL OTHER POP', '马标旗舰店'],
            ['tmall', 177178640, 'TMALL OTHER POP', '玛嘉唯诺酒类旗舰店'],
            ['tmall', 190634211, 'TMALL OTHER POP', '玛隆酒类旗舰店'],
            ['tmall', 193120192, 'TMALL OTHER POP', '梅乃宿旗舰店'],
            ['tmall', 188950240, 'TMALL OTHER POP', '梦德斯诺旗舰店'],
            ['tmall', 192637786, 'TMALL OTHER POP', '谋勤酒类专营店'],
            ['tmall', 193160265, 'TMALL OTHER POP', '乔治金瀚旗舰店'],
            ['tmall', 70053226, 'TMALL OTHER POP', '首悠汇酒类专营店'],
            ['tmall', 193097743, 'TMALL OTHER POP', '铄今葡萄酒专营店'],
            ['tmall', 190853387, 'TMALL OTHER POP', '天猫优品官方直营'],
            ['tmall', 193197368, 'TMALL OTHER POP', '万来喜酒类专营店'],
            ['tmall', 164489419, 'TMALL OTHER POP', '威龙酒类官方旗舰店'],
            ['tmall', 192858212, 'TMALL OTHER POP', '希雅斯酒庄酒类旗舰店'],
            ['tmall', 12577893, 'TMALL OTHER POP', '香格里拉酒类旗舰店'],
            ['tmall', 174535535, 'TMALL OTHER POP', '姚明葡萄酒官方旗舰店'],
            ['tmall', 189068868, 'TMALL OTHER POP', '意纯酒类专营店'],
            ['tmall', 192620534, 'TMALL OTHER POP', '优西优西酒类专营店'],
            ['tmall', 192866403, 'TMALL OTHER POP', '长城简拙专卖店'],
            ['tmall', 191911772, 'TMALL OTHER POP', '长城圣玖专卖店'],
            ['tmall', 192830002, 'TMALL OTHER POP', '长城长庆和专卖店'],
            ['tmall', 191503070, 'TMALL OTHER POP', '紫桐葡萄酒旗舰店']
        ]
        type = None
        for v in data:
            if source == v[0] and str(sid) == str(v[1]):
                type = v[2]
        return type


    def type_map2(source, sid):
        data = [
            ['jd', 1000097462, 'JD SELF RUN', '保乐力加洋酒京东自营专区'],
            ['jd', 0, 'JD SELF RUN', '京东自营'],
            ['jd', 75371, 'JD PRC POP', '宝树行官方旗舰店'],
            ['jd', 1000016184, 'JD SELF RUN', '人头马君度京东自营专区'],
            ['jd', 1000082661, 'JD SELF RUN', '百加得京东自营旗舰店'],
            ['jd', 1000003530, 'JD SELF RUN', '杰克丹尼京东自营旗舰店'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京东自营旗舰店'],
            ['jd', 1000006863, 'JD SELF RUN', '帝亚吉欧京东自营专区'],
            ['jd', 1000074742, 'JD SELF RUN', '格兰父子洋酒京东自营专区'],
            ['jd', 1000101704, 'JD SELF RUN', '日韩小美酒京东自营专区'],
            ['jd', 46762, 'JD OTHER FSS', '人头马旗舰店'],
            ['jd', 110303, 'JD OTHER POP', '加枫红酒类官方旗舰店'],
            ['jd', 1000097502, 'JD SELF RUN', '好来喜洋酒京东自营专区'],
            ['jd', 1000114166, 'JD SELF RUN', '尊尼获加自营旗舰店'],
            ['jd', 663138, 'JD PRC POP', '宝树行洋酒旗舰店'],
            ['jd', 628046, 'JD PRC POP', '高华仕酒类专营店'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗重新发现中国味'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营店'],
            ['jd', 140747, 'JD OTHER POP', '智恒达商食品专营店'],
            ['jd', 173122, 'JD OTHER POP', '优红酒类官方旗舰店'],
            ['jd', 188438, 'JD OTHER FSS', '酩悦轩尼诗官方旗舰店'],
            ['jd', 208868, 'JD JX POP', '酒仙网红酒官方旗舰店'],
            ['jd', 1000102032, 'JD SELF RUN', '菊正宗京东自营 旗舰店'],
            ['jd', 1000100862, 'JD SELF RUN', '汇泉洋酒京东自营专区'],
            ['jd', 1000102030, 'JD SELF RUN', '梅乃宿京东自营旗舰店'],
            ['jd', 598847, 'JD OTHER POP', '山姆会员商店官方旗舰店'],
            ['jd', 77552, 'JD PRC EC POP', '御玖轩酒类专营店'],
            ['jd', 1000097501, 'JD SELF RUN', '琥珀洋酒京东自营专区'],
            ['jd', 196822, 'JD OTHER POP', '智恒达商冲调专营店'],
            ['jd', 628019, 'JD OTHER POP', '酒号魔方官方旗舰店'],
            ['jd', 745347, 'JD OTHER POP', '海荟码头酒类旗舰店'],
            ['jd', 'not found', 'JD OTHER POP', '伊樽酒水专营店'],
            ['jd', 721466, 'JD OTHER FSS', '帝亚吉欧洋酒官方旗舰店'],
            ['jd', 151121, 'JD OTHER POP', '梦扬酒类专营店'],
            ['jd', 79797, 'JD OTHER POP', '江秀旗舰店'],
            ['jd', 630047, 'JD PRC EC POP', '郎家园官方旗舰店'],
            ['jd', 621222, 'JD OTHER POP', '侠风官方旗舰店'],
            ['jd', 668773, 'JD PRC EC POP', '酒牧官方旗舰店'],
            ['jd', 81840, 'JD PRC EC POP', '宏放酒业专营店'],
            ['jd', 883600, 'JD PRC FSS', '保乐力加官方旗舰店'],
            ['jd', 1000130883, 'JD SELF RUN', '单一麦芽威士忌京东自营专区'],
            ['jd', 857001, 'JD PRC EC POP', '奢蕾汇官方旗舰店'],
            ['jd', 1000121041, 'JD SELF RUN', '铂珑（BOUTILLIER）洋酒京东自营专区'],
            ['jd', 10596, 'JD OTHER POP', 'BeWine酒类旗舰店'],
            ['jd', 784130, 'JD OTHER FSS', 'Camus官方旗舰店'],
            ['jd', 1000125574, 'JD SELF RUN', 'CAMUS卡慕京东自营官方旗舰店'],
            ['jd', 1000102998, 'JD SELF RUN', 'fourloko京东自营旗舰店'],
            ['jd', 739078, 'JD OTHER FSS', 'fourloko洋酒旗舰店'],
            ['jd', 644990, 'JD OTHER FSS', 'Lautergold酒类官方旗舰店'],
            ['jd', 69687, 'JD OTHER FSS', 'rio锐澳旗舰店'],
            ['jd', 1000139741, 'JD OTHER POP', 'TOPWINE'],
            ['jd', 54945, 'JD OTHER FSS', 'VC红酒 官方旗舰店'],
            ['jd', 813144, 'JD OTHER POP', '艾芙罗蒂酒类专营店'],
            ['jd', 666331, 'JD OTHER POP', '艾森特酒类专营店'],
            ['jd', 796667, 'JD OTHER POP', '爱尔兰 馆'],
            ['jd', 1000081125, 'JD SELF RUN', '爱之湾葡萄酒京东自营旗舰店'],
            ['jd', 1000124421, 'JD SELF RUN', '安努克（AnCnoc）京东自营专区'],
            ['jd', 141967, 'JD OTHER POP', '安酌酒类专营店'],
            ['jd', 1000091165, 'JD SELF RUN', '澳洲葡萄酒京东自营专区'],
            ['jd', 839480, 'JD OTHER FSS', '百富门洋酒旗舰店'],
            ['jd', 719592, 'JD OTHER FSS', '百加得官方旗舰店'],
            ['jd', 1000004357, 'JD SELF RUN', '百龄坛京东自营官方旗舰店'],
            ['jd', 652423, 'JD OTHER POP', '奔弛酒类专营店'],
            ['jd', 206443, 'JD OTHER POP', '奔富红酒品尚专卖店'],
            ['jd', 1000085511, 'JD SELF RUN', '卞氏米酒京东自营旗舰店'],
            ['jd', 1000086792, 'JD SELF RUN', '冰青青梅酒自营旗舰店'],
            ['jd', 730875, 'JD OTHER POP', '布多格酒类旗舰店'],
            ['jd', 166256, 'JD OTHER POP', '畅健酒类专营店'],
            ['jd', 101847, 'JD OTHER POP', '澄意生活旗舰店'],
            ['jd', 109668, 'JD PRC EC POP', '池陈辉酒类专营店'],
            ['jd', 667023, 'JD OTHER POP', '础石酒类专营店'],
            ['jd', 697745, 'JD OTHER POP', '春融食品专营店'],
            ['jd', 1000090402, 'JD SELF RUN', '大关清酒京东自营专区'],
            ['jd', 177770, 'JD OTHER POP', '德万利酒类官方旗舰店'],
            ['jd', 138519, 'JD OTHER POP', '登辉酒类专营店'],
            ['jd', 708739, 'JD OTHER POP', '迪欧葡萄酒专营店'],
            ['jd', 818570, 'JD OTHER POP', '蒂优合酒类官方旗舰店'],
            ['jd', 29380, 'JD OTHER POP', '东方聚仙酒类专营店'],
            ['jd', 198485, 'JD OTHER POP', '发九国际酒类专营店'],
            ['jd', 1000085804, 'JD SELF RUN', '法国红酒京东自营专区'],
            ['jd', 1000090564, 'JD SELF RUN', '翡马葡萄酒京东自营旗舰店'],
            ['jd', 652268, 'JD OTHER POP', '枫羿酒类专营店'],
            ['jd', 54739, 'JD OTHER POP', '福乐盟官方旗舰店'],
            ['jd', 1000086342, 'JD SELF RUN', '富邑京 东自营专区'],
            ['jd', 659245, 'JD OTHER FSS', '富邑葡萄酒集团旗舰店'],
            ['jd', 722661, 'JD OTHER POP', '噶瑪蘭威士忌官方旗舰店'],
            ['jd', 1000112681, 'JD SELF RUN', '干露酒厂京东自营旗舰店'],
            ['jd', 114398, 'JD OTHER POP', '给力酒类专营店'],
            ['jd', 197583, 'JD OTHER POP', '购酒网官方旗舰店'],
            ['jd', 785856, 'JD OTHER POP', '好来喜酒类专营店'],
            ['jd', 1000102041, 'JD SELF RUN', '好天好饮京东自营旗舰店'],
            ['jd', 52340, 'JD OTHER POP', '和道酒坊官方旗舰店'],
            ['jd', 893241, 'JD OTHER POP', '红冠雄鸡旗舰店'],
            ['jd', 786458, 'JD OTHER POP', '红魔鬼官方旗舰店'],
            ['jd', 602015, 'JD OTHER POP', '浣熙酒类专营店'],
            ['jd', 821932, 'JD OTHER FSS', '黄尾袋鼠葡萄酒官方旗舰店'],
            ['jd', 1000080704, 'JD SELF RUN', '黄尾袋鼠葡萄酒京东自营旗舰店'],
            ['jd', 1000095543, 'JD SELF RUN', '加州乐事葡萄酒京东自营旗舰店'],
            ['jd', 757577, 'JD OTHER POP', '嘉食和食品旗舰店'],
            ['jd', 789177, 'JD OTHER POP', '贾斯汀旗舰店'],
            ['jd', 891054, 'JD OTHER POP', '建清（建昌）酒类专 营店'],
            ['jd', 780857, 'JD OTHER POP', '建清葡萄酒专营店'],
            ['jd', 1000007181, 'JD SELF RUN', '杰卡斯葡萄酒京东自营专区'],
            ['jd', 678436, 'JD OTHER POP', '金采龙凤官方旗舰店'],
            ['jd', 667985, 'JD OTHER POP', '金都恒泰官方旗舰店'],
            ['jd', 27999, 'JD OTHER POP', '金都恒泰酒类旗舰店'],
            ['jd', 180040, 'JD OTHER POP', '金豪酒类专营店'],
            ['jd', 664135, 'JD OTHER POP', '金豪葡萄酒专营店'],
            ['jd', 1000076153, 'JD SELF RUN', '京东1小时达'],
            ['jd', 1000097094, 'JD SELF RUN', '京东海外直采官方自营店'],
            ['jd', 1000096602, 'JD OTHER POP', '京东京造官方旗舰店'],
            ['jd', 671105, 'JD OTHER POP', '九珑因特酒类专营店'],
            ['jd', 139272, 'JD OTHER POP', '久赞四海官方旗舰店'],
            ['jd', 85561, 'JD OTHER POP', '久之盈酒类专营店'],
            ['jd', 158631, 'JD OTHER POP', '酒多多酒类专营店'],
            ['jd', 901096, 'JD OTHER POP', '酒多多酒水专营店'],
            ['jd', 623605, 'JD OTHER POP', '酒富盛酩官方旗舰店'],
            ['jd', 156927, 'JD OTHER POP', '酒立有旗舰店'],
            ['jd', 196146, 'JD OTHER POP', '酒廷1990旗舰店'],
            ['jd', 653268, 'JD OTHER POP', '酒玩家酒类旗舰店'],
            ['jd', 601324, 'JD OTHER POP', '酒玩家旗舰店'],
            ['jd', 14796, 'JD JX POP', '酒仙网官方旗舰 店'],
            ['jd', 31348, 'JD JX POP', '酒仙网世界名酒旗舰店'],
            ['jd', 781742, 'JD PRC EC POP', '酒宿旗舰店'],
            ['jd', 623621, 'JD OTHER POP', '酒庄直通官方旗舰店'],
            ['jd', 691398, 'JD OTHER POP', '酒庄直通精品酒旗舰店'],
            ['jd', 784773, 'JD OTHER POP', '酒庄直通洋酒旗舰店'],
            ['jd', 1000004383, 'JD SELF RUN', '绝对伏特加京东自营专卖店'],
            ['jd', 149421, 'JD OTHER POP', '俊阁酒类专营店'],
            ['jd', 202038, 'JD OTHER POP', '卡巴酒类专营店'],
            ['jd', 202038, 'JD OTHER POP', '卡巴商贸酒类 专营店'],
            ['jd', 1000128562, 'JD SELF RUN', '卡伯纳葡萄酒京东自营旗舰店'],
            ['jd', 28244, 'JD OTHER POP', '开为进口食品专营店'],
            ['jd', 714754, 'JD OTHER POP', '跨理洋酒专营店'],
            ['jd', 64501, 'JD OTHER POP', '拉菲红酒VC专卖店'],
            ['jd', 1000084051, 'JD SELF RUN', '拉菲葡萄酒京东自营专卖店'],
            ['jd', 610735, 'JD OTHER POP', '拉斐红酒官方旗舰店'],
            ['jd', 28717, 'JD PRC EC POP', '郎家园酒类旗舰店'],
            ['jd', 28717, 'JD PRC EC POP', '郎家园酒类专营店'],
            ['jd', 787326, 'JD OTHER POP', '雷诺官方旗舰店'],
            ['jd', 762801, 'JD OTHER POP', '雷司酒类官方旗舰店'],
            ['jd', 761565, 'JD OTHER POP', '领亿来旗舰店'],
            ['jd', 149158, 'JD OTHER POP', ' 泸州老窖官方旗舰店'],
            ['jd', 889770, 'JD OTHER POP', '路易卡丹旗舰店'],
            ['jd', 1000102749, 'JD SELF RUN', '路易老爷京东自营旗舰店'],
            ['jd', 1000101425, 'JD SELF RUN', '罗莎庄园葡萄酒京东自营旗舰店'],
            ['jd', 1000004365, 'JD SELF RUN', '马爹利京东自营专卖店'],
            ['jd', 606881, 'JD OTHER POP', '玛吉欧酒类专营店'],
            ['jd', 686018, 'JD OTHER POP', '麦吉欧克酒类专营店'],
            ['jd', 109668, 'JD OTHER POP', '麦摇吧酒类专营店'],
            ['jd', 1000097097, 'JD SELF RUN', '曼戈洋酒京东自营旗舰店'],
            ['jd', 136746, 'JD OTHER POP', '曼拉维酒类旗舰店'],
            ['jd', 672469, 'JD OTHER POP', '猫呗旗舰店'],
            ['jd', 711165, 'JD OTHER POP', '每日壹品葡萄酒专营店'],
            ['jd', 708953, 'JD OTHER POP', '美酒汇酒类专营店'],
            ['jd', 785821, 'JD OTHER POP', '美尼尔城堡旗舰店'],
            ['jd', 723940, 'JD OTHER POP', '蒙特斯官方旗舰店'],
            ['jd', 1000083361, 'JD SELF RUN', '蒙特斯葡萄酒京东自营旗舰店'],
            ['jd', 1000088001, 'JD SELF RUN', '米客米酒京东自营旗舰店'],
            ['jd', 653667, 'JD OTHER POP', '米赞国际中外名酒专营店'],
            ['jd', 12961, 'JD OTHER POP', '民酒网官方旗舰店'],
            ['jd', 672082, 'JD OTHER POP', '民酒网红酒旗舰店'],
            ['jd', 51819, 'JD OTHER FSS', '莫高官 方旗舰店'],
            ['jd', 1000089564, 'JD SELF RUN', '莫高葡萄酒京东自营旗舰店'],
            ['jd', 804272, 'JD OTHER POP', '慕拉酒类旗舰店'],
            ['jd', 743540, 'JD OTHER POP', '南非国家酒馆'],
            ['jd', 712497, 'JD OTHER POP', '偶醉酒类专营店'],
            ['jd', 48459, 'JD OTHER POP', '派斯顿酒类专卖店'],
            ['jd', 896815, 'JD OTHER POP', '派斯顿旗舰店'],
            ['jd', 847606, 'JD OTHER POP', '派斯顿乔森专卖店'],
            ['jd', 942341, 'JD SELF RUN', '派斯顿洋酒京东自营旗舰店'],
            ['jd', 828450, 'JD OTHER POP', '彭索酒庄旗舰店'],
            ['jd', 850893, 'JD OTHER POP', '品鉴洋酒旗舰店'],
            ['jd', 693096, 'JD OTHER POP', '品茗酒类专营店'],
            ['jd', 51360, 'JD OTHER POP', '品牌贸易直营店'],
            ['jd', 35773, 'JD OTHER POP', '品尚汇旗舰店'],
            ['jd', 622463, 'JD OTHER POP', '品尚优选官方旗舰店'],
            ['jd', 618618, 'JD OTHER POP', '品正源清酒类旗舰店'],
            ['jd', 584089, 'JD OTHER POP', '榀酒酒类专营店'],
            ['jd', 1000086792, 'JD SELF RUN', '圃田冰青青梅酒京东自营旗舰店'],
            ['jd', 814913, 'JD OTHER POP', '七酒酒类专营店'],
            ['jd', 652491, 'JD OTHER POP', '千叶酒类专营店'],
            ['jd', 135933, 'JD OTHER POP', '谦法酒类专营店'],
            ['jd', 629185, 'JD OTHER POP', '谦法中外名酒专营店'],
            ['jd', 690222, 'JD OTHER POP', '乔森酒类专营店'],
            ['jd', 627258, 'JD OTHER POP', '青出于蓝酒类专营店'],
            ['jd', 729877, 'JD OTHER POP', '青出于蓝中外名酒专营店'],
            ['jd', 662066, 'JD OTHER POP', '融众恒泰酒类专营店'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京东自营官方旗舰店'],
            ['jd', 784180, 'JD OTHER POP', '锐达酒类专营店'],
            ['jd', 690779, 'JD OTHER POP', '瑞丰壹品官方旗舰店'],
            ['jd', 182149, 'JD OTHER POP', '萨拉维官方旗舰店'],
            ['jd', 27064, 'JD OTHER POP', '萨拉维酒业旗舰店'],
            ['jd', 617595, 'JD OTHER POP', '塞德菲贸易官方旗舰店'],
            ['jd', 1000100881, 'JD SELF RUN', '三得利洋酒自营店'],
            ['jd', 598847, 'JD SELF RUN', '山姆会员商店京 东自营官方旗舰店'],
            ['jd', 1000097196, 'JD SELF RUN', '山图葡萄酒京东自营旗舰店'],
            ['jd', 1000091648, 'JD SELF RUN', '圣丽塔葡萄酒京东自营旗舰店'],
            ['jd', 64881, 'JD OTHER POP', '圣品食品专营店'],
            ['jd', 1000114761, 'JD SELF RUN', '圣芝葡萄酒京东自营旗舰店'],
            ['jd', 619819, 'JD OTHER POP', '盛世普达酒类专营店'],
            ['jd', 716773, 'JD OTHER POP', '师捷酒水专营店'],
            ['jd', 876785, 'JD OTHER POP', '十里九品旗舰店'],
            ['jd', 37889, 'JD OTHER POP', '拾全酒美官方旗舰店'],
            ['jd', 598669, 'JD OTHER POP', '世界名酒专营店'],
            ['jd', 123515, 'JD OTHER POP', '双宏天乐酒类旗舰店'],
            ['jd', 1000154003, 'JD SELF RUN', '顺昌源京东自营旗舰店'],
            ['jd', 601663, 'JD OTHER POP', '思萨美康酒类专营店'],
            ['jd', 816861, 'JD OTHER POP', '苏麦威官方旗舰店'],
            ['jd', 1000105659, 'JD SELF RUN', '苏州桥京东自营旗舰店'],
            ['jd', 751994, 'JD OTHER POP', '苏州桥旗舰店'],
            ['jd', 609057, 'JD OTHER POP', '随猴酒类专营店'],
            ['jd', 1000225181, 'JD SELF RUN', '獭祭京东自营旗舰店'],
            ['jd', 178142, 'JD OTHER POP', '泰和酩庄官方旗舰店'],
            ['jd', 1000121721, 'JD SELF RUN', '桃乐丝葡萄酒京东自营旗舰店'],
            ['jd', 741352, 'JD OTHER POP', '特马桐酒类专营店'],
            ['jd', 79194, 'JD OTHER POP', '腾易红酒官方旗舰店'],
            ['jd', 645612, 'JD OTHER POP', '天阶官方旗舰店'],
            ['jd', 726062, 'JD OTHER POP', '听花语旗舰店'],
            ['jd', 810137, 'JD OTHER POP', '同福永官方旗舰店'],
            ['jd', 776922, 'JD OTHER POP', '兔兔冷泉酒官方旗舰店'],
            ['jd', 1000089230, 'JD SELF RUN', '王朝葡萄酒京东自营旗舰店'],
            ['jd', 685997, 'JD OTHER POP', '威爵酒类专营店'],
            ['jd', 1000080006, 'JD SELF RUN', '威龙葡萄酒京东自营旗舰店'],
            ['jd', 23686, 'JD OTHER POP', '威 赛帝斯官方旗舰店'],
            ['jd', 920233, 'JD SELF RUN', '威赛帝斯酒水京东自营旗舰店'],
            ['jd', 653155, 'JD OTHER POP', '威赛帝斯葡萄酒精品店'],
            ['jd', 1000130161, 'JD SELF RUN', '微醺洋酒京东自营专区'],
            ['jd', 659016, 'JD OTHER POP', '沃尔玛官方旗舰店'],
            ['jd', 659016, 'JD SELF RUN', '沃尔玛京东自营官方旗舰店'],
            ['jd', 790230, 'JD OTHER POP', '五福源酒类专营店'],
            ['jd', 778289, 'JD OTHER POP', '五洲海购酒类旗舰店'],
            ['jd', 192182, 'JD OTHER POP', '伍壹玖酒类专营店'],
            ['jd', 1000090387, 'JD SELF RUN', '西班牙葡萄酒京东自营专区'],
            ['jd', 1000078272, 'JD SELF RUN', '西夫拉姆葡萄酒京东自营旗舰店'],
            ['jd', 806900, 'JD OTHER POP', '西域烈焰 旗舰店'],
            ['jd', 179651, 'JD OTHER POP', '昔澜酒类专营店'],
            ['jd', 1000112381, 'JD SELF RUN', '香奈葡萄酒京东自营旗舰店'],
            ['jd', 40837, 'JD OTHER POP', '橡树 之约葡萄酒优选店'],
            ['jd', 135482, 'JD OTHER POP', '学权酒类专营店'],
            ['jd', 17784, 'JD PRC EC POP', '也买酒官方旗舰店'],
            ['jd', 31942, 'JD PRC EC POP', '也买 酒旗舰店'],
            ['jd', 47992, 'JD OTHER POP', '伊樽月亮谷酒类专营店'],
            ['jd', 144102, 'JD OTHER POP', '壹捌捌久官方旗舰店'],
            ['jd', 174090, 'JD OTHER POP', '宜信酒类旗舰店'],
            ['jd', 774766, 'JD OTHER POP', '邑品酒类专营店'],
            ['jd', 204363, 'JD OTHER POP', '崟泰酒类专营店'],
            ['jd', 943285, 'JD SELF RUN', '优红国际京东自营旗舰店'],
            ['jd', 14838, 'JD OTHER POP', '优红酒'],
            ['jd', 685911, 'JD OTHER POP', '优葡荟官方旗舰店'],
            ['jd', 12193, 'JD OTHER POP', '有家红酒官方旗舰店'],
            ['jd', 790107, 'JD PRC EC POP', '御玖轩官方旗舰店'],
            ['jd', 128287, 'JD OTHER POP', '御隆轩红酒专营店'],
            ['jd', 617388, 'JD OTHER POP', '元合陶艺红酒旗舰店'],
            ['jd', 54761, 'JD OTHER POP', '原产团旗舰店'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷酒类专营店'],
            ['jd', 47992, 'JD OTHER POP', '月氏良园'],
            ['jd', 650742, 'JD OTHER POP', '月氏人酒类专营店'],
            ['jd', 751364, 'JD OTHER POP', '月氏人洋酒专营店'],
            ['jd', 659112, 'JD OTHER POP', '云 的酒类专营店'],
            ['jd', 58502, 'JD OTHER FSS', '张裕官方旗舰店'],
            ['jd', 1000004718, 'JD SELF RUN', '张裕葡萄酒京东自营旗舰店'],
            ['jd', 1000090684, 'JD SELF RUN', '长白山京东自营旗舰店'],
            ['jd', 50080, 'JD OTHER POP', '长白山特产专营店'],
            ['jd', 757260, 'JD OTHER POP', '长城葡萄酒官方旗舰店'],
            ['jd', 1000004719, 'JD SELF RUN', '长城葡萄酒京东自营旗舰店'],
            ['jd', 38608, 'JD OTHER POP', '真露旗舰店'],
            ['jd', 1000090830, 'JD SELF RUN', '智利葡萄酒京东自营专区'],
            ['jd', 1000080527, 'JD SELF RUN', '智象葡萄酒京东自营旗舰店'],
            ['jd', 547312, 'JD OTHER POP', '中国特产·泸州馆'],
            ['jd', 704519, 'JD OTHER POP', '中粮名庄荟酒类旗舰店'],
            ['jd', 582537, 'JD OTHER POP', '中粮名庄荟旗舰店'],
            ['jd', 765450, 'JD OTHER POP', '中葡汇官方旗舰店'],
            ['jd', 199202, 'JD OTHER POP', '中浦耐杯酒类专营店'],
            ['jd', 34451, 'JD OTHER POP', '醉鹅娘葡萄酒旗舰店'],
            ['jd', 793598, 'JD OTHER POP', '醉美酒酒类专营店'],
            ['jd', 846070, 'JD OTHER POP', '醉梦红酒旗舰店'],
            ['tmall', 181936762, 'TMALL PRC FSS', 'chivas芝华士官方旗舰店'],
            ['tmall', 67726749, 'TMALL PRC POP', '宝树行酒类旗舰店'],
            ['tmall', 9730213, 'TMALL PRC FSS', '保乐力 加官方旗舰店'],
            ['tmall', 181925703, 'TMALL PRC EC POP', '池陈辉酒类专营店'],
            ['tmall', 945237, 'TMALL PRC EC POP', '登岑酒水专营店'],
            ['tmall', 182340153, 'TMALL PRC POP', '高华仕酒类专营店'],
            ['tmall', 64070709, 'TMALL OTHER FSS', '杰克丹尼官方旗舰店'],
            ['tmall', 61276625, 'TMALL PRC FSS', 'absolut官方旗舰店'],
            ['tmall', 2561079, 'TMALL PRC EC POP', '郎家园酒类专营店'],
            ['tmall', 189367760, 'TMALL OTHER FSS', '酩悦轩尼诗官方旗舰店'],
            ['tmall', 60553350, 'TMALL OTHER FSS', '人头马官方旗舰店'],
            ['tmall', 9281929, 'TMALL SUPER', '天猫超市'],
            ['tmall', 189673674, 'TMALL OTHER POP', '侠风酒类专营店'],
            ['tmall', 59180296, 'TMALL PRC EC POP', '旭隆久合酒类专营店'],
            ['tmall', 190864630, 'TMALL PRC EC POP', '寅瑜酒类专营店'],
            ['tmall', 16994272, 'TMALL OTHER FSS', '尊尼获加官方旗舰店'],
            ['tmall', 192635616, 'TMALL PRC EC POP', '奈思酒类专营店'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利官方旗舰店'],
            ['tmall', 60309695, 'TMALL PRC EC POP', '御玖轩酒类专营店'],
            ['tmall', 168343869, 'TMALL OTHER FSS', 'macallan旗舰店'],
            ['tmall', 61123847, 'TMALL OTHER FSS', 'rio锐澳旗舰店'],
            ['tmall', 63282256, 'TMALL OTHER FSS', '百利官方旗舰店'],
            ['tmall', 67937129, 'TMALL OTHER FSS', '帝亚吉欧洋酒官方旗舰店'],
            ['tmall', 163407529, 'TMALL OTHER FSS', '翰格蓝爵官方 旗舰店'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利旗舰店'],
            ['tmall', 16433652, 'TMALL OTHER FSS', 'asc官方旗舰店'],
            ['tmall', 186938208, 'TMALL OTHER POP', '云的食品专营店'],
            ['tmall', 10795084, 'TMALL OTHER POP', '艾森特酒类专营店'],
            ['tmall', 174541689, 'TMALL OTHER POP', '谦法酒类专营店'],
            ['tmall', 179501854, 'TMALL OTHER POP', '梦扬酒类专营店'],
            ['tmall', 945362, 'TMALL OTHER POP', '月亮谷酒类专营店'],
            ['tmall', 167186023, 'TMALL OTHER POP', '中浦耐杯酒类专营店'],
            ['tmall', 64886808, 'TMALL OTHER POP', '富水食品专营店'],
            ['tmall', 9282437, 'TMALL OTHER POP', '江秀酒类专营店'],
            ['tmall', 189890305, 'TMALL OTHER POP', '泰圣原酒类专营店'],
            ['tmall', 66986595, 'TMALL OTHER POP', '广存禄酒类专营店'],
            ['tmall', 190184104, 'TMALL OTHER POP', 'ak47酒类旗舰店'],
            ['tmall', 190859232, 'TMALL OTHER FSS', 'aldi旗舰店'],
            ['tmall', 167940348, 'TMALL OTHER FSS', 'asahi朝日旗舰店'],
            ['tmall', 186802661, 'TMALL OTHER FSS', 'baraka酒类旗舰店'],
            ['tmall', 190833445, 'TMALL OTHER FSS', 'beamsuntory宾三得利旗舰'],
            ['tmall', 181931354, 'TMALL OTHER FSS', 'bols波士力娇酒旗舰店'],
            ['tmall', 190759564, 'TMALL OTHER FSS', 'camus卡慕官方旗舰店'],
            ['tmall', 192980474, 'TMALL OTHER FSS', 'choya俏雅旗舰店'],
            ['tmall', 9338011, 'TMALL OTHER FSS', 'cmp旗舰店'],
            ['tmall', 189712524, 'TMALL OTHER FSS', 'costco官方旗舰店'],
            ['tmall', 189222031, 'TMALL OTHER FSS', 'dekuyper海外旗舰店'],
            ['tmall', 187805167, 'TMALL OTHER FSS', 'emp海外旗舰店'],
            ['tmall', 192599080, 'TMALL OTHER FSS', 'fastking莎堡皇旗舰店'],
            ['tmall', 186605891, 'TMALL OTHER FSS', 'fourloko旗舰店'],
            ['tmall', 186902811, 'TMALL OTHER FSS', 'giv旗舰店'],
            ['tmall', 188755410, 'TMALL OTHER FSS', 'hilbaton希伯顿旗舰店'],
            ['tmall', 192127385, 'TMALL OTHER FSS', 'ibanesas伊柏妮莎旗舰店'],
            ['tmall', 188185494, 'TMALL OTHER FSS', 'lffo旗舰店'],
            ['tmall', 188150563, 'TMALL OTHER FSS', 'meukow墨高旗舰店'],
            ['tmall', 186963464, 'TMALL OTHER FSS', 'montes旗舰店'],
            ['tmall', 181936150, 'TMALL OTHER FSS', 'rekorderlig旗舰店'],
            ['tmall', 189285528, 'TMALL OTHER FSS', 'sinodrink华饮旗舰店'],
            ['tmall', 68372177, 'TMALL OTHER FSS', 'suamgy圣芝官方旗舰店'],
            ['tmall', 192825647, 'TMALL OTHER FSS', 'wemyss旗舰店'],
            ['tmall', 187740248, 'TMALL OTHER FSS', '阿道克旗舰店'],
            ['tmall', 60268731, 'TMALL OTHER POP', '爱屋食品专营店'],
            ['tmall', 187217166, 'TMALL OTHER FSS', '安特旗舰店'],
            ['tmall', 67036345, 'TMALL OTHER POP', '安酌酒类专营店'],
            ['tmall', 186905859, 'TMALL OTHER POP', '奥蒂兰丝旗舰店'],
            ['tmall', 63676728, 'TMALL OTHER POP', '奥美圣酒类专营店'],
            ['tmall', 190745141, 'TMALL OTHER POP', '奥图纳旗舰店'],
            ['tmall', 190152783, 'TMALL OTHER POP', '奥雪谷酒类专营店'],
            ['tmall', 169343891, 'TMALL OTHER POP', '澳迪尼旗舰店'],
            ['tmall', 185708057, 'TMALL OTHER POP', '芭诺斯酒类专营店'],
            ['tmall', 181935488, 'TMALL OTHER POP', '白洋河酒类旗舰店'],
            ['tmall', 189458091, 'TMALL OTHER POP', '百醇品逸酒类专营店'],
            ['tmall', 192831218, 'TMALL OTHER POP', '百富门酒类旗舰店'],
            ['tmall', 5666084, 'TMALL PRC EC POP', '百正利酒类专营店'],
            ['tmall', 3743705, 'TMALL OTHER POP', '柏特酒类专营店'],
            ['tmall', 10794997, 'TMALL OTHER POP', '杯中时光酒类专营店'],
            ['tmall', 191604668, 'TMALL OTHER POP', '北京榜样酒类 专营店'],
            ['tmall', 64030717, 'TMALL OTHER POP', '北京酒葫芦酒类专营店'],
            ['tmall', 60006673, 'TMALL OTHER POP', '北京亿万典藏酒类专营店'],
            ['tmall', 69685590, 'TMALL OTHER POP', '北京智恒达商食品专营店'],
            ['tmall', 7602595, 'TMALL OTHER POP', '奔弛酒类专营店'],
            ['tmall', 192617571, 'TMALL OTHER POP', '奔富极昼专卖店'],
            ['tmall', 69111791, 'TMALL OTHER POP', '本初酒类专营店'],
            ['tmall', 187739761, 'TMALL OTHER POP', '必得利旗舰店'],
            ['tmall', 189534060, 'TMALL OTHER POP', '碧波林海酒类专营店'],
            ['tmall', 192127388, 'TMALL OTHER POP', '镖士旗舰店'],
            ['tmall', 8121431, 'TMALL OTHER POP', '槟悦酒类专营店'],
            ['tmall', 189239808, 'TMALL OTHER POP', '波尔亚葡萄酒旗舰店'],
            ['tmall', 163744402, 'TMALL OTHER POP', '波菲摩根酒类专营店'],
            ['tmall', 191517232, 'TMALL OTHER POP', '伯爵酒类专营店'],
            ['tmall', 191976497, 'TMALL OTHER POP', '布多格旗舰店'],
            ['tmall', 64528280, 'TMALL OTHER POP', '昌盛德酒类专营店'],
            ['tmall', 945418, 'TMALL OTHER POP', '畅健酒类专营店'],
            ['tmall', 189151241, 'TMALL OTHER POP', '诚善堂酒类专营'],
            ['tmall', 181936162, 'TMALL OTHER POP', '初生活酒类企业店'],
            ['tmall', 192103483, 'TMALL OTHER POP', '触角食品专营店'],
            ['tmall', 188943083, 'TMALL OTHER POP', '春融食品专营店'],
            ['tmall', 163796968, 'TMALL OTHER POP', '春彦佳业酒类专营店'],
            ['tmall', 181936050, 'TMALL OTHER POP', '第7元素旗舰店'],
            ['tmall', 945291, 'TMALL OTHER POP', '东晟酒类专营店'],
            ['tmall', 190610751, 'TMALL OTHER POP', '法多 克旗舰店'],
            ['tmall', 188811200, 'TMALL OTHER POP', '法尔城堡旗舰店'],
            ['tmall', 186503318, 'TMALL OTHER POP', '法夫尔堡旗舰店'],
            ['tmall', 7665680, 'TMALL OTHER POP', '菲利特酒类专营店'],
            ['tmall', 165603201, 'TMALL OTHER POP', '菲尼雅酒类专营店'],
            ['tmall', 187418569, 'TMALL OTHER POP', '枫羿酒类专营店'],
            ['tmall', 945320, 'TMALL OTHER POP', '福客仕酒类专营店'],
            ['tmall', 190892990, 'TMALL OTHER POP', '福洮食品专营店'],
            ['tmall', 187236984, 'TMALL OTHER POP', '富瑞斯旗舰店'],
            ['tmall', 192827257, 'TMALL PRC EC POP', '富以邻酒类专营店'],
            ['tmall', 181903836, 'TMALL OTHER POP', '富邑葡萄酒集团旗舰店'],
            ['tmall', 191834815, 'TMALL OTHER POP', '高地旗舰店'],
            ['tmall', 191669504, 'TMALL OTHER POP', '歌然酒类专营店'],
            ['tmall', 184272306, 'TMALL OTHER FSS', '格拉洛旗舰店'],
            ['tmall', 192175320, 'TMALL OTHER FSS', '格兰父子旗舰店'],
            ['tmall', 11712171, 'TMALL OTHER POP', '购酒网官方旗舰店'],
            ['tmall', 188192538, 'TMALL OTHER POP', '购酒网葡萄酒 旗舰店'],
            ['tmall', 62171152, 'TMALL OTHER POP', '古仓酒类专营店'],
            ['tmall', 189705131, 'TMALL OTHER POP', '顾悦酒类专营店'],
            ['tmall', 188405924, 'TMALL OTHER POP', '冠振酒类专营店'],
            ['tmall', 191234834, 'TMALL OTHER POP', '广州美助达酒类专营店'],
            ['tmall', 190785471, 'TMALL OTHER POP', '哈力高旗舰店'],
            ['tmall', 190145492, 'TMALL OTHER POP', '海优食品专营店'],
            ['tmall', 191900950, 'TMALL OTHER POP', '浩丰隆睿酒类专营店'],
            ['tmall', 8437261, 'TMALL OTHER POP', '和道酒类专营店'],
            ['tmall', 192144831, 'TMALL PRC EC POP', '宏放酒类专营店'],
            ['tmall', 9282647, 'TMALL OTHER POP', '泓利酒类专营店'],
            ['tmall', 59504827, 'TMALL OTHER POP', '洪英华泰酒类专营店'],
            ['tmall', 190174879, 'TMALL OTHER POP', '浣熙食品专营店'],
            ['tmall', 191166470, 'TMALL OTHER FSS', '黄尾袋鼠官方旗舰店'],
            ['tmall', 189224565, 'TMALL OTHER POP', '汇泉酒类专营店'],
            ['tmall', 10182220, 'TMALL OTHER POP', '吉量食品专营店'],
            ['tmall', 945372, 'TMALL OTHER POP', '加枫红进口 红酒专营'],
            ['tmall', 191717130, 'TMALL OTHER POP', '加州乐事旗舰店'],
            ['tmall', 945585, 'TMALL OTHER POP', '嘉美醇酒类专营店'],
            ['tmall', 70102414, 'TMALL OTHER POP', '简拙酒类专营店'],
            ['tmall', 190802928, 'TMALL OTHER POP', '将军井山东专卖店'],
            ['tmall', 70420850, 'TMALL OTHER POP', '将军井梓轩专卖店'],
            ['tmall', 163055240, 'TMALL OTHER POP', '杰奥森酒类专营店'],
            ['tmall', 191071067, 'TMALL OTHER POP', '今朝美酒类专营店'],
            ['tmall', 192027065, 'TMALL OTHER POP', '金巴厘旗舰店'],
            ['tmall', 19694041, 'TMALL OTHER POP', '金车食品专营店'],
            ['tmall', 945227, 'TMALL OTHER POP', '金都恒泰酒类专营'],
            ['tmall', 192345241, 'TMALL OTHER POP', '金利莎旗舰店'],
            ['tmall', 12346794, 'TMALL OTHER POP', '金源鹏祥酒类专营店'],
            ['tmall', 189584370, 'TMALL OTHER POP', '金泽恒酒类专营'],
            ['tmall', 19869513, 'TMALL OTHER POP', '金樽格兰酒类专营店'],
            ['tmall', 68606331, 'TMALL OTHER POP', '京鲁信达酒类专营店'],
            ['tmall', 192847223, 'TMALL OTHER POP', '京瑞酒类专营店'],
            ['tmall', 190603106, 'TMALL OTHER POP', '精酉酒类专营店'],
            ['tmall', 67721348, 'TMALL OTHER POP', '景泰蓝酒类专营店'],
            ['tmall', 3928664, 'TMALL OTHER POP', '久加久酒博汇旗舰店'],
            ['tmall', 70101091, 'TMALL OTHER POP', '玖品酒类专营店'],
            ['tmall', 188099938, 'TMALL OTHER POP', '酒富盛酩酒类专营店'],
            ['tmall', 191944097, 'TMALL OTHER POP', '酒购乐酒类专营店'],
            ['tmall', 163340849, 'TMALL OTHER POP', '酒嗨酒酒类专营店'],
            ['tmall', 188435285, 'TMALL OTHER POP', '酒 玩家酒类专营店'],
            ['tmall', 6112291, 'TMALL JX POP', '酒仙网官方旗舰店'],
            ['tmall', 186803036, 'TMALL JX POP', '酒仙网葡萄酒旗舰店'],
            ['tmall', 166872422, 'TMALL OTHER POP', '酒怡酒类专营店'],
            ['tmall', 192837172, 'TMALL OTHER POP', '卡爹拉酒类旗舰店'],
            ['tmall', 191669304, 'TMALL OTHER POP', '卡普酒类专营店'],
            ['tmall', 188297880, 'TMALL OTHER POP', '卡诗图旗舰店'],
            ['tmall', 192066196, 'TMALL OTHER POP', '卡图酒类专营店'],
            ['tmall', 8440231, 'TMALL OTHER POP', '开为酒类 专营店'],
            ['tmall', 169821549, 'TMALL OTHER POP', '蓝利酒类专营店'],
            ['tmall', 190651317, 'TMALL OTHER POP', '乐喝酒类专营店'],
            ['tmall', 187445065, 'TMALL OTHER POP', '乐斯卡酒类专营店'],
            ['tmall', 163237776, 'TMALL OTHER POP', '雷辉酒类专营店'],
            ['tmall', 65759084, 'TMALL OTHER POP', '类人首旗舰店'],
            ['tmall', 69239662, 'TMALL OTHER POP', '李氏兄弟酒类专营店'],
            ['tmall', 189528060, 'TMALL OTHER POP', '力泉酒类专营店'],
            ['tmall', 189179357, 'TMALL OTHER POP', '丽乐威酒类 专营店'],
            ['tmall', 69665044, 'TMALL OTHER POP', '丽日蓝天酒类专营店'],
            ['tmall', 189131471, 'TMALL OTHER POP', '刘嘉玲生活品味海外旗舰店'],
            ['tmall', 168772523, 'TMALL OTHER POP', '龙马酒类专营店'],
            ['tmall', 64526011, 'TMALL OTHER POP', '隆斐堡旗舰店'],
            ['tmall', 185245516, 'TMALL OTHER POP', '路易马西尼旗舰店'],
            ['tmall', 190554679, 'TMALL OTHER POP', '路易十三官方旗舰店'],
            ['tmall', 181622804, 'TMALL OTHER POP', '罗莎酒类旗舰店'],
            ['tmall', 190826300, 'TMALL OTHER POP', '罗提斯酒类旗舰店'],
            ['tmall', 9666574, 'TMALL OTHER POP', '罗提斯酒类专营店'],
            ['tmall', 192857829, 'TMALL OTHER POP', '洛克之羽旗舰店'],
            ['tmall', 172944703, 'TMALL OTHER POP', '麦德龙官方海外旗舰店'],
            ['tmall', 170355320, 'TMALL OTHER POP', '麦德龙官方旗舰店'],
            ['tmall', 162888716, 'TMALL OTHER POP', '美圣酒类专营店'],
            ['tmall', 181928781, 'TMALL OTHER POP', '美助达酒类专营店'],
            ['tmall', 173542702, 'TMALL OTHER POP', '蒙大菲旗舰店'],
            ['tmall', 20101735, 'TMALL OTHER POP', '孟特罗斯庄园酒类专营店'],
            ['tmall', 190406543, 'TMALL OTHER POP', '米宝拓酒类专营店'],
            ['tmall', 192107315, 'TMALL OTHER POP', '名仕罗纳德酒类旗舰店'],
            ['tmall', 10177878, 'TMALL OTHER POP', '莫高官方旗舰店'],
            ['tmall', 185163884, 'TMALL OTHER POP', '慕拉旗舰店'],
            ['tmall', 167698271, 'TMALL OTHER POP', '尼雅 酒类旗舰店'],
            ['tmall', 186920958, 'TMALL OTHER POP', '宁夏葡萄酒官方旗舰店'],
            ['tmall', 190733669, 'TMALL OTHER POP', '欧利千隆酒类旗舰店'],
            ['tmall', 190743351, 'TMALL OTHER POP', '欧绅酒类旗舰店'],
            ['tmall', 17437054, 'TMALL OTHER POP', '派斯顿酒类专营店'],
            ['tmall', 189816418, 'TMALL OTHER POP', '潘果旗舰店'],
            ['tmall', 185200804, 'TMALL OTHER POP', '朋珠酒类旗舰店'],
            ['tmall', 68315639, 'TMALL OTHER POP', '品厨食品专营店'],
            ['tmall', 4200399, 'TMALL OTHER POP', '品尚红酒官方旗舰店'],
            ['tmall', 165081244, 'TMALL OTHER POP', '品天下酒类专营店'],
            ['tmall', 191885632, 'TMALL OTHER POP', '葡萄酒小皮旗舰店'],
            ['tmall', 192040162, 'TMALL OTHER POP', '葡萄酿酒类专营店'],
            ['tmall', 187357317, 'TMALL OTHER POP', '祺彬酒类专营店'],
            ['tmall', 20619935, 'TMALL OTHER POP', '巧厨食品专营店'],
            ['tmall', 64545265, 'TMALL OTHER POP', '清之光食品专营店'],
            ['tmall', 190745134, 'TMALL OTHER POP', '锐澳师捷专卖店'],
            ['tmall', 189528055, 'TMALL OTHER POP', '锐澳硕悦专卖店'],
            ['tmall', 192898261, 'TMALL OTHER POP', '上海酒玩家酒类专营店'],
            ['tmall', 182339809, 'TMALL OTHER POP', '上海学权酒类专营店'],
            ['tmall', 191407553, 'TMALL OTHER POP', '舍岭酒类专营店'],
            ['tmall', 59208730, 'TMALL OTHER POP', '升升食品专营店'],
            ['tmall', 192324546, 'TMALL OTHER POP', '晟阳酒类专营 店'],
            ['tmall', 167840211, 'TMALL OTHER POP', '盛世普达酒类专营店'],
            ['tmall', 191205218, 'TMALL OTHER POP', '盛卫酒类专营店'],
            ['tmall', 192830888, 'TMALL OTHER POP', '守望之鹰葡萄酒旗舰店'],
            ['tmall', 192855845, 'TMALL OTHER POP', '曙云食品专营店'],
            ['tmall', 189118013, 'TMALL OTHER POP', '苏佳利旗舰店'],
            ['tmall', 172696000, 'TMALL OTHER POP', '苏宁易购官方旗舰店'],
            ['tmall', 190709131, 'TMALL OTHER POP', '塔希葡萄酒旗舰店'],
            ['tmall', 192848674, 'TMALL OTHER POP', '獭 祭旗舰店'],
            ['tmall', 170846288, 'TMALL OTHER POP', '特其拉之家'],
            ['tmall', 16476202, 'TMALL OTHER POP', '腾易永立酒类专营店'],
            ['tmall', 61304831, 'TMALL OTHER POP', '天尝地酒酒类专营店'],
            ['tmall', 170685533, 'TMALL OTHER POP', '天猫国际官方直营'],
            ['tmall', 181871121, 'TMALL OTHER POP', '天猫国际官方直营国内现 货'],
            ['tmall', 61531947, 'TMALL OTHER POP', '天添乐酒类专营店'],
            ['tmall', 188453101, 'TMALL OTHER POP', '天源食品专营店'],
            ['tmall', 186990195, 'TMALL OTHER POP', '通化旗舰店'],
            ['tmall', 191715391, 'TMALL OTHER POP', '兔兔冷泉酒旗舰店'],
            ['tmall', 181932183, 'TMALL OTHER POP', '万酒酒类专营'],
            ['tmall', 9327449, 'TMALL OTHER POP', '王松酒类专营店'],
            ['tmall', 945416, 'TMALL OTHER POP', '威赛帝斯酒类专营'],
            ['tmall', 191900921, 'TMALL OTHER POP', '薇林酒类旗舰店'],
            ['tmall', 190654972, 'TMALL OTHER POP', '维定酒类专营店'],
            ['tmall', 190338990, 'TMALL OTHER POP', '伟贞酒类专营店'],
            ['tmall', 190760637, 'TMALL OTHER POP', '卫宾 酒类旗舰店'],
            ['tmall', 169359891, 'TMALL OTHER POP', '武若酒类专营店'],
            ['tmall', 60318412, 'TMALL OTHER POP', '西夫拉姆旗舰店'],
            ['tmall', 190597213, 'TMALL OTHER POP', '夏多纳酒类专营店'],
            ['tmall', 17374987, 'TMALL OTHER POP', '相拓酒类专营店'],
            ['tmall', 181931513, 'TMALL OTHER POP', '祥栈酒类专营店'],
            ['tmall', 189345451, 'TMALL OTHER POP', '小冰酒类专营店'],
            ['tmall', 169403866, 'TMALL OTHER POP', '小飞象酒类专营店'],
            ['tmall', 12837183, 'TMALL OTHER POP', '新发食品专营店'],
            ['tmall', 188471711, 'TMALL OTHER POP', '新启达酒类专营店'],
            ['tmall', 163519396, 'TMALL OTHER POP', '星韩亚食品专营店'],
            ['tmall', 184984900, 'TMALL OTHER POP', '炫品酒类专营店'],
            ['tmall', 59504847, 'TMALL OTHER POP', '雅醇酒类专营店'],
            ['tmall', 67643980, 'TMALL OTHER POP', '阳光万国酒类专营店'],
            ['tmall', 945356, 'TMALL OTHER POP', '也买酒官方旗舰店'],
            ['tmall', 181939573, 'TMALL OTHER POP', '依仕迪旗舰店'],
            ['tmall', 9282683, 'TMALL OTHER POP', '壹玖壹玖官方旗舰店'],
            ['tmall', 188113645, 'TMALL OTHER POP', '艺术玻璃旗舰店'],
            ['tmall', 192626419, 'TMALL OTHER POP', '邑品酒类专营店'],
            ['tmall', 187259490, 'TMALL OTHER POP', '逸香酒类旗舰店'],
            ['tmall', 182339346, 'TMALL OTHER POP', '崟泰食品专营店'],
            ['tmall', 61754686, 'TMALL OTHER POP', '雍天酒类专营店'],
            ['tmall', 64482742, 'TMALL OTHER POP', '裕诚酒类旗舰店'],
            ['tmall', 165334654, 'TMALL OTHER POP', '裕泉酒类专营店'],
            ['tmall', 70606664, 'TMALL OTHER POP', '誉佳顺酒类专营店'],
            ['tmall', 6697591, 'TMALL OTHER POP', '源宏酒类折扣店'],
            ['tmall', 945015, 'TMALL OTHER FSS', '张裕官方旗舰店'],
            ['tmall', 188057778, 'TMALL OTHER POP', ' 张裕长庆和专卖店'],
            ['tmall', 5400262, 'TMALL OTHER POP', '长城葡萄酒官方旗舰店'],
            ['tmall', 174412493, 'TMALL OTHER POP', '赵薇梦陇酒庄旗舰店'],
            ['tmall', 69208154, 'TMALL OTHER POP', '哲畅酒类专营店'],
            ['tmall', 186600047, 'TMALL OTHER POP', '喆园酒类专营店'],
            ['tmall', 185354125, 'TMALL OTHER POP', '真韩食品专营店'],
            ['tmall', 190319525, 'TMALL OTHER POP', '真露酒类旗舰店'],
            ['tmall', 187567202, 'TMALL OTHER POP', '正鸿食品专营店'],
            ['tmall', 192876165, 'TMALL OTHER POP', '正善酒类专营店'],
            ['tmall', 60948875, 'TMALL OTHER POP', '中酒网官方旗舰店'],
            ['tmall', 186903121, 'TMALL OTHER POP', '中粮名庄荟酒类旗舰店'],
            ['tmall', 189685253, 'TMALL OTHER POP', '众思创酒类专营店'],
            ['tmall', 192017787, 'TMALL OTHER POP', '众酉酒类专营店'],
            ['tmall', 186342503, 'TMALL OTHER POP', '卓羿酒类专营店'],
            ['tmall', 192075749, 'TMALL OTHER POP', '纵答食品专营店'],
            ['tmall', 190318752, 'TMALL OTHER POP', '醉鹅娘葡萄酒旗舰店'],
            ['tmall', 13262082, 'TMALL OTHER POP', '醉梦酒类专营店'],
            ['jd', 783024, 'JD OTHER POP', '京糖我要酒官方旗舰店'],
            ['jd', 778221, 'JD OTHER POP', '醴鱼酒类专营店'],
            ['jd', 35946, 'JD OTHER POP', 'ASC官方旗舰店'],
            ['jd', 117815, 'JD OTHER POP', '京糖我要酒旗舰店'],
            ['jd', 723860, 'JD OTHER POP', '旻彤精品酒业旗舰店'],
            ['tmall', 189890305, 'TMALL OTHER POP', 'whiskyl旗舰店'],
            ['tmall', 170685533, 'TMALL INTERNATIONAL', '天猫国际进口超市'],
            ['tmall', 181871121, 'TMALL INTERNATIONAL', '天猫国际进口超市国内现货'],
            ['jd', 670462, 'JD OTHER POP', 'Bannychoice海外旗舰店'],
            ['jd', 109604, 'JD OTHER POP', 'CMP巴黎庄园葡萄酒官方旗舰店'],
            ['jd', 1000084051, 'JD SELF RUN', 'DBR拉菲葡萄酒京东自营专卖店'],
            ['jd', 206860, 'JD OTHER POP', 'JOYVIO佳沃葡萄酒旗舰店'],
            ['jd', 1000189381, 'JD SELF RUN', 'MO�0�9T酩悦香槟自营官方旗舰店'],
            ['jd', 888220, 'JD OTHER FSS', 'POWER STATION动力火车旗舰店'],
            ['jd', 213398, 'JD OTHER FSS', '奥贝尔庄园红酒旗舰店'],
            ['jd', 692398, 'JD OTHER POP', '澳享葡萄酒专营店'],
            ['jd', 1000165881, 'JD SELF RUN', '八角星葡萄酒自营旗舰店'],
            ['jd', 880552, 'JD OTHER FSS', '八芒星酒类旗舰店'],
            ['jd', 777644, 'JD OTHER POP', '百醇品逸酒类专营店'],
            ['jd', 626202, 'JD OTHER FSS', '奔富麦克斯旗舰店'],
            ['jd', 1000100881, 'JD SELF RUN', '宾三得利洋酒自营店'],
            ['jd', 734930, 'JD OTHER FSS', '伯克英达官方旗舰店'],
            ['jd', 1000120985, 'JD SELF RUN', '布多格酒类京东自营专区'],
            ['jd', 588286, 'JD OTHER FSS', '超级波精品旗舰店'],
            ['jd', 140755, 'JD OTHER POP', '当歌国际酒类专营店'],
            ['jd', 1000090349, 'JD SELF RUN', '法国列级名庄酒京东自营专区'],
            ['jd', 863174, 'JD OTHER POP', '法国美酒精品店'],
            ['jd', 949978, 'JD SELF RUN', '菲特瓦葡萄酒京东自营官方旗舰店'],
            ['jd', 691720, 'JD OTHER POP', '福易浩酒类专营店'],
            ['jd', 720539, 'JD OTHER POP', '富德酒类专营店'],
            ['jd', 953900, 'JD OTHER FSS', '富瑞拉酒类官方旗舰店'],
            ['jd', 777602, 'JD OTHER FSS', '富森丽安葡萄酒官方旗舰店'],
            ['jd', 686900, 'JD OTHER FSS', '富邑葡萄酒海 外旗舰店'],
            ['jd', 1000091188, 'JD SELF RUN', '贺兰山葡萄酒京东自营旗舰店'],
            ['jd', 801770, 'JD OTHER POP', '恒众伟业葡萄酒专营店'],
            ['jd', 1000076993, 'JD SELF RUN', '红酒海外自营旗舰店'],
            ['jd', 709725, 'JD OTHER POP', '鸿大葡萄酒专营店'],
            ['jd', 183917, 'JD OTHER POP', '环球时代酒类专营店'],
            ['jd', 885816, 'JD OTHER POP', '汇泉洋酒专营店'],
            ['jd', 602024, 'JD OTHER FSS', '吉卖汇官方旗舰店'],
            ['jd', 98982, 'JD OTHER FSS', '集美旗舰店'],
            ['jd', 807471, 'JD OTHER FSS', '集颜旗舰店'],
            ['jd', 182514, 'JD OTHER POP', '加枫国际进口食品专营店'],
            ['jd', 688680, 'JD OTHER FSS', '加州乐事官方旗舰店'],
            ['jd', 925445, 'JD OTHER POP', '贾 真酒类专营店'],
            ['jd', 665479, 'JD OTHER POP', '杰奥森红酒专营店'],
            ['jd', 205310, 'JD OTHER POP', '京方丹酒类专营店'],
            ['jd', 106117, 'JD OTHER FSS', '京酒汇 官方旗舰店'],
            ['jd', 583357, 'JD OTHER POP', '九荣图酒类专营店'],
            ['jd', 783878, 'JD OTHER POP', '久悦久酒类专营店'],
            ['jd', 53424, 'JD OTHER FSS', '酒葫芦官方旗舰店'],
            ['jd', 201977, 'JD OTHER FSS', '酒惠网精品旗舰店'],
            ['jd', 106116, 'JD OTHER FSS', '酒联聚旗舰店'],
            ['jd', 606501, 'JD OTHER FSS', '酒品惠官方旗舰店'],
            ['jd', 868288, 'JD OTHER FSS', '酒速汇旗舰店'],
            ['jd', 32252, 'JD OTHER POP', '酒仙网精品旗舰店'],
            ['jd', 173196, 'JD OTHER FSS', '酒一站旗舰店'],
            ['jd', 724349, 'JD OTHER FSS', '聚藏旗舰店'],
            ['jd', 868417, 'JD OTHER FSS', '卡伯纳官方旗舰店'],
            ['jd', 802859, 'JD OTHER FSS', '卡聂高旗舰店'],
            ['jd', 992265, 'JD OTHER FSS', '凯特伊曼酒庄葡萄酒旗舰店'],
            ['jd', 1000091242, 'JD SELF RUN', '拉蒙葡萄酒京东自营旗舰店'],
            ['jd', 1000091203, 'JD SELF RUN', '乐朗1374葡萄酒京东自营 旗舰店'],
            ['jd', 766970, 'JD OTHER FSS', '勒度官方旗舰店'],
            ['jd', 841646, 'JD OTHER FSS', '雷司葡萄酒旗舰店'],
            ['jd', 659031, 'JD OTHER FSS', '雷司旗舰店'],
            ['jd', 1000072603, 'JD SELF RUN', '类人首葡萄酒京东自营旗舰店'],
            ['jd', 862679, 'JD OTHER POP', '利港葡萄酒专营店'],
            ['jd', 891054, 'JD OTHER FSS', '利藤葡萄酒 官方旗舰店'],
            ['jd', 708133, 'JD OTHER POP', '利盈红酒专营店'],
            ['jd', 853486, 'JD OTHER FSS', '琳赛葡萄酒旗舰店'],
            ['jd', 676859, 'JD OTHER FSS', '零利官方旗 舰店'],
            ['jd', 870978, 'JD OTHER FSS', '泸州老窖进口葡萄酒官方旗舰店'],
            ['jd', 717858, 'JD OTHER FSS', '吕森堡酒类旗舰店'],
            ['jd', 733753, 'JD OTHER FSS', '曼 妥思酒类旗舰店'],
            ['jd', 740451, 'JD OTHER FSS', '玫嘉官方旗舰店'],
            ['jd', 776736, 'JD OTHER FSS', '玫嘉旗舰店'],
            ['jd', 663701, 'JD OTHER POP', '美国国家酒馆'],
            ['jd', 728304, 'JD OTHER FSS', '美景庄园官方旗舰店'],
            ['jd', 685998, 'JD OTHER FSS', '美玫圣官方旗舰店'],
            ['jd', 622892, 'JD OTHER FSS', '酩品大师酒类旗舰店'],
            ['jd', 1000187770, 'JD SELF RUN', '酩悦轩尼诗帝亚吉欧MHD自营旗舰店'],
            ['jd', 107067, 'JD OTHER FSS', '南航发现会葡萄酒官方旗舰店'],
            ['jd', 780774, 'JD OTHER FSS', '宁夏贺兰山东麓青铜峡产区官方旗舰店'],
            ['jd', 809546, 'JD OTHER FSS', '女爵酒庄官方旗舰店'],
            ['jd', 780857, 'JD OTHER FSS', '佩蒂克斯葡萄酒官方旗舰店'],
            ['jd', 965804, 'JD SELF RUN', '品尚汇京东自营旗舰店'],
            ['jd', 10803, 'JD OTHER FSS', '品尚汇酒类旗舰店'],
            ['jd', 912282, 'JD OTHER FSS', '青岛保税港区官方旗 舰店'],
            ['jd', 1000135321, 'JD SELF RUN', '清酒屋京东自营专区'],
            ['jd', 753952, 'JD OTHER FSS', '萨克森堡酒类旗舰店'],
            ['jd', 190315, 'JD OTHER POP', '赛普利斯红酒专营店'],
            ['jd', 1000103573, 'JD OTHER POP', '三全测试店铺333'],
            ['jd', 804351, 'JD OTHER POP', '莎玛拉庄园酒类专营店'],
            ['jd', 1000281684, 'JD SELF RUN', '圣侯爵葡萄酒京东自营旗舰店'],
            ['jd', 583005, 'JD OTHER FSS', '十点品酒酒类旗舰店'],
            ['jd', 988739, 'JD OTHER FSS', '蜀兼香酒类旗舰店'],
            ['jd', 785877, 'JD OTHER POP', '蜀兼香酒类专营店'],
            ['jd', 1000159902, 'JD SELF RUN', '苏州桥酒京东自营旗舰店'],
            ['jd', 1000178125, 'JD SELF RUN', '獭祭（DASSAI）海外自营旗舰店'],
            ['jd', 779694, 'JD OTHER POP', '萄醇葡萄酒专营店'],
            ['jd', 1000078445, 'JD SELF RUN', '通化葡萄酒京东自营旗舰店'],
            ['jd', 654757, 'JD OTHER FSS', '万酒网红酒旗 舰店'],
            ['jd', 626312, 'JD OTHER FSS', '王朝葡萄酒旗舰店'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营专区'],
            ['jd', 1000091521, 'JD SELF RUN', '唯浓葡萄 酒京东自营旗舰店'],
            ['jd', 1000156067, 'JD SELF RUN', '夏桐Chandon自营旗舰店'],
            ['jd', 184977, 'JD OTHER FSS', '橡树之约旗舰店'],
            ['jd', 777642, 'JD OTHER POP', '新华酒类专营店'],
            ['jd', 1000101205, 'JD SELF RUN', '星得斯葡萄酒京东自营旗舰店'],
            ['jd', 852797, 'JD OTHER FSS', '兴利臻酒汇旗舰店'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗Hennessy自营旗舰店'],
            ['jd', 956156, 'JD OTHER FSS', '一号城堡酒类官方旗舰店'],
            ['jd', 789982, 'JD OTHER POP', '壹扬扬酒类专营店'],
            ['jd', 583708, 'JD OTHER POP', '怡九福酒类专营店'],
            ['jd', 583268, 'JD OTHER POP', '怡九怡五酒类专营店'],
            ['jd', 1000094450, 'JD SELF RUN', '易指酒类京东自营专区'],
            ['jd', 618447, 'JD OTHER POP', '逸隆酒业专营店'],
            ['jd', 975050, 'JD OTHER POP', '银泰葡萄酒专营店'],
            ['jd', 210015, 'JD OTHER POP', '昱嵘酒类专营店'],
            ['jd', 790179, 'JD OTHER POP', '云朵葡萄酒专营店'],
            ['jd', 1000101743, 'JD SELF RUN', '张裕葡萄酒京东自营专区'],
            ['jd', 1000118610, 'JD SELF RUN', '张裕先锋葡萄酒京东 自营旗舰店'],
            ['jd', 629979, 'JD OTHER POP', '臻品尚酒类专营店'],
            ['jd', 837457, 'JD OTHER FSS', '中酒网红酒官方旗舰店'],
            ['jd', 653600, 'JD OTHER FSS', '中淘 网官方旗舰店'],
            ['tmall', 193018741, 'TMALL OTHER POP', '0葡萄酒旗舰店'],
            ['tmall', 188273793, 'TMALL OTHER POP', 'Aldi海外旗舰店'],
            ['tmall', 187646666, 'TMALL OTHER POP', 'delegat酒类旗舰店'],
            ['tmall', 170680601, 'TMALL OTHER POP', 'jessicassuitcase海外旗舰'],
            ['tmall', 190257181, 'TMALL OTHER POP', 'js90高分葡萄 酒旗舰店'],
            ['tmall', 192904481, 'TMALL OTHER POP', 'mestia梅斯蒂亚酒类旗舰店'],
            ['tmall', 185263311, 'TMALL OTHER POP', 'paologuidi旗舰店'],
            ['tmall', 189744812, 'TMALL OTHER POP', 'vino75海外旗舰店'],
            ['tmall', 192926450, 'TMALL OTHER POP', 'woegobo沃歌堡旗舰店'],
            ['tmall', 191236011, 'TMALL OTHER POP', '爱龙堡酒类旗舰店'],
            ['tmall', 192882168, 'TMALL OTHER POP', '澳淘酒类专营店'],
            ['tmall', 190558245, 'TMALL OTHER POP', '柏尔兰堡旗舰店'],
            ['tmall', 192486369, 'TMALL OTHER POP', '奔富品尚汇专卖店'],
            ['tmall', 193211468, 'TMALL OTHER POP', '川富旗舰店'],
            ['tmall', 164537968, 'TMALL OTHER POP', '创群酒类专营店'],
            ['tmall', 10674908, 'TMALL OTHER POP', '德龙宝真酒类旗舰店'],
            ['tmall', 193000541, 'TMALL OTHER POP', '丁戈树旗舰店'],
            ['tmall', 192144830, 'TMALL OTHER POP', '鼎沃酒类专营店'],
            ['tmall', 192915033, 'TMALL OTHER POP', '福事多酒类专营店'],
            ['tmall', 68643227, 'TMALL OTHER POP', '谷度酒类专营店'],
            ['tmall', 192906330, 'TMALL OTHER POP', '海日新酒类专营店'],
            ['tmall', 192011951, 'TMALL OTHER POP', '红魔鬼官方旗舰店'],
            ['tmall', 193020949, 'TMALL OTHER POP', '酒奢酒类专营店'],
            ['tmall', 190446898, 'TMALL OTHER POP', '马标旗舰店'],
            ['tmall', 177178640, 'TMALL OTHER POP', '玛嘉唯诺酒类旗舰店'],
            ['tmall', 190634211, 'TMALL OTHER POP', '玛隆酒类旗舰店'],
            ['tmall', 193120192, 'TMALL OTHER POP', '梅乃宿旗舰店'],
            ['tmall', 188950240, 'TMALL OTHER POP', '梦德斯诺旗舰店'],
            ['tmall', 192637786, 'TMALL OTHER POP', '谋勤酒类专营店'],
            ['tmall', 193160265, 'TMALL OTHER POP', '乔治金瀚旗舰店'],
            ['tmall', 70053226, 'TMALL OTHER POP', '首悠汇酒类专营店'],
            ['tmall', 193097743, 'TMALL OTHER POP', '铄今葡萄酒专营店'],
            ['tmall', 190853387, 'TMALL OTHER POP', '天猫优品官方直营'],
            ['tmall', 193197368, 'TMALL OTHER POP', '万来喜酒类专营店'],
            ['tmall', 164489419, 'TMALL OTHER POP', '威龙酒类官方旗舰店'],
            ['tmall', 192858212, 'TMALL OTHER POP', '希雅斯酒庄酒类旗舰店'],
            ['tmall', 12577893, 'TMALL OTHER POP', '香格里拉酒类旗舰店'],
            ['tmall', 174535535, 'TMALL OTHER POP', '姚明葡萄酒官方旗舰店'],
            ['tmall', 189068868, 'TMALL OTHER POP', '意纯酒类专营店'],
            ['tmall', 192620534, 'TMALL OTHER POP', '优西优西酒类专营店'],
            ['tmall', 192866403, 'TMALL OTHER POP', '长城简拙专卖店'],
            ['tmall', 191911772, 'TMALL OTHER POP', '长城圣玖专卖店'],
            ['tmall', 192830002, 'TMALL OTHER POP', '长城长庆和专卖店'],
            ['tmall', 191503070, 'TMALL OTHER POP', '紫桐葡萄酒旗舰店'],
            ['jd', 23686, 'JD PRC EC POP', '威赛帝斯官方旗舰店'],
            ['jd', 64501, 'JD PRC EC POP', '拉菲红酒VC专卖店'],
            ['jd', 54945, 'JD PRC EC POP', 'VC红酒官方旗舰店'],
            ['jd', 128884, 'JD PRC EC POP', '威赛帝斯酒类专营店'],
            ['jd', 653155, 'JD PRC EC POP', '威赛帝斯葡萄酒精品店'],
            ['jd', 920233, 'JD PRC EC POP', '威赛帝斯酒水京东自营旗舰店'],
            ['jd', 887619, 'JD PRC EC POP', '酒库网葡萄酒专营店'],
            ['jd', 47224, 'JD PRC EC POP', '醉醇酒类专营店'],
            ['jd', 977466, 'JD PRC EC POP', '拉菲大绥京东自营专卖 店'],
            ['jd', 965804, 'JD PRC EC POP', '品尚汇京东自营旗舰店'],
            ['jd', 10048953, 'JD PRC EC POP', '品尚汇酒水旗舰店'],
            ['jd', 598669, 'JD PRC EC POP', '世界名酒专营店'],
            ['jd', 35773, 'JD PRC EC POP', '品尚汇旗舰店'],
            ['jd', 622463, 'JD PRC EC POP', '品尚优选官方旗舰店'],
            ['jd', 10803, 'JD PRC EC POP', '品尚汇酒类旗舰店'],
            ['tmall', 4200399, 'TMALL PRC EC POP', '品尚红酒官方旗舰店'],
            ['tmall', 68316124, 'TMALL PRC EC POP', '品尚汇名庄精品店'],
            ['tmall', 193011266, 'TMALL PRC EC POP', '尚多多的酒铺'],
            ['tmall', 5666084, 'TMALL PRC EC POP', '百正利酒类专营店'],
            ['tmall', 187717567, 'TMALL PRC EC POP', '乐享美酒'],
            ['tmall', 190209982, 'TMALL PRC EC POP', '臻乐酒类专营店'],
            ['jd', 154429, 'JD PRC EC POP', '臻乐酒水旗舰店'],
            ['jx', 'not found', 'JX PRC POP', '华溪国际官方旗舰店'],
            ['jx', 'not found', 'JX PRC POP', '酒库精品店'],
            ['jd', 10034304, 'JD PRC EC POP', '北粮酩庄官方旗舰店'],
            ['jd', 10074887, 'JD PRC EC POP', '列级名庄官方旗舰店'],
            ['jd', 10053009, 'JD PRC EC POP', '列级庄官方旗舰店'],
            ['tmall', 945372, 'TMALL PRC EC POP', '加枫红进口红酒专营'],
            ['jd', 12193, 'JD PRC EC POP', '有家红酒官方旗舰店'],
            ['jd', 110303, 'JD PRC EC POP', '加枫红酒类官方旗舰店'],
            ['jd', 5666084, 'JD PRC EC POP', '百正利官方旗舰店']
        ]
        type = None
        for v in data:
            if source == v[0] and str(sid) == str(v[1]):
                type = v[2]
        return type


    def type_map3(source=None, sid=None):
        data = [
            ['jd', 208868, 'JD JX POP', '酒仙网红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 77552, 'JD PRC EC POP', '御玖轩酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 630047, 'JD PRC EC POP', '郎家园官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 668773, 'JD PRC EC POP', '酒牧官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 81840, 'JD PRC EC POP', '宏放酒业专营店', 'B2C-Standalone B2C'],
            ['jd', 857001, 'JD PRC EC POP', '奢蕾汇官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 109668, 'JD PRC EC POP', '池陈辉酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 14796, 'JD JX POP', '酒仙网官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 31348, 'JD JX POP', '酒仙网世界名酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 781742, 'JD PRC EC POP', '酒宿旗舰店', 'B2C-Standalone B2C'],
            ['jd', 28717, 'JD PRC EC POP', '郎家园酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 28717, 'JD PRC EC POP', '郎家园酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 17784, 'JD PRC EC POP', '也买酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 31942, 'JD PRC EC POP', '也买酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 790107, 'JD PRC EC POP', '御玖轩官方旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 9730213, 'TMALL PRC FSS', '保乐力加官方旗舰店', 'B2C-Tmall'],
            ['tmall', 181925703, 'TMALL PRC EC POP', '池陈辉酒类专营店', 'B2C-Tmall'],
            ['tmall', 945237, 'TMALL PRC EC POP', '登岑酒水专营店', 'B2C-Tmall'],
            ['tmall', 182340153, 'TMALL PRC POP', '高华仕酒类专营店', 'B2C-Tmall'],
            ['tmall', 2561079, 'TMALL PRC EC POP', '郎家园酒类专营店', 'B2C-Tmall'],
            ['tmall', 189367760, 'TMALL OTHER FSS', '酩悦轩尼诗官方旗舰店', 'B2C-Tmall'],
            ['tmall', 59180296, 'TMALL PRC EC POP', '旭隆久合酒类专营店', 'B2C-Tmall'],
            ['tmall', 190864630, 'TMALL PRC EC POP', '寅瑜酒类专营店', 'B2C-Tmall'],
            ['tmall', 192635616, 'TMALL PRC EC POP', '奈思酒类专营店', 'B2C-Tmall'],
            ['tmall', 60309695, 'TMALL PRC EC POP', '御玖轩酒类专营店', 'B2C-Tmall'],
            ['tmall', 61123847, 'TMALL OTHER FSS', 'rio锐澳旗舰店', 'B2C-Tmall'],
            ['tmall', 67937129, 'TMALL OTHER FSS', '帝亚吉欧洋酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 10795084, 'TMALL OTHER POP', '艾森特酒类专营店', 'B2C-Tmall'],
            ['tmall', 174541689, 'TMALL OTHER POP', '谦法酒类专营店', 'B2C-Tmall'],
            ['tmall', 179501854, 'TMALL OTHER POP', '梦扬酒类专营店', 'B2C-Tmall'],
            ['tmall', 945362, 'TMALL OTHER POP', '月亮谷酒类专营店', 'B2C-Tmall'],
            ['tmall', 167186023, 'TMALL OTHER POP', '中浦耐杯酒类专营店', 'B2C-Tmall'],
            ['tmall', 67036345, 'TMALL OTHER POP', '安酌酒类专营店', 'B2C-Tmall'],
            ['tmall', 5666084, 'TMALL PRC EC POP', '百正利酒类专营店', 'B2C-Tmall'],
            ['tmall', 7602595, 'TMALL OTHER POP', '奔弛酒类专营店', 'B2C-Tmall'],
            ['tmall', 945418, 'TMALL OTHER POP', '畅健酒类专营店', 'B2C-Tmall'],
            ['tmall', 188943083, 'TMALL OTHER POP', '春融食品专营店', 'B2C-Tmall'],
            ['tmall', 187418569, 'TMALL OTHER POP', '枫羿酒类专营店', 'B2C-Tmall'],
            ['tmall', 192827257, 'TMALL PRC EC POP', '富以邻酒类专营店', 'B2C-Tmall'],
            ['tmall', 181903836, 'TMALL OTHER POP', '富邑葡萄酒集团旗舰店', 'B2C-Tmall'],
            ['tmall', 11712171, 'TMALL OTHER POP', '购酒网官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192144831, 'TMALL PRC EC POP', '宏放酒类专营店', 'B2C-Tmall'],
            ['tmall', 6112291, 'TMALL JX POP', '酒仙网官方旗舰店', 'B2C-Tmall'],
            ['tmall', 186803036, 'TMALL JX POP', '酒仙网葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 10177878, 'TMALL OTHER POP', '莫高官方旗舰店', 'B2C-Tmall'],
            ['tmall', 167840211, 'TMALL OTHER POP', '盛世普达酒类专营店', 'B2C-Tmall'],
            ['tmall', 945356, 'TMALL OTHER POP', '也买酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192626419, 'TMALL OTHER POP', '邑品酒类专营店', 'B2C-Tmall'],
            ['tmall', 945015, 'TMALL OTHER FSS', '张裕官方旗舰店', 'B2C-Tmall'],
            ['tmall', 757260, 'TMALL OTHER POP', '长城葡萄酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 186903121, 'TMALL OTHER POP', '中粮名庄荟酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190318752, 'TMALL OTHER POP', '醉鹅娘葡萄酒旗舰店', 'B2C-Tmall'],
            ['jd', 35946, 'JD OTHER POP', 'ASC官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 777644, 'JD OTHER POP', '百醇品逸酒类专营店', 'B2C-Standalone B2C'],
            ['tmall', 192011951, 'TMALL OTHER POP', '红魔鬼官方旗舰店', 'B2C-Tmall'],
            ['jd', 23686, 'JD PRC EC POP', '威赛帝斯官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 64501, 'JD PRC EC POP', '拉菲红酒VC专卖店', 'B2C-Standalone B2C'],
            ['jd', 54945, 'JD PRC EC POP', 'VC红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 653155, 'JD PRC EC POP', '威赛帝斯葡萄酒精品店', 'B2C-Standalone B2C'],
            ['jd', 920233, 'JD PRC EC POP', '威赛帝斯酒水京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 965804, 'JD PRC EC POP', '品尚汇京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 598669, 'JD PRC EC POP', '世界名酒专营店', 'B2C-Standalone B2C'],
            ['jd', 35773, 'JD PRC EC POP', '品尚汇旗舰店', 'B2C-Standalone B2C'],
            ['jd', 622463, 'JD PRC EC POP', '品尚优选官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 10803, 'JD PRC EC POP', '品尚汇酒类旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 4200399, 'TMALL PRC EC POP', '品尚红酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 945372, 'TMALL PRC EC POP', '加枫红进口红酒专营', 'B2C-Tmall'],
            ['jd', 12193, 'JD PRC EC POP', '有家红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 110303, 'JD PRC EC POP', '加枫红酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 128884, 'JD PRC EC POP', '威赛帝斯酒类专营店', 'B2C-Standalone B2C'],
            ['jx', 'not found', 'JX PRC POP', '酒库精品店', 'B2C-Standalone B2C'],
            ['jd', 887619, 'JD PRC EC POP', '酒库网葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 47224, 'JD PRC EC POP', '醉醇酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 'not found', 'JD PRC EC POP', '拉菲大绥京东自营专卖店', 'B2C-Standalone B2C'],
            ['jd', 10048953, 'JD PRC EC POP', '品尚汇酒水旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 'not found', 'TMALL PRC EC POP', '品尚汇名庄精品店', 'B2C-Tmall'],
            ['tmall', 'not found', 'TMALL PRC EC POP', '尚多多的酒铺', 'B2C-Tmall'],
            ['tmall', 187717567, 'TMALL PRC EC POP', '乐享美酒', 'B2C-Tmall'],
            ['tmall', 190209982, 'TMALL PRC EC POP', '臻乐酒类专营店', 'B2C-Tmall'],
            ['jd', 154429, 'JD PRC EC POP', '臻乐酒水旗舰店', 'B2C-Standalone B2C'],
            ['jx', 'not found', 'JX PRC POP', '华溪国际官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 10034304, 'JD PRC EC POP', '北粮酩庄官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 10074887, 'JD PRC EC POP', '列级名庄官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 10053009, 'JD PRC EC POP', '列级庄官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 10096880, 'JD PRC EC POP', '百正利官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000097462, 'JD SELF RUN', '保乐力加洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 0, 'JD SELF RUN', '京东自营', 'B2C-Standalone B2C'],
            ['jd', 75371, 'JD PRC POP', '宝树行官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000016184, 'JD SELF RUN', '人头马君度京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000082661, 'JD SELF RUN', '百加得京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000003530, 'JD SELF RUN', '杰克丹尼京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000006863, 'JD SELF RUN', '帝亚吉欧京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000074742, 'JD SELF RUN', '格兰父子洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000101704, 'JD SELF RUN', '日韩小美酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 46762, 'JD OTHER FSS', '人头马旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000097502, 'JD SELF RUN', '好来喜洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000114166, 'JD SELF RUN', '尊尼获加自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 663138, 'JD PRC POP', '宝树行洋酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 628046, 'JD PRC POP', '高华仕酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗重新发现中国味', 'B2C-Standalone B2C'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营店', 'B2C-Standalone B2C'],
            ['jd', 140747, 'JD OTHER POP', '智恒达商食品专营店', 'B2C-Standalone B2C'],
            ['jd', 173122, 'JD OTHER POP', '优红酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 188438, 'JD OTHER FSS', '酩悦轩尼诗官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000102032, 'JD SELF RUN', '菊正宗京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000100862, 'JD SELF RUN', '汇泉洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000102030, 'JD SELF RUN', '梅乃宿京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 598847, 'JD OTHER POP', '山姆会员商店官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000097501, 'JD SELF RUN', '琥珀洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 196822, 'JD OTHER POP', '智恒达商冲调专营店', 'B2C-Standalone B2C'],
            ['jd', 628019, 'JD OTHER POP', '酒号魔方官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 745347, 'JD OTHER POP', '海荟码头酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 'not found', 'JD OTHER POP', '伊樽酒水专营店', 'B2C-Standalone B2C'],
            ['jd', 721466, 'JD OTHER FSS', '帝亚吉欧洋酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 151121, 'JD OTHER POP', '梦扬酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 79797, 'JD OTHER POP', '江秀旗舰店', 'B2C-Standalone B2C'],
            ['jd', 621222, 'JD OTHER POP', '侠风官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 883600, 'JD PRC FSS', '保乐力加官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000130883, 'JD SELF RUN', '单一麦芽威士忌京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000121041, 'JD SELF RUN', '铂珑（BOUTILLIER）洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 10596, 'JD OTHER POP', 'BeWine酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 784130, 'JD OTHER FSS', 'Camus官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000125574, 'JD SELF RUN', 'CAMUS卡慕京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000102998, 'JD SELF RUN', 'fourloko京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 739078, 'JD OTHER FSS', 'fourloko洋酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 644990, 'JD OTHER FSS', 'Lautergold酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 69687, 'JD OTHER FSS', 'rio锐澳旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000139741, 'JD OTHER POP', 'TOPWINE', 'B2C-Standalone B2C'],
            ['jd', 813144, 'JD OTHER POP', '艾芙罗蒂酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 666331, 'JD OTHER POP', '艾森特酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 796667, 'JD OTHER POP', '爱尔兰馆', 'B2C-Standalone B2C'],
            ['jd', 1000081125, 'JD SELF RUN', '爱之湾葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000124421, 'JD SELF RUN', '安努克（AnCnoc）京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 141967, 'JD OTHER POP', '安酌酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000091165, 'JD SELF RUN', '澳洲葡萄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 839480, 'JD OTHER FSS', '百富门洋酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 719592, 'JD OTHER FSS', '百加得官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000004357, 'JD SELF RUN', '百龄坛京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 652423, 'JD OTHER POP', '奔弛酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 206443, 'JD OTHER POP', '奔富红酒品尚专卖店', 'B2C-Standalone B2C'],
            ['jd', 1000085511, 'JD SELF RUN', '卞氏米酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000086792, 'JD SELF RUN', '冰青青梅酒自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 730875, 'JD OTHER POP', '布多格酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 166256, 'JD OTHER POP', '畅健酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 101847, 'JD OTHER POP', '澄意生活旗舰店', 'B2C-Standalone B2C'],
            ['jd', 667023, 'JD OTHER POP', '础石酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 697745, 'JD OTHER POP', '春融食品专营店', 'B2C-Standalone B2C'],
            ['jd', 1000090402, 'JD SELF RUN', '大关清酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 177770, 'JD OTHER POP', '德万利酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 138519, 'JD OTHER POP', '登辉酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 708739, 'JD OTHER POP', '迪欧葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 818570, 'JD OTHER POP', '蒂优合酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 29380, 'JD OTHER POP', '东方聚仙酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 198485, 'JD OTHER POP', '发九国际酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000085804, 'JD SELF RUN', '法国红酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000090564, 'JD SELF RUN', '翡马葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 652268, 'JD OTHER POP', '枫羿酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 54739, 'JD OTHER POP', '福乐盟官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000086342, 'JD SELF RUN', '富邑京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 659245, 'JD OTHER FSS', '富邑葡萄酒集团旗舰店', 'B2C-Standalone B2C'],
            ['jd', 722661, 'JD OTHER POP', '噶瑪蘭威士忌官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000112681, 'JD SELF RUN', '干露酒厂京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 114398, 'JD OTHER POP', '给力酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 197583, 'JD OTHER POP', '购酒网官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 785856, 'JD OTHER POP', '好来喜酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000102041, 'JD SELF RUN', '好天好饮京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 52340, 'JD OTHER POP', '和道酒坊官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 893241, 'JD OTHER POP', '红冠雄鸡旗舰店', 'B2C-Standalone B2C'],
            ['jd', 786458, 'JD OTHER POP', '红魔鬼官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 602015, 'JD OTHER POP', '浣熙酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 821932, 'JD OTHER FSS', '黄尾袋鼠葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000080704, 'JD SELF RUN', '黄尾袋鼠葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000095543, 'JD SELF RUN', '加州乐事葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 757577, 'JD OTHER POP', '嘉食和食品旗舰店', 'B2C-Standalone B2C'],
            ['jd', 789177, 'JD OTHER POP', '贾斯汀旗舰店', 'B2C-Standalone B2C'],
            ['jd', 891054, 'JD OTHER POP', '建清（建昌）酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 780857, 'JD OTHER POP', '建清葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000007181, 'JD SELF RUN', '杰卡斯葡萄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 678436, 'JD OTHER POP', '金采龙凤官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 667985, 'JD OTHER POP', '金都恒泰官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 27999, 'JD OTHER POP', '金都恒泰酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 180040, 'JD OTHER POP', '金豪酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 664135, 'JD OTHER POP', '金豪葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000076153, 'JD SELF RUN', '京东1小时达', 'B2C-Standalone B2C'],
            ['jd', 1000097094, 'JD SELF RUN', '京东海外直采官方自营店', 'B2C-Standalone B2C'],
            ['jd', 1000096602, 'JD OTHER POP', '京东京造官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 671105, 'JD OTHER POP', '九珑因特酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 139272, 'JD OTHER POP', '久赞四海官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 85561, 'JD OTHER POP', '久之盈酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 158631, 'JD OTHER POP', '酒多多酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 901096, 'JD OTHER POP', '酒多多酒水专营店', 'B2C-Standalone B2C'],
            ['jd', 623605, 'JD OTHER POP', '酒富盛酩官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 156927, 'JD OTHER POP', '酒立有旗舰店', 'B2C-Standalone B2C'],
            ['jd', 196146, 'JD OTHER POP', '酒廷1990旗舰店', 'B2C-Standalone B2C'],
            ['jd', 653268, 'JD OTHER POP', '酒玩家酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 601324, 'JD OTHER POP', '酒玩家旗舰店', 'B2C-Standalone B2C'],
            ['jd', 623621, 'JD OTHER POP', '酒庄直通官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 691398, 'JD OTHER POP', '酒庄直通精品酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 784773, 'JD OTHER POP', '酒庄直通洋酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000004383, 'JD SELF RUN', '绝对伏特加京东自营专卖店', 'B2C-Standalone B2C'],
            ['jd', 149421, 'JD OTHER POP', '俊阁酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 202038, 'JD OTHER POP', '卡巴酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 202038, 'JD OTHER POP', '卡巴商贸酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000128562, 'JD SELF RUN', '卡伯纳葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 28244, 'JD OTHER POP', '开为进口食品专营店', 'B2C-Standalone B2C'],
            ['jd', 714754, 'JD OTHER POP', '跨理洋酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000084051, 'JD SELF RUN', '拉菲葡萄酒京东自营专卖店', 'B2C-Standalone B2C'],
            ['jd', 610735, 'JD OTHER POP', '拉斐红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 787326, 'JD OTHER POP', '雷诺官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 762801, 'JD OTHER POP', '雷司酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 761565, 'JD OTHER POP', '领亿来旗舰店', 'B2C-Standalone B2C'],
            ['jd', 149158, 'JD OTHER POP', '泸州老窖官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 889770, 'JD OTHER POP', '路易卡丹旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000102749, 'JD SELF RUN', '路易老爷京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000101425, 'JD SELF RUN', '罗莎庄园葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000004365, 'JD SELF RUN', '马爹利京东自营专卖店', 'B2C-Standalone B2C'],
            ['jd', 606881, 'JD OTHER POP', '玛吉欧酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 686018, 'JD OTHER POP', '麦吉欧克酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 109668, 'JD OTHER POP', '麦摇吧酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000097097, 'JD SELF RUN', '曼戈洋酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 136746, 'JD OTHER POP', '曼拉维酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 672469, 'JD OTHER POP', '猫呗旗舰店', 'B2C-Standalone B2C'],
            ['jd', 711165, 'JD OTHER POP', '每日壹品葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 708953, 'JD OTHER POP', '美酒汇酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 785821, 'JD OTHER POP', '美尼尔城堡旗舰店', 'B2C-Standalone B2C'],
            ['jd', 723940, 'JD OTHER POP', '蒙特斯官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000083361, 'JD SELF RUN', '蒙特斯葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000088001, 'JD SELF RUN', '米客米酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 653667, 'JD OTHER POP', '米赞国际中外名酒专营店', 'B2C-Standalone B2C'],
            ['jd', 12961, 'JD OTHER POP', '民酒网官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 672082, 'JD OTHER POP', '民酒网红酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 51819, 'JD OTHER FSS', '莫高官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000089564, 'JD SELF RUN', '莫高葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 804272, 'JD OTHER POP', '慕拉酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 743540, 'JD OTHER POP', '南非国家酒馆', 'B2C-Standalone B2C'],
            ['jd', 712497, 'JD OTHER POP', '偶醉酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 48459, 'JD OTHER POP', '派斯顿酒类专卖店', 'B2C-Standalone B2C'],
            ['jd', 896815, 'JD OTHER POP', '派斯顿旗舰店', 'B2C-Standalone B2C'],
            ['jd', 847606, 'JD OTHER POP', '派斯顿乔森专卖店', 'B2C-Standalone B2C'],
            ['jd', 942341, 'JD SELF RUN', '派斯顿洋酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 828450, 'JD OTHER POP', '彭索酒庄旗舰店', 'B2C-Standalone B2C'],
            ['jd', 850893, 'JD OTHER POP', '品鉴洋酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 693096, 'JD OTHER POP', '品茗酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 51360, 'JD OTHER POP', '品牌贸易直营店', 'B2C-Standalone B2C'],
            ['jd', 618618, 'JD OTHER POP', '品正源清酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 584089, 'JD OTHER POP', '榀酒酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000086792, 'JD SELF RUN', '圃田冰青青梅酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 814913, 'JD OTHER POP', '七酒酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 652491, 'JD OTHER POP', '千叶酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 135933, 'JD OTHER POP', '谦法酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 629185, 'JD OTHER POP', '谦法中外名酒专营店', 'B2C-Standalone B2C'],
            ['jd', 690222, 'JD OTHER POP', '乔森酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 627258, 'JD OTHER POP', '青出于蓝酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 729877, 'JD OTHER POP', '青出于蓝中外名酒专营店', 'B2C-Standalone B2C'],
            ['jd', 662066, 'JD OTHER POP', '融众恒泰酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000003583, 'JD SELF RUN', '锐澳京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 784180, 'JD OTHER POP', '锐达酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 690779, 'JD OTHER POP', '瑞丰壹品官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 182149, 'JD OTHER POP', '萨拉维官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 27064, 'JD OTHER POP', '萨拉维酒业旗舰店', 'B2C-Standalone B2C'],
            ['jd', 617595, 'JD OTHER POP', '塞德菲贸易官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000100881, 'JD SELF RUN', '三得利洋酒自营店', 'B2C-Standalone B2C'],
            ['jd', 598847, 'JD SELF RUN', '山姆会员商店京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000097196, 'JD SELF RUN', '山图葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000091648, 'JD SELF RUN', '圣丽塔葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 64881, 'JD OTHER POP', '圣品食品专营店', 'B2C-Standalone B2C'],
            ['jd', 1000114761, 'JD SELF RUN', '圣芝葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 619819, 'JD OTHER POP', '盛世普达酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 716773, 'JD OTHER POP', '师捷酒水专营店', 'B2C-Standalone B2C'],
            ['jd', 876785, 'JD OTHER POP', '十里九品旗舰店', 'B2C-Standalone B2C'],
            ['jd', 37889, 'JD OTHER POP', '拾全酒美官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 123515, 'JD OTHER POP', '双宏天乐酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000154003, 'JD SELF RUN', '顺昌源京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 601663, 'JD OTHER POP', '思萨美康酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 816861, 'JD OTHER POP', '苏麦威官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000105659, 'JD SELF RUN', '苏州桥京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 751994, 'JD OTHER POP', '苏州桥旗舰店', 'B2C-Standalone B2C'],
            ['jd', 609057, 'JD OTHER POP', '随猴酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000225181, 'JD SELF RUN', '獭祭京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 178142, 'JD OTHER POP', '泰和酩庄官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000121721, 'JD SELF RUN', '桃乐丝葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 741352, 'JD OTHER POP', '特马桐酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 79194, 'JD OTHER POP', '腾易红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 645612, 'JD OTHER POP', '天阶官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 726062, 'JD OTHER POP', '听花语旗舰店', 'B2C-Standalone B2C'],
            ['jd', 810137, 'JD OTHER POP', '同福永官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 776922, 'JD OTHER POP', '兔兔冷泉酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000089230, 'JD SELF RUN', '王朝葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 685997, 'JD OTHER POP', '威爵酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000080006, 'JD SELF RUN', '威龙葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000130161, 'JD SELF RUN', '微醺洋酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 659016, 'JD OTHER POP', '沃尔玛官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 659016, 'JD SELF RUN', '沃尔玛京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 790230, 'JD OTHER POP', '五福源酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 778289, 'JD OTHER POP', '五洲海购酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 192182, 'JD OTHER POP', '伍壹玖酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000090387, 'JD SELF RUN', '西班牙葡萄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000078272, 'JD SELF RUN', '西夫拉姆葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 806900, 'JD OTHER POP', '西域烈焰旗舰店', 'B2C-Standalone B2C'],
            ['jd', 179651, 'JD OTHER POP', '昔澜酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000112381, 'JD SELF RUN', '香奈葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 40837, 'JD OTHER POP', '橡树之约葡萄酒优选店', 'B2C-Standalone B2C'],
            ['jd', 135482, 'JD OTHER POP', '学权酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 47992, 'JD OTHER POP', '伊樽月亮谷酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 144102, 'JD OTHER POP', '壹捌捌久官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 174090, 'JD OTHER POP', '宜信酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 774766, 'JD OTHER POP', '邑品酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 204363, 'JD OTHER POP', '崟泰酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 943285, 'JD SELF RUN', '优红国际京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 14838, 'JD OTHER POP', '优红酒', 'B2C-Standalone B2C'],
            ['jd', 685911, 'JD OTHER POP', '优葡荟官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 128287, 'JD OTHER POP', '御隆轩红酒专营店', 'B2C-Standalone B2C'],
            ['jd', 617388, 'JD OTHER POP', '元合陶艺红酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 54761, 'JD OTHER POP', '原产团旗舰店', 'B2C-Standalone B2C'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷', 'B2C-Standalone B2C'],
            ['jd', 42152, 'JD OTHER POP', '月亮谷酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 47992, 'JD OTHER POP', '月氏良园', 'B2C-Standalone B2C'],
            ['jd', 650742, 'JD OTHER POP', '月氏人酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 751364, 'JD OTHER POP', '月氏人洋酒专营店', 'B2C-Standalone B2C'],
            ['jd', 659112, 'JD OTHER POP', '云的酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 58502, 'JD OTHER FSS', '张裕官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000004718, 'JD SELF RUN', '张裕葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000090684, 'JD SELF RUN', '长白山京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 50080, 'JD OTHER POP', '长白山特产专营店', 'B2C-Standalone B2C'],
            ['jd', 757260, 'JD OTHER POP', '长城葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000004719, 'JD SELF RUN', '长城葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 38608, 'JD OTHER POP', '真露旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000090830, 'JD SELF RUN', '智利葡萄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000080527, 'JD SELF RUN', '智象葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 547312, 'JD OTHER POP', '中国特产·泸州馆', 'B2C-Standalone B2C'],
            ['jd', 704519, 'JD OTHER POP', '中粮名庄荟酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 582537, 'JD OTHER POP', '中粮名庄荟旗舰店', 'B2C-Standalone B2C'],
            ['jd', 765450, 'JD OTHER POP', '中葡汇官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 199202, 'JD OTHER POP', '中浦耐杯酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 34451, 'JD OTHER POP', '醉鹅娘葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 793598, 'JD OTHER POP', '醉美酒酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 846070, 'JD OTHER POP', '醉梦红酒旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 181936762, 'TMALL PRC FSS', 'chivas芝华士官方旗舰店', 'B2C-Tmall'],
            ['tmall', 67726749, 'TMALL PRC POP', '宝树行酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 64070709, 'TMALL OTHER FSS', '杰克丹尼官方旗舰店', 'B2C-Tmall'],
            ['tmall', 61276625, 'TMALL PRC FSS', 'absolut官方旗舰店', 'B2C-Tmall'],
            ['tmall', 60553350, 'TMALL OTHER FSS', '人头马官方旗舰店', 'B2C-Tmall'],
            ['tmall', 9281929, 'TMALL SUPER', '天猫超市', 'B2C-Tmall'],
            ['tmall', 189673674, 'TMALL OTHER POP', '侠风酒类专营店', 'B2C-Tmall'],
            ['tmall', 16994272, 'TMALL OTHER FSS', '尊尼获加官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利官方旗舰店', 'B2C-Tmall'],
            ['tmall', 168343869, 'TMALL OTHER FSS', 'macallan旗舰店', 'B2C-Tmall'],
            ['tmall', 63282256, 'TMALL OTHER FSS', '百利官方旗舰店', 'B2C-Tmall'],
            ['tmall', 163407529, 'TMALL OTHER FSS', '翰格蓝爵官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192723120, 'TMALL PRC FSS', 'martell马爹利旗舰店', 'B2C-Tmall'],
            ['tmall', 16433652, 'TMALL OTHER FSS', 'asc官方旗舰店', 'B2C-Tmall'],
            ['tmall', 186938208, 'TMALL OTHER POP', '云的食品专营店', 'B2C-Tmall'],
            ['tmall', 64886808, 'TMALL OTHER POP', '富水食品专营店', 'B2C-Tmall'],
            ['tmall', 9282437, 'TMALL OTHER POP', '江秀酒类专营店', 'B2C-Tmall'],
            ['tmall', 189890305, 'TMALL OTHER POP', '泰圣原酒类专营店', 'B2C-Tmall'],
            ['tmall', 66986595, 'TMALL OTHER POP', '广存禄酒类专营店', 'B2C-Tmall'],
            ['tmall', 190184104, 'TMALL OTHER POP', 'ak47酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190859232, 'TMALL OTHER FSS', 'aldi旗舰店', 'B2C-Tmall'],
            ['tmall', 167940348, 'TMALL OTHER FSS', 'asahi朝日旗舰店', 'B2C-Tmall'],
            ['tmall', 186802661, 'TMALL OTHER FSS', 'baraka酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190833445, 'TMALL OTHER FSS', 'beamsuntory宾三得利旗舰', 'B2C-Tmall'],
            ['tmall', 181931354, 'TMALL OTHER FSS', 'bols波士力娇酒旗舰店', 'B2C-Tmall'],
            ['tmall', 190759564, 'TMALL OTHER FSS', 'camus卡慕官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192980474, 'TMALL OTHER FSS', 'choya俏雅旗舰店', 'B2C-Tmall'],
            ['tmall', 9338011, 'TMALL OTHER FSS', 'cmp旗舰店', 'B2C-Tmall'],
            ['tmall', 189712524, 'TMALL OTHER FSS', 'costco官方旗舰店', 'B2C-Tmall'],
            ['tmall', 189222031, 'TMALL OTHER FSS', 'dekuyper海外旗舰店', 'B2C-Tmall'],
            ['tmall', 187805167, 'TMALL OTHER FSS', 'emp海外旗舰店', 'B2C-Tmall'],
            ['tmall', 192599080, 'TMALL OTHER FSS', 'fastking莎堡皇旗舰店', 'B2C-Tmall'],
            ['tmall', 186605891, 'TMALL OTHER FSS', 'fourloko旗舰店', 'B2C-Tmall'],
            ['tmall', 186902811, 'TMALL OTHER FSS', 'giv旗舰店', 'B2C-Tmall'],
            ['tmall', 188755410, 'TMALL OTHER FSS', 'hilbaton希伯顿旗舰店', 'B2C-Tmall'],
            ['tmall', 192127385, 'TMALL OTHER FSS', 'ibanesas伊柏妮莎旗舰店', 'B2C-Tmall'],
            ['tmall', 188185494, 'TMALL OTHER FSS', 'lffo旗舰店', 'B2C-Tmall'],
            ['tmall', 188150563, 'TMALL OTHER FSS', 'meukow墨高旗舰店', 'B2C-Tmall'],
            ['tmall', 186963464, 'TMALL OTHER FSS', 'montes旗舰店', 'B2C-Tmall'],
            ['tmall', 181936150, 'TMALL OTHER FSS', 'rekorderlig旗舰店', 'B2C-Tmall'],
            ['tmall', 189285528, 'TMALL OTHER FSS', 'sinodrink华饮旗舰店', 'B2C-Tmall'],
            ['tmall', 68372177, 'TMALL OTHER FSS', 'suamgy圣芝官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192825647, 'TMALL OTHER FSS', 'wemyss旗舰店', 'B2C-Tmall'],
            ['tmall', 187740248, 'TMALL OTHER FSS', '阿道克旗舰店', 'B2C-Tmall'],
            ['tmall', 60268731, 'TMALL OTHER POP', '爱屋食品专营店', 'B2C-Tmall'],
            ['tmall', 187217166, 'TMALL OTHER FSS', '安特旗舰店', 'B2C-Tmall'],
            ['tmall', 186905859, 'TMALL OTHER POP', '奥蒂兰丝旗舰店', 'B2C-Tmall'],
            ['tmall', 63676728, 'TMALL OTHER POP', '奥美圣酒类专营店', 'B2C-Tmall'],
            ['tmall', 190745141, 'TMALL OTHER POP', '奥图纳旗舰店', 'B2C-Tmall'],
            ['tmall', 190152783, 'TMALL OTHER POP', '奥雪谷酒类专营店', 'B2C-Tmall'],
            ['tmall', 169343891, 'TMALL OTHER POP', '澳迪尼旗舰店', 'B2C-Tmall'],
            ['tmall', 185708057, 'TMALL OTHER POP', '芭诺斯酒类专营店', 'B2C-Tmall'],
            ['tmall', 181935488, 'TMALL OTHER POP', '白洋河酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 189458091, 'TMALL OTHER POP', '百醇品逸酒类专营店', 'B2C-Tmall'],
            ['tmall', 192831218, 'TMALL OTHER POP', '百富门酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 3743705, 'TMALL OTHER POP', '柏特酒类专营店', 'B2C-Tmall'],
            ['tmall', 10794997, 'TMALL OTHER POP', '杯中时光酒类专营店', 'B2C-Tmall'],
            ['tmall', 191604668, 'TMALL OTHER POP', '北京榜样酒类专营店', 'B2C-Tmall'],
            ['tmall', 64030717, 'TMALL OTHER POP', '北京酒葫芦酒类专营店', 'B2C-Tmall'],
            ['tmall', 60006673, 'TMALL OTHER POP', '北京亿万典藏酒类专营店', 'B2C-Tmall'],
            ['tmall', 69685590, 'TMALL OTHER POP', '北京智恒达商食品专营店', 'B2C-Tmall'],
            ['tmall', 192617571, 'TMALL OTHER POP', '奔富极昼专卖店', 'B2C-Tmall'],
            ['tmall', 69111791, 'TMALL OTHER POP', '本初酒类专营店', 'B2C-Tmall'],
            ['tmall', 187739761, 'TMALL OTHER POP', '必得利旗舰店', 'B2C-Tmall'],
            ['tmall', 189534060, 'TMALL OTHER POP', '碧波林海酒类专营店', 'B2C-Tmall'],
            ['tmall', 192127388, 'TMALL OTHER POP', '镖士旗舰店', 'B2C-Tmall'],
            ['tmall', 8121431, 'TMALL OTHER POP', '槟悦酒类专营店', 'B2C-Tmall'],
            ['tmall', 189239808, 'TMALL OTHER POP', '波尔亚葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 163744402, 'TMALL OTHER POP', '波菲摩根酒类专营店', 'B2C-Tmall'],
            ['tmall', 191517232, 'TMALL OTHER POP', '伯爵酒类专营店', 'B2C-Tmall'],
            ['tmall', 191976497, 'TMALL OTHER POP', '布多格旗舰店', 'B2C-Tmall'],
            ['tmall', 64528280, 'TMALL OTHER POP', '昌盛德酒类专营店', 'B2C-Tmall'],
            ['tmall', 189151241, 'TMALL OTHER POP', '诚善堂酒类专营', 'B2C-Tmall'],
            ['tmall', 181936162, 'TMALL OTHER POP', '初生活酒类企业店', 'B2C-Tmall'],
            ['tmall', 192103483, 'TMALL OTHER POP', '触角食品专营店', 'B2C-Tmall'],
            ['tmall', 163796968, 'TMALL OTHER POP', '春彦佳业酒类专营店', 'B2C-Tmall'],
            ['tmall', 181936050, 'TMALL OTHER POP', '第7元素旗舰店', 'B2C-Tmall'],
            ['tmall', 945291, 'TMALL OTHER POP', '东晟酒类专营店', 'B2C-Tmall'],
            ['tmall', 190610751, 'TMALL OTHER POP', '法多克旗舰店', 'B2C-Tmall'],
            ['tmall', 188811200, 'TMALL OTHER POP', '法尔城堡旗舰店', 'B2C-Tmall'],
            ['tmall', 186503318, 'TMALL OTHER POP', '法夫尔堡旗舰店', 'B2C-Tmall'],
            ['tmall', 7665680, 'TMALL OTHER POP', '菲利特酒类专营店', 'B2C-Tmall'],
            ['tmall', 165603201, 'TMALL OTHER POP', '菲尼雅酒类专营店', 'B2C-Tmall'],
            ['tmall', 945320, 'TMALL OTHER POP', '福客仕酒类专营店', 'B2C-Tmall'],
            ['tmall', 190892990, 'TMALL OTHER POP', '福洮食品专营店', 'B2C-Tmall'],
            ['tmall', 187236984, 'TMALL OTHER POP', '富瑞斯旗舰店', 'B2C-Tmall'],
            ['tmall', 191834815, 'TMALL OTHER POP', '高地旗舰店', 'B2C-Tmall'],
            ['tmall', 191669504, 'TMALL OTHER POP', '歌然酒类专营店', 'B2C-Tmall'],
            ['tmall', 184272306, 'TMALL OTHER FSS', '格拉洛旗舰店', 'B2C-Tmall'],
            ['tmall', 192175320, 'TMALL OTHER FSS', '格兰父子旗舰店', 'B2C-Tmall'],
            ['tmall', 188192538, 'TMALL OTHER POP', '购酒网葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 62171152, 'TMALL OTHER POP', '古仓酒类专营店', 'B2C-Tmall'],
            ['tmall', 189705131, 'TMALL OTHER POP', '顾悦酒类专营店', 'B2C-Tmall'],
            ['tmall', 188405924, 'TMALL OTHER POP', '冠振酒类专营店', 'B2C-Tmall'],
            ['tmall', 191234834, 'TMALL OTHER POP', '广州美助达酒类专营店', 'B2C-Tmall'],
            ['tmall', 190785471, 'TMALL OTHER POP', '哈力高旗舰店', 'B2C-Tmall'],
            ['tmall', 190145492, 'TMALL OTHER POP', '海优食品专营店', 'B2C-Tmall'],
            ['tmall', 191900950, 'TMALL OTHER POP', '浩丰隆睿酒类专营店', 'B2C-Tmall'],
            ['tmall', 8437261, 'TMALL OTHER POP', '和道酒类专营店', 'B2C-Tmall'],
            ['tmall', 9282647, 'TMALL OTHER POP', '泓利酒类专营店', 'B2C-Tmall'],
            ['tmall', 59504827, 'TMALL OTHER POP', '洪英华泰酒类专营店', 'B2C-Tmall'],
            ['tmall', 190174879, 'TMALL OTHER POP', '浣熙食品专营店', 'B2C-Tmall'],
            ['tmall', 191166470, 'TMALL OTHER FSS', '黄尾袋鼠官方旗舰店', 'B2C-Tmall'],
            ['tmall', 189224565, 'TMALL OTHER POP', '汇泉酒类专营店', 'B2C-Tmall'],
            ['tmall', 10182220, 'TMALL OTHER POP', '吉量食品专营店', 'B2C-Tmall'],
            ['tmall', 191717130, 'TMALL OTHER POP', '加州乐事旗舰店', 'B2C-Tmall'],
            ['tmall', 945585, 'TMALL OTHER POP', '嘉美醇酒类专营店', 'B2C-Tmall'],
            ['tmall', 70102414, 'TMALL OTHER POP', '简拙酒类专营店', 'B2C-Tmall'],
            ['tmall', 190802928, 'TMALL OTHER POP', '将军井山东专卖店', 'B2C-Tmall'],
            ['tmall', 70420850, 'TMALL OTHER POP', '将军井梓轩专卖店', 'B2C-Tmall'],
            ['tmall', 163055240, 'TMALL OTHER POP', '杰奥森酒类专营店', 'B2C-Tmall'],
            ['tmall', 191071067, 'TMALL OTHER POP', '今朝美酒类专营店', 'B2C-Tmall'],
            ['tmall', 192027065, 'TMALL OTHER POP', '金巴厘旗舰店', 'B2C-Tmall'],
            ['tmall', 19694041, 'TMALL OTHER POP', '金车食品专营店', 'B2C-Tmall'],
            ['tmall', 945227, 'TMALL OTHER POP', '金都恒泰酒类专营', 'B2C-Tmall'],
            ['tmall', 192345241, 'TMALL OTHER POP', '金利莎旗舰店', 'B2C-Tmall'],
            ['tmall', 12346794, 'TMALL OTHER POP', '金源鹏祥酒类专营店', 'B2C-Tmall'],
            ['tmall', 189584370, 'TMALL OTHER POP', '金泽恒酒类专营', 'B2C-Tmall'],
            ['tmall', 19869513, 'TMALL OTHER POP', '金樽格兰酒类专营店', 'B2C-Tmall'],
            ['tmall', 68606331, 'TMALL OTHER POP', '京鲁信达酒类专营店', 'B2C-Tmall'],
            ['tmall', 192847223, 'TMALL OTHER POP', '京瑞酒类专营店', 'B2C-Tmall'],
            ['tmall', 190603106, 'TMALL OTHER POP', '精酉酒类专营店', 'B2C-Tmall'],
            ['tmall', 67721348, 'TMALL OTHER POP', '景泰蓝酒类专营店', 'B2C-Tmall'],
            ['tmall', 3928664, 'TMALL OTHER POP', '久加久酒博汇旗舰店', 'B2C-Tmall'],
            ['tmall', 70101091, 'TMALL OTHER POP', '玖品酒类专营店', 'B2C-Tmall'],
            ['tmall', 188099938, 'TMALL OTHER POP', '酒富盛酩酒类专营店', 'B2C-Tmall'],
            ['tmall', 191944097, 'TMALL OTHER POP', '酒购乐酒类专营店', 'B2C-Tmall'],
            ['tmall', 163340849, 'TMALL OTHER POP', '酒嗨酒酒类专营店', 'B2C-Tmall'],
            ['tmall', 188435285, 'TMALL OTHER POP', '酒玩家酒类专营店', 'B2C-Tmall'],
            ['tmall', 166872422, 'TMALL OTHER POP', '酒怡酒类专营店', 'B2C-Tmall'],
            ['tmall', 192837172, 'TMALL OTHER POP', '卡爹拉酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 191669304, 'TMALL OTHER POP', '卡普酒类专营店', 'B2C-Tmall'],
            ['tmall', 188297880, 'TMALL OTHER POP', '卡诗图旗舰店', 'B2C-Tmall'],
            ['tmall', 192066196, 'TMALL OTHER POP', '卡图酒类专营店', 'B2C-Tmall'],
            ['tmall', 8440231, 'TMALL OTHER POP', '开为酒类专营店', 'B2C-Tmall'],
            ['tmall', 169821549, 'TMALL OTHER POP', '蓝利酒类专营店', 'B2C-Tmall'],
            ['tmall', 190651317, 'TMALL OTHER POP', '乐喝酒类专营店', 'B2C-Tmall'],
            ['tmall', 187445065, 'TMALL OTHER POP', '乐斯卡酒类专营店', 'B2C-Tmall'],
            ['tmall', 163237776, 'TMALL OTHER POP', '雷辉酒类专营店', 'B2C-Tmall'],
            ['tmall', 65759084, 'TMALL OTHER POP', '类人首旗舰店', 'B2C-Tmall'],
            ['tmall', 69239662, 'TMALL OTHER POP', '李氏兄弟酒类专营店', 'B2C-Tmall'],
            ['tmall', 189528060, 'TMALL OTHER POP', '力泉酒类专营店', 'B2C-Tmall'],
            ['tmall', 189179357, 'TMALL OTHER POP', '丽乐威酒类专营店', 'B2C-Tmall'],
            ['tmall', 69665044, 'TMALL OTHER POP', '丽日蓝天酒类专营店', 'B2C-Tmall'],
            ['tmall', 189131471, 'TMALL OTHER POP', '刘嘉玲生活品味海外旗舰店', 'B2C-Tmall'],
            ['tmall', 168772523, 'TMALL OTHER POP', '龙马酒类专营店', 'B2C-Tmall'],
            ['tmall', 64526011, 'TMALL OTHER POP', '隆斐堡旗舰店', 'B2C-Tmall'],
            ['tmall', 185245516, 'TMALL OTHER POP', '路易马西尼旗舰店', 'B2C-Tmall'],
            ['tmall', 190554679, 'TMALL OTHER POP', '路易十三官方旗舰店', 'B2C-Tmall'],
            ['tmall', 181622804, 'TMALL OTHER POP', '罗莎酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190826300, 'TMALL OTHER POP', '罗提斯酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 9666574, 'TMALL OTHER POP', '罗提斯酒类专营店', 'B2C-Tmall'],
            ['tmall', 192857829, 'TMALL OTHER POP', '洛克之羽旗舰店', 'B2C-Tmall'],
            ['tmall', 172944703, 'TMALL OTHER POP', '麦德龙官方海外旗舰店', 'B2C-Tmall'],
            ['tmall', 170355320, 'TMALL OTHER POP', '麦德龙官方旗舰店', 'B2C-Tmall'],
            ['tmall', 162888716, 'TMALL OTHER POP', '美圣酒类专营店', 'B2C-Tmall'],
            ['tmall', 181928781, 'TMALL OTHER POP', '美助达酒类专营店', 'B2C-Tmall'],
            ['tmall', 173542702, 'TMALL OTHER POP', '蒙大菲旗舰店', 'B2C-Tmall'],
            ['tmall', 20101735, 'TMALL OTHER POP', '孟特罗斯庄园酒类专营店', 'B2C-Tmall'],
            ['tmall', 190406543, 'TMALL OTHER POP', '米宝拓酒类专营店', 'B2C-Tmall'],
            ['tmall', 192107315, 'TMALL OTHER POP', '名仕罗纳德酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 185163884, 'TMALL OTHER POP', '慕拉旗舰店', 'B2C-Tmall'],
            ['tmall', 167698271, 'TMALL OTHER POP', '尼雅酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 186920958, 'TMALL OTHER POP', '宁夏葡萄酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 190733669, 'TMALL OTHER POP', '欧利千隆酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190743351, 'TMALL OTHER POP', '欧绅酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 17437054, 'TMALL OTHER POP', '派斯顿酒类专营店', 'B2C-Tmall'],
            ['tmall', 189816418, 'TMALL OTHER POP', '潘果旗舰店', 'B2C-Tmall'],
            ['tmall', 185200804, 'TMALL OTHER POP', '朋珠酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 68315639, 'TMALL OTHER POP', '品厨食品专营店', 'B2C-Tmall'],
            ['tmall', 165081244, 'TMALL OTHER POP', '品天下酒类专营店', 'B2C-Tmall'],
            ['tmall', 191885632, 'TMALL OTHER POP', '葡萄酒小皮旗舰店', 'B2C-Tmall'],
            ['tmall', 192040162, 'TMALL OTHER POP', '葡萄酿酒类专营店', 'B2C-Tmall'],
            ['tmall', 187357317, 'TMALL OTHER POP', '祺彬酒类专营店', 'B2C-Tmall'],
            ['tmall', 20619935, 'TMALL OTHER POP', '巧厨食品专营店', 'B2C-Tmall'],
            ['tmall', 64545265, 'TMALL OTHER POP', '清之光食品专营店', 'B2C-Tmall'],
            ['tmall', 190745134, 'TMALL OTHER POP', '锐澳师捷专卖店', 'B2C-Tmall'],
            ['tmall', 189528055, 'TMALL OTHER POP', '锐澳硕悦专卖店', 'B2C-Tmall'],
            ['tmall', 192898261, 'TMALL OTHER POP', '上海酒玩家酒类专营店', 'B2C-Tmall'],
            ['tmall', 182339809, 'TMALL OTHER POP', '上海学权酒类专营店', 'B2C-Tmall'],
            ['tmall', 191407553, 'TMALL OTHER POP', '舍岭酒类专营店', 'B2C-Tmall'],
            ['tmall', 59208730, 'TMALL OTHER POP', '升升食品专营店', 'B2C-Tmall'],
            ['tmall', 192324546, 'TMALL OTHER POP', '晟阳酒类专营店', 'B2C-Tmall'],
            ['tmall', 191205218, 'TMALL OTHER POP', '盛卫酒类专营店', 'B2C-Tmall'],
            ['tmall', 192830888, 'TMALL OTHER POP', '守望之鹰葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 192855845, 'TMALL OTHER POP', '曙云食品专营店', 'B2C-Tmall'],
            ['tmall', 189118013, 'TMALL OTHER POP', '苏佳利旗舰店', 'B2C-Tmall'],
            ['tmall', 172696000, 'TMALL OTHER POP', '苏宁易购官方旗舰店', 'B2C-Tmall'],
            ['tmall', 190709131, 'TMALL OTHER POP', '塔希葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 192848674, 'TMALL OTHER POP', '獭祭旗舰店', 'B2C-Tmall'],
            ['tmall', 170846288, 'TMALL OTHER POP', '特其拉之家', 'B2C-Tmall'],
            ['tmall', 16476202, 'TMALL OTHER POP', '腾易永立酒类专营店', 'B2C-Tmall'],
            ['tmall', 61304831, 'TMALL OTHER POP', '天尝地酒酒类专营店', 'B2C-Tmall'],
            ['tmall', 170685533, 'TMALL OTHER POP', '天猫国际官方直营', 'B2C-Tmall'],
            ['tmall', 181871121, 'TMALL OTHER POP', '天猫国际官方直营国内现货', 'B2C-Tmall'],
            ['tmall', 61531947, 'TMALL OTHER POP', '天添乐酒类专营店', 'B2C-Tmall'],
            ['tmall', 188453101, 'TMALL OTHER POP', '天源食品专营店', 'B2C-Tmall'],
            ['tmall', 186990195, 'TMALL OTHER POP', '通化旗舰店', 'B2C-Tmall'],
            ['tmall', 191715391, 'TMALL OTHER POP', '兔兔冷泉酒旗舰店', 'B2C-Tmall'],
            ['tmall', 181932183, 'TMALL OTHER POP', '万酒酒类专营', 'B2C-Tmall'],
            ['tmall', 9327449, 'TMALL OTHER POP', '王松酒类专营店', 'B2C-Tmall'],
            ['tmall', 945416, 'TMALL OTHER POP', '威赛帝斯酒类专营', 'B2C-Tmall'],
            ['tmall', 191900921, 'TMALL OTHER POP', '薇林酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190654972, 'TMALL OTHER POP', '维定酒类专营店', 'B2C-Tmall'],
            ['tmall', 190338990, 'TMALL OTHER POP', '伟贞酒类专营店', 'B2C-Tmall'],
            ['tmall', 190760637, 'TMALL OTHER POP', '卫宾酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 169359891, 'TMALL OTHER POP', '武若酒类专营店', 'B2C-Tmall'],
            ['tmall', 60318412, 'TMALL OTHER POP', '西夫拉姆旗舰店', 'B2C-Tmall'],
            ['tmall', 190597213, 'TMALL OTHER POP', '夏多纳酒类专营店', 'B2C-Tmall'],
            ['tmall', 17374987, 'TMALL OTHER POP', '相拓酒类专营店', 'B2C-Tmall'],
            ['tmall', 181931513, 'TMALL OTHER POP', '祥栈酒类专营店', 'B2C-Tmall'],
            ['tmall', 189345451, 'TMALL OTHER POP', '小冰酒类专营店', 'B2C-Tmall'],
            ['tmall', 169403866, 'TMALL OTHER POP', '小飞象酒类专营店', 'B2C-Tmall'],
            ['tmall', 12837183, 'TMALL OTHER POP', '新发食品专营店', 'B2C-Tmall'],
            ['tmall', 188471711, 'TMALL OTHER POP', '新启达酒类专营店', 'B2C-Tmall'],
            ['tmall', 163519396, 'TMALL OTHER POP', '星韩亚食品专营店', 'B2C-Tmall'],
            ['tmall', 184984900, 'TMALL OTHER POP', '炫品酒类专营店', 'B2C-Tmall'],
            ['tmall', 59504847, 'TMALL OTHER POP', '雅醇酒类专营店', 'B2C-Tmall'],
            ['tmall', 67643980, 'TMALL OTHER POP', '阳光万国酒类专营店', 'B2C-Tmall'],
            ['tmall', 181939573, 'TMALL OTHER POP', '依仕迪旗舰店', 'B2C-Tmall'],
            ['tmall', 9282683, 'TMALL OTHER POP', '壹玖壹玖官方旗舰店', 'B2C-Tmall'],
            ['tmall', 188113645, 'TMALL OTHER POP', '艺术玻璃旗舰店', 'B2C-Tmall'],
            ['tmall', 187259490, 'TMALL OTHER POP', '逸香酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 182339346, 'TMALL OTHER POP', '崟泰食品专营店', 'B2C-Tmall'],
            ['tmall', 61754686, 'TMALL OTHER POP', '雍天酒类专营店', 'B2C-Tmall'],
            ['tmall', 64482742, 'TMALL OTHER POP', '裕诚酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 165334654, 'TMALL OTHER POP', '裕泉酒类专营店', 'B2C-Tmall'],
            ['tmall', 70606664, 'TMALL OTHER POP', '誉佳顺酒类专营店', 'B2C-Tmall'],
            ['tmall', 6697591, 'TMALL OTHER POP', '源宏酒类折扣店', 'B2C-Tmall'],
            ['tmall', 188057778, 'TMALL OTHER POP', '张裕长庆和专卖店', 'B2C-Tmall'],
            ['tmall', 174412493, 'TMALL OTHER POP', '赵薇梦陇酒庄旗舰店', 'B2C-Tmall'],
            ['tmall', 69208154, 'TMALL OTHER POP', '哲畅酒类专营店', 'B2C-Tmall'],
            ['tmall', 186600047, 'TMALL OTHER POP', '喆园酒类专营店', 'B2C-Tmall'],
            ['tmall', 185354125, 'TMALL OTHER POP', '真韩食品专营店', 'B2C-Tmall'],
            ['tmall', 190319525, 'TMALL OTHER POP', '真露酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 187567202, 'TMALL OTHER POP', '正鸿食品专营店', 'B2C-Tmall'],
            ['tmall', 192876165, 'TMALL OTHER POP', '正善酒类专营店', 'B2C-Tmall'],
            ['tmall', 60948875, 'TMALL OTHER POP', '中酒网官方旗舰店', 'B2C-Tmall'],
            ['tmall', 189685253, 'TMALL OTHER POP', '众思创酒类专营店', 'B2C-Tmall'],
            ['tmall', 192017787, 'TMALL OTHER POP', '众酉酒类专营店', 'B2C-Tmall'],
            ['tmall', 186342503, 'TMALL OTHER POP', '卓羿酒类专营店', 'B2C-Tmall'],
            ['tmall', 192075749, 'TMALL OTHER POP', '纵答食品专营店', 'B2C-Tmall'],
            ['tmall', 13262082, 'TMALL OTHER POP', '醉梦酒类专营店', 'B2C-Tmall'],
            ['jd', 783024, 'JD OTHER POP', '京糖我要酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 778221, 'JD OTHER POP', '醴鱼酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 117815, 'JD OTHER POP', '京糖我要酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 723860, 'JD OTHER POP', '旻彤精品酒业旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 189890305, 'TMALL OTHER POP', 'whiskyl旗舰店', 'B2C-Tmall'],
            ['tmall', 170685533, 'TMALL INTERNATIONAL', '天猫国际进口超市', 'B2C-Tmall'],
            ['tmall', 181871121, 'TMALL INTERNATIONAL', '天猫国际进口超市国内现货', 'B2C-Tmall'],
            ['jd', 670462, 'JD OTHER POP', 'Bannychoice海外旗舰店', 'B2C-Standalone B2C'],
            ['jd', 109604, 'JD OTHER POP', 'CMP巴黎庄园葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000084051, 'JD SELF RUN', 'DBR拉菲葡萄酒京东自营专卖店', 'B2C-Standalone B2C'],
            ['jd', 206860, 'JD OTHER POP', 'JOYVIO佳沃葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000189381, 'JD SELF RUN', 'MO�0�9T酩悦香槟自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 888220, 'JD OTHER FSS', 'POWER STATION动力火车旗舰店', 'B2C-Standalone B2C'],
            ['jd', 213398, 'JD OTHER FSS', '奥贝尔庄园红酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 692398, 'JD OTHER POP', '澳享葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000165881, 'JD SELF RUN', '八角星葡萄酒自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 880552, 'JD OTHER FSS', '八芒星酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 626202, 'JD OTHER FSS', '奔富麦克斯旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000100881, 'JD SELF RUN', '宾三得利洋酒自营店', 'B2C-Standalone B2C'],
            ['jd', 734930, 'JD OTHER FSS', '伯克英达官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000120985, 'JD SELF RUN', '布多格酒类京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 588286, 'JD OTHER FSS', '超级波精品旗舰店', 'B2C-Standalone B2C'],
            ['jd', 140755, 'JD OTHER POP', '当歌国际酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000090349, 'JD SELF RUN', '法国列级名庄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 863174, 'JD OTHER POP', '法国美酒精品店', 'B2C-Standalone B2C'],
            ['jd', 949978, 'JD SELF RUN', '菲特瓦葡萄酒京东自营官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 691720, 'JD OTHER POP', '福易浩酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 720539, 'JD OTHER POP', '富德酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 953900, 'JD OTHER FSS', '富瑞拉酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 777602, 'JD OTHER FSS', '富森丽安葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 686900, 'JD OTHER FSS', '富邑葡萄酒海外旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000091188, 'JD SELF RUN', '贺兰山葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 801770, 'JD OTHER POP', '恒众伟业葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000076993, 'JD SELF RUN', '红酒海外自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 709725, 'JD OTHER POP', '鸿大葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 183917, 'JD OTHER POP', '环球时代酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 885816, 'JD OTHER POP', '汇泉洋酒专营店', 'B2C-Standalone B2C'],
            ['jd', 602024, 'JD OTHER FSS', '吉卖汇官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 98982, 'JD OTHER FSS', '集美旗舰店', 'B2C-Standalone B2C'],
            ['jd', 807471, 'JD OTHER FSS', '集颜旗舰店', 'B2C-Standalone B2C'],
            ['jd', 182514, 'JD OTHER POP', '加枫国际进口食品专营店', 'B2C-Standalone B2C'],
            ['jd', 688680, 'JD OTHER FSS', '加州乐事官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 925445, 'JD OTHER POP', '贾真酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 665479, 'JD OTHER POP', '杰奥森红酒专营店', 'B2C-Standalone B2C'],
            ['jd', 205310, 'JD OTHER POP', '京方丹酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 106117, 'JD OTHER FSS', '京酒汇官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 583357, 'JD OTHER POP', '九荣图酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 783878, 'JD OTHER POP', '久悦久酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 53424, 'JD OTHER FSS', '酒葫芦官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 201977, 'JD OTHER FSS', '酒惠网精品旗舰店', 'B2C-Standalone B2C'],
            ['jd', 106116, 'JD OTHER FSS', '酒联聚旗舰店', 'B2C-Standalone B2C'],
            ['jd', 606501, 'JD OTHER FSS', '酒品惠官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 868288, 'JD OTHER FSS', '酒速汇旗舰店', 'B2C-Standalone B2C'],
            ['jd', 32252, 'JD OTHER POP', '酒仙网精品旗舰店', 'B2C-Standalone B2C'],
            ['jd', 173196, 'JD OTHER FSS', '酒一站旗舰店', 'B2C-Standalone B2C'],
            ['jd', 724349, 'JD OTHER FSS', '聚藏旗舰店', 'B2C-Standalone B2C'],
            ['jd', 868417, 'JD OTHER FSS', '卡伯纳官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 802859, 'JD OTHER FSS', '卡聂高旗舰店', 'B2C-Standalone B2C'],
            ['jd', 992265, 'JD OTHER FSS', '凯特伊曼酒庄葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000091242, 'JD SELF RUN', '拉蒙葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000091203, 'JD SELF RUN', '乐朗1374葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 766970, 'JD OTHER FSS', '勒度官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 841646, 'JD OTHER FSS', '雷司葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 659031, 'JD OTHER FSS', '雷司旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000072603, 'JD SELF RUN', '类人首葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 862679, 'JD OTHER POP', '利港葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 891054, 'JD OTHER FSS', '利藤葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 708133, 'JD OTHER POP', '利盈红酒专营店', 'B2C-Standalone B2C'],
            ['jd', 853486, 'JD OTHER FSS', '琳赛葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 676859, 'JD OTHER FSS', '零利官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 870978, 'JD OTHER FSS', '泸州老窖进口葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 717858, 'JD OTHER FSS', '吕森堡酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 733753, 'JD OTHER FSS', '曼妥思酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 740451, 'JD OTHER FSS', '玫嘉官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 776736, 'JD OTHER FSS', '玫嘉旗舰店', 'B2C-Standalone B2C'],
            ['jd', 663701, 'JD OTHER POP', '美国国家酒馆', 'B2C-Standalone B2C'],
            ['jd', 728304, 'JD OTHER FSS', '美景庄园官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 685998, 'JD OTHER FSS', '美玫圣官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 622892, 'JD OTHER FSS', '酩品大师酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 'not found', 'JD SELF RUN', '酩悦轩尼诗帝亚吉欧MHD自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 107067, 'JD OTHER FSS', '南航发现会葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 780774, 'JD OTHER FSS', '宁夏贺兰山东麓青铜峡产区官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 809546, 'JD OTHER FSS', '女爵酒庄官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 780857, 'JD OTHER FSS', '佩蒂克斯葡萄酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 912282, 'JD OTHER FSS', '青岛保税港区官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000135321, 'JD SELF RUN', '清酒屋京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 753952, 'JD OTHER FSS', '萨克森堡酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 190315, 'JD OTHER POP', '赛普利斯红酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000103573, 'JD OTHER POP', '三全测试店铺333', 'B2C-Standalone B2C'],
            ['jd', 804351, 'JD OTHER POP', '莎玛拉庄园酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000281684, 'JD SELF RUN', '圣侯爵葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 583005, 'JD OTHER FSS', '十点品酒酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 988739, 'JD OTHER FSS', '蜀兼香酒类旗舰店', 'B2C-Standalone B2C'],
            ['jd', 785877, 'JD OTHER POP', '蜀兼香酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000159902, 'JD SELF RUN', '苏州桥酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000178125, 'JD SELF RUN', '獭祭（DASSAI）海外自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 779694, 'JD OTHER POP', '萄醇葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000078445, 'JD SELF RUN', '通化葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 654757, 'JD OTHER FSS', '万酒网红酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 626312, 'JD OTHER FSS', '王朝葡萄酒旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000100882, 'JD SELF RUN', '微醺洋酒自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000091521, 'JD SELF RUN', '唯浓葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000156067, 'JD SELF RUN', '夏桐Chandon自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 184977, 'JD OTHER FSS', '橡树之约旗舰店', 'B2C-Standalone B2C'],
            ['jd', 777642, 'JD OTHER POP', '新华酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000101205, 'JD SELF RUN', '星得斯葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 852797, 'JD OTHER FSS', '兴利臻酒汇旗舰店', 'B2C-Standalone B2C'],
            ['jd', 1000140024, 'JD SELF RUN', '轩尼诗Hennessy自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 956156, 'JD OTHER FSS', '一号城堡酒类官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 789982, 'JD OTHER POP', '壹扬扬酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 583708, 'JD OTHER POP', '怡九福酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 583268, 'JD OTHER POP', '怡九怡五酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 1000094450, 'JD SELF RUN', '易指酒类京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 618447, 'JD OTHER POP', '逸隆酒业专营店', 'B2C-Standalone B2C'],
            ['jd', 975050, 'JD OTHER POP', '银泰葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 210015, 'JD OTHER POP', '昱嵘酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 790179, 'JD OTHER POP', '云朵葡萄酒专营店', 'B2C-Standalone B2C'],
            ['jd', 1000101743, 'JD SELF RUN', '张裕葡萄酒京东自营专区', 'B2C-Standalone B2C'],
            ['jd', 1000118610, 'JD SELF RUN', '张裕先锋葡萄酒京东自营旗舰店', 'B2C-Standalone B2C'],
            ['jd', 629979, 'JD OTHER POP', '臻品尚酒类专营店', 'B2C-Standalone B2C'],
            ['jd', 837457, 'JD OTHER FSS', '中酒网红酒官方旗舰店', 'B2C-Standalone B2C'],
            ['jd', 653600, 'JD OTHER FSS', '中淘网官方旗舰店', 'B2C-Standalone B2C'],
            ['tmall', 193018741, 'TMALL OTHER POP', '0葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 188273793, 'TMALL OTHER POP', 'Aldi海外旗舰店', 'B2C-Tmall'],
            ['tmall', 187646666, 'TMALL OTHER POP', 'delegat酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 170680601, 'TMALL OTHER POP', 'jessicassuitcase海外旗舰', 'B2C-Tmall'],
            ['tmall', 190257181, 'TMALL OTHER POP', 'js90高分葡萄酒旗舰店', 'B2C-Tmall'],
            ['tmall', 192904481, 'TMALL OTHER POP', 'mestia梅斯蒂亚酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 185263311, 'TMALL OTHER POP', 'paologuidi旗舰店', 'B2C-Tmall'],
            ['tmall', 189744812, 'TMALL OTHER POP', 'vino75海外旗舰店', 'B2C-Tmall'],
            ['tmall', 192926450, 'TMALL OTHER POP', 'woegobo沃歌堡旗舰店', 'B2C-Tmall'],
            ['tmall', 191236011, 'TMALL OTHER POP', '爱龙堡酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 192882168, 'TMALL OTHER POP', '澳淘酒类专营店', 'B2C-Tmall'],
            ['tmall', 190558245, 'TMALL OTHER POP', '柏尔兰堡旗舰店', 'B2C-Tmall'],
            ['tmall', 192486369, 'TMALL OTHER POP', '奔富品尚汇专卖店', 'B2C-Tmall'],
            ['tmall', 193211468, 'TMALL OTHER POP', '川富旗舰店', 'B2C-Tmall'],
            ['tmall', 164537968, 'TMALL OTHER POP', '创群酒类专营店', 'B2C-Tmall'],
            ['tmall', 10674908, 'TMALL OTHER POP', '德龙宝真酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 193000541, 'TMALL OTHER POP', '丁戈树旗舰店', 'B2C-Tmall'],
            ['tmall', 192144830, 'TMALL OTHER POP', '鼎沃酒类专营店', 'B2C-Tmall'],
            ['tmall', 192915033, 'TMALL OTHER POP', '福事多酒类专营店', 'B2C-Tmall'],
            ['tmall', 68643227, 'TMALL OTHER POP', '谷度酒类专营店', 'B2C-Tmall'],
            ['tmall', 192906330, 'TMALL OTHER POP', '海日新酒类专营店', 'B2C-Tmall'],
            ['tmall', 193020949, 'TMALL OTHER POP', '酒奢酒类专营店', 'B2C-Tmall'],
            ['tmall', 190446898, 'TMALL OTHER POP', '马标旗舰店', 'B2C-Tmall'],
            ['tmall', 177178640, 'TMALL OTHER POP', '玛嘉唯诺酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 190634211, 'TMALL OTHER POP', '玛隆酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 193120192, 'TMALL OTHER POP', '梅乃宿旗舰店', 'B2C-Tmall'],
            ['tmall', 188950240, 'TMALL OTHER POP', '梦德斯诺旗舰店', 'B2C-Tmall'],
            ['tmall', 192637786, 'TMALL OTHER POP', '谋勤酒类专营店', 'B2C-Tmall'],
            ['tmall', 193160265, 'TMALL OTHER POP', '乔治金瀚旗舰店', 'B2C-Tmall'],
            ['tmall', 70053226, 'TMALL OTHER POP', '首悠汇酒类专营店', 'B2C-Tmall'],
            ['tmall', 193097743, 'TMALL OTHER POP', '铄今葡萄酒专营店', 'B2C-Tmall'],
            ['tmall', 190853387, 'TMALL OTHER POP', '天猫优品官方直营', 'B2C-Tmall'],
            ['tmall', 193197368, 'TMALL OTHER POP', '万来喜酒类专营店', 'B2C-Tmall'],
            ['tmall', 164489419, 'TMALL OTHER POP', '威龙酒类官方旗舰店', 'B2C-Tmall'],
            ['tmall', 192858212, 'TMALL OTHER POP', '希雅斯酒庄酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 12577893, 'TMALL OTHER POP', '香格里拉酒类旗舰店', 'B2C-Tmall'],
            ['tmall', 174535535, 'TMALL OTHER POP', '姚明葡萄酒官方旗舰店', 'B2C-Tmall'],
            ['tmall', 189068868, 'TMALL OTHER POP', '意纯酒类专营店', 'B2C-Tmall'],
            ['tmall', 192620534, 'TMALL OTHER POP', '优西优西酒类专营店', 'B2C-Tmall'],
            ['tmall', 192866403, 'TMALL OTHER POP', '长城简拙专卖店', 'B2C-Tmall'],
            ['tmall', 191911772, 'TMALL OTHER POP', '长城圣玖专卖店', 'B2C-Tmall'],
            ['tmall', 192830002, 'TMALL OTHER POP', '长城长庆和专卖店', 'B2C-Tmall'],
            ['tmall', 191503070, 'TMALL OTHER POP', '紫桐葡萄酒旗舰店', 'B2C-Tmall'],
            ['jd', 1000004382, 'JD SELF RUN', '芝华士自营旗舰店', 'B2C-Standalone B2C'],
        ]
        if source is None:
            return data
        type = None
        for v in data:
            if source == v[0] and str(sid) == str(v[1]):
                type = v[2]
        return type


    def item_list(self):
        a = [
            ['jd', '100000692609'],
            ['jd', '100001329855'],
            ['jd', '100001562785'],
            ['jd', '100001741146'],
            ['jd', '100001891323'],
            ['jd', '100002007551'],
            ['jd', '100002007553'],
            ['jd', '100002089772'],
            ['jd', '100002089832'],
            ['jd', '100002828472'],
            ['jd', '100002828474'],
            ['jd', '100002828482'],
            ['jd', '100002828484'],
            ['jd', '100002829853'],
            ['jd', '100002829855'],
            ['jd', '100002829863'],
            ['jd', '100002951434'],
            ['jd', '100003251823'],
            ['jd', '100003251871'],
            ['jd', '100003251941'],
            ['jd', '100003272853'],
            ['jd', '100003463883'],
            ['jd', '100003539365'],
            ['jd', '100003539377'],
            ['jd', '100003543357'],
            ['jd', '100003606011'],
            ['jd', '100003606013'],
            ['jd', '100003606015'],
            ['jd', '100003970350'],
            ['jd', '100003970352'],
            ['jd', '100004079909'],
            ['jd', '100004471854'],
            ['jd', '100004471856'],
            ['jd', '100004478167'],
            ['jd', '100004478169'],
            ['jd', '100005316780'],
            ['jd', '100005316802'],
            ['jd', '100005316810'],
            ['jd', '100005316820'],
            ['jd', '100005358786'],
            ['jd', '100005358788'],
            ['jd', '100005468307'],
            ['jd', '100005619892'],
            ['jd', '100005739946'],
            ['jd', '100005892108'],
            ['jd', '100005892174'],
            ['jd', '100005892244'],
            ['jd', '100005899978'],
            ['jd', '100005909152'],
            ['jd', '100006026722'],
            ['jd', '100006967882'],
            ['jd', '100006967886'],
            ['jd', '100006967888'],
            ['jd', '100006967890'],
            ['jd', '100007764252'],
            ['jd', '100007764254'],
            ['jd', '100007764280'],
            ['jd', '100007764282'],
            ['jd', '100008420844'],
            ['jd', '100009105464'],
            ['jd', '100009742802'],
            ['jd', '100009932008'],
            ['jd', '1014615'],
            ['jd', '1014620'],
            ['jd', '1082012'],
            ['jd', '1182927'],
            ['jd', '1312519'],
            ['jd', '1844808'],
            ['jd', '1847595'],
            ['jd', '1847606'],
            ['jd', '202567'],
            ['jd', '2214425'],
            ['jd', '302813'],
            ['jd', '331304'],
            ['jd', '331306'],
            ['jd', '331307'],
            ['jd', '331308'],
            ['jd', '331309'],
            ['jd', '331310'],
            ['jd', '331311'],
            ['jd', '331314'],
            ['jd', '331315'],
            ['jd', '331326'],
            ['jd', '331329'],
            ['jd', '331330'],
            ['jd', '331332'],
            ['jd', '331333'],
            ['jd', '331335'],
            ['jd', '331337'],
            ['jd', '331339'],
            ['jd', '331340'],
            ['jd', '331342'],
            ['jd', '331343'],
            ['jd', '331347'],
            ['jd', '331348'],
            ['jd', '331349'],
            ['jd', '331350'],
            ['jd', '331351'],
            ['jd', '331352'],
            ['jd', '331355'],
            ['jd', '331371'],
            ['jd', '331374'],
            ['jd', '331375'],
            ['jd', '331377'],
            ['jd', '331600'],
            ['jd', '3869388'],
            ['jd', '394187'],
            ['jd', '394188'],
            ['jd', '394191'],
            ['jd', '3991123'],
            ['jd', '4212081'],
            ['jd', '4212083'],
            ['jd', '4679172'],
            ['jd', '4794361'],
            ['jd', '4794417'],
            ['jd', '4794545'],
            ['jd', '4885650'],
            ['jd', '506756'],
            ['jd', '5121182'],
            ['jd', '5121204'],
            ['jd', '5121206'],
            ['jd', '5121214'],
            ['jd', '5684337'],
            ['jd', '5771629'],
            ['jd', '5811170'],
            ['jd', '5811172'],
            ['jd', '6223863'],
            ['jd', '631417'],
            ['jd', '631418'],
            ['jd', '631419'],
            ['jd', '7038924'],
            ['jd', '7038944'],
            ['jd', '7252399'],
            ['jd', '7252401'],
            ['jd', '7252403'],
            ['jd', '7261670'],
            ['jd', '7261690'],
            ['jd', '7475449'],
            ['jd', '7490838'],
            ['jd', '764749'],
            ['jd', '7705085'],
            ['jd', '8750824'],
            ['jd', '931075'],
            ['jd', '955273'],
            ['tmall', '20055598572'],
            ['tmall', '20068621121'],
            ['tmall', '22354227999'],
            ['tmall', '22355231398'],
            ['tmall', '27361028375'],
            ['tmall', '27363432402'],
            ['tmall', '27484696060'],
            ['', ''],
            ['tmall', '36719239428'],
            ['tmall', '523246390191'],
            ['tmall', '523255528338'],
            ['tmall', '539962239055'],
            ['tmall', '545022918668'],
            ['tmall', '547083784711'],
            ['tmall', '554413761791'],
            ['tmall', '555681922476'],
            ['tmall', '555696650533'],
            ['tmall', '555736639896'],
            ['tmall', '557112972933'],
            ['tmall', '557113836816'],
            ['tmall', '557151648677'],
            ['tmall', '557165338413'],
            ['tmall', '557165478771'],
            ['tmall', '557201917121'],
            ['tmall', '557244358478'],
            ['tmall', '557302351384'],
            ['tmall', '557305051748'],
            ['tmall', '557342411349'],
            ['tmall', '557464816216'],
            ['tmall', '557552257447'],
            ['tmall', '557599414953'],
            ['tmall', '557659659003'],
            ['tmall', '559299297585'],
            ['tmall', '559306766267'],
            ['tmall', '559355618908'],
            ['tmall', '559422055023'],
            ['tmall', '561289341757'],
            ['tmall', '561429315647'],
            ['tmall', '562142723941'],
            ['tmall', '566004897184'],
            ['tmall', '574843120367'],
            ['tmall', '575809952232'],
            ['tmall', '580003299822'],
            ['tmall', '580035928608'],
            ['tmall', '580062756637'],
            ['tmall', '580099215224'],
            ['tmall', '580142256702'],
            ['tmall', '580367834114'],
            ['tmall', '582889908830'],
            ['tmall', '583159810478'],
            ['tmall', '599338114098'],
            ['tmall', '603649204774'],
            ['tmall', '604110734167'],
            ['tmall', '604316123705'],
            ['tmall', '608482418279'],
            ['tmall', '528241271569'],
            ['tmall', '528287494918'],
            ['tmall', '528735279400'],
            ['tmall', '528783154969'],
            ['tmall', '528795309293'],
            ['tmall', '528803389790'],
            ['tmall', '528810266814'],
            ['tmall', '528825672179'],
            ['tmall', '528962527202'],
            ['tmall', '529311019384'],
            ['tmall', '529769840201'],
            ['tmall', '536471614515'],
            ['tmall', '536709135042'],
            ['tmall', '538911017018'],
            ['tmall', '541025671687'],
            ['tmall', '548613450765'],
            ['tmall', '557120256359'],
            ['tmall', '557204505740'],
            ['tmall', '557523912282'],
            ['tmall', '557533086847'],
            ['tmall', '561813351791'],
            ['tmall', '563705146890'],
            ['tmall', '575188435573'],
            ['tmall', '577524012392'],
            ['tmall', '577539360008'],
            ['tmall', '577542008761'],
            ['tmall', '577713637254'],
            ['tmall', '577832290829'],
            ['tmall', '578007847522'],
            ['tmall', '578440559869'],
            ['tmall', '580101924511'],
            ['tmall', '580282420483'],
            ['tmall', '580732859685'],
            ['tmall', '584960265507'],
            ['tmall', '584978441066'],
            ['tmall', '597822669573'],
            ['tmall', '601076499941'],
            ['tmall', '602309100166'],
            ['tmall', '603897517760'],
            ['tmall', '581014072267'],
            ['tmall', '581015248682'],
            ['tmall', '581207229752'],
            ['tmall', '581209021158'],
            ['tmall', '581209301659'],
            ['tmall', '581209981328'],
            ['tmall', '581331694976'],
            ['tmall', '581398764881'],
            ['tmall', '581461739469'],
            ['tmall', '581594309677'],
            ['tmall', '581715570160'],
            ['tmall', '581717126701'],
            ['tmall', '581717710519'],
            ['tmall', '584246968984'],
            ['tmall', '585921568416'],
            ['tmall', '586263118349'],
            ['tmall', '586265378507'],
            ['tmall', '586443436742'],
            ['tmall', '586450171148'],
            ['tmall', '586457852207'],
            ['tmall', '586658417176'],
            ['tmall', '586659389107'],
            ['tmall', '586673829956'],
            ['tmall', '586784207140'],
            ['tmall', '586791138267'],
            ['tmall', '586806962224'],
            ['tmall', '586963699086'],
            ['tmall', '592240675765'],
            ['tmall', '594759062080'],
            ['tmall', '594930043722'],
            ['tmall', '598851900335'],
            ['tmall', '600380086405'],
            ['tmall', '600726588032'],
            ['tmall', '600960473482'],
            ['tmall', '600963457084'],
            ['tmall', '603039725351'],
            ['tmall', '604493010245'],
            ['tmall', '12360409593'],
            ['tmall', '12412195074'],
            ['tmall', '12517883599'],
            ['tmall', '12529292952'],
            ['tmall', '12533508226'],
            ['tmall', '12606812952'],
            ['tmall', '16137438539'],
            ['tmall', '16536453072'],
            ['tmall', '16810345046'],
            ['tmall', '18353899004'],
            ['tmall', '18360123946'],
            ['tmall', '19461467707'],
            ['tmall', '20370100231'],
            ['tmall', '20403668607'],
            ['tmall', '20858036556'],
            ['tmall', '23440440079'],
            ['tmall', '26554184197'],
            ['tmall', '35092667999'],
            ['tmall', '36966436539'],
            ['tmall', '39840182253'],
            ['tmall', '39923271564'],
            ['tmall', '39991588582'],
            ['tmall', '40329030244'],
            ['tmall', '41736078143'],
            ['tmall', '41781512707'],
            ['tmall', '42130298574'],
            ['tmall', '42550003037'],
            ['tmall', '43162417877'],
            ['tmall', '43329855250'],
            ['tmall', '44948075191'],
            ['tmall', '521386400598'],
            ['tmall', '522177423590'],
            ['tmall', '523049326736'],
            ['tmall', '523242098839'],
            ['tmall', '523373996196'],
            ['tmall', '525502458604'],
            ['tmall', '525530686405'],
            ['tmall', '535994274319'],
            ['tmall', '535996294585'],
            ['tmall', '536011033067'],
            ['tmall', '536030189054'],
            ['tmall', '536411775823'],
            ['tmall', '536733204461'],
            ['tmall', '538326477510'],
            ['tmall', '538800684927'],
            ['tmall', '539378830765'],
            ['tmall', '542141934549'],
            ['tmall', '544399241223'],
            ['tmall', '547183055866'],
            ['tmall', '556020685596'],
            ['tmall', '556062246585'],
            ['tmall', '556496454729'],
            ['tmall', '556694664470'],
            ['tmall', '556906318677'],
            ['tmall', '556963183042'],
            ['tmall', '557195457812'],
            ['tmall', '557302587691'],
            ['tmall', '557302935705'],
            ['tmall', '557594018898'],
            ['tmall', '557600118502'],
            ['tmall', '557728173882'],
            ['tmall', '557933118989'],
            ['tmall', '558860764411'],
            ['tmall', '559298729097'],
            ['tmall', '559387521006'],
            ['tmall', '559423503952'],
            ['tmall', '561445560340'],
            ['tmall', '561771745272'],
            ['tmall', '562712208899'],
            ['tmall', '562712424963'],
            ['tmall', '562712640714'],
            ['tmall', '562876906171'],
            ['tmall', '562938016178'],
            ['tmall', '562953895754'],
            ['tmall', '563109837284'],
            ['tmall', '563725455586'],
            ['tmall', '563791671357'],
            ['tmall', '563998696640'],
            ['tmall', '564134708069'],
            ['tmall', '564238606292'],
            ['tmall', '564439277234'],
            ['tmall', '564489256805'],
            ['tmall', '564668911107'],
            ['tmall', '569142060235'],
            ['tmall', '569257784981'],
            ['tmall', '569272041731'],
            ['tmall', '569369562806'],
            ['tmall', '569453751378'],
            ['tmall', '571874055289'],
            ['tmall', '572547665959'],
            ['tmall', '572548081629'],
            ['tmall', '574960124317'],
            ['tmall', '575118413228'],
            ['tmall', '575333635919'],
            ['tmall', '575360257563'],
            ['tmall', '575698108486'],
            ['tmall', '575776961088'],
            ['tmall', '575855301931'],
            ['tmall', '576047483248'],
            ['tmall', '576812825759'],
            ['tmall', '577526988077'],
            ['tmall', '577609016574'],
            ['tmall', '577688480626'],
            ['tmall', '577759825412'],
            ['tmall', '577818878666'],
            ['tmall', '577834065213'],
            ['tmall', '577900950677'],
            ['tmall', '577973510306'],
            ['tmall', '577985690675'],
            ['tmall', '578020215030'],
            ['tmall', '578023388310'],
            ['tmall', '578030243549'],
            ['tmall', '578076251189'],
            ['tmall', '578099467830'],
            ['tmall', '578105603669'],
            ['tmall', '578316854423'],
            ['tmall', '578318526295'],
            ['tmall', '578525746709'],
            ['tmall', '578725483307'],
            ['tmall', '579376556280'],
            ['tmall', '580003966115'],
            ['tmall', '580182508285'],
            ['tmall', '580202048797'],
            ['tmall', '580289368169'],
            ['tmall', '580373049765'],
            ['tmall', '580386937860'],
            ['tmall', '580482941248'],
            ['tmall', '580496014149'],
            ['tmall', '580512162773'],
            ['tmall', '580605790119'],
            ['tmall', '580631947111'],
            ['tmall', '580734071596'],
            ['tmall', '580914424725'],
            ['tmall', '582998157952'],
            ['tmall', '583300368626'],
            ['tmall', '584659684514'],
            ['tmall', '585403660079'],
            ['tmall', '585468784514'],
            ['tmall', '585484279378'],
            ['tmall', '585823949860'],
            ['tmall', '586636281182'],
            ['tmall', '587615999949'],
            ['tmall', '588280222397'],
            ['tmall', '588605938809'],
            ['tmall', '588761815700'],
            ['tmall', '589336693675'],
            ['tmall', '591312600016'],
            ['tmall', '592029134836'],
            ['tmall', '592865010817'],
            ['tmall', '594567481812'],
            ['tmall', '595470307359'],
            ['tmall', '595765832538'],
            ['tmall', '596329549762'],
            ['tmall', '597396177399'],
            ['tmall', '597396565270'],
            ['tmall', '597402525994'],
            ['tmall', '597582238493'],
            ['tmall', '597589630750'],
            ['tmall', '598472067610'],
            ['tmall', '599550338767'],
            ['tmall', '599642093695'],
            ['tmall', '599959892453'],
            ['tmall', '599966942739'],
            ['tmall', '600511220903'],
            ['tmall', '601422214091'],
            ['tmall', '603636316706'],
            ['tmall', '603881117348'],
            ['tmall', '604301043592'],
            ['tmall', '604301495865'],
            ['tmall', '604645527580'],
            ['tmall', '605475280195'],
            ['tmall', '605486020448'],
            ['tmall', '605915876358'],
            ['tmall', '605916420588'],
            ['tmall', '605936994707'],
            ['tmall', '605957782137'],
            ['tmall', '605995104149'],
            ['tmall', '606166689859'],
            ['tmall', '606245153551'],
            ['tmall', '606469534384'],
            ['tmall', '606470138450'],
            ['tmall', '606470978046'],
            ['tmall', '606681311666'],
            ['tmall', '21509523405'],
            ['tmall', '21608407173'],
            ['tmall', '40563962650'],
            ['tmall', '40583297018'],
            ['tmall', '40771948085'],
            ['tmall', '40840871131'],
            ['tmall', '40892520886'],
            ['tmall', '43271271824'],
            ['tmall', '43293130479'],
            ['tmall', '43293290177'],
            ['tmall', '43327249891'],
            ['tmall', '43349169642'],
            ['tmall', '522555381924'],
            ['tmall', '522556002235'],
            ['tmall', '524396724966'],
            ['tmall', '524396784848'],
            ['tmall', '537669707111'],
            ['tmall', '537741722408'],
            ['tmall', '537876129382'],
            ['tmall', '537908288962'],
            ['tmall', '537909148466'],
            ['tmall', '537910776962'],
            ['tmall', '541054898688'],
            ['tmall', '550327226274'],
            ['tmall', '550365007937'],
            ['tmall', '553372518943'],
            ['tmall', '559358661797'],
            ['tmall', '559416774459'],
            ['tmall', '559417554429'],
            ['tmall', '570208990388'],
            ['tmall', '581721612585'],
            ['tmall', '594162560328'],
            ['tmall', '595168308632'],
            ['tmall', '595564030405'],
            ['tmall', '595567550995'],
            ['tmall', '597628363126'],
            ['tmall', '598907836885'],
            ['jd', '100005468297'],
            ['jd', '100005468299'],
            ['jd', '100005468301'],
            ['jd', '100005933637'],
            ['jd', '100005933653'],
            ['jd', '100005933663'],
            ['jd', '100005987469'],
            ['jd', '100009742800'],
            ['jd', '100009742804'],
            ['jd', '100009742806'],
            ['jd', '100010502108'],
            ['jd', '6170124'],
            ['jd', '7038920'],
            ['jd', '7038942'],
            ['tmall', '557459272417'],
            ['tmall', '557459964645'],
            ['tmall', '575767176105'],
            ['tmall', '578602638797'],
            ['tmall', '608163232985'],
            ['tmall', '609410428294'],
            ['tmall', '609515484038'],
            ['tmall', '540633806752'],
            ['tmall', '563825543499'],
            ['tmall', '581333706846'],
            ['tmall', '586792330820'],
            ['tmall', '586945895068'],
            ['tmall', '602421029143'],
            ['tmall', '610543134750'],
            ['tmall', '16970457684'],
            ['tmall', '19800426870'],
            ['tmall', '525167180958'],
            ['tmall', '540343670762'],
            ['tmall', '556777900559'],
            ['tmall', '556861185627'],
            ['tmall', '557343052405'],
            ['tmall', '557431341139'],
            ['tmall', '567671127863'],
            ['tmall', '575232866792'],
            ['tmall', '575981247404'],
            ['tmall', '577777853613'],
            ['tmall', '609318065540'],
            ['tmall', '610115317584'],
            ['tmall', '610118441669'],
            ['tmall', '599334478781'],
            ['tmall', '606474802928'],
            ['tmall', '606685747166'],
            ['tmall', '606686615353'],
            ['tmall', '609020439150'],
            ['jd', '100005933657'],
            ['jd', '100006026712'],
            ['jd', '100006136661'],
            ['jd', '100006967884'],
            ['jd', '100010667340'],
            ['jd', '100010667348'],
            ['jd', '100010667354'],
            ['jd', '100010667368'],
            ['jd', '7038946'],
            ['tmall', '578309885654'],
            ['tmall', '580222361823'],
            ['tmall', '608406356905'],
            ['tmall', '608407520821'],
            ['tmall', '12537892027'],
            ['tmall', '18900413565'],
            ['tmall', '556800525909'],
            ['tmall', '569487638015'],
            ['tmall', '580372273671'],
            ['tmall', '606907742289'],
            ['tmall', '610753826406'],
            ['tmall', '611119820977'],
            ['tmall', '611120224775'],
            ['tmall', '40583033884'],
            ['tmall', '599521439524'],
            ['tmall', '608300324901'],
            ['tmall', '610418674434'],
            ['jd', '100003547971'],
            ['tmall', '558817048914'],
            ['tmall', '12386513058'],
            ['tmall', '40382589959'],
            ['tmall', '42569834165'],
            ['tmall', '556787737816'],
            ['tmall', '557300007952'],
            ['tmall', '557654183036'],
            ['tmall', '611124401872'],
            ['tmall', '611630774870'],
            ['tmall', '612759870713'],
            ['tmall', '553769424325'],
            ['tmall', '553838725088'],
            ['tmall', '557454006435'],
            ['tmall', '566825979764'],
            ['tmall', '570050616417'],
            ['tmall', '570207816991'],
            ['tmall', '570275226484'],
            ['tmall', '570329765795'],
            ['tmall', '570334637069'],
            ['tmall', '570335129413'],
            ['tmall', '570335701211'],
            ['tmall', '570355743539'],
            ['tmall', '570433550221'],
            ['tmall', '570435022482'],
            ['tmall', '570436230692'],
            ['tmall', '570518927164'],
            ['tmall', '570523151482'],
            ['tmall', '573041007268'],
            ['tmall', '575730840541'],
            ['tmall', '588010740096'],
            ['tmall', '588014676287'],
            ['tmall', '594182440091'],
            ['tmall', '599421402033'],
            ['tmall', '599816038803'],
            ['tmall', '599819098714'],
            ['tmall', '606474106890'],
            ['tmall', '608794638413'],
            ['tmall', '612812060047'],
            ['tmall', '612812856230'],
            ['tmall', '26031952094'],
            ['tmall', '529024440466'],
            ['tmall', '570274595639'],
            ['tmall', '613518262181'],
            ['tmall', '570207444809'],
            ['jd', '100006671335'],
            ['tmall', '615020982492'],
            ['tmall', '613908328343'],
            ['tmall', '616599079474'],
            ['tmall', '41637168566'],
            ['tmall', '548931827427'],
            ['tmall', '558446568479'],
            ['tmall', '558650123914'],
            ['tmall', '578313321592'],
            ['tmall', '596740898241'],
            ['tmall', '612218139908'],
            ['tmall', '614079144359'],
            ['tmall', '614264260406'],
            ['tmall', '614266380640'],
            ['tmall', '614279896470'],
            ['tmall', '614543837643'],
            ['tmall', '614685436560'],
            ['tmall', '614802198276'],
            ['tmall', '614812714961'],
            ['tmall', '614818118329'],
            ['tmall', '614850480460'],
            ['tmall', '614973698569'],
            ['tmall', '615051843772'],
            ['tmall', '615307542890'],
            ['tmall', '615555723141'],
            ['tmall', '615557511128'],
            ['tmall', '570520535122'],
            ['tmall', '610957660762'],
            ['tmall', '611227921193'],
            ['tmall', '611228017763'],
            ['jd', '100007017599'],
            ['jd', '100007017667'],
            ['jd', '100012829478'],
            ['tmall', '613764202261'],
            ['tmall', '615677066902'],
            ['tmall', '617747720543'],
            ['tmall', '618216602656'],
            ['tmall', '618301810722'],
            ['tmall', '618475167775'],
            ['tmall', '618560967278'],
            ['tmall', '595166968927'],
            ['tmall', '613551004450'],
            ['tmall', '613564112947'],
            ['tmall', '613565244401'],
            ['tmall', '613657678323'],
            ['tmall', '614334395645'],
            ['tmall', '614334403213'],
            ['jd', '100013209958'],
            ['jd', '100013209962'],
            ['jd', '100013209984'],
            ['tmall', '526382950734'],
            ['tmall', '557283626592'],
            ['tmall', '559353914470'],
            ['tmall', '618375193717'],
            ['tmall', '577945411443'],
            ['tmall', '616647936252'],
            ['tmall', '618622639705'],
            ['tmall', '43670999094'],
            ['tmall', '550447617508'],
            ['tmall', '565941519280'],
            ['tmall', '569974748403'],
            ['tmall', '575605680614'],
            ['tmall', '579220277956'],
            ['tmall', '611464552858'],
            ['tmall', '614961121188'],
            ['tmall', '594169348336'],
            ['tmall', '613835073482'],
            ['tmall', '614333751916'],
            ['tmall', '614335263334'],
            ['tmall', '619889470168']
        ]
        return a

    def pre_brush_modify(self, v, products, prefix):
        for pid in products:
            if products[pid]['spid21'] != '':
                break
            v = products[pid]['spid6'].split('*')
            products[pid]['spid21'] = v[0]
            products[pid]['spid22'] = '1' if len(v) == 1 else v[1]


    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if v['flag'] == 2 and vv['sku_name'].find('___') == -1:
                vv['sp5'] = str(int(vv['sp22']) * vv['number'])
            if vv['sp31'] != '':
                v5 = vv['sp5'].replace('瓶','') or 1
                v32 = eval(vv['sp31'].replace('ml','').replace(u'\u200e',''))
                vv['sp32'] = '{:g}ml'.format(round(v32))
                vv['sp6'] = '{:g}ml'.format(round(v32/int(v5)))
            elif vv['sp6'] != '':
                v6 = eval(vv['sp6'].replace('ml',''))
                vv['sp6'] = '{:g}ml'.format(round(v6))
            if vv['sp5']:
                vv['sp5'] = vv['sp5'].replace('瓶','')+'瓶'


    def finish_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `spBulk Sales` = IF(
                clean_alias_all_bid in (139066,380545)
                AND sid in (194641035,189367760,1000140024,188438,192723120,9730213,1000097462,883600,11804722)
                AND clean_sku_id > 0 AND clean_brush_id > 0
                AND `sp人头马_子品类` = '干邑白兰地' AND `sp人头马_三厂商干邑级别` != 'LXIII'
                AND clean_sales/clean_num/100/toInt32OrZero(`sp出数用内包装瓶数`) >= 20000,
                '是',
                '否'
            )
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `spBulk Sales` = IF(
                clean_alias_all_bid in (139066)
                AND sid in (194641035,189367760,1000140024,188438)
                AND `sp人头马_三厂商干邑级别` != 'LXIII'
                AND clean_sales/clean_num/100/toInt32OrZero(`sp出数用内包装瓶数`) >= 20000
                AND multiSearchAny(concat(name, toString(`trade_props.value`)), ['企业团购','团购专属','南方专用','不拍了','测试款','积分兑换']),
                '是',
                `spBulk Sales`
            )
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        self.hotfix_tex(tbl, dba, prefix)
        self.hotfix_items(tbl, dba, prefix)

        db26 = self.cleaner.get_db('26_apollo')
        sql = ''' SELECT DISTINCT pid FROM product_lib.product_90526 WHERE name LIKE '\_\_\_%' '''
        ret = db26.query_all(sql)
        pids = ','.join([str(v) for v, in ret])

        sql = '''
            ALTER TABLE {} UPDATE `sp系列` = 'Others'
            WHERE (`source` != 1 OR `shop_type` not in [11,12]) AND `sp人头马_子品类` = '干邑白兰地'
              AND (clean_brush_id=0 OR clean_pid IN [{}]) AND pkey >= '2024-01-01'
            SETTINGS mutations_sync = 1
        '''.format(tbl, pids)
        dba.execute(sql)


    def hotfix_tex(self, tbl, dba, prefix):
        if prefix.find('edrington') != -1:
            return

        sql = '''
            WITH IF(fee NOT LIKE '%商家承担%' AND fee NOT LIKE '%已含税%' AND fee NOT LIKE '%包税%', rate, '0') AS r
            SELECT item_id, toYYYYMM(modified) m, r FROM artificial.ali_tax
            WHERE rate NOT IN ['','海关认定税率','组合商品税率由子商品税率组成'] ORDER BY item_id, m
        '''
        ret = dba.query_all(sql)

        sql = 'SELECT toYYYYMM(date) m FROM {} GROUP BY m ORDER BY m'.format(tbl)
        mmm = dba.query_all(sql)

        rr1, rr2 = [], {}
        for itemid, m, rate, in ret:
            itemid = str(itemid)
            rate = rate.replace('%','').split('-')
            rate = [float(r) for r in rate]
            rate = 1+sum(rate)/len(rate)/100
            rate = round(rate,5)
            rr1.append([str(itemid), rate])

            for mm, in mmm:
                if mm >= m:
                    rr2[(itemid, mm)] = [str(itemid), mm, rate]

        if len(rr1):
            dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
            dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))

            dba.execute('CREATE TABLE {}_join (item_id String, rate Float32) ENGINE = Join(ANY, LEFT, item_id)'.format(tbl))
            dba.execute('INSERT INTO {}_join VALUES'.format(tbl), rr1)
            dba.execute('CREATE TABLE {}_join2 (item_id String, m UInt32, rate Float32) ENGINE = Join(ANY, LEFT, item_id, m)'.format(tbl))
            dba.execute('INSERT INTO {}_join2 VALUES'.format(tbl), list(rr2.values()))

            sql = '''
                ALTER TABLE {t} UPDATE clean_sales = clean_sales*ifNull(
                    joinGet('{t}_join2', 'rate', item_id, toYYYYMM(date)),
                    ifNull(joinGet('{t}_join', 'rate', item_id),1)
                )
                WHERE `source` = 1 AND shop_type IN [22,24,27,9] AND NOT isNull(joinGet('{t}_join', 'rate', item_id))
            '''.format(t=tbl)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

            dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
            dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))


    def hotfix_items(self, tbl, dba, prefix):
        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202108'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2021,8,1),14738,50510004,'10227360632','','人头马（Remy Martin）CLUB优质香槟区干邑白兰地洋酒350ml 【下单享赠品】',46762,12,'14661',363187,363187,'',26900,29900,26900,'http://img11.360buyimg.com/n1/jfs/t1/77996/18/21313/68245/63235c49E3cb08794/ae860eb1867d975a.jpg',18023000,670,2,352,5895,'人头马','Single pack','CLUB优质香槟区洋酒350ML [下单享赠品]','1瓶','350ml','人头马Others干邑白兰地350ml','人头马CLUB干邑白兰地350ml','1','350','人头马特级 RemyClub','JD OTHER FSS','B2C-Standalone B2C','Single','JD OTHER FSS','瓶装','','','350ml','1','干邑白兰地350ml','干邑重点','其他产地','','','','单品','','','1','人头马CLUB干邑白兰地[350ml]','','有效链接','否','','前后月覆盖','干邑白兰地','白兰地','CLUB/VSOP','人头马','Remy CLUB','CLUB/VSOP','干邑白兰地','JD-FSS','',4742417,363187,26900,1,26900,7237,1,1335867],
                [ret[0][1],1,datetime.datetime(2021,8,1),14738,50510004,'55601625328','','人头马（Remy Martin） CLUB优质香槟区干邑白兰地洋酒350ml法国【入会有好礼】',46762,12,'14661',363187,363187,'',25900,29900,25900,'http://img11.360buyimg.com/n1/jfs/t1/185902/21/28935/62179/63234c35E84ec7e99/788d512ecb684a3e.jpg',117586000,4540,2,352,5895,'人头马','Single pack','CLUB优质香槟区洋酒350ML法国[入会有好礼]','1瓶','350ml','人头马Others干邑白兰地350ml','人头马CLUB干邑白兰地350ml','1','350','人头马特级 RemyClub','JD OTHER FSS','B2C-Standalone B2C','Multi Same Sku','JD OTHER FSS','瓶装','','','350ml','1','干邑白兰地350ml','干邑重点','其他产地','','','','单品','','','1','人头马CLUB干邑白兰地[350ml]','','有效链接','否','','前后月覆盖','干邑白兰地','白兰地','CLUB/VSOP','人头马','Remy CLUB','CLUB/VSOP','干邑白兰地','JD-FSS','',4742417,363187,25900,15,388500,7237,1,1335954]
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`clean_all_bid`,`clean_alias_all_bid`,`region_str`,`clean_price`,`org_price`,`promo_price`,`img`,`clean_sales`,`clean_num`,`source`,`clean_cid`,`clean_pid`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`,`sp套包宝贝`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202109'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2021,9,1),14738,50510004,'10227360632','','人头马（Remy Martin）CLUB优质香槟区干邑白兰地洋酒350ml 【下单享赠品】',46762,12,'14661',363187,363187,'',26900,29900,26900,'http://img11.360buyimg.com/n1/jfs/t1/77996/18/21313/68245/63235c49E3cb08794/ae860eb1867d975a.jpg',11836000,440,2,352,5895,'人头马','Single pack','CLUB优质香槟区洋酒350ML [下单享赠品]','1瓶','350ml','人头马Others干邑白兰地350ml','人头马CLUB干邑白兰地350ml','1','350','人头马特级 RemyClub','JD OTHER FSS','B2C-Standalone B2C','Single','JD OTHER FSS','瓶装','','','350ml','1','干邑白兰地350ml','干邑重点','其他产地','','','','单品','','','1','人头马CLUB干邑白兰地[350ml]','','有效链接','否','','前后月覆盖','干邑白兰地','白兰地','CLUB/VSOP','人头马','Remy CLUB','CLUB/VSOP','干邑白兰地','JD-FSS','',4742417,363187,26900,1,26900,7237,1,1335867],
                [ret[0][1],1,datetime.datetime(2021,9,1),14738,50510004,'55601625328','','人头马（Remy Martin） CLUB优质香槟区干邑白兰地洋酒350ml法国【入会有好礼】',46762,12,'14661',363187,363187,'',25900,29900,25900,'http://img11.360buyimg.com/n1/jfs/t1/185902/21/28935/62179/63234c35E84ec7e99/788d512ecb684a3e.jpg',14245000,550,2,352,5895,'人头马','Single pack','CLUB优质香槟区洋酒350ML法国[入会有好礼]','1瓶','350ml','人头马Others干邑白兰地350ml','人头马CLUB干邑白兰地350ml','1','350','人头马特级 RemyClub','JD OTHER FSS','B2C-Standalone B2C','Multi Same Sku','JD OTHER FSS','瓶装','','','350ml','1','干邑白兰地350ml','干邑重点','其他产地','','','','单品','','','1','人头马CLUB干邑白兰地[350ml]','','有效链接','否','','前后月覆盖','干邑白兰地','白兰地','CLUB/VSOP','人头马','Remy CLUB','CLUB/VSOP','干邑白兰地','JD-FSS','',4742417,363187,25900,15,388500,7237,1,1335954]
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`clean_all_bid`,`clean_alias_all_bid`,`region_str`,`clean_price`,`org_price`,`promo_price`,`img`,`clean_sales`,`clean_num`,`source`,`clean_cid`,`clean_pid`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`,`sp套包宝贝`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202112'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2021,12,1),14738,50510004,'10227360632','','人头马（Remy Martin）CLUB优质香槟区干邑白兰地洋酒350ml 【下单享赠品】',46762,12,'14661',363187,363187,'',26900,29900,26900,'http://img11.360buyimg.com/n1/jfs/t1/77996/18/21313/68245/63235c49E3cb08794/ae860eb1867d975a.jpg',85855200,3448,2,352,5895,'人头马','Single pack','CLUB优质香槟区洋酒350ML [下单享赠品]','1瓶','350ml','人头马Others干邑白兰地350ml','人头马CLUB干邑白兰地350ml','1','350','人头马特级 RemyClub','JD OTHER FSS','B2C-Standalone B2C','Single','JD OTHER FSS','瓶装','','','350ml','1','干邑白兰地350ml','干邑重点','其他产地','','','','单品','','','1','人头马CLUB干邑白兰地[350ml]','','有效链接','否','','前后月覆盖','干邑白兰地','白兰地','CLUB/VSOP','人头马','Remy CLUB','CLUB/VSOP','干邑白兰地','JD-FSS','',4742417,363187,26900,1,26900,7237,1,1335867],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`real_cid`,`item_id`,`sku_id`,`name`,`sid`,`shop_type`,`brand`,`clean_all_bid`,`clean_alias_all_bid`,`region_str`,`clean_price`,`org_price`,`promo_price`,`img`,`clean_sales`,`clean_num`,`source`,`clean_cid`,`clean_pid`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`,`sp套包宝贝`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202204'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2022,4,30),50522003,'668737895361','麦卡伦 THE MACALLAN 12年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,64500,0,0,117712500,1825,117712500,1825,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 12年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'668384370855','麦卡伦 THE MACALLAN 12年单一麦芽苏格兰威士忌三桶系列',195852076,21,'',421497,421497,421497,421497,49900,0,0,5738500,115,5738500,115,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 12年单一麦芽苏格兰威士忌三桶系列','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'668034913723','麦卡伦 THE MACALLAN 18年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,289007,0,0,114157925,395,114157925,395,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 18年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'667667548029','麦卡伦 THE MACALLAN 25年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,1740000,0,0,27840000,16,27840000,16,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 25年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','否','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'667671472734','麦卡伦 THE MACALLAN 30年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,3450000,0,0,6900000,2,6900000,2,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 30年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'682061444298','麦卡伦 THE MACALLAN 大师典藏酒樽系列璀璨 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,4340000,0,0,21700000,5,21700000,5,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'667646572788','麦卡伦 THE MACALLAN 焕新礼盒',195852076,21,'',421497,421497,421497,421497,61000,0,0,60207000,987,60207000,987,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 焕新礼盒','1瓶','700ml','麦卡伦Others其他威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','其他威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'667644908650','麦卡伦 THE MACALLAN 精萃2021限量版 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,93000,0,0,28272000,304,28272000,304,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 精萃2021限量版 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'668380290849','麦卡伦 THE MACALLAN 蓝钻12年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,56000,0,0,55160000,985,55160000,985,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 蓝钻12年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','出题宝贝','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'668738579961','麦卡伦 THE MACALLAN 蓝钻15年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,136000,0,0,14008000,103,14008000,103,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 蓝钻15年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'668037065455','麦卡伦 THE MACALLAN 甄选雪莉桶系列 皓钻 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,299900,0,0,26991000,90,26991000,90,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 甄选雪莉桶系列 皓钻 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'672236145023','【官方直营】 Naked Malt裸雀混合麦芽苏格兰威士忌雪莉桶酒700ml',195957474,21,'',6335590,6335590,6335590,6335590,24548,0,0,957400,39,957400,39,1,ret[0][1],0,0,1,0,'裸雀','Single pack','NAKED MALT混合麦芽威士忌雪莉桶 苏格兰进口洋酒700ML','1瓶','700ml','裸雀Others调和麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','调和麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','','前后月覆盖','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'672255321221','【官方直营】Naked Malt裸雀混合麦芽雪莉桶威士忌洋酒调酒杯礼盒',195957474,21,'',6335590,6335590,6335590,6335590,34700,0,0,104100,3,104100,3,1,ret[0][1],0,0,1,0,'裸雀','Bundle pack','NAKED MALT混合麦芽威士忌雪莉桶苏格兰洋酒调酒杯套装','1瓶','700ml','裸雀Others调和麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','礼盒装','','','700ml','','调和麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','','否','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'671857376260','【官方直营】Naked Malt裸雀混合麦芽雪莉桶苏格兰威士忌 50ml×6',195957474,21,'',6335590,6335590,6335590,6335590,18900,0,0,75600,4,75600,4,1,ret[0][1],0,0,1,0,'裸雀','Single pack','NAKED MALT混合麦芽威士忌雪莉桶 苏格兰洋酒分享瓶50ML','6瓶','50ml','裸雀Others调和麦芽威士忌50ml','','6','50','','TMALL OTHER POP','B2C-Tmall','Multi Same Sku','TMALL OTHER POP','礼盒装','','','50ml','','调和麦芽威士忌50ml','','英国/苏格兰','','','','单品','50ml*6','300ml','','','','有效链接','否','','否','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'672916819836','【官方直营】Naked Malt裸雀混合麦芽雪莉桶苏格兰威士忌礼盒套装',195957474,21,'',6335590,6335590,6335590,6335590,27000,0,0,108000,4,108000,4,1,ret[0][1],0,0,1,0,'裸雀','Bundle pack','NAKED MALT混合麦芽威士忌雪莉桶苏格兰官方洋酒组合套装','1瓶','700ml','裸雀Others调和麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','礼盒装','','','700ml','','调和麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','','前后月覆盖','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'672917519908','【官方直营】Naked Malt裸雀混合麦芽雪莉桶苏格兰威士忌分享瓶装',195957474,21,'',6335590,6335590,6335590,6335590,33500,0,0,67000,2,67000,2,1,ret[0][1],0,0,1,0,'裸雀','Single pack','NAKED MALT混合麦芽威士忌雪莉桶苏格兰洋酒礼盒装分享瓶','1瓶','700ml','裸雀Others调和麦芽威士忌700ml','','2','700','','TMALL OTHER POP','B2C-Tmall','Multi Same Sku','TMALL OTHER POP','礼盒装','','','700ml','','调和麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','','否','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
                [1,datetime.datetime(2022,4,30),50522003,'671862844088','【官方直营】Naked Malt裸雀混合麦芽雪莉桶威士忌洋酒搪瓷杯礼盒',195957474,21,'',6335590,6335590,6335590,6335590,31100,0,0,62200,2,62200,2,1,ret[0][1],0,0,1,0,'裸雀','Bundle pack','NAKED MALT混合麦芽威士忌雪莉桶苏格兰洋酒搪瓷杯礼盒装','1瓶','700ml','裸雀Others调和麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','礼盒装','','','700ml','','调和麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','','否','调和麦芽威士忌','威士忌','','爱丁顿','Others','others','调和威士忌','TM-FSS'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202205'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2022,5,31),50522003,'668737895361','麦卡伦 THE MACALLAN 12年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,64500,0,0,190791000,2958,190791000,2958,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 12年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668384370855','麦卡伦 THE MACALLAN 12年单一麦芽苏格兰威士忌三桶系列',195852076,21,'',421497,421497,421497,421497,49900,0,0,7934100,159,7934100,159,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 12年单一麦芽苏格兰威士忌三桶系列','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668034913723','麦卡伦 THE MACALLAN 18年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,297000,0,0,210870000,710,210870000,710,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 18年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'667667548029','麦卡伦 THE MACALLAN 25年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,1740000,0,0,3480000,2,3480000,2,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 25年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','否','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'667647028428','麦卡伦 THE MACALLAN 波普大师珍藏系列 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,680000,0,0,680000,1,680000,1,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','否','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'667646572788','麦卡伦 THE MACALLAN 焕新礼盒',195852076,21,'',421497,421497,421497,421497,61000,0,0,104920000,1720,104920000,1720,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 焕新礼盒','1瓶','700ml','麦卡伦Others其他威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','其他威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'667644908650','麦卡伦 THE MACALLAN 精萃2021限量版 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,93000,0,0,38130000,410,38130000,410,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 精萃2021限量版 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668380290849','麦卡伦 THE MACALLAN 蓝钻12年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,56000,0,0,86688000,1548,86688000,1548,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 蓝钻12年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','出题宝贝','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668738579961','麦卡伦 THE MACALLAN 蓝钻15年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,136000,0,0,66096000,486,66096000,486,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 蓝钻15年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'667644700212','麦卡伦 THE MACALLAN 蓝钻18年 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,258000,0,0,56244000,218,56244000,218,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 蓝钻18年 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668037065455','麦卡伦 THE MACALLAN 甄选雪莉桶系列 皓钻 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,299900,0,0,55481500,185,55481500,185,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 甄选雪莉桶系列 皓钻 单一麦芽苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'668740779872','麦卡伦 THE MACALLAN 庄园 单一麦芽 苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,165000,0,0,14025000,85,14025000,85,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 庄园 单一麦芽 苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'673281452318','【官方直营】奥克尼高原骑士单一麦芽苏格兰威士忌雪莉桶21年限量',195957474,21,'',500177,500177,500177,500177,220000,0,0,12100000,55,12100000,55,1,ret[0][1],0,0,1,0,'Highland Park 高原骑士','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,5,31),50522003,'673281496827','【官方直营】奥克尼高原骑士原桶强度单一麦芽苏格兰威士忌雪莉桶',195957474,21,'',500177,500177,500177,500177,58000,0,0,10150000,175,10150000,175,1,ret[0][1],0,0,1,0,'Highland Park 高原骑士','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202206'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [1,datetime.datetime(2022,6,30),50522003,'667647028428','麦卡伦 THE MACALLAN 波普大师珍藏系列 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,680000,0,0,2040000,3,2040000,3,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 庄园 单一麦芽 苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'683025103248','麦卡伦 THE MACALLAN 大师典藏酒樽系列璀璨?黑 单一麦芽威士忌',195852076,21,'',421497,421497,421497,421497,4900000,0,0,34300000,7,34300000,7,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 庄园 单一麦芽 苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'682061444298','麦卡伦 THE MACALLAN大师典藏酒樽系列璀璨 单一麦芽苏格兰威士忌',195852076,21,'',421497,421497,421497,421497,4340000,0,0,43400000,10,43400000,10,1,ret[0][1],0,0,1,0,'麦卡伦','Single pack','THE 庄园 单一麦芽 苏格兰威士忌','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'674333186989','【官方直营】奥克尼高原骑士单一麦芽苏格兰威士忌礼盒雪莉桶30年',195957474,21,'',500177,500177,500177,500177,1060000,0,0,29680000,28,29680000,28,1,ret[0][1],0,0,1,0,'Highland Park 高原骑士','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'673629408832','【官方直营】Glenrothes格兰路思单一麦芽苏格兰威士忌雪莉桶30年',195957474,21,'',2773836,2773836,2773836,2773836,760000,0,0,17480000,23,17480000,23,1,ret[0][1],0,0,1,0,'格兰路思','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'673281452318','【官方直营】奥克尼高原骑士单一麦芽苏格兰威士忌雪莉桶21年限量',195957474,21,'',500177,500177,500177,500177,220000,0,0,2640000,12,2640000,12,1,ret[0][1],0,0,1,0,'Highland Park 高原骑士','Single pack','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
                [1,datetime.datetime(2022,6,30),50522003,'673281496827','【官方直营】奥克尼高原骑士原桶强度单一麦芽苏格兰威士忌雪莉桶',195957474,21,'',500177,500177,500177,500177,58000,0,0,522000,9,522000,9,1,ret[0][1],0,0,1,0,'Highland Park 高原骑士','','','1瓶','700ml','麦卡伦Others单一麦芽威士忌700ml','','1','700','','TMALL OTHER POP','B2C-Tmall','Single','TMALL OTHER POP','瓶装','','','700ml','','单一麦芽威士忌700ml','','英国/苏格兰','','','','单品','700ml','700ml','','','','有效链接','否','MACALLAN/麦卡伦','前后月覆盖','单一麦芽威士忌','威士忌','','爱丁顿','Others','others','麦芽威士忌','TM-FSS'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`org_price`,`promo_price`,`sales`,`num`,`clean_sales`,`clean_num`,`source`,`clean_ver`,`clean_pid`,`clean_sku_id`,`clean_split_rate`,`clean_brush_id`,`sp品牌`,`sp是否套包`,`sp品名`,`sp瓶数`,`sp单品规格`,`spSKU(拼接)`,`sp客户sku出数名`,`sp出数用瓶数`,`sp出数用规格`,`sp出数用PRC`,`sp出数用渠道`,`sp出数用细分渠道`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp包装`,`sp版本`,`sp年份`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp辅助_交易属性`,`sp辅助-重点干邑品牌`,`sp产地`,`sp辅助_尊尼获加威士忌`,`sp威士忌年份`,`sp辅助_波本威士忌`,`spWGS套包类型`,`spWGS详细毫升数`,`spWGS总毫升数`,`spSKU件数`,`sp答题原始SKU`,`sp人头马_辅助剔除`,`sp是否无效链接`,`spBulk Sales`,`sp辅助_品牌`,`sp是否人工答题`,`sp子品类`,`sp一级类目`,`sp干邑级别`,`sp厂商`,`sp系列`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp店铺分类`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202208'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,1,datetime.datetime(2022,8,22),50522003,'680456173223','麦卡伦 THE MACALLAN 25年 单一麦芽苏格兰威士忌',195852076,21,421497,421497,421497,421497,1740000,125,217500000,125,217500000,'否','麦卡伦Others单一麦芽威士忌700ml','','单品','','','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','700ml','爱丁顿','THE 25年 单一麦芽苏格兰威士忌','麦卡伦','','单一麦芽威士忌','','','','TM-FSS','否','Single pack','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','MACALLAN/麦卡伦','','','','',''],
                [ret[0][1],1,1,datetime.datetime(2022,8,22),50522003,'674333186989','【官方直营】奥克尼高原骑士单一麦芽苏格兰威士忌礼盒雪莉桶30年',195957474,21,500177,500177,500177,500177,1060000,9,9540000,9,9540000,'否','Highland Park 高原骑士Others单一麦芽威士忌700ml','','单品','700ml','700ml','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','700ml','爱丁顿','','Highland Park 高原骑士','','单一麦芽威士忌','','','','TM-FSS','前后月覆盖','Single pack','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','MACALLAN/麦卡伦','','','','',''],
                [ret[0][1],1,1,datetime.datetime(2022,8,22),50522003,'677300316561','【官方直营】奥克尼高原骑士单一麦芽苏格兰泥煤威士忌雪莉桶25年',195957474,21,500177,500177,500177,500177,490000,12,5880000,12,5880000,'否','Highland Park 高原骑士Others单一麦芽威士忌700ml','','单品','700ml','700ml','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','700ml','爱丁顿','[官方直营]奥克尼单一麦芽苏格兰泥煤威士忌雪莉桶25年','Highland Park 高原骑士','','单一麦芽威士忌','','','','TM-FSS','前后月覆盖','Single pack','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','','','','','',''],
                [ret[0][1],1,1,datetime.datetime(2022,8,22),50522003,'684090090661','【官方直营】奥克尼高原骑士原桶强度单一麦芽苏格兰威士忌雪莉桶',195957474,21,500177,500177,500177,500177,58000,92,5336000,92,5336000,'否','Highland Park 高原骑士Others单一麦芽威士忌700ml','','单品','700ml','700ml','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','700ml','爱丁顿','','Highland Park 高原骑士','','单一麦芽威士忌','','','','TM-FSS','前后月覆盖','','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','MACALLAN/麦卡伦','','','','',''],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`source`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`all_bid`,`alias_all_bid`,`clean_all_bid`,`clean_alias_all_bid`,`price`,`num`,`sales`,`clean_num`,`clean_sales`,`spBulk Sales`,`spSKU(拼接)`,`spSKU件数`,`spWGS套包类型`,`spWGS总毫升数`,`spWGS详细毫升数`,`sp一级类目`,`sp产地`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp人头马_辅助剔除`,`sp出数用PRC`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp出数用渠道`,`sp出数用瓶数`,`sp出数用细分渠道`,`sp出数用规格`,`sp包装`,`sp单品规格`,`sp厂商`,`sp品名`,`sp品牌`,`sp套包宝贝`,`sp子品类`,`sp客户sku出数名`,`sp干邑级别`,`sp年份`,`sp店铺分类`,`sp是否人工答题`,`sp是否套包`,`sp是否无效链接`,`sp版本`,`sp瓶数`,`sp答题原始SKU`,`sp系列`,`sp辅助-重点干邑品牌`,`sp辅助_交易属性`,`sp辅助_品牌`,`sp辅助_尊尼获加威士忌`,`sp辅助_波本威士忌`,`sp威士忌年份`,`sp疑似新品`,`sp店铺英文名`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202301'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2023,1,1),14739,'10067572505546','布赫拉迪（Bruichladdich）洋酒 单一麦芽苏格兰威士忌 微物源单桶系列700ml 限量款 年货',12052289,12,'141110',4773316,3931622,133794,260,35054001,'',1,2,3931622,3931622,260,35054001,15510,'否','布赫拉迪Others单一麦芽威士忌700ml','1','单品','','','威士忌','其他产地','others','麦芽威士忌','','','700ml','1','Single','JD OTHER POP','JD OTHER POP','1','B2C-Standalone B2C','700','瓶装','700ml','人头马','洋酒 单一麦芽苏格兰威士忌 微物源单桶系列700ml 限量款 年货','布赫拉迪','','单一麦芽威士忌','','','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','布赫拉迪（Bruichladdich）微物源单桶系列2010/11年单一麦芽威士忌[700ml]','微物源单桶','','单一麦芽威士忌700ml','','','','','PHD JD FSS',''],
                [ret[0][1],1,datetime.datetime(2023,1,1),14739,'10067607880132','布赫拉迪（Bruichladdich）洋酒 泥煤怪兽11.2超重泥煤单一麦芽苏格兰威士忌 泥煤怪兽11.2 700ml',12052289,12,'141110',2492859,2492859,156162,97,15147727,'',1,2,2492859,2492859,97,15147727,15433,'否','布赫拉迪Others单一麦芽威士忌700ml','1','单品','','','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','1','Single','JD OTHER POP','JD OTHER POP','1','B2C-Standalone B2C','700','瓶装','700ml','人头马','洋酒 11.2超重泥煤单一麦芽苏格兰威士忌 OCTOMORE11.2 700ML','布赫拉迪','','单一麦芽威士忌','','','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','布赫拉迪（Bruichladdich）泥煤怪兽Octomore 11.2号单一麦芽威士忌[700ml]','11.2号','','单一麦芽威士忌700ml','','','','','PHD JD FSS','']
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`price`,`num`,`sales`,`img`,`tip`,`source`,`clean_alias_all_bid`,`clean_all_bid`,`clean_num`,`clean_sales`,`clean_pid`,`spBulk Sales`,`spSKU(拼接)`,`spSKU件数`,`spWGS套包类型`,`spWGS总毫升数`,`spWGS详细毫升数`,`sp一级类目`,`sp产地`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp人头马_辅助剔除`,`sp出数用PRC`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp出数用渠道`,`sp出数用瓶数`,`sp出数用细分渠道`,`sp出数用规格`,`sp包装`,`sp单品规格`,`sp厂商`,`sp品名`,`sp品牌`,`sp套包宝贝`,`sp子品类`,`sp客户sku出数名`,`sp干邑级别`,`sp年份`,`sp店铺分类`,`sp是否人工答题`,`sp是否套包`,`sp是否无效链接`,`sp版本`,`sp瓶数`,`sp答题原始SKU`,`sp系列`,`sp辅助-重点干邑品牌`,`sp辅助_交易属性`,`sp辅助_品牌`,`sp辅助_尊尼获加威士忌`,`sp辅助_波本威士忌`,`sp疑似新品`,`sp店铺英文名`,`sp威士忌年份`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)
        if ret[0][0] > 0 and (prefix.find('edrington') > -1 or prefix[-1] == 'E'):
            cccc = ["uuid2","sign","ver","date","cid","real_cid","item_id","sku_id","name","sid","shop_type","brand","rbid","all_bid","alias_all_bid","sub_brand","region","region_str","price","org_price","promo_price","trade","num","sales","img","trade_props.name","trade_props.value","props.name","props.value","tip","source","created","trade_props_hash","trade_props_arr","clean_props.name","clean_props.value","clean_alias_all_bid","clean_all_bid","clean_cid","clean_num","clean_pid","clean_price","clean_sales","clean_sku_id","clean_time","clean_ver","old_alias_all_bid","old_all_bid","old_num","old_sales","old_sign","old_ver","alias_pid","clean_brush_id","clean_split_rate","clean_type","clean_tokens.name","clean_tokens.value","spBulk Sales","spSKU(拼接)","spSKU件数","spWGS套包类型","spWGS总毫升数","spWGS详细毫升数","sp一级类目","sp产地","sp人头马_三厂商干邑级别","sp人头马_子品类","sp人头马_辅助剔除","sp出数用PRC","sp单瓶规格","sp出数用内包装瓶数","sp出数用套包类型","sp出数用店铺类型","sp出数用渠道","sp出数用瓶数","sp出数用细分渠道","sp出数用规格","sp包装","sp单品规格","sp厂商","sp品名","sp品牌","sp套包宝贝","sp子品类","sp客户sku出数名","sp干邑级别","sp年份","sp店铺分类","sp是否人工答题","sp是否套包","sp是否无效链接","sp版本","sp瓶数","sp答题原始SKU","sp系列","sp辅助-重点干邑品牌","sp辅助_交易属性","sp辅助_品牌","sp辅助_尊尼获加威士忌","sp辅助_波本威士忌","sp（废弃备用）人头马_白兰地子品类","sp疑似新品","sp店铺英文名","sp威士忌年份"]
            dddd = ["","1","","2023-01-1","50522003","","668034913723","","麦卡伦 THE MACALLAN 18年 单一麦芽苏格兰威士忌","195852076","21","","","421497","421497","","","","297000","","","","621","184437000","","","","['颜色分类','产地','酒精度数','厂址','厂家联系方式','包装方式','体积(ml)','保质期','厂名','净含量','储存条件','包装种类','度数','系列']","['18年单一麦芽威士忌700ml','苏格兰','中度酒','详情见背标','详情见背标','包装','700','36500','麦卡伦酒厂','700ml','常温','瓶装','43%Vol.','18年单一麦芽苏格兰威士忌']","","1","","","","['Bulk Sales','SKU(拼接)','SKU件数','WGS套包类型','WGS总毫升数','WGS详细毫升数','一级类目','产地','人头马_三厂商干邑级别','人头马_子品类','人头马_辅助剔除','出数用PRC','出数用sku毫升数','出数用内包装瓶数','出数用套包类型','出数用店铺类型','出数用渠道','出数用瓶数','出数用细分渠道','出数用规格','包装','单瓶规格(ml)','厂商','品名','品牌','套包宝贝','子品类','客户sku出数名','干邑级别','年份','店铺分类','是否人工答题','是否套包','是否无效链接','版本','瓶数','答题原始SKU','系列','辅助-重点干邑品牌','辅助_交易属性','辅助_品牌','辅助_尊尼获加威士忌','辅助_波本威士忌','（废弃备用）人头马_白兰地子品类','店铺英文名','疑似新品','威士忌年份','单瓶规格','单品规格']","['否','麦卡伦Others单一麦芽威士忌700ml','','单品','700ml','700ml','威士忌','英国/苏格兰','others','麦芽威士忌','','','','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','','爱丁顿','THE 18年 单一麦芽苏格兰威士忌','麦卡伦','','单一麦芽威士忌','','','','TM-FSS','前后月覆盖','Single pack','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','MACALLAN/麦卡伦','','','','','','18年','700ml','']","421497","421497","","","","","","","","","","","","","","","","","1","","","","否","麦卡伦Others单一麦芽威士忌700ml","","单品","","","威士忌","英国/苏格兰","others","麦芽威士忌","","","700ml","1","Single","TMALL OTHER POP","TMALL OTHER POP","1","B2C-Tmall","700","瓶装","700ml","爱丁顿","THE 18年 单一麦芽苏格兰威士忌","麦卡伦","","单一麦芽威士忌","","","","TM-FSS","前后月覆盖","Single pack","有效链接","","1瓶","","Others","","单一麦芽威士忌700ml","MACALLAN/麦卡伦","","","","","","18年"]
            self.add_items(ret[0][1], cccc, [dddd], dba, tbl)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202302'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            data = [
                [ret[0][1],1,datetime.datetime(2023,2,1),14738,'55601625328','人头马（Remy Martin） CLUB优质香槟区干邑白兰地洋酒350ml法国【入会有好礼】',46762,12,'14661',363187,363187,25543,257,6564551,257,6564551,['商品名称','商品编号','商品毛重','商品产地','货号','类别','国产/进口','单件容量','特性','适用场景','酒精度','单件容量','包装规格','包装清单'],['人头马（Remy Martin）CLUB','55601625328','1.0kg','法国','tz0019206','干邑','进口','300-500mL','VSOP','婚宴，纪念日，聚会，喜宴，宴请，自饮','40%vol','350mL','1瓶','暂无'],2,363187,363187,5895,'否','人头马Others干邑白兰地350ml','1','单品','','','白兰地','其他产地','CLUB/VSOP','干邑白兰地','','人头马特级 RemyClub','350ml','1','Single','JD OTHER FSS','JD OTHER FSS','1','B2C-Standalone B2C','700','瓶装','350ml','人头马','CLUB优质香槟区洋酒350ML法国[入会有好礼]','人头马','','干邑白兰地','人头马CLUB干邑白兰地350ml','CLUB/VSOP','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','人头马CLUB干邑白兰地[350ml]','Remy CLUB','干邑重点','干邑白兰地350ml','','','','','','Remy JD Pop Store'],
                [ret[0][1],1,datetime.datetime(2023,2,1),14738,'10227360632','人头马（Remy Martin）CLUB优质香槟区干邑白兰地洋酒350ml 【下单享赠品】',46762,12,'14661',363187,363187,26521,336,8911056,336,8911056,['商品名称','商品编号','商品毛重','货号','国产/进口','类别','特性','适用场景','酒精度','包装清单'],['人头马（Remy Martin）CLUB','10227360632','0.88kg','tz0019205','进口','干邑','VSOP','婚宴，纪念日，聚会，喜宴，宴请，自饮','40%vol','暂无'],2,363187,363187,5895,'否','人头马Others干邑白兰地350ml','1','单品','','','白兰地','其他产地','CLUB/VSOP','干邑白兰地','','人头马特级 RemyClub','350ml','1','Single','JD OTHER FSS','JD OTHER FSS','1','B2C-Standalone B2C','700','瓶装','350ml','人头马','CLUB优质香槟区洋酒350ML [下单享赠品]','人头马','','干邑白兰地','人头马CLUB干邑白兰地350ml','CLUB/VSOP','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','人头马CLUB干邑白兰地[350ml]','Remy CLUB','干邑重点','干邑白兰地350ml','','','','','','Remy JD Pop Store'],
                [ret[0][1],1,datetime.datetime(2023,2,1),50510004,'655687040622','【品牌旗舰】人头马VSOP流光邑彩优质香槟区干邑白兰地700ml进口',60553350,21,'9245681',363187,363187,44000,103,4532000,103,4532000,[],[],1,363187,363187,15305,'否','人头马Others干邑白兰地700ml','1','单品','','','白兰地','其他产地','CLUB/VSOP','干邑白兰地','','','700ml','1','Single','TMALL OTHER FSS','TMALL OTHER FSS','1','B2C-Tmall','700','瓶装','700ml','人头马','[618开门红]VSOP流光邑彩优质香槟区700ML进口','人头马','','干邑白兰地','','CLUB/VSOP','','TM-FSS','前后月覆盖','Single pack','有效链接','流光邑彩限量版','1瓶','人头马VSOP流光邑彩限量版干邑白兰地[700ml]','Remy VSOP','干邑重点','干邑白兰地700ml','','','','','','Remy Tmall Flagship Store'],
                [ret[0][1],1,datetime.datetime(2023,2,1),14738,'10029407736359','人头马CLUB优质香槟区干邑白兰地3000ml原装进口',46762,12,'14661',363187,363187,282400,64,18073600,64,18073600,['商品名称','商品编号','商品毛重','货号','国产/进口','类别','特性','适用场景','酒精度','包装清单'],['人头马（Remy Martin）CLUB','10029407736359','100.00g','030618075','进口','干邑','VS','婚宴，纪念日，聚会，喜宴，宴请，自饮','40%vol','暂无'],2,363187,363187,7151,'否','人头马Others干邑白兰地3000ml','1','单品','','','白兰地','其他产地','CLUB/VSOP','干邑白兰地','','人头马特级 RemyClub','3000ml','1','Single','JD OTHER FSS','JD OTHER FSS','1','B2C-Standalone B2C','700','瓶装','3000ml','人头马','CLUB优质香槟区3000ML原装进口','人头马','','干邑白兰地','人头马CLUB干邑白兰地3000ml','CLUB/VSOP','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','人头马CLUB干邑白兰地[3000ml]','Remy CLUB','干邑重点','干邑白兰地3000ml','','','','','','Remy JD Pop Store'],
                [ret[0][1],1,datetime.datetime(2023,2,1),14739,'10064259165522','布赫拉迪（Bruichladdich）洋酒 13.1超重泥煤单一麦芽苏格兰威士忌 Octomore 700ml 年货',12052289,12,'141110',2492859,2492859,217225,4,868900,4,868900,['商品名称','商品编号','商品毛重','适用场景','桶原料','桶类型','年份','类别','包装形式','单件容量','产区','酒精度','保质期','单件容量','包装规格','包装清单'],['布赫拉迪（Bruichladdich）洋酒 13.1超重泥煤单一麦芽苏格兰威士忌 Octomore 700ml','10064259165522','1.0kg','聚会，宴请，自饮','波本桶','单桶','其他','单一麦芽威士忌','瓶装','＞500ml','艾雷岛','59.2%vol','长期保存','700mL','1瓶','暂无'],2,2492859,2492859,15452,'否','布赫拉迪Others单一麦芽威士忌700ml','1','单品','','','威士忌','英国/苏格兰','others','麦芽威士忌','','','700ml','1','Single','JD OTHER POP','JD OTHER POP','1','B2C-Standalone B2C','700','瓶装','700ml','人头马','洋酒 13.1超重泥煤单一麦芽苏格兰威士忌 OCTOMORE 700ML','布赫拉迪','','单一麦芽威士忌','','','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','布赫拉迪（Bruichladdich）泥煤怪兽Octomore 13.1号单一麦芽威士忌[700ml]','13.1号','','单一麦芽威士忌700ml','','','','','','PHD JD FSS'],
            ]

            # 补链接
            sql = '''
                INSERT INTO {} (`clean_ver`,`sign`,`date`,`cid`,`item_id`,`name`,`sid`,`shop_type`,`brand`,`all_bid`,`alias_all_bid`,`price`,`clean_num`,`clean_sales`,`num`,`sales`,`props.name`,`props.value`,`source`,`clean_alias_all_bid`,`clean_all_bid`,`clean_pid`,`spBulk Sales`,`spSKU(拼接)`,`spSKU件数`,`spWGS套包类型`,`spWGS总毫升数`,`spWGS详细毫升数`,`sp一级类目`,`sp产地`,`sp人头马_三厂商干邑级别`,`sp人头马_子品类`,`sp人头马_辅助剔除`,`sp出数用PRC`,`sp单瓶规格`,`sp出数用内包装瓶数`,`sp出数用套包类型`,`sp出数用店铺类型`,`sp出数用渠道`,`sp出数用瓶数`,`sp出数用细分渠道`,`sp出数用规格`,`sp包装`,`sp单品规格`,`sp厂商`,`sp品名`,`sp品牌`,`sp套包宝贝`,`sp子品类`,`sp客户sku出数名`,`sp干邑级别`,`sp年份`,`sp店铺分类`,`sp是否人工答题`,`sp是否套包`,`sp是否无效链接`,`sp版本`,`sp瓶数`,`sp答题原始SKU`,`sp系列`,`sp辅助-重点干邑品牌`,`sp辅助_交易属性`,`sp辅助_品牌`,`sp辅助_尊尼获加威士忌`,`sp辅助_波本威士忌`,`sp威士忌年份`,`sp疑似新品`,`sp店铺英文名`) VALUES
            '''.format(tbl)
            dba.execute(sql, data)
        if ret[0][0] > 0 and (prefix.find('edrington') > -1 or prefix[-1] == 'E'):
            cccc = ["uuid2","sign","ver","date","cid","real_cid","item_id","sku_id","name","sid","shop_type","brand","rbid","all_bid","alias_all_bid","sub_brand","region","region_str","price","org_price","promo_price","trade","num","sales","img","trade_props.name","trade_props.value","props.name","props.value","tip","source","created","trade_props_hash","trade_props_arr","clean_props.name","clean_props.value","clean_alias_all_bid","clean_all_bid","clean_cid","clean_num","clean_pid","clean_price","clean_sales","clean_sku_id","clean_time","clean_ver","old_alias_all_bid","old_all_bid","old_num","old_sales","old_sign","old_ver","alias_pid","clean_brush_id","clean_split_rate","clean_type","clean_tokens.name","clean_tokens.value","spBulk Sales","spSKU(拼接)","spSKU件数","spWGS套包类型","spWGS总毫升数","spWGS详细毫升数","sp一级类目","sp产地","sp人头马_三厂商干邑级别","sp人头马_子品类","sp人头马_辅助剔除","sp出数用PRC","sp单瓶规格","sp出数用内包装瓶数","sp出数用套包类型","sp出数用店铺类型","sp出数用渠道","sp出数用瓶数","sp出数用细分渠道","sp出数用规格","sp包装","sp单品规格","sp厂商","sp品名","sp品牌","sp套包宝贝","sp子品类","sp客户sku出数名","sp干邑级别","sp年份","sp店铺分类","sp是否人工答题","sp是否套包","sp是否无效链接","sp版本","sp瓶数","sp答题原始SKU","sp系列","sp辅助-重点干邑品牌","sp辅助_交易属性","sp辅助_品牌","sp辅助_尊尼获加威士忌","sp辅助_波本威士忌","sp（废弃备用）人头马_白兰地子品类","sp疑似新品","sp店铺英文名","sp威士忌年份"]
            dddd = ["","1","","2023-02-1","50522003","","668034913723","","麦卡伦 THE MACALLAN 18年 单一麦芽苏格兰威士忌","195852076","21","","","421497","421497","","","","297000","","","","862","256014000","","","","['颜色分类','产地','酒精度数','厂址','厂家联系方式','包装方式','体积(ml)','保质期','厂名','净含量','储存条件','包装种类','度数','系列']","['18年单一麦芽威士忌700ml','苏格兰','中度酒','详情见背标','详情见背标','包装','700','36500','麦卡伦酒厂','700ml','常温','瓶装','43%Vol.','18年单一麦芽苏格兰威士忌']","","1","","","","['Bulk Sales','SKU(拼接)','SKU件数','WGS套包类型','WGS总毫升数','WGS详细毫升数','一级类目','产地','人头马_三厂商干邑级别','人头马_子品类','人头马_辅助剔除','出数用PRC','出数用sku毫升数','出数用内包装瓶数','出数用套包类型','出数用店铺类型','出数用渠道','出数用瓶数','出数用细分渠道','出数用规格','包装','单瓶规格(ml)','厂商','品名','品牌','套包宝贝','子品类','客户sku出数名','干邑级别','年份','店铺分类','是否人工答题','是否套包','是否无效链接','版本','瓶数','答题原始SKU','系列','辅助-重点干邑品牌','辅助_交易属性','辅助_品牌','辅助_尊尼获加威士忌','辅助_波本威士忌','（废弃备用）人头马_白兰地子品类','店铺英文名','疑似新品','威士忌年份','单瓶规格','单品规格']","['否','麦卡伦Others单一麦芽威士忌700ml','','单品','700ml','700ml','威士忌','英国/苏格兰','others','麦芽威士忌','','','','','Single','TMALL OTHER POP','TMALL OTHER POP','1','B2C-Tmall','700','瓶装','','爱丁顿','THE 18年 单一麦芽苏格兰威士忌','麦卡伦','','单一麦芽威士忌','','','','TM-FSS','前后月覆盖','Single pack','有效链接','','1瓶','','Others','','单一麦芽威士忌700ml','MACALLAN/麦卡伦','','','','','','18年','700ml','']","421497","421497","","","","","","","","","","","","","","","","","1","","","","否","麦卡伦Others单一麦芽威士忌700ml","","单品","","","威士忌","英国/苏格兰","others","麦芽威士忌","","","700ml","1","Single","TMALL OTHER POP","TMALL OTHER POP","1","B2C-Tmall","700","瓶装","700ml","爱丁顿","THE 18年 单一麦芽苏格兰威士忌","麦卡伦","","单一麦芽威士忌","","","","TM-FSS","前后月覆盖","Single pack","有效链接","","1瓶","","Others","","单一麦芽威士忌700ml","MACALLAN/麦卡伦","","","","","","18年"]
            self.add_items(ret[0][1], cccc, [dddd], dba, tbl)

        sql = 'SELECT COUNT(*), MAX(clean_ver) FROM {} WHERE toYYYYMM(date) = 202303'.format(tbl)
        ret = dba.query_all(sql)
        if ret[0][0] > 0:
            cccc = ["uuid2","sign","ver","date","cid","real_cid","item_id","sku_id","name","sid","shop_type","brand","rbid","all_bid","alias_all_bid","sub_brand","region","region_str","price","org_price","promo_price","trade","num","sales","img","trade_props.name","trade_props.value","props.name","props.value","tip","source","created","trade_props_hash","trade_props_arr","clean_props.name","clean_props.value","clean_alias_all_bid","clean_all_bid","clean_cid","clean_num","clean_pid","clean_price","clean_sales","clean_sku_id","clean_time","clean_ver","old_alias_all_bid","old_all_bid","old_num","old_sales","old_sign","old_ver","alias_pid","clean_brush_id","clean_split_rate","clean_type","clean_tokens.name","clean_tokens.value","spBulk Sales","spSKU(拼接)","spSKU件数","spWGS套包类型","spWGS总毫升数","spWGS详细毫升数","sp一级类目","sp产地","sp人头马_三厂商干邑级别","sp人头马_子品类","sp人头马_辅助剔除","sp出数用PRC","sp单瓶规格","sp出数用内包装瓶数","sp出数用套包类型","sp出数用店铺类型","sp出数用渠道","sp出数用瓶数","sp出数用细分渠道","sp出数用规格","sp包装","sp单品规格","sp厂商","sp品名","sp品牌","sp套包宝贝","sp子品类","sp客户sku出数名","sp干邑级别","sp年份","sp店铺分类","sp是否人工答题","sp是否套包","sp是否无效链接","sp版本","sp瓶数","sp答题原始SKU","sp系列","sp辅助-重点干邑品牌","sp辅助_交易属性","sp辅助_品牌","sp辅助_尊尼获加威士忌","sp辅助_波本威士忌","sp（废弃备用）人头马_白兰地子品类","sp疑似新品","sp店铺英文名","sp威士忌年份"]
            dddd = ["","1","","2023-03-1","14738","","55601625328","","人头马（Remy Martin） CLUB优质香槟区干邑白兰地洋酒350ml法国【入会有好礼】","46762","12","14661","","363187","363187","","","","26000","","",""," 6,554 "," 170,404,000 ","","","","['商品名称','商品编号','商品毛重','商品产地','货号','类别','国产/进口','单件容量','特性','适用场景','酒精度','单件容量','包装规格','包装清单']","['人头马（Remy Martin）CLUB','55601625328','1.0kg','法国','tz0019206','干邑','进口','300-500mL','VSOP','婚宴，纪念日，聚会，喜宴，宴请，自饮','40%vol','350mL','1瓶','暂无']","","2","","","","['Bulk Sales','SKU(拼接)','SKU件数','WGS套包类型','WGS总毫升数','WGS详细毫升数','一级类目','产地','人头马_三厂商干邑级别','人头马_子品类','人头马_辅助剔除','出数用PRC','出数用sku毫升数','出数用内包装瓶数','出数用套包类型','出数用店铺类型','出数用渠道','出数用瓶数','出数用细分渠道','出数用规格','包装','单瓶规格(ml)','厂商','品名','品牌','套包宝贝','子品类','客户sku出数名','干邑级别','年份','店铺分类','是否人工答题','是否套包','是否无效链接','版本','瓶数','答题原始SKU','系列','辅助-重点干邑品牌','辅助_交易属性','辅助_品牌','辅助_尊尼获加威士忌','辅助_波本威士忌','（废弃备用）人头马_白兰地子品类','疑似新品','店铺英文名','威士忌年份']","['否','人头马Others干邑白兰地350ml','1','单品','','','白兰地','其他产地','CLUB/VSOP','干邑白兰地','','人头马特级 RemyClub','350ml','1','Single','JD OTHER FSS','JD OTHER FSS','1','B2C-Standalone B2C','700','瓶装','350ml','人头马','CLUB优质香槟区洋酒350ML法国[入会有好礼]','人头马','','干邑白兰地','人头马CLUB干邑白兰地350ml','CLUB/VSOP','','JD-FSS','前后月覆盖','Single pack','有效链接','','1瓶','人头马CLUB干邑白兰地[350ml]','Remy CLUB','干邑重点','干邑白兰地350ml','','','','','','Remy JD Pop Store','']","363187","363187","","","5895","","","","","","","","","","","","5895","","1","","","","否","人头马Others干邑白兰地350ml","1","单品","","","白兰地","其他产地","CLUB/VSOP","干邑白兰地","","人头马特级 RemyClub","350ml","1","Single","JD OTHER FSS","JD OTHER FSS","1","B2C-Standalone B2C","350","瓶装","350ml","人头马","CLUB优质香槟区洋酒350ML [下单享赠品]","人头马","","干邑白兰地","人头马CLUB干邑白兰地350ml","CLUB/VSOP","","JD-FSS","前后月覆盖","Single pack","有效链接","","1瓶","人头马CLUB干邑白兰地[350ml]","Remy CLUB","干邑重点","干邑白兰地350ml","","","","","","Remy JD Pop Store",""]
            self.add_items(ret[0][1], cccc, [dddd], dba, tbl)
        if ret[0][0] > 0 and (prefix.find('edrington') > -1 or prefix[-1] == 'E'):
            cccc = ["uuid2","sign","ver","date","cid","real_cid","item_id","sku_id","name","sid","shop_type","brand","rbid","all_bid","alias_all_bid","sub_brand","region","region_str","price","org_price","promo_price","trade","num","sales","img","trade_props.name","trade_props.value","props.name","props.value","tip","source","created","trade_props_hash","trade_props_arr","clean_props.name","clean_props.value","clean_alias_all_bid","clean_all_bid","clean_cid","clean_num","clean_pid","clean_price","clean_sales","clean_sku_id","clean_time","clean_ver","old_alias_all_bid","old_all_bid","old_num","old_sales","old_sign","old_ver","alias_pid","clean_brush_id","clean_split_rate","clean_type","clean_tokens.name","clean_tokens.value","spBulk Sales","spSKU(拼接)","spSKU件数","spWGS套包类型","spWGS总毫升数","spWGS详细毫升数","sp一级类目","sp产地","sp人头马_三厂商干邑级别","sp人头马_子品类","sp人头马_辅助剔除","sp出数用PRC","sp单瓶规格","sp出数用内包装瓶数","sp出数用套包类型","sp出数用店铺类型","sp出数用渠道","sp出数用瓶数","sp出数用细分渠道","sp出数用规格","sp包装","sp单品规格","sp厂商","sp品名","sp品牌","sp套包宝贝","sp子品类","sp客户sku出数名","sp干邑级别","sp年份","sp店铺分类","sp是否人工答题","sp是否套包","sp是否无效链接","sp版本","sp瓶数","sp答题原始SKU","sp系列","sp辅助-重点干邑品牌","sp辅助_交易属性","sp辅助_品牌","sp辅助_尊尼获加威士忌","sp辅助_波本威士忌","sp（废弃备用）人头马_白兰地子品类","sp疑似新品","sp店铺英文名","sp威士忌年份"]
            dddd = ["","1","","2023-03-1","50522003","","668034781502","","麦卡伦 THE MACALLAN 珍藏档案系列第六册","195852076","21","","","421497","421497","","","","699900","","","","198","138580200","","","","","","","1","","","","","","421497","421497","","","","","","","","","","","","","","","","","1","","","","否","麦卡伦Others单一麦芽威士忌700ml","","单品","","","威士忌","英国/苏格兰","others","麦芽威士忌","","","700ml","1","Single","TMALL OTHER POP","TMALL OTHER POP","1","B2C-Tmall","700","礼盒装","700ml","爱丁顿","THE 珍藏档案系列第六册","麦卡伦","","单—麦芽威士忌","","","","TM-FSS","前后月覆盖","Single pack","有效链接","","1瓶","","Others","","","","","","","","",""]
            self.add_items(ret[0][1], cccc, [dddd], dba, tbl)

        self.cleaner.merge_etbl_and_items(tbl, prefix)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp威士忌年份` = transform(item_id, ['667667548029','667671472734','668034913723','668380290849','668384370855','668737895361','668738579961','680456173223','674333186989','677300316561','673281452318','673629408832','667644700212'], ['25年','30年','18年','12年','12年','12年','15年','25年','30年','25年','21年','30年','18年'], '')
            WHERE uuid2 = 0
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def add_items(self, ver, cccc, dddd, dba, tbl):
        cols = self.cleaner.get_cols(tbl, dba)

        ccccc = cccc + ['clean_ver','clean_all_bid','clean_alias_all_bid','clean_sales','clean_num']
        ccccc = list(set(ccccc))
        ccccc = [c for c in ccccc if c not in ['created','clean_time'] and c in cols]

        data = []
        for d in dddd:
            item = {c:d[i] for i,c in enumerate(cccc)}
            item['clean_ver'] = ver
            item['clean_all_bid'] = item['all_bid']
            item['clean_alias_all_bid'] = item['alias_all_bid']
            item['clean_sales'] = item['sales'] = item['sales'].replace(',','').strip()
            item['clean_num'] = item['num'] = item['num'].replace(',','').strip()
            data.append([self.cleaner.safe_insert(cols[c], item[c]) for c in ccccc])

        sql = 'INSERT INTO {} ({}) VALUES'.format(tbl, ','.join(['`{}`'.format(c) for c in ccccc]))
        print(data)
        dba.execute(sql, data)


    def hotfix_new(self, tbl, dba, prefix):
        # self.hotfix_trade_sales(self, tbl, dba, prefix)

        self.cleaner.add_miss_cols(tbl, {'sp出数用瓶数':'String','sp出数用规格':'String','sp出数用PRC':'String','sp出数用渠道':'String','sp出数用细分渠道':'String','sp出数用套包类型':'String','sp出数用店铺类型':'String'})

        k12, v12 = [], []
        tmp = main.prc_map()
        for k in tmp:
            k12.append('\'{}\''.format(k.replace('\'', '\\\'')))
            v12.append('\'{}\''.format(tmp[k].replace('\'', '\\\'')))

        k13, v13 = [], []
        for v in main.type_map3():
            source, sid, type = v[0], v[1], v[2]
            source = {'tmall':1, 'tb':1, 'jd':2, 'jx':9}[source]
            k13.append('{}-{}'.format(source, sid))
            v13.append(type)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp客户sku出数名` = IF(`sp客户sku出数名` = '0', '', REPLACE(`sp客户sku出数名`, '其他', '其它')),
                `sp出数用瓶数` = IF(clean_sku_id > 0, toString(b_number), REPLACE(LOWER(`sp瓶数`), '瓶', '')),
                `sp出数用渠道` = transform(concat(toString(source),'-',toString(sid)), {}, {},
                    transform(concat(toString(source),'-',toString(shop_type)), ['1-23', '1-22', '1-24', '9-11'], ['TMALL SUPER', 'TMALL INTERNATIONAL', 'TMALL OTHER POP', 'JX PRC POP'],
                        transform(source, [1, 2, 9], ['TMALL OTHER POP', 'JD OTHER POP', 'JX PLATFORM'], '')
                    )
                )
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl, str(k13), str(v13))
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用瓶数` = '1' WHERE `sp出数用瓶数` = '' OR `sp出数用瓶数` = '0'
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp出数用PRC` = transform(`sp客户sku出数名`, [{}], [{}], ''),
                `sp出数用规格` = multiIf(
                    `sp客户sku出数名` LIKE '%其它%' AND clean_alias_all_bid IN (1883586,2492729,5170791), '500',
                    `sp客户sku出数名` LIKE '%其它%' OR `sp出数用规格` IN ('','0'), '700',
                    `sp出数用规格`
                ),
                `sp出数用细分渠道` = multiIf(
                    `sp出数用渠道` LIKE '%C2C%', 'C2C',
                    `sp出数用渠道` LIKE '%TMALL%', 'B2C-Tmall',
                    'B2C-Standalone B2C'
                ),
                `sp出数用店铺类型` = `sp出数用渠道`,
                `sp出数用套包类型` = multiIf(
                    clean_split_rate < 1, `sp出数用套包类型`,
                    toInt32(`sp出数用瓶数`) > 1, 'Multi Same Sku',
                    'Single'
                )
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl, ','.join(k12), ','.join(v12))
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp单瓶规格` = `sp单品规格` WHERE `sp单瓶规格` = ''
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp单品规格` = '' WHERE `clean_sku_id` IN (0,2079393,2079379,2079333,2079331,2079332,2079384,2079385,2079386,2079387,2079389,2079402,2079472,2079474,2079475,2079499,2079508,2079509,2079562,2079565,2079568,2079570,2079571,2079572,2079576,2079581)
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        tips = self.hotfix_trade_sales(tbl, dba, prefix)

        if prefix.find('DIAGEO') != -1:
            sql = '''
                alter table {tbl}
                update `spDiageo_子品类` = multiIf(alias_all_bid not in (4902185,487198,3031169,6045995,20362,104176,2601115,98740,139065,970370,180197,4736157,167585,28647,5509691,1882613,4496236,2283255,1532659,1532657,3459249,3146576,2711913,487212,2891581,820859,4788713,5885830,5572319,1988705,4269407,4612211,3459250,105883,5626725,2283256,4806594,500121,1211770,7373098), '单一麦芽威士忌',
                alias_all_bid in (4902185,487198,3031169,6045995,20362,104176,2601115,98740,139065,970370,180197,4736157,167585,28647,5509691,1882613,4496236,2283255,1532659,1532657,3459249,3146576,2711913,487212,2891581,820859,4788713,5885830,5572319,1988705,4269407,4612211,3459250,105883,5626725,2283256,4806594,500121,1211770,7373098) and `spDiageo_子品类` ='单一麦芽威士忌', '调和威士忌',
                alias_all_bid in (4902185,487198,3031169,6045995,20362,104176,2601115,98740,139065,970370,180197,4736157,167585,28647,5509691,1882613,4496236,2283255,1532659,1532657,3459249,3146576,2711913,487212,2891581,820859,4788713,5885830,5572319,1988705,4269407,4612211,3459250,105883,5626725,2283256,4806594,500121,1211770,7373098) and `spDiageo_子品类` ='调和威士忌', '单一麦芽威士忌',
                `spDiageo_子品类`),
                clean_alias_all_bid = multiIf(extract(name,'传世臻品|风味探索') != '',487212,alias_all_bid not in (4902185,487198,3031169,6045995,20362,104176,2601115,98740,139065,970370,180197,4736157,167585,28647,5509691,1882613,4496236,2283255,1532659,1532657,3459249,3146576,2711913,487212,2891581,820859,4788713,5885830,5572319,1988705,4269407,4612211,3459250,105883,5626725,2283256,4806594,500121,1211770,7373098),487212,alias_all_bid)
                where  pkey>='2024-01-01'
                and `spDiageo_子品类` like '%威士忌%'
                and  [`spDiageo_子品类`,toString(alias_all_bid)] not in (['调和威士忌','4902185'],['调和威士忌','487198'],['调和威士忌','3031169'],['调和威士忌','6045995'],['调和威士忌','20362'],['调和威士忌','104176'],['调和威士忌','2601115'],['调和威士忌','98740'],['调和威士忌','139065'],['调和威士忌','970370'],['调和威士忌','180197'],['调和威士忌','4736157'],['调和威士忌','167585'],['调和威士忌','28647'],['单一麦芽威士忌','5509691'],['单一麦芽威士忌','1882613'],['单一麦芽威士忌','4496236'],['单一麦芽威士忌','2283255'],['单一麦芽威士忌','1532659'],['单一麦芽威士忌','1532657'],['单一麦芽威士忌','3459249'],['单一麦芽威士忌','3146576'],['单一麦芽威士忌','2711913'],['单一麦芽威士忌','487212'],['单一麦芽威士忌','2891581'],['单一麦芽威士忌','820859'],['单一麦芽威士忌','4788713'],['单一麦芽威士忌','5885830'],['单一麦芽威士忌','5572319'],['单一麦芽威士忌','1988705'],['单一麦芽威士忌','4269407'],['单一麦芽威士忌','4612211'],['单一麦芽威士忌','3459250'],['单一麦芽威士忌','105883'],['单一麦芽威士忌','5626725'],['单一麦芽威士忌','2283256'],['单一麦芽威士忌','4806594'],['单一麦芽威士忌','500121'],['单一麦芽威士忌','1211770'],['单一麦芽威士忌','7373098'])
                and alias_all_bid in (select bid from artificial.all_brand_90526_release where manu_cn = '帝亚吉欧')
            '''.format(tbl=tbl)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)
            

        if prefix.find('RM') == -1:
            return tips

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = 0, `clean_alias_all_bid` = 0 WHERE `clean_alias_all_bid` = 6669939
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        return tips


    def hotfix_trade_sales(self, tbl, dba, prefix):
        if prefix.find('edrington') == -1:
            return ''
        # entity_prod_90526_E_edrington 要用jd1老算法

        sql = 'SELECT max(clean_ver), toYYYYMM(min(date)), toYYYYMM(max(date)) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        ver, smonth, emonth = ret[0][0], ret[0][1], ret[0][2]

        db68 = self.cleaner.get_db('68_apollo')
        sql = '''
            SELECT CAST(REPLACE(description,'应用榜单热销新算法','') AS UNSIGNED) AS `month`, least(version,add_version) AS ver
            FROM jdnew.trade_fix_task WHERE description LIKE '%应用榜单热销新算法'
            GROUP BY description HAVING `month` >= {} AND `month` <= {} ORDER BY id
        '''.format(smonth, emonth)
        ret = db68.query_all(sql)

        if len(ret) == 0:
            return ''

        ttt = 'artificial.trade_all_{}'.format(int(time.time()))
        dba.execute('DROP TABLE IF EXISTS {}'.format(ttt))

        sql = '''
            CREATE TABLE {}
            (
                `item_id` UInt64 CODEC(LZ4HC(0)),
                `date` Date CODEC(LZ4HC(0)),
                `sales` Int64 CODEC(LZ4HC(0)),
                `num` Int32 CODEC(LZ4HC(0)),
                `trade_props.name` Array(String) CODEC(LZ4HC(0)),
                `trade_props.value` Array(String) CODEC(LZ4HC(0)),
                `cid` UInt32 CODEC(LZ4HC(0)),
                `pkey` Date CODEC(LZ4HC(0)),
                `sign` Int8 CODEC(LZ4HC(0))
            )
            ENGINE = Log
        '''.format(ttt)
        dba.execute(sql)

        a, b = tbl.split('.')

        for m, v, in ret:
            sql = '''
                INSERT INTO {}
                SELECT item_id, date, sales, num, trade_props.name, trade_props.value, cid, pkey, sign
                FROM remote('192.168.40.195', 'jd', 'trade_all', 'sop_jd4', 'awa^Nh799F#jh0e0')
                WHERE ver < {} AND (pkey, cid, item_id) IN (
                    SELECT pkey, cid, toUInt64(item_id) FROM remote('192.168.30.192','{}','{}','{}','{}')
                    WHERE toYYYYMM(date) = {} AND source = 2
                )
            '''.format(ttt, v, a, b, dba.user, dba.passwd, m)
            dba.execute(sql)

        rrr = dba.query_all('SELECT COUNT(*) FROM {}'.format(ttt))
        if rrr[0][0] == 0:
            dba.execute('DROP TABLE IF EXISTS {}'.format(ttt))
            return ''

        tips = self.cleaner.process_trade_sales({'2':ttt}, ver, tbl, dba)

        dba.execute('DROP TABLE IF EXISTS {}'.format(ttt))
        return tips


    def process_exx(self, tbl, prefix, logId=0):
        if tbl.find('RM') == -1:
            return

        dba = self.cleaner.get_db('chsop')
        sql = '''
            ALTER TABLE {} UPDATE alias_pid = 0, `sp单品规格` = ''
            WHERE `source` = 1 AND shop_type > 10 AND shop_type < 20
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)