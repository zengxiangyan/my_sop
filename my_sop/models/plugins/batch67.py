import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import numpy as np
import csv

class main(Batch.main):
    def brush_bak(self, smonth, emonth):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        sql = '''
            select uniq_k from artificial.entity_{}_item where month >= '{}' and month < '{}' and sp1 = '钙' group by uniq_k
        '''.format(self.cleaner.eid, smonth, emonth)
        ret = self.cleaner.db26.query_all(sql)
        uniq_ks = [str(v[0]) for v in ret]

        uuids = []
        sales_by_uuid = {}
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, limit=1000, where='uniq_k in ({})'.format(','.join(uniq_ks)))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)

        sql = 'SELECT source, cid FROM {}_parts GROUP BY source, cid'.format(self.get_entity_tbl())
        cids = self.cleaner.dbch.query_all(sql)
        bids = [
            [326236, 'bio island'],
            [273629, 'Centrum/善存'],
            [48678, 'childlife'],
            [104447, 'CONBA/康恩贝'],
            [393278, 'elevit'],
            [819397, 'Eric Favre'],
            [401951, 'MoveFree'],
            [333391, 'ostelin'],
            [131009, 'Swisse'],
            [59463, '修正'],
            [1856905, '健力多'],
            [59590, '哈药'],
            [387334, '新盖中盖'],
            [110054, '朗迪'],
            [112244, '汤臣倍健'],
            [387336, '迪巧'],
            [4834144, '金钙尔奇'],
            [53021, '钙尔奇'],
            [272287, '麦金利'],
            [387135, '黄金搭档'],
            [326026, 'SCHIFF'],
            [4713434,'精朗迪']
        ]
        for bid, name, in bids:
            sql = 'SELECT bid FROM brush.all_brand WHERE bid = {b} OR alias_bid = {b}'.format(b=bid)
            ret = self.cleaner.db26.query_all(sql)
            bbb = [str(v[0]) for v in ret]
            for source, cid, in cids:
                ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.9, where='source=\'{}\' and cid={} and all_bid in ({}) and uniq_k in ({})'.format(source, cid, ','.join(bbb), ','.join(uniq_ks)))
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                    uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add new brush visible 1 {}'.format(len(uuids)))

        uuids = []
        for cid in set(np.array(cids)[:, 1]):
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=' cid = {} and uniq_k in ({})'.format(cid, ','.join(uniq_ks)))
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        print('add new brush visble 2 {}'.format(len(uuids)))

        # sql = 'SELECT b.name, count(*) FROM {} a JOIN brush.all_brand b ON (a.alias_all_bid=b.bid) GROUP BY b.bid WHERE a.clean_flag={}'.format(clean_flag)
        # ret = self.cleaner.db26.query_all(sql)
        # print(ret)

        # print('add new brush {}'.format(len(uuids)))

        return True

    def update_pid_num(self):

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)

        ### 0. 提取无效pid

        #######百洋钙产品库不规范，暂用枚举的方式
        # sql_bad_pid_67 = '''
        # select pid from product_lib.product_90591 where name like '\_\_\_%%%'
        # '''
        # bad_pid_67 = ','.join(self.cleaner.db26.query_all(sql_bad_pid_67))
        # print(bad_pid_67)
        bad_pid_67 = "528,529,909"

        sql_bad_pid_141 = '''
        select pid from product_lib.product_91130 where name like '\_\_\_%%%'
        '''
        bad_pid_141 = ','.join([str(v[0]) for v in self.cleaner.db26.query_all(sql_bad_pid_141)])


        #cast(now() as date)

        ### 1.取需要映射的抖音题目

        sql_dy_orgin = '''
        select id, snum, tb_item_id, real_p1 from product_lib.entity_90591_item where cast(created as date) = cast(now() as date) and flag = 0 -- and snum = 11
        and (snum, tb_item_id, real_p1) in (select snum, tb_item_id, real_p1 from product_lib.entity_91130_item where flag=2 and pid in (select pid from product_lib.product_91130 where name not like '\_\_\_%%%'))
        '''
        lists = [[v[0], v[1], v[2], v[3]] for v in self.cleaner.db26.query_all(sql_dy_orgin)]
        for each_id, each_source, each_itemid, each_p1 in lists:
        ### 2.查找141的pid
            sql_141_pid = '''
            select max(pid) m_pid, max(number) n_num from product_lib.entity_91130_item where  snum={each_source} and tb_item_id=\'{each_itemid}\' and  real_p1 = {each_p1} and flag = 2
            and pid not in ({bad_pid_141})
            '''.format(each_source=each_source, each_itemid=each_itemid, each_p1=each_p1, bad_pid_141=bad_pid_141)
            res_141_pid,  res_141_num = self.cleaner.db26.query_all(sql_141_pid)[0][0], self.cleaner.db26.query_all(sql_141_pid)[0][1]

        ### 3. 查找67答得最多的
            if res_141_pid == 0:
                continue
            else:
                sql_67_topSKU = '''
                select pid, count(1) from product_lib.entity_90591_item where flag=2 and pid not in ({bad_pid_67}) and (snum,tb_item_id,real_p1) 
                in (select snum,tb_item_id,real_p1 from product_lib.entity_91130_item where pid={res_141_pid}) order by count(1) desc limit 1
                '''.format(bad_pid_67=bad_pid_67, res_141_pid=res_141_pid)
                print(sql_67_topSKU)
                res_67_pid = self.cleaner.db26.query_all(sql_67_topSKU)[0][0]
                if isinstance(res_67_pid, int):
                    sql_67_update = '''
                    update product_lib.entity_90591_item set clean_pid={res_67_pid}, number={res_141_num} where flag=0 and id={each_id}
                    '''.format(res_67_pid=res_67_pid, res_141_num=res_141_num, each_id=each_id)
                    print(sql_67_update)
                    self.cleaner.db26.execute(sql_67_update)
                    self.cleaner.db26.commit()

        #### batch67中找出 pid=529（不是钙片）的数据,将这部分数据放入batch141进行查找pid
        pid_141_select = '''
        select distinct pid from product_lib.entity_91130_item where flag=2 and pid>0 and (snum, tb_item_id, real_p1) in 
        (select snum, tb_item_id, real_p1 from product_lib.entity_90591_item where pid=529)
        and pid not in (select pid from product_lib.product_91130 where name like '\_\_\_%%%')
        '''
        pids_141 = [v[0] for v in self.cleaner.db26.query_all(pid_141_select)]

        ### 将batch141选出的pid找到对应item+交易属性去匹配batch67中clean_pid为0的数据，测试阶段用clean_pid_bak替代clean_pid
        item_trade_67_select = '''
        select id from product_lib.entity_90591_item where cast(created as date) = cast(now() as date) and clean_pid=0 
        and (snum, tb_item_id, real_p1) in (select snum, tb_item_id, real_p1 from product_lib.entity_91130_item where pid in ({pids}))
        '''.format(pids=','.join([str(v) for v in pids_141]))
        ids_67 = [v[0] for v in self.cleaner.db26.query_all(item_trade_67_select)]

        ### 更新不是钙片的
        for each_id in ids_67:
            sql_67_update = '''
            update product_lib.entity_90591_item set clean_pid=529 where id={each_id} and clean_pid=0
            '''.format(each_id=each_id)
            print(sql_67_update)
            self.cleaner.db26.execute(sql_67_update)
            self.cleaner.db26.commit()


    def brush(self,smonth,emonth, logId=-1):


        sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        clean_flag = self.cleaner.last_clean_flag() + 1

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        fsql_not_909 = 'SELECT  snum, tb_item_id, real_p1, flag, id FROM {} where pid != 909'.format(self.cleaner.get_tbl())
        print(fsql_not_909)

        sql = 'SELECT snum, tb_item_id, real_p1, id FROM {} where pid = 909'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        pid909_item = {'{}-{}-{}'.format(v[0],v[1],v[2]):v[3] for v in ret}
        print(pid909_item)
        # 历史上sku名为 不建sku产品
        delete_id = []
        uuids = []
        uuids3 = [] # 新加组
        uuids3_info = []
        sales_by_uuid = {}
        uuid_request = []
        where_sql = '''
        select uuid2 from {} where pkey >= '{}' and pkey < '{}' and c_sp1 = '钙'
        '''.format(ctbl, smonth, emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=1000, where='uuid2 in ({})'.format(where_sql))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_request.append(uuid2)  ##do not foget delete
            if self.skip_brush(source, tb_item_id, p1, sql=fsql_not_909, if_flag=False):
                continue
            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid909_item:
                uuids3_info.append([uuid2, source, tb_item_id, p1])
                uuids3.append(uuid2)
                delete_id.append(pid909_item['{}-{}-{}'.format(source, tb_item_id, p1)])
            else:
                uuids.append(uuid2)

        sql = 'SELECT source, cid FROM {} GROUP BY source, cid'.format(atbl)
        cids = self.cleaner.dbch.query_all(sql)
        bids = [
            [326236, 'bio island'],
            [273629, 'Centrum/善存'],
            [48678, 'childlife'],
            [104447, 'CONBA/康恩贝'],
            [393278, 'elevit'],
            [819397, 'Eric Favre'],
            [401951, 'MoveFree'],
            [333391, 'ostelin'],
            [131009, 'Swisse'],
            [59463, '修正'],
            [1856905, '健力多'],
            [59590, '哈药'],
            [387334, '新盖中盖'],
            [110054, '朗迪'],
            [112244, '汤臣倍健'],
            [387336, '迪巧'],
            [4834144, '金钙尔奇'],
            [53021, '钙尔奇'],
            [272287, '麦金利'],
            [387135, '黄金搭档'],
            [326026, 'SCHIFF'],
            [4713434,'精朗迪']
        ]

        uuids2 = []
        uuids4 = []  # 新加组
        uuids4_info = []
        for bid, name, in bids:
            sql = 'SELECT bid FROM brush.all_brand WHERE bid = {b} OR alias_bid = {b}'.format(b=bid)
            ret = self.cleaner.db26.query_all(sql)
            bbb = [str(v[0]) for v in ret]
            for each_source, each_cid, in cids:
                where = 'source=\'{}\' and cid={} and all_bid in ({}) and uuid2 in ({})'.format(each_source, each_cid, ','.join(bbb), where_sql)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.9, where=where)
                sales_by_uuid.update(sales_by_uuid_1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_request.append(uuid2)  ##do not foget delete
                    if self.skip_brush(source, tb_item_id, p1, sql=fsql_not_909, if_flag=False):
                        continue
                    else:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in pid909_item:
                            uuids3.append(uuid2)
                            uuids3_info.append([uuid2, source, tb_item_id, p1])
                            delete_id.append(pid909_item['{}-{}-{}'.format(source, tb_item_id, p1)])
                        else:
                            uuids.append(uuid2)



        # sql2 = 'SELECT item_id,sum(sales) ss FROM {} GROUP BY item_id order by sum(sales) desc limit 3'.format(atbl)
        # item_ids = self.cleaner.dbch.query_all(sql2)


        for each_cid in set(np.array(cids)[:, 1]):
            where = ' cid = {} and uuid2 in ({})'.format(each_cid, where_sql)
            ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuid_request.append(uuid2)  ##do not foget delete
                if self.skip_brush(source, tb_item_id, p1, sql=fsql_not_909, if_flag=False):
                    continue
                else:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in pid909_item:
                        uuids4.append(uuid2)
                        uuids4_info.append([uuid2, source, tb_item_id, p1])
                        delete_id.append(pid909_item['{}-{}-{}'.format(source, tb_item_id, p1)])
                    else:
                        uuids2.append(uuid2)

        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        print('add new brush visible 1 {}'.format(len(uuids)))
        print('add new brush visible 2 {}'.format(len(uuids2)))
        # print('add new brush visible 1 新加组 {}, {}'.format(len(uuids3),len(uuids4)))
        db = self.cleaner.get_db(aname)
        uuids5 = []
        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 11']
        for each_bid, each_name in bids:
            for each_source in sources:
                item_id_top ='''
                select item_id,sum(sales) ss from {atbl} where uuid2 in(select uuid2 from {ctbl} where c_sp1 = \'其它\' and pkey>='{smonth}' and pkey<'{emonth}') and alias_all_bid={each_bid} and {each_source}  group by item_id order by sum(sales) desc limit 3
                '''.format(atbl=atbl, ctbl=ctbl, each_source=each_source, each_bid=each_bid, smonth=smonth, emonth=emonth)
                item_ids = [v[0] for v in db.query_all(item_id_top)]
                for each_item_id in item_ids:
                    where ='''
                    uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'其它\' and pkey>='{smonth}' and pkey<'{emonth}') and alias_all_bid={each_bid} and {each_source} and item_id=\'{each_item_id}\'
                    '''.format(ctbl=ctbl, each_source=each_source, each_item_id=each_item_id, each_bid=each_bid, smonth=smonth, emonth=emonth)
                    ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, limit=10000, where=where)
                    sales_by_uuid.update(sales_by_uuid_1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_request.append(uuid2)  ##do not foget delete
                        if self.skip_brush(source, tb_item_id, p1, sql=fsql_not_909, if_flag=False):
                            continue
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in pid909_item:
                            delete_id.append(pid909_item['{}-{}-{}'.format(source, tb_item_id, p1)])
                        else:
                            uuids5.append(uuid2)

        print(len(uuids5),len(delete_id))
        self.cleaner.add_brush(uuids5, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids100 = []
        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2', 'source = 11']
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for each_bid, each_name in bids:
                for each_source in sources:
                    item_id_top = '''
                                    select item_id,sum(sales) ss from {atbl} where uuid2 in(select uuid2 from {ctbl} where c_sp1 = \'其它\' and pkey>='{ssmonth}' and pkey<'{eemonth}') and alias_all_bid={each_bid} and {each_source}  group by item_id order by sum(sales) desc limit 10
                                    '''.format(atbl=atbl, ctbl=ctbl, each_source=each_source, each_bid=each_bid,
                                               ssmonth=ssmonth,
                                               eemonth=eemonth)
                    item_ids = [v[0] for v in db.query_all(item_id_top)]
                    for each_item_id in item_ids:
                        where = '''
                                        uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'其它\' and pkey>='{ssmonth}' and pkey<'{eemonth}') and alias_all_bid={each_bid} and {each_source} and item_id=\'{each_item_id}\'
                                        '''.format(ctbl=ctbl, each_source=each_source, each_item_id=each_item_id,
                                                   each_bid=each_bid,
                                                   ssmonth=ssmonth, eemonth=eemonth)
                        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=10000, where=where)
                        sales_by_uuid.update(sales_by_uuid_1)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            uuid_request.append(uuid2)  ##do not foget delete
                            if self.skip_brush(source, tb_item_id, p1, sql=fsql_not_909, if_flag=False):
                                continue
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in pid909_item:
                                delete_id.append(pid909_item['{}-{}-{}'.format(source, tb_item_id, p1)])
                            else:
                                uuids100.append(uuid2)

        self.cleaner.add_brush(uuids100, clean_flag, visible=100, sales_by_uuid=sales_by_uuid)

        print(delete_id)
        if len(delete_id) > 0:
            sql = 'delete from {} where id in ({})'.format(self.cleaner.get_tbl(),
                                                           ','.join([str(v) for v in delete_id]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()

        ### update抖音部分
        # 1、update visible
        update_douyin_sql_v1 = '''
        update {} set visible = 2 where snum = 11 and cast(created as date) = cast(now() as date) and visible = 1
        '''.format(self.cleaner.get_tbl())
        self.cleaner.db26.execute(update_douyin_sql_v1)
        self.cleaner.db26.commit()

        update_douyin_sql_v100 = '''
        update {} set visible = 101 where snum = 11 and cast(created as date) = cast(now() as date) and visible = 100
        '''.format(self.cleaner.get_tbl())
        self.cleaner.db26.execute(update_douyin_sql_v100)
        self.cleaner.db26.commit()

        # 2、update 默认clean_pid&num

        self.update_pid_num()

        self.cleaner.set_default_val(type=1)

        return True

    def brush_xxx(self, smonth, emonth):
        uuids = []
        where = 'uuid2 in (select uuid2 from {} where sp1=\'钙\')'.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            # if self.skip_brush(source, tb_item_id, p1, remove=False):
            #     continue
            uuids.append(uuid2)
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('add new brush {}'.format(len(set(uuids))))
        return True

    def brush_temp(self, smonth, emonth):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]): True for v in ret}

        uuids = []
        sales_by_uuid = {}

        where_sql = '''
        select uuid2 from {} where pkey >= '{}' and pkey < '{}' and sp1 = '钙'
        '''.format(self.get_clean_tbl(), smonth, emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, limit=1000, where='uuid2 in ({})'.format(where_sql))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)
        sql = 'SELECT source, cid FROM {}_parts GROUP BY source, cid'.format(self.get_entity_tbl())
        cids = self.cleaner.dbch.query_all(sql)
        bids = [
            [326236, 'bio island'],
            [273629, 'Centrum/善存'],
            [48678, 'childlife'],
            [104447, 'CONBA/康恩贝'],
            [393278, 'elevit'],
            [819397, 'Eric Favre'],
            [401951, 'MoveFree'],
            [333391, 'ostelin'],
            [131009, 'Swisse'],
            [59463, '修正'],
            [1856905, '健力多'],
            [59590, '哈药'],
            [387334, '新盖中盖'],
            [110054, '朗迪'],
            [112244, '汤臣倍健'],
            [387336, '迪巧'],
            [4834144, '金钙尔奇'],
            [53021, '钙尔奇'],
            [272287, '麦金利'],
            [387135, '黄金搭档'],
            [326026, 'SCHIFF'],
            [4713434,'精朗迪']
        ]

        out = []

        for bid, name, in bids:
            sql = 'SELECT bid FROM brush.all_brand WHERE bid = {b} OR alias_bid = {b}'.format(b=bid)
            ret = self.cleaner.db26.query_all(sql)
            bbb = [str(v[0]) for v in ret]
            for source, cid, in cids:
                where = 'source=\'{}\' and cid={} and all_bid in ({}) and uuid2 in ({})'.format(source, cid, ','.join(bbb), where_sql)
                ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where)
                sales_by_uuid.update(sales_by_uuid_1)
                if len(sales_by_uuid_1) > 0:
                    out.append(['要分要建', name, cid, min(list(sales_by_uuid_1.values()))])
                for uuid2, source, tb_item_id, p1, in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
                    uuids.append(uuid2)


        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # print('add new brush visible 1 {}'.format(len(uuids)))

        uuids = []
        for cid in set(np.array(cids)[:, 1]):
            where = ' cid = {} and uuid2 in ({})'.format(cid, where_sql)
            ret, sales_by_uuid_1 = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            if len(sales_by_uuid_1) > 0:
                out.append(['只分不建', cid, min(list(sales_by_uuid_1.values()))])
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)

        # self.cleaner.add_brush(uuids, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        # print('add new brush visible 2 {}'.format(len(uuids)))
        for i in out:
            print(i)
        return True

    def brush_custom(self, smonth, emonth):
        # 不然就直接 查 每个月分到只分不建的宝贝  实际上在TOP 的出题范围的 宝贝 条数 和销售额
        # 就是理论上要清洗  但是因为去重 所以没出题的
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where pid = 909'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]): True for v in ret}

        uuids = []
        sales_by_uuid = {}
        out = []

        where_sql = '''
        select uuid2 from {} where pkey >= '{}' and pkey < '{}' and sp1 = '钙'
        '''.format(self.get_clean_tbl(), smonth, emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_count(smonth, emonth, limit=1000, where='uuid2 in ({})'.format(where_sql))
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, num in ret:
            # 在历史出题里，且被分到visible_check=2
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                add = [source, tb_item_id, p1, num, sales_by_uuid_1[uuid2]]
                if add not in out:
                    out.append(add)
        sql = 'SELECT source, cid FROM {}_parts GROUP BY source, cid'.format(self.get_entity_tbl())
        cids = self.cleaner.dbch.query_all(sql)
        bids = [
            [326236, 'bio island'],
            [273629, 'Centrum/善存'],
            [48678, 'childlife'],
            [104447, 'CONBA/康恩贝'],
            [393278, 'elevit'],
            [819397, 'Eric Favre'],
            [401951, 'MoveFree'],
            [333391, 'ostelin'],
            [131009, 'Swisse'],
            [59463, '修正'],
            [1856905, '健力多'],
            [59590, '哈药'],
            [387334, '新盖中盖'],
            [110054, '朗迪'],
            [112244, '汤臣倍健'],
            [387336, '迪巧'],
            [4834144, '金钙尔奇'],
            [53021, '钙尔奇'],
            [272287, '麦金利'],
            [387135, '黄金搭档'],
            [326026, 'SCHIFF'],
            [4713434,'精朗迪']
        ]

        for bid, name, in bids:
            sql = 'SELECT bid FROM brush.all_brand WHERE bid = {b} OR alias_bid = {b}'.format(b=bid)
            ret = self.cleaner.db26.query_all(sql)
            bbb = [str(v[0]) for v in ret]
            for source, cid, in cids:
                where = 'source=\'{}\' and cid={} and all_bid in ({}) and uuid2 in ({})'.format(source, cid, ','.join(bbb), where_sql)
                ret, sales_by_uuid_1 = self.cleaner.process_top_count(smonth, emonth, rate=0.9, where=where)
                sales_by_uuid.update(sales_by_uuid_1)
                if len(ret) > 0:
                    for uuid2, source, tb_item_id, p1, num in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            add = [source, tb_item_id, p1, num, sales_by_uuid_1[uuid2]]
                            if add not in out:
                                out.append(add)

        for i in out:
            print(i)
        with open(app.output_path('yaofenyaojian.csv'),'w', newline='',encoding='gb18030') as f:
            writer = csv.writer(f)
            writer.writerow(['source','item_id','p1','count','sales'])
            for i in out:
                writer.writerow(i)
        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        uuids, sales_by_uuid = self.cleaner.process_top('2016-01-01', '2020-05-01', limit=100000, rate=0.9, where='all_bid in (326026,401951) or alias_all_bid in (326026,401951)')
        uuids = [str(t[0]) for t in uuids]

        sql = 'select uuid2 from product_lib.entity_90591_item where all_bid in (326026,401951) or alias_all_bid in (326026,401951) and flag=0 and pkey < \'2020-05-01\''
        ret = self.cleaner.db26.query_all(sql)
        uuids0 = [str(t[0]) for t in ret]

        sql = 'select uuid2 from product_lib.entity_90591_item where all_bid in (326026,401951) or alias_all_bid in (326026,401951) and flag !=0  and pkey < \'2020-05-01\''
        ret = self.db26.query_all(sql)
        uuids1 = [str(t[0]) for t in ret]
        # print(len(uuids0), len(uuids1))
        uuids_brush = uuids0 + uuids1
        no_brush_uuids = []
        for i in uuids:
            if i not in uuids_brush:
                no_brush_uuids.append(i)
        no_brush_uuids = no_brush_uuids + uuids0

        # no_brush_uuids = (set(uuids)-set(uuids_brush)) | set(uuids0)
        # uuids_list = ['\''+str(v[0])+'\'' for v in uuids]
        # print(len(uuids), len(uuids0), len(no_brush_uuids))

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush visible {}'.format(len(no_brush_uuids)))

        return True

    def brush_old(self, smonth, emonth):
        tbl = self.get_entity_tbl()
        sql = '''
            select uniq_k from artificial.entity_{}_item where month >= '{}' and month < '{}' and sp1 = '钙' group by uniq_k
        '''.format(self.cleaner.eid, smonth, emonth)
        ret = self.cleaner.db26.query_all(sql)
        uniq_ks = [str(v[0]) for v in ret]

        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        clean_flag = self.cleaner.last_clean_flag() + 1

        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=1000, where='uniq_k in ({})'.format(','.join(uniq_ks)))
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where='uniq_k in ({})'.format(','.join(uniq_ks)))
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 2, sales_by_uuid=sales_by_uuid)

        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_test(self, smonth, emonth):
        uuids = []
        where = 'uuid2 in (select uuid2 from {} where sp1=\'钙\') and source != \'tb\''.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, limit=99999999,where=where)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:

            uuids.append(uuid2)
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('67 add new brush {}'.format(len(set(uuids))))
        return True

# e表2个项目合并
# DESC sop_e.entity_prod_90996_E_m90591;
# CREATE TABLE sop_e.entity_prod_90996_E_m90591 AS sop_e.entity_prod_90591_E;

# INSERT INTO sop_e.entity_prod_90996_E_m90591(uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,clean_cid,clean_pid,all_bid,alias_all_bid,clean_props.name,clean_props.value)
# SELECT uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,
# transform(clean_props.value[indexOf(clean_props.name, '子品类')], ['其它','维生素','钙'], [1,2,3],0),0,all_bid,alias_all_bid,['子品类','品牌','是否含微量元素/矿物质','人群','中西药属性','剂型','总规格','sku','sku件数'],[
# 	clean_props.value[indexOf(clean_props.name, '子品类')],clean_props.value[indexOf(clean_props.name, '品牌')],clean_props.value[indexOf(clean_props.name, '分类')],
# 	clean_props.value[indexOf(clean_props.name, '人群')],clean_props.value[indexOf(clean_props.name, '中西药属性')],clean_props.value[indexOf(clean_props.name, '剂型')],
# 	clean_props.value[indexOf(clean_props.name, '总规格')],clean_props.value[indexOf(clean_props.name, 'sku')],clean_props.value[indexOf(clean_props.name, 'sku件数')]
# ]
# FROM sop_e.entity_prod_90591_E WHERE uuid2 NOT IN (SELECT uuid2 FROM sop_e.entity_prod_90996_E);

# INSERT INTO sop_e.entity_prod_90996_E_m90591(uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,clean_cid,clean_pid,all_bid,alias_all_bid,clean_props.name,clean_props.value)
# SELECT uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,
# transform(clean_props.value[indexOf(clean_props.name, '子品类')], ['其它','维生素','钙'], [1,2,3],0),0,all_bid,alias_all_bid,['子品类','品牌','分类','维生素种类','是否含微量元素/矿物质','人群','中西药属性','剂型','总规格','sku','sku件数'],[
# 	clean_props.value[indexOf(clean_props.name, '子品类')],clean_props.value[indexOf(clean_props.name, '品牌')],clean_props.value[indexOf(clean_props.name, '分类')],
# 	clean_props.value[indexOf(clean_props.name, '维生素种类')],clean_props.value[indexOf(clean_props.name, '是否含微量元素/矿物质')],
# 	clean_props.value[indexOf(clean_props.name, '人群')],clean_props.value[indexOf(clean_props.name, '中西药属性')],clean_props.value[indexOf(clean_props.name, '剂型')],
# 	clean_props.value[indexOf(clean_props.name, '总规格')],clean_props.value[indexOf(clean_props.name, 'sku')],clean_props.value[indexOf(clean_props.name, 'sku件数')]
# ]
# FROM sop_e.entity_prod_90996_E WHERE uuid2 NOT IN (SELECT uuid2 FROM sop_e.entity_prod_90591_E);

# INSERT INTO sop_e.entity_prod_90996_E_m90591(uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,clean_cid,clean_pid,all_bid,alias_all_bid,clean_props.name,clean_props.value)
# SELECT uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,
# transform(clean_props.value[indexOf(clean_props.name, '子品类')], ['其它','维生素','钙'], [1,2,3],0),0,all_bid,alias_all_bid,['子品类','品牌','是否含微量元素/矿物质','人群','中西药属性','剂型','总规格','sku','sku件数'],[
# 	clean_props.value[indexOf(clean_props.name, '子品类')],clean_props.value[indexOf(clean_props.name, '品牌')],clean_props.value[indexOf(clean_props.name, '分类')],
# 	clean_props.value[indexOf(clean_props.name, '人群')],clean_props.value[indexOf(clean_props.name, '中西药属性')],clean_props.value[indexOf(clean_props.name, '剂型')],
# 	clean_props.value[indexOf(clean_props.name, '总规格')],clean_props.value[indexOf(clean_props.name, 'sku')],clean_props.value[indexOf(clean_props.name, 'sku件数')]
# ]
# FROM sop_e.entity_prod_90591_E WHERE uuid2 IN (SELECT uuid2 FROM sop_e.entity_prod_90996_E WHERE alias_all_bid != 273629 OR clean_props.value[indexOf(clean_props.name, '子品类')] = '其它');

# INSERT INTO sop_e.entity_prod_90996_E_m90591(uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,clean_cid,clean_pid,all_bid,alias_all_bid,clean_props.name,clean_props.value)
# SELECT uuid2,sign,ver,uuid2,old_sign,old_ver,pkey,date,cid,real_cid,item_id,sku_id,name,sid,shop_type,brand,region_str,price,org_price,promo_price,img,sales,num,trade_props.name,trade_props.value,trade_props_hash,source,created,
# transform(clean_props.value[indexOf(clean_props.name, '子品类')], ['其它','维生素','钙'], [1,2,3],0),0,all_bid,alias_all_bid,['子品类','品牌','分类','维生素种类','是否含微量元素/矿物质','人群','中西药属性','剂型','总规格','sku','sku件数'],[
# 	clean_props.value[indexOf(clean_props.name, '子品类')],clean_props.value[indexOf(clean_props.name, '品牌')],clean_props.value[indexOf(clean_props.name, '分类')],
# 	clean_props.value[indexOf(clean_props.name, '维生素种类')],clean_props.value[indexOf(clean_props.name, '是否含微量元素/矿物质')],
# 	clean_props.value[indexOf(clean_props.name, '人群')],clean_props.value[indexOf(clean_props.name, '中西药属性')],clean_props.value[indexOf(clean_props.name, '剂型')],
# 	clean_props.value[indexOf(clean_props.name, '总规格')],clean_props.value[indexOf(clean_props.name, 'sku')],clean_props.value[indexOf(clean_props.name, 'sku件数')]
# ]
# FROM sop_e.entity_prod_90996_E WHERE uuid2 NOT IN (SELECT uuid2 FROM sop_e.entity_prod_90996_E_m90591);
