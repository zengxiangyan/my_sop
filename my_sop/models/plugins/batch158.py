import sys
import time
import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
import application as app
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch

class main(Batch.main):

    def brush_cwh(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        sales_by_uuid = {}
        cc={}

        sources = ['source = 1', 'source = 2']
        sp5s = ['鸡部位肉', '整鸡/半鸡']
        sp6s = ['整鸡/半鸡', '鸡翅中', '鸡翅根', '鸡胸', '鸡腿', '其它', '混合', '鸡肉块']

        ## 条件1
        uuids = []
        for each_source in sources:
            for each_sp5 in sp5s:
                top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp5='{each_sp5}' and sp1 = '鸡肉生品' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 32
                                '''.format(atbl=atbl, ctbl=ctbl, each_sp5=each_sp5, each_source=each_source, smonth=smonth, emonth=emonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]

                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp5='{each_sp5}' and sp1 = '鸡肉生品' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source}
                    '''.format(ctbl=ctbl, each_bid=each_bid, each_sp5=each_sp5, smonth=smonth, emonth=emonth, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=7, sales_by_uuid=sales_by_uuid)


        ## 条件2
        uuids = []
        for each_source in sources:
            for each_sp6 in sp6s:
                top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp6='{each_sp6}' and sp1 = '鸡肉生品' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 10
                                '''.format(atbl=atbl, ctbl=ctbl, each_sp6=each_sp6, each_source=each_source,
                                           smonth=smonth, emonth=emonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]

                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp6='{each_sp6}' and sp1 = '鸡肉生品' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source}
                    '''.format(ctbl=ctbl, each_bid=each_bid, each_sp6=each_sp6, smonth=smonth, emonth=emonth, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=7, sales_by_uuid=sales_by_uuid)


        ## 条件3
        uuids = []
        for each_source in sources:

            where ='''
            uuid2 in (select uuid2 from {ctbl} where sp1 = '鸡肉生品' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, rate=0.85, where=where)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=7, sales_by_uuid=sales_by_uuid)


        for i in cc:
            print(i, cc[i])


        return True

    def brush_1229(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)
        sales_by_uuid = {}
        cc = {}
        uuids = []

        where = '''
        uuid2 = '73100c2f-f8b9-40ff-9c81-af698226e156'
        '''
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=1, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):
        sql = 'SELECT snum, tb_item_id, real_p1, uuid2 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        self.mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}
        uuid_list_old = [v[3] for v in ret]
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        chmaster = self.cleaner.get_db(cname)

        rets1 = []
        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉半成品')'''.format(ctbl)
        for each_source in [' and source = 1', ' and source = 2']:
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where + each_source, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            rets1 = rets1 + ret

        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉即食品')'''.format(ctbl)
        for each_source in [' and source = 1', ' and source = 2']:
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where + each_source, rate=0.85)
            sales_by_uuid.update(sales_by_uuid1)
            rets1 = rets1 + ret

        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉生品') and source !=11'''.format(ctbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=20)
        sales_by_uuid.update(sales_by_uuid1)
        rets1 = rets1 + ret

        where = '''((source=1 and shop_type>20 and sid in (192565241, 190338754)) or (source = 2 and sid in (10146402,1000080647,1000135202,742821)) or (source=1 and (shop_type<20 and shop_type>10) and sid=190898475))'''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets1 = rets1 + ret

        rets3 = []
        sql = '''select distinct(sp6) from {ctbl}'''.format(ctbl=ctbl)
        sp6s = [v[0] for v in chmaster.query_all(sql)]
        top_brand_sql_template = '''
        select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp6 = '{sp6}') and pkey >= '{smonth}' and pkey < '{emonth}' {source} group by alias_all_bid order by ss desc limit 40'''
        for sp6 in sp6s:
            for each_source in [' and source = 1', ' and source = 2']:
                sql = top_brand_sql_template.format(atbl=atbl, ctbl=ctbl, sp6=sp6, smonth=smonth, emonth=emonth, source=each_source)
                bids = [v[0] for v in chmaster.query_all(sql)]
                for each_bid in bids:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp6 = '{sp6}') and alias_all_bid = {each_bid} {source}'''.format(ctbl=ctbl, sp6=sp6, each_bid=each_bid, source=each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1)
                    sales_by_uuid.update(sales_by_uuid1)
                    rets3 = rets3 + ret

        uuid_list_new = []
        uks = []
        for row in (rets1 + rets3):
            uuid_list_new.append(row[0])
            uks.append([row[1], row[2], row[3]])

        p_b_by_uuid, p_c_by_uuid, sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk = self.skip_helper(uuid_list_new, uuid_list_old, smonth, uks)
        # print('sp6ssp11s')
        # print(sp6sp11_by_uuid)
        uuids1 = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in rets1:
            if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid) and self.skip_brush_via_sp6(source, tb_item_id, p1, str(uuid2), sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk):
            # if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid):
            # if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
                print('continue')
                continue
            uuids1.append(uuid2)
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        # print('add', len(uuids1))
        # print('sc: ', self.sc)

        # uuids3 = []
        # for uuid2, source, tb_item_id, p1, cid, alias_all_bid in rets3:
        #     if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid) and self.skip_brush_via_sp6(source, tb_item_id, p1, str(uuid2), sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk):
        #         print('continue')
        #         continue
        #     uuids3.append(uuid2)
        #     self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids3, clean_flag, 1, visible=3, sales_by_uuid=sales_by_uuid)
        # print('add', len(uuids1), len(uuids3))
        print('sc: ', self.sc)
        return True

    def brush_0124(self, smonth, emonth, logId=-1):
        sql = 'SELECT snum, tb_item_id, real_p1, uuid2 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        self.mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): v[3] for v in ret}
        uuid_list_old = [v[3] for v in ret]
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        chmaster = self.cleaner.get_db(cname)

        rets1 = []
        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉半成品') and cid=50050734'''.format(ctbl)
        for each_source in [' and source = 1', ' and source = 2']:
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where + each_source, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            rets1 = rets1 + ret

        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉即食品') and cid=50050734'''.format(ctbl)
        for each_source in [' and source = 1', ' and source = 2']:
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where + each_source, rate=0.85)
            sales_by_uuid.update(sales_by_uuid1)
            rets1 = rets1 + ret

        where = '''uuid2 in (select uuid2 from {} where sp1='鸡肉生品') and cid=50050734'''.format(ctbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=20)
        sales_by_uuid.update(sales_by_uuid1)
        rets1 = rets1 + ret

        where = '''((source=1 and shop_type>20 and sid in (192565241, 190338754)) or (source = 2 and sid in (10146402,1000080647,1000135202,742821)) or (source=1 and (shop_type<20 and shop_type>10) and sid=190898475))  and cid=50050734'''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        rets1 = rets1 + ret

        rets3 = []
        sql = '''select distinct(sp6) from {ctbl}'''.format(ctbl=ctbl)
        sp6s = [v[0] for v in chmaster.query_all(sql)]
        top_brand_sql_template = '''
        select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp6 = '{sp6}') and pkey >= '{smonth}' and pkey < '{emonth}' {source}  and cid=50050734 group by alias_all_bid order by ss desc limit 40'''
        for sp6 in sp6s:
            for each_source in [' and source = 1', ' and source = 2']:
                sql = top_brand_sql_template.format(atbl=atbl, ctbl=ctbl, sp6=sp6, smonth=smonth, emonth=emonth, source=each_source)
                bids = [v[0] for v in chmaster.query_all(sql)]
                for each_bid in bids:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp6 = '{sp6}') and alias_all_bid = {each_bid} {source}  and cid=50050734'''.format(ctbl=ctbl, sp6=sp6, each_bid=each_bid, source=each_source)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=1)
                    sales_by_uuid.update(sales_by_uuid1)
                    rets3 = rets3 + ret

        uuid_list_new = []
        uks = []
        for row in (rets1 + rets3):
            uuid_list_new.append(row[0])
            uks.append([row[1], row[2], row[3]])

        p_b_by_uuid, p_c_by_uuid, sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk = self.skip_helper(uuid_list_new, uuid_list_old, smonth, uks)
        # print('sp6ssp11s')
        # print(sp6sp11_by_uuid)
        uuids1 = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in rets1:
            if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid) and self.skip_brush_via_sp6(source, tb_item_id, p1, str(uuid2), sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk):
            # if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid):
            # if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
                print('continue')
                continue
            uuids1.append(uuid2)
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        # print('add', len(uuids1))
        # print('sc: ', self.sc)

        uuids3 = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in rets3:
            if self.skip_brush_custom(source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid) and self.skip_brush_via_sp6(source, tb_item_id, p1, str(uuid2), sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk):
                print('continue')
                continue
            uuids3.append(uuid2)
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = 0

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids3, clean_flag, 1, visible=3, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids1), len(uuids3))
        print('sc: ', self.sc)
        return True

    def brush_0830x(self, smonth, emonth, logId=-1):
        uuids1 = []
        where = '''
        item_id = '647537123750' and trade_props.value = ['【烧烤味】120g*1袋']
        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        # sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids1.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid1)
        return True

    def read_sp(self, filename):
        file = app.output_path(filename)
        # table = pd.read_excel(file, 0, header=None, dtype=str)
        table = pd.read_excel(file, 0, dtype=str)
        table = table.fillna('')
        table = table.values.tolist()
        return table

    def skip_helper(self, uuid_list_new, uuid_list_old, smonth, uks):
        # 取B,C表均价
        dbch = self.cleaner.dbch
        aname, atbl = self.get_a_tbl()
        bname, btbl = self.get_b_tbl()
        cname, ctbl = self.get_c_tbl()
        sql = '''
        select argMax(sales/num/toFloat64(if(sp10=\'1000*1\',\'1000\',sp10))/toFloat64(sp11), date),toString(uuid2) from {} where sp10 != '' and sp11 != '' and uuid2 in ({}) group by uuid2
        '''.format(btbl, ','.join(['\'{}\''.format(v) for v in uuid_list_old]))
        p_b_by_uuid = {v[1]: v[0] for v in dbch.query_all(sql)}

        sql = '''
        select argMax(sales/num/toFloat64(if(sp10=\'1000*1\',\'1000\',sp10))/toFloat64(sp11), date),toString(uuid2) from {} where sp10 != '' and sp11 != '' and uuid2 in ({}) group by uuid2
        '''.format(ctbl, ','.join(['\'{}\''.format(v) for v in uuid_list_new]))
        p_c_by_uuid = {v[1]: v[0] for v in dbch.query_all(sql)}

        sql = 'select toString(uuid2), sp6, sp11,sp30 from {} where uuid2 in ({})'.format(ctbl, ','.join(['\'{}\''.format(v) for v in uuid_list_old + uuid_list_new]))
        sp6sp11_by_uuid = {v[0]: [v[1], v[2], v[3]] for v in dbch.query_all(sql)}

        # sql = '''
        # select any(sp30),toString(uuid2) from {} where uuid2 in ({}) group by uuid2
        # '''.format(btbl, ','.join(['\'{}\''.format(v) for v in uuid_list_old]))
        # sp6_by_uuid_old = {v[1]: v[0] for v in dbch.query_all(sql)}

        sql = '''
        select sp30,toString(uuid2) from {} where uuid2 in ({})
        '''.format(ctbl, ','.join(['\'{}\''.format(v) for v in uuid_list_new]))
        sp6_by_uuid_new = {v[1]: v[0] for v in dbch.query_all(sql)}

        # a表取出的新题uuid，查询在C表中的历史机洗结果
        # 取时段内最新的一条结果
        str_uks = []
        for i in uks:
            str_uks.append('({},\'{}\',{})'.format(i[0],i[1],i[2]))
        str_uks = ','.join(str_uks)
        sql = '''
        select * from (select a.source, a.item_id,a.p1,a.pkey,c.sp30 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        select uuid2, source, item_id, p1,pkey from {atbl} where (source,item_id,p1) in ({})) a join {ctbl} c on c.uuid2 = a.uuid2 where a.pkey > '2021-01-01' and a.pkey < '{smonth}')
        order by pkey desc limit 1 by source,item_id,p1
        '''.format(str_uks, atbl=atbl, ctbl=ctbl, smonth=smonth)
        tmp = dbch.query_all(sql)
        sp30_by_uk = {}
        for v in tmp:
            sp30_by_uk['{}-{}-{}'.format(v[0], v[1], v[2])] = v[4]
        iiii = list(sp30_by_uk.keys())
        for i in range(0,100):
            print(iiii[i], sp30_by_uk[iiii[i]])
            print('\n')
        return p_b_by_uuid, p_c_by_uuid, sp6sp11_by_uuid,sp6_by_uuid_new, sp30_by_uk

    def skip_brush_custom(self, source, tb_item_id, p1, uuid2, p_c_by_uuid, p_b_by_uuid):
        uuid2 = str(uuid2)
        if uuid2 not in p_c_by_uuid:
            return True
        if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
            if self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] == 0:  # 新增uuid
                return True
            if int(p1) == 0:
                # print('新排重ww')
                uuid_old = self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)]
                if uuid_old not in p_b_by_uuid:
                    return True
                price_old = p_b_by_uuid[uuid_old]
                price_new = p_c_by_uuid[uuid2]
                ratio = price_new / price_old
                if ratio >= 0.7 and ratio <= 1.3:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    sc = 0

    def skip_brush_via_sp6_xx(self, source, tb_item_id, p1, uuid2, sp6sp11_by_uuid, sp6_by_uuid_old):
        # 实际比较的是sp30
        if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
            if self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] == 0:  # 新增uuid
                return True
            else:
                uuid_old = self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)]
                sp6_old = sp6_by_uuid_old.get(uuid_old, '')
                sp6_new = sp6sp11_by_uuid.get(uuid2, [''])[-1]
                print('old: ', sp6_old, 'new: ', sp6_new)
                if sp6_old == sp6_new:
                    return True
                else:
                    return False
        else:
            return False

    def skip_brush_via_sp6(self, source, tb_item_id, p1, uuid2, sp6sp11_by_uuid, sp6_by_uuid_new, sp30_by_uk):
        # 实际比较的是sp30
        if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
            if self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] == 0:  # 新增uuid
                return True
            else:
                sp30_old = sp30_by_uk.get('{}-{}-{}'.format(source, tb_item_id, p1), '')
                sp30_new = sp6_by_uuid_new.get(uuid2, '')
                print('old: ', sp30_old, 'new: ', sp30_new)
                if sp30_old == sp30_new:
                    return True
                else:
                    self.sc = self.sc + 1
                    return False
        else:
            return False

    def skip_brush_via_sp(self, source, tb_item_id, p1, new_uuid, sp6sp11_by_uuid):
        uuid2 = self.mpp.get('{}-{}-{}'.format(source, tb_item_id, p1), False)
        if uuid2:
            if uuid2 not in sp6sp11_by_uuid or new_uuid not in sp6sp11_by_uuid:
                return False
            sp6s = [sp6sp11_by_uuid[uuid2][0], sp6sp11_by_uuid[new_uuid][0]]
            sp11s = [sp6sp11_by_uuid[uuid2][1], sp6sp11_by_uuid[new_uuid][1]]
            # print(uuid2, new_uuid)
            # print('sp6: ',sp6s)
            # print('sp11: ', sp11s)
            sp6s = set(sp6s)
            sp11s = set(sp11s)
            if len(sp6s) == 1 and len(sp11s) == 1:
                return True
            else:
                return False
        else:
            return False

    def pre_brush_modify(self, v, products, prefix):
        if v['flag'] == 2 and v['split'] > 0:
            v['split'] = 0
        return v

    def brush_modify(self, v, bru_items):
        if v['pid'] > 0:
            for vv in v['split_pids']:
                vv['sp19'] = v['sp17']
                vv['sp20'] = v['sp9']
                vv['sp21'] = v['sp10']
                vv['sp22'] = v['sp1']
                vv['sp23'] = v['sp4']
                vv['sp24'] = v['sp5']
                vv['sp25'] = v['sp6']
                vv['sp26'] = v['sp8']
                vv['sp27'] = v['sp11']
                vv['sp28'] = v['sp13']

    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        self.hotfix_ecshop(tbl, dba, '店铺分类')
        self.hotfix_xinpin(tbl, dba, prefix, params)

        db26 = self.cleaner.get_db('26_apollo')

        sql = ''' SELECT pid FROM product_lib.product_{} WHERE name NOT LIKE '\\\\_\\\\_\\\\_%' '''.format(self.cleaner.eid)
        ret = db26.query_all(sql)
        pids = [str(v) for v, in ret]

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['人工是否确认', '出数用店铺分类', '总销售量(KG)', '规格', '销售量（包数）'], `c_props.name`),
                    ['人工是否确认', '出数用店铺分类', '总销售量(KG)', '规格', '销售量（包数）']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['人工是否确认', '出数用店铺分类', '总销售量(KG)', '规格', '销售量（包数）'], `c_props.value`, `c_props.name`),
                    [
                        IF(c_brush_id > 0, '是', '否'),
                        multiIf(
                            source = 1 AND (shop_type > 20 or shop_type < 10 ) AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] = 'FSS', '天猫品牌官方旗舰店',
                            source = 1 AND (shop_type > 20 or shop_type < 10 ) AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] IN ('EKA','EKA_FSS'), '天猫平台自营 ',
                            source = 1 AND (shop_type > 20 or shop_type < 10 ) AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] = 'EDT', '天猫其他',
                            source = 2 AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] = 'FSS', '京东品牌官方旗舰店',
                            source = 2 AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] IN ('EKA','EKA_FSS'), '京东平台自营',
                            source = 2 AND `c_props.value`[indexOf(`c_props.name`, '店铺分类')] = 'EDT', '京东POP(专营/专卖店)',
                            source = 1 AND (shop_type < 20 and shop_type > 10 ) AND sid IN (190644702,190898475), '淘宝',
                            ''
                        ),
                        toString(ROUND(
                            toFloat32OrZero(`c_props.value`[indexOf(`c_props.name`, '单包规格(g)')]) / 1000
                          * toFloat32OrZero(IF(c_sku_id IN ([{p}]), `c_props.value`[indexOf(`c_props.name`, 'SKU件数')], `c_props.value`[indexOf(`c_props.name`, '包数')])),
                        3)),
                        CONCAT(`c_props.value`[indexOf(`c_props.name`, '单包规格(g)')], 'G*', IF(c_sku_id IN ([{p}]), `c_props.value`[indexOf(`c_props.name`, 'SKU件数')], `c_props.value`[indexOf(`c_props.name`, '包数')])),
                        IF(c_sku_id IN ([{p}]), `c_props.value`[indexOf(`c_props.name`, 'SKU件数')], `c_props.value`[indexOf(`c_props.name`, '包数')])
                    ]
                )
            WHERE 1
        '''.format(tbl, p=','.join(pids))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.name`),
                    ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.value`, `c_props.name`),
                    [
                        toString(ROUND(
                            toFloat32OrZero(`c_props.value`[indexOf(`c_props.name`, '不拆套包单包规格')]) / 1000
                          * toFloat32OrZero(IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])),
                        3)),
                        CONCAT(`c_props.value`[indexOf(`c_props.name`, '不拆套包单包规格')], 'G*', IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])),
                        IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])
                    ]
                )
            WHERE c_split_rate < 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['不拆套包sku名','不拆套包口味','不拆套包单包规格','不拆套包子品类','不拆套包是否套包','不拆套包一级品类','不拆套包二级品类','不拆套包保存温度','不拆套包包数','不拆套包SKU件数','不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.name`),
                    ['不拆套包sku名','不拆套包口味','不拆套包单包规格','不拆套包子品类','不拆套包是否套包','不拆套包一级品类','不拆套包二级品类','不拆套包保存温度','不拆套包包数','不拆套包SKU件数','不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['不拆套包sku名','不拆套包口味','不拆套包单包规格','不拆套包子品类','不拆套包是否套包','不拆套包一级品类','不拆套包二级品类','不拆套包保存温度','不拆套包包数','不拆套包SKU件数','不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.value`, `c_props.name`),
                    [
                        `c_props.value`[indexOf(`c_props.name`, '客户sku出数名')],
                        `c_props.value`[indexOf(`c_props.name`, '口味')],
                        `c_props.value`[indexOf(`c_props.name`, '单包规格(g)')],
                        `c_props.value`[indexOf(`c_props.name`, '子品类')],
                        `c_props.value`[indexOf(`c_props.name`, '是否套包')],
                        `c_props.value`[indexOf(`c_props.name`, '一级品类')],
                        `c_props.value`[indexOf(`c_props.name`, '二级品类')],
                        `c_props.value`[indexOf(`c_props.name`, '保存温度')],
                        `c_props.value`[indexOf(`c_props.name`, '包数')],
                        `c_props.value`[indexOf(`c_props.name`, 'SKU件数')],
                        `c_props.value`[indexOf(`c_props.name`, '总销售量(KG)')],
                        `c_props.value`[indexOf(`c_props.name`, '规格')],
                        `c_props.value`[indexOf(`c_props.name`, '销售量（包数）')]
                    ]
                )
            WHERE c_split_rate = 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.name`),
                    ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['不拆套包总销售量(KG)', '不拆套包规格', '不拆套包销售量（包数）'], `c_props.value`, `c_props.name`),
                    [
                        toString(ROUND(
                            toFloat32OrZero(`c_props.value`[indexOf(`c_props.name`, '不拆套包单包规格')]) / 1000
                          * toFloat32OrZero(IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])),
                        3)),
                        CONCAT(`c_props.value`[indexOf(`c_props.name`, '不拆套包单包规格')], 'G*', IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])),
                        IF(c_sku_id > 0, `c_props.value`[indexOf(`c_props.name`, '不拆套包SKU件数')], `c_props.value`[indexOf(`c_props.name`, '不拆套包包数')])
                    ]
                )
            WHERE c_split_rate < 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k,v) -> IF(k='出数用店铺分类','天猫其他',v), c_props.name,c_props.value)
            WHERE sid = 192546886
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k,v) -> IF(k='不拆套包二级品类','鸡肉混合',v), c_props.name,c_props.value)
            WHERE `c_props.value`[indexOf(`c_props.name`, '不拆套包子品类')] = '鸡肉半成品' AND `c_props.value`[indexOf(`c_props.name`, '不拆套包二级品类')] = '混合'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

        sql = '''
            ALTER TABLE {}
            UPDATE `c_alias_all_bid` = 3717258
            WHERE `c_props.value`[indexOf(`c_props.name`, '子品类')] = '鸡肉即食品' AND `c_alias_all_bid` = 4679840
              AND multiSearchFirstPosition(lower(concat(name,' ',toString(`trade_props.value`))), ['优形','ishape'])
              AND c_brush_id = 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)

    def hotfix_xinpin(self, tbl, dba, prefix, params):
        _, atbl = self.get_a_tbl()
        _, btbl = self.get_b_tbl()
        _, etbl = self.get_e_tbl()

        db26 = self.cleaner.get_db('26_apollo')
        sql = '''
            DROP TABLE IF EXISTS {}_xinpin
        '''.format(etbl)
        dba.execute(sql)

        sql = '''
            SELECT pid FROM product_lib.product_{} WHERE alias_all_bid = 0
        '''.format(self.cleaner.eid)
        ret = db26.query_all(sql)
        other_pids = [str(v[0]) for v in ret]

        brush_p1, filter_p1 = self.filter_brush_props()
        sql = '''
           CREATE TABLE {}_xinpin ENGINE Log AS
           WITH arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray', {})) AS p1
           SELECT a.`source`, a.item_id, a.`date`, p1, ifNull(b.similarity,0) csim, ifNull(b.pid,0) cpid
           FROM {} a LEFT JOIN (SELECT * FROM {} WHERE similarity >= 1 AND pid NOT IN ({})) b USING (uuid2)
           WHERE a.sign = 1
        '''.format(etbl, filter_p1, atbl, btbl, ','.join(other_pids))
        dba.execute(sql)

        sql = '''
          SELECT cpid, m FROM (
              SELECT cpid, min(date) m, toYYYYMM(addMonths(m, -3)) per2month FROM {t}_xinpin WHERE cpid > 0 AND csim >= 2 GROUP BY cpid
          ) a JOIN (
              SELECT cpid, toYYYYMM(min(date)) minmonth FROM {t}_xinpin WHERE cpid > 0 AND csim >= 1 GROUP BY cpid
          ) b USING (cpid)
          WHERE b.minmonth >= a.per2month
        '''.format(t=etbl)
        ret = dba.query_all(sql)

        a, b = [str(v[0]) for v in ret], ['\'{}疑似新品\''.format(v[1].strftime("%Y-%m")) for v in ret]

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['疑似新品'], `c_props.name`),
                    ['疑似新品']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['疑似新品'], `c_props.value`, `c_props.name`),
                    [
                        transform(c_sku_id, [{}], [{}], '')
                    ]
                )
            WHERE 1
        '''.format(tbl, ','.join(a), ','.join(b))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(60)


    def process_exx(self, tbl, prefix, logId=0):
        super().process_exx(tbl)

        # 这项目以后pid不能用了  就算要用需要整理代码逻辑
        dba, ptbl = self.get_product_tbl()
        dba = self.cleaner.get_db(dba)

        sql = '''
            SELECT toString(groupArray(pid)), toString(groupArray(name)) FROM {}
        '''.format(ptbl)
        rr1 = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                `clean_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['SKU名old','SKU名new','SKU名nosplit'], `clean_props.name`),
                    ['SKU名old','SKU名new','SKU名nosplit']
                ),
                `clean_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['SKU名old','SKU名new','SKU名nosplit'], `clean_props.value`, `clean_props.name`),
                    [transform(clean_pid, {a}, {b}, ''),transform(clean_pid, {a}, {b}, ''),clean_props.value[indexOf(clean_props.name, '不拆套包sku名')]]
                )
            WHERE 1
        '''.format(tbl, a=rr1[0][0], b=rr1[0][1])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `clean_props.value` = arrayMap((k, v) -> multiIf(
                k='SKU名new', concat(
                    dictGetOrDefault('all_brand', 'name', tuple(alias_all_bid), ''),
                    REPLACE(clean_props.value[indexOf(clean_props.name, 'SKU名old')], '___', ''),
                    '-', clean_props.value[indexOf(clean_props.name, '口味')],
                    '[', clean_props.value[indexOf(clean_props.name, '单包规格(g)')], 'g]'
                ),
            v), clean_props.name, clean_props.value)
            WHERE clean_props.value[indexOf(clean_props.name, '是否套包')] = '套包'
              AND clean_pid > 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `clean_props.value` = arrayMap((k, v) -> multiIf(
                k='SKU名nosplit', concat(
                    dictGetOrDefault('all_brand', 'name', tuple(alias_all_bid), ''),
                    REPLACE(clean_props.value[indexOf(clean_props.name, '不拆套包sku名')], '___', ''),
                    '-', clean_props.value[indexOf(clean_props.name, '不拆套包口味')],
                    '[', clean_props.value[indexOf(clean_props.name, '不拆套包单包规格')], 'g]'
                ),
            v), clean_props.name, clean_props.value)
            WHERE clean_props.value[indexOf(clean_props.name, '不拆套包sku名')] LIKE '\\\\_\\\\_\\\\_%'
              AND clean_pid > 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
        # dba, ptbl = self.get_product_tbl()
        # dba = self.cleaner.get_db(dba)

        # sql = '''
        #     SELECT toString(groupArray(pid)), toString(groupArray(replace(name, '___', '')))
        #     FROM {} WHERE name LIKE '%套包%' AND sku_id > 0
        # '''.format(ptbl)
        # rr1 = dba.query_all(sql)

        # sql = '''
        #     WITH concat(
        #             dictGetOrDefault('all_brand', 'name', tuple(alias_all_bid), ''),
        #             transform(clean_pid, {}, {}, ''),
        #             '-',
        #             clean_props.value[indexOf(clean_props.name, '口味')],
        #             '[', clean_props.value[indexOf(clean_props.name, '单包规格(g)')],
        #             'g]'
        #         ) AS sku_name
        #     SELECT alias_all_bid, sku_name FROM {}
        #     WHERE clean_pid IN {}
        #     GROUP BY alias_all_bid, sku_name
        # '''.format(rr1[0][0], rr1[0][1], tbl, rr1[0][0])
        # ret = dba.query_all(sql)0

        # sql = 'SELECT max(pid) FROM {}'.format(ptbl)
        # pid = dba.query_all(sql)[0][0]

        # transa, transb = [], []
        # for bid, name, in ret:
        #     sql = 'SELECT pid FROM {} WHERE all_bid = {} AND name = %(a)s'.format(ptbl, bid)
        #     rrr = dba.query_all(sql, {'a':name})

        #     if len(rrr) == 0:
        #         pid += 1
        #         sql = '''
        #             INSERT INTO {} (pid, all_bid, name, img, market_price, sku_id, model_id, alias_pid) VALUES
        #         '''.format(ptbl)
        #         dba.execute(sql, [[pid, bid, name, '', 0, 0, 0, 0]])
        #         transa.append('\'{}\''.format(name))
        #         transb.append(str(pid))
        #     else:
        #         transa.append('\'{}\''.format(name))
        #         transb.append(str(rrr[0][0]))

        # sql = '''
        #     ALTER TABLE {} UPDATE alias_pid = transform(
        #         concat(
        #             dictGetOrDefault('all_brand', 'name', tuple(alias_all_bid), ''),
        #             transform(clean_pid, {}, {}, ''),
        #             '-', clean_props.value[indexOf(clean_props.name, '口味')],
        #             '[', clean_props.value[indexOf(clean_props.name, '单包规格(g)')], 'g]'
        #         ),
        #     [{}], [{}], alias_pid) WHERE clean_pid IN {}
        # '''.format(tbl, rr1[0][0], rr1[0][1], ','.join(transa), ','.join(transb), rr1[0][0])
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(60)


    # 对需要出题的uuid定制，比如单价不同就不排重
    def process_brush_uuid(self, ver, flag, excl_visible):
        tbl = self.cleaner.get_tbl()
        db26 = self.cleaner.get_db('26_apollo')

        dba, atbl = self.get_a_tbl()
        dba, ctbl = self.get_c_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT min(smonth), max(emonth) FROM sop_b.brush_log WHERE eid = {} AND ver = {}'.format(self.cleaner.eid, ver)
        ret = dba.query_all(sql)
        smonth, emonth = ret[0]

        sql = '''
            SELECT snum, tb_item_id, real_p1, org_bid, avg_price, uuid2
            FROM {} WHERE flag >= {} AND visible NOT IN ({})
            ORDER BY month
        '''.format(tbl, flag, excl_visible)
        rr1 = db26.query_all(sql)
        rr1 = {'{}-{}-{}'.format(v[0],v[1],v[2]): v[3:] for v in rr1}

        sql = '''
            SELECT snum, tb_item_id, real_p1, id
            FROM {} WHERE flag < {} OR visible IN ({})
        '''.format(tbl, flag, excl_visible)
        rr2 = db26.query_all(sql)
        rr2 = {'{}-{}-{}'.format(v[0],v[1],v[2]): v[3] for v in rr2}

        brush_p1, format_p1 = self.filter_brush_props()
        format_p1 = '''arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',{}))))'''.format(format_p1)

        a, b = tbl.split('.')
        sql = '''
            SELECT source, item_id, {} p1, sum(sales*sign)/sum(num*sign) p
            FROM {}
            WHERE (source, item_id, p1) IN (
                SELECT source, item_id, p1 FROM sop_b.pre_brush WHERE eid={} AND ver={}
            )
            AND pkey >= '{}' AND pkey < '{}'
            GROUP BY source, item_id, p1
        '''.format(format_p1, atbl, self.cleaner.eid, ver, smonth, emonth)
        rr3 = dba.query_all(sql)
        rr3 = {'{}-{}-{}'.format(v[0],v[1],v[2]): v[3] for v in rr3}

        sql = '''
            SELECT uuid2, source, item_id, p1, all_bid, idx, key
            FROM sop_b.pre_brush WHERE eid={} AND ver={}
            ORDER BY idx LIMIT 1 BY source, item_id, p1, all_bid
        '''.format(self.cleaner.eid, ver)
        ret = dba.query_all(sql)

        uids = [v[0] for v in ret]+[rr1[k][2] for k in rr1]
        sql = '''
            SELECT uuid2, sp6, sp11
            FROM {} WHERE uuid2 IN ({})
        '''.format(ctbl, ','.join(['\'{}\''.format(v) for v in uids]))
        rr4 = dba.query_all(sql)
        rr4 = {str(v[0]): v[1:] for v in rr4}

        uuids, rmids = [], []
        for v in ret:
            k = '{}-{}-{}'.format(v[1],v[2],v[3])

            if k not in rr1:
                # 没出过题
                pass
            elif rr1[k][0] != v[4]:
                # 品牌变了
                pass
            elif k not in rr3:
                # 没找到
                pass
            elif abs(rr1[k][1]-rr3[k])/rr1[k][1] >= 0.3:
                # 均价差异超过30%
                pass
            elif str(v[0]) in rr4 and str(rr1[k][2]) in rr4 and rr4[str(v[0])] != rr4[str(rr1[k][2])]:
                # sp6, sp11 机洗结果不一致
                pass
            else:
                # 不出题
                continue

            if k in rr2:
                # 在删除的范围内
                rmids.append(rr2[k])

            uuids.append(list(v))

        return uuids, rmids
