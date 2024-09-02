import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def brush_xxx(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}


        alias_all_bid = [111365,64054,67955,119261,113785]
        # sql = 'select distinct(alias_all_bid) from  artificial_local.entity_90674_origin_parts'
        # alias_all_bid = [v[0] for v in self.cleaner.dbch.query_all(sql)]

        sales_by_uuid = {}
        uuids = []
        for bid in alias_all_bid:
            where = 'source = \'tuhu\' and alias_all_bid = {}'.format(bid)
            ret, sales_by_uuid_1= self.cleaner.process_top(smonth, emonth, rate=1, where=where)
            sales_by_uuid.update(sales_by_uuid_1)
            for uuid, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        uuids = []
        where = 'source = \'tuhu\''
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.9, where=where)
        for uuid, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_3(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        uuids = []
        # where = 'source = \'tuhu\''
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.5)
        for uuid, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))

        return True

    def each_month(self, s, e):
        i = []
        for y in range(s[0], e[0] + 1):
            if y == s[0] and s[0] < s[1]:
                for m in range(s[1], 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            elif y == e[0]:
                for m in range(1, e[1]):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            else:
                for m in range(1, 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
        return i

    def brush_bak20201116(self, smonth, emonth):
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 1'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        s = [int(smonth.split('-')[0]), int(smonth.split('-')[1])]
        e = [int(emonth.split('-')[0]), int(emonth.split('-')[1])]
        # s = [2017, 1]
        # e = [2020, 8]
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []
        c = []
        where = ['source = \'jd\'',
                 'source in (\'tmall\',\'tb\')',
                 'source = \'tuhu\'']
        # for each_smonth, each_emonth in self.each_month(s, e):
        #     cc = 0
        #     for each_where in where:
        #         ret, sales_by_uuid = self.cleaner.process_top(each_smonth, each_emonth,where=each_where, rate=0.1)
        #         for uuid, source, tb_item_id, p1, in ret:
        #             if self.skip_brush(source, tb_item_id, p1,remove=False):
        #                 continue
        #             uuids.append(uuid)
        #             cc = cc + 1
        #         # print('add new brush {}'.format(len(uuids)))
        #         c.append([each_smonth, each_emonth, each_where, cc])

        for each_where in where:
            cc = 0
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=each_where, rate=0.25)
            for uuid, source, tb_item_id, p1, in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=True, flag=True):
                    continue
                uuids.append(uuid)
                cc = cc + 1
            c.append([each_where, cc])
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))
        for i in c:
            print(i)
        print(len(uuids))
        return True

    def brush_xxxxxx(self, smonth, emonth):
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 1'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        s = [int(smonth.split('-')[0]), int(smonth.split('-')[1])]
        e = [int(emonth.split('-')[0]), int(emonth.split('-')[1])]
        # s = [2017, 1]
        # e = [2020, 8]
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []
        c = []
        where = ['source = \'tuhu\'']
        # for each_smonth, each_emonth in self.each_month(s, e):
        #     cc = 0
        #     for each_where in where:
        #         ret, sales_by_uuid = self.cleaner.process_top(each_smonth, each_emonth,where=each_where, rate=0.1)
        #         for uuid, source, tb_item_id, p1, in ret:
        #             if self.skip_brush(source, tb_item_id, p1,remove=False):
        #                 continue
        #             uuids.append(uuid)
        #             cc = cc + 1
        #         # print('add new brush {}'.format(len(uuids)))
        #         c.append([each_smonth, each_emonth, each_where, cc])

        for each_where in where:
            cc = 0
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=each_where, rate=0.25)
            for uuid, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=False, flag=True):
                    continue
                uuids.append(uuid)
                cc = cc + 1
            c.append([each_where, cc])
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))
        for i in c:
            print(i)
        print(len(uuids))
        return True

    def brush(self,smonth,emonth, logId=-1):
        remove = False
        # 出一个百分比区间的题
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)

        # sql = 'SELECT distinct(source) FROM {}_parts '.format(self.get_entity_tbl())
        sql = 'SELECT distinct(source) FROM {} '.format(atbl)
        adb = self.cleaner.get_db(aname)
        # source = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        source = [v[0] for v in adb.query_all(sql)]
        source_and_type = []
        for i in source:
            if i == 1:
                source_and_type.append([1,' and (shop_type > 20 or shop_type < 10 )'])
                source_and_type.append([1, ' and (shop_type < 20 and shop_type > 10 )'])
            else:
                source_and_type.append([i, ''])
        uuids = []
        # count_by_source = dict.fromkeys(source, 0)
        for each_source,each_type in source_and_type:
            uuids25 = {}
            uuids50 = {}
            c = 0
            # if each_source in ['tmall','jd']:
            #     rate1=0.45
            #     rate2 = 0.50
            # else:
            #     rate1 = 0.45
            #     rate2 = 0.50
            rate1 = 0.00
            rate2 = 0.20
            # where = '''
            # uuid in (select uuid from {ctbl} where sp1='轮胎' and pkey>='{smonth}' and pkey<'{emonth}') and source = {each_source} {each_type}
            # '''.format(ctbl=ctbl, each_source=each_source, each_type=each_type, smonth=smonth, emonth=emonth)
            where = '''
            source = {each_source} {each_type}
            '''.format(ctbl=ctbl, each_source=each_source, each_type=each_type)
            ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, rate=rate1, where=where)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=True, add=False, remove=remove):
                    continue
                # uuids25.append([uuid,tb_item_id,p1])
                uuids25[(tb_item_id,p1)] = uuid2
            ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, rate=rate2, where=where)
            for uuid2, source,tb_item_id,p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=True, add=False, remove=remove):
                    continue
                # uuids50.appendba't([uuid,tb_item_id,p1])
                uuids50[(tb_item_id, p1)] = uuid2
            # print(uuids25.keys())
            # print(uuids50.keys())

            for i in uuids50:
                if i not in uuids25:
                    uuids.append(uuids50[i])
                    c = c+1
            print(len(uuids50),len(uuids25),len(uuids50)-len(uuids25), c)
        #     count_by_source[each_source] = len(uuids50)-len(uuids25)
        # for i in count_by_source:
        #     print('{} : {}'.format(i,count_by_source[i]))

        ##针对满足区间段的uuid，只取sp1='轮胎'的，即uuids_update部分
        luntai_uuid_sql = '''
        select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='轮胎'
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        luntai_uuids = [v[0] for v in db.query_all(luntai_uuid_sql)]

        uuids_update = []
        uuids_update_not = []
        for each_uuid in uuids:
            if each_uuid in luntai_uuids:
                uuids_update.append(each_uuid)
            else:
                uuids_update_not.append(each_uuid)


        print(len(uuids),len(uuids_update))
        # print(uuids_update_not)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids_update, clean_flag, 1)

        return True



    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            vv['sp5'] = '空'
            if vv['sp1'] == '轮胎' and vv['sp6'] != '' and vv['sp7'] != '' and vv['sp8'] != '' and vv['sp6'] != '空' and vv['sp7'] != '空' and vv['sp8'] != '空':
                vv['sp5'] = '{}/{}R{}'.format(vv['sp6'],vv['sp7'],vv['sp8'])
            if vv['sp1'] == '轮胎' and vv['sp7'] in ('','空') and vv['sp6'] != '' and vv['sp8'] != '' and vv['sp7'] == '空' and vv['sp6'] != '空' and vv['sp8'] != '空':
                vv['sp5'] = '{}R{}'.format(vv['sp6'],vv['sp8'])


    def hotfix_new(self, tbl, dba, prefix):
        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        sql = '''
            CREATE TABLE {t}_join ENGINE = Join(ANY, LEFT, `item_id`) AS
            SELECT item_id, argMax(`sp轮胎类型`, sales) v FROM {t} WHERE `source` = 11 AND LENGTH(`trade_props.name`) > 0 AND item_id IN (
                SELECT item_id FROM {t} WHERE LENGTH(`trade_props.name`) = 0 AND `source` = 11 AND `sp轮胎类型` = 'ignore'
            ) GROUP BY item_id
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp轮胎类型` = ifNull(joinGet('{t}_join', 'v', `item_id`), '')
            WHERE LENGTH(`trade_props.name`) = 0 AND `source` = 11 AND `sp轮胎类型` = 'ignore'
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))



    #     sql = '''
    #         ALTER TABLE {} UPDATE c_sales = toInt64(sales/num*toInt32(num*{r})), c_num = toInt32(num*{r})
    #         WHERE alias_all_bid = 55043
    #     '''.format(tbl, r=params or 1)
    #     dba.execute(sql)

    #     while not self.cleaner.check_mutations_end(dba, tbl):
    #         time.sleep(5)


    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        sql = 'SELECT min(pkey), max(pkey), sum(c_sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        dba, etbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT sum(sales*sign), sum(sign) FROM {} WHERE pkey >= \'{}\' AND pkey <= \'{}\' AND sales > 0 AND num > 0'.format(
            etbl, smonth, emonth
        )
        ret = dba.query_all(sql)
        salesb, countb = ret[0][0], ret[0][1]

        if counta != countb:
            raise Exception("output failed salesa:{} salesb:{} counta:{} countb:{}".format(salesa, salesb, counta, countb))

        if salesa != salesb:
            self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)
    
    def finish_new(self, tbl, dba, prefix):
        sql = '''
            alter table {t}
            update `spPI Standardized Pattern_倍耐力2024` = `sp花纹`, `spPI Standardized Seasonality_倍耐力2024` = upperUTF8(`spSeasonality`)
            where `spPI Standardized Pattern_倍耐力2024` = '' and `spPI Standardized Seasonality_倍耐力2024` = ''
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
