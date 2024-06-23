import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import ujson
import models.entity_manager as entity_manager

class main(Batch.main):
    def filter_brush_props(self):
        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all, item):
            r = self.cleaner.json_decode(trade_prop_all) or {}
            p = self.cleaner.json_decode(item['prop_all']) or {}
            if isinstance(r, dict):
                p1 = r
            else:
                p1 = trade_prop_all.split(',')
                p2 = trade_prop_all.split('|')
                p1 = p1 if len(p1) > len(p2) else p2
                p1 = {i:str(v) for i,v in enumerate(p1)}

            if source == 2:
                p1 = []
            else:
                p1 = [p1[k] for k in p1]

            if item['split_pids'][0]['sp1'] != '摩托车机油':
                b = {
                    1: ['型号'],
                    2: ['商品名称', '包装清单'],
                    3: ['型号'],
                    6: ['型号', '包装清单'],
                    10: ['oil_series'],
                }
                p1 += [p[k] for k in p if k in b[source]]
                if source == 3:
                    p1 += item['name']
            p1 = [v.replace(' ','') for v in p1 if v.replace(' ','')!='']
            p1 = list(set(p1))
            p1.sort()
            return p1

        db26 = self.cleaner.get_db('26_apollo')
        return format, '''
            arraySort( arrayFilter(y->trim(y)<>'', arrayMap(y->replace(y,' ',''), arrayDistinct(
                arrayConcat(
                    IF(source=2,[],trade_props.value),
                    multiIf(
                        item_id IN (SELECT tb_item_id FROM mysql('192.168.30.93','product_lib','entity_{}_item','{}','{}') WHERE spid1 = '摩托车机油' AND flag > 0), [''],
                        source = 1, arrayFilter((k, x) -> x='型号', `props.value`, `props.name`),
                        source = 2, arrayFilter((k, x) -> x IN ['商品名称','包装清单'], `props.value`, `props.name`),
                        source = 10, arrayFilter((k, x) -> x='oil_series', `props.value`, `props.name`),
                        source = 3, arrayConcat(arrayFilter((k, x) -> x='型号', `props.value`, `props.name`), [name]),
                        source = 6, arrayFilter((k, x) -> x IN ['型号','包装清单'], `props.value`, `props.name`),
                        ['']
                    )
                )
            ) ) ) )
        '''.format(self.cleaner.eid, db26.user, db26.passwd)

    sc = 0
    def brush(self, smonth, emonth, logId=-1):
        # ent = entity_manager.get_entity(self.cleaner.bid)
        self.mpp = {}
        bru = entity_manager.get_brush(self.cleaner.bid)
        bru.process_brush(smonth, emonth, force=True)

        sales_by_uuid = {}
        tmp = []
        uuid1s = []
        adb, atbl = self.get_a_tbl()
        bdb, btbl = self.get_b_tbl()
        cdb, ctbl = self.get_c_tbl()

        uks = []
        uuid4s = []
        uuids_new = []
        where = '''
        uuid2 in (select uuid2 from {} where sp1 = '机油' and pkey >= '{smonth}' and pkey < '{emonth}')
        and uuid2 in (select uuid2 from {} where similarity >= 1)
        '''.format(ctbl, btbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        for row in ret:
            uuids_new.append(row[0])
            uks.append([row[1], row[2], row[3]])
        sp_by_uuid_new, sp_by_uk = self.skip_helper('sp11', uuids_new, smonth, uks)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush_via_sp(source, tb_item_id, p1, uuid2, sp_by_uuid_new, sp_by_uk):
                continue
            uuid4s.append(uuid2)
        # print('uuid4s: ', len(uuid4))
        # exit()
        ####

        where = '''
        uuid2 in (select uuid2 from {} where sp1 = '机油' AND sp3 != '非TOP品牌' and pkey >= '{smonth}' and pkey < '{emonth}')
        and uuid2 not in (select uuid2 from {} where similarity >= 1)
        '''.format(ctbl, btbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=20000, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuid1s.append(uuid2)
        ####

        uuid2s = []
        where = '''
        uuid2 in (select uuid2 from {} where sp1 = '机油' AND sp3 = '非TOP品牌' and pkey >= '{smonth}' and pkey < '{emonth}')
        and uuid2 not in (select uuid2 from {} where similarity >= 1)
        '''.format(ctbl, btbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=20000, where='{}'.format(where))
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuid2s.append(uuid2)

        print(len(uuid1s), len(uuid2s))

        uuid3s = []
        for each_source in ['source = 1 and (shop_type < 20 and shop_type > 10 )', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
            where = '''
            uuid2 in (select uuid2 from {} where sp1 = '摩托车机油' and pkey >= '{smonth}' and pkey < '{emonth}') and {source}
            '''.format(ctbl, btbl, smonth=smonth, emonth=emonth, source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuid3s.append(uuid2)
            where = '''
            uuid2 in (select uuid2 from {} where sp1 = '摩托车机油' and sp3 in ('美孚','MOTUL 摩特') and pkey >= '{smonth}' and pkey < '{emonth}') and {source}
            '''.format(ctbl, btbl, smonth=smonth, emonth=emonth, source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.95, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuid3s.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuid1s, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuid2s, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuid4s, clean_flag, 3, sales_by_uuid=sales_by_uuid)

        #
        self.cleaner.add_brush(uuid3s, clean_flag, visible_check=100, sales_by_uuid=sales_by_uuid)
        self.cleaner.set_default_val()
        sql = 'update {} set visible_check = 101 where spid1 = \'摩托车机油\' and spid3 = \'非TOP品牌\' and clean_flag = {}'.format('product_lib.entity_{}_item'.format(self.cleaner.eid), clean_flag)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()
        #

        print('add new brush a:{} b:{} c:{}'.format(len(uuid1s), len(uuid2s), len(uuid3s)))
        for i in tmp:
            print(i)
        return True

    def brush_0917(self,smonth,emonth,logId=-1):
        # items = [['595271587495', '2020-07-01', '2021-08-01'],
        #         ['599129246659', '2020-06-01','2021-07-01'],
        #         ['601365440325', '2020-07-01', '2021-8-01']]
        # for row in items:
        #     each_item = row[0]
        #     ssmonth = row[1]
        #     eemonth = row[2]
        #     sales_by_uuid = {}
        #     uuids = []
        #     where = '''
        #     item_id in '{}'
        #     '''.format(each_item)
        #     ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where)
        #     sales_by_uuid.update(sales_by_uuid1)
        #     for uuid2, source, tb_item_id, p1, cid, bid in ret:
        #         uuids.append(uuid2)
        #     clean_flag = self.cleaner.last_clean_flag() + 1
        #     self.cleaner.add_brush(uuids, clean_flag, visible_check=1, sales_by_uuid=sales_by_uuid)

        uuids = ['bb86ba73-67cd-4651-8c7d-6809e8bd58cc','8b3e75f4-add5-452b-8f40-ba4c97b291ed','888c363e-32a0-4f6e-909a-5ed9481eb609','03b7e6ab-f410-4aea-b9a9-136333d8e3e4',
        '4d85358d-9793-4197-88ce-309ef8f5a2c4','9f3e5903-c935-403d-b853-fd632cb67cf7','b725a879-fe0d-4dd7-b476-e56ddc963c49','17932cc6-a52b-4486-b030-7069a9e8db12',
        '418166cd-dda4-4fc1-a4f3-b81ae8de9202','89aee6b2-baab-400d-8cb0-89bd2080412e','b7ad4e28-8627-494f-9024-562c68c5dbf1','532875aa-5677-41f9-9f02-e4a8e5e5d6fd']
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1)
        self.cleaner.set_default_val()
        return True

    def brush_0929(self, smonth, emonth, logId=-1):
        ent = entity_manager.get_entity(self.cleaner.bid)
        self.mpp = {}
        # bru = entity_manager.get_brush(self.cleaner.bid)
        # bru.process_brush(smonth, emonth, force=True)

        sales_by_uuid = {}
        tmp = []
        uuid1s = []
        adb, atbl = self.get_a_tbl()
        bdb, btbl = self.get_b_tbl()
        cdb, ctbl = self.get_c_tbl()

        where = '''
        uuid2 in (select uuid2 from {} where sp1 = '机油' and sp20 = '是' and sp11 in ('SJ','SL','SM','SN','SN Plus','SN/SM','SA','SB','SC','SD','SE','SF','SG','SH') and pkey >= '{smonth}' and pkey < '{emonth}')
        or uuid2 in (select uuid2 from {} where similarity >= 1 and sp1 = '机油' and sp20 = '是' and sp11 in ('SJ','SL','SM','SN','SN Plus','SN/SM','SA','SB','SC','SD','SE','SF','SG','SH'))
        '''.format(ctbl, btbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, limit=20000, where=where)
        sales_by_uuid.update(sales_by_uuid1)
        xx = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuid1s.append(uuid2)
            xx.append('({},{},{})'.format(source, tb_item_id, p1))
        ####
        print('add', len(uuid1s))
        tbl = 'product_lib.entity_90518_item'
        sql = 'update {} set visible = 80 where id in ' \
              '(select id from (select id from {} where (snum,tb_item_id,real_p1) in ({})) a)'.format(tbl, tbl, ','.join(xx))
        db26 = self.cleaner.get_db(self.cleaner.db26name)
        tmp = db26.query_all(sql)
        for i in tmp:
            print((i))
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuid1s, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.set_default_val()
        return True

    def skip_helper(self, which_sp, uuid_list_new, smonth, uks):
        # 时段: 2021-01-01到smonth
        # which_sp = 'sp11'
        dbch = self.cleaner.dbch
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        sp_by_uuid_new = {}
        sp_by_uk = {}
        # 查询当前新题uuid在C表中的机洗结果
        if len(uuid_list_new) > 0:
            sql = '''
            select {} ,uuid2 from {} where uuid2 in ({})
            '''.format(which_sp, ctbl, ','.join(['\'{}\''.format(v) for v in uuid_list_new]))
            sp_by_uuid_new = {v[1]: v[0] for v in dbch.query_all(sql)}

            # a表取出的新题uuid，查询在C表中的历史机洗结果
            # 取时段内最新的一条结果
            str_uks = []
            for i in uks:
                str_uks.append('({},\'{}\',{})'.format(i[0], i[1], i[2]))
            str_uks = ','.join(str_uks)
            sql = '''
            select * from (select a.source, a.item_id,a.p1,a.pkey,c.{} from
            (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
            select uuid2, source, item_id, p1,pkey from {atbl} where (source,item_id,p1) in ({})) a join {ctbl} c on c.uuid2 = a.uuid2
            where a.pkey > '2021-01-01' and a.pkey < '{smonth}')
            order by pkey desc limit 1 by source,item_id,p1
            '''.format(which_sp, str_uks, atbl=atbl, ctbl=ctbl, smonth=smonth)
            tmp = dbch.query_all(sql)
            for v in tmp:
                sp_by_uk['{}-{}-{}'.format(v[0], v[1], v[2])] = v[4]
        return sp_by_uuid_new, sp_by_uk

    def skip_brush_via_sp(self, source, tb_item_id, p1, uuid2, sp_by_uuid_new, sp_by_uk):
        if '{}-{}-{}'.format(source, tb_item_id, p1) in self.mpp:
            return True
        sp_old = sp_by_uk.get('{}-{}-{}'.format(source, tb_item_id, p1), '')
        sp_new = sp_by_uuid_new.get(str(uuid2), '')
        print('old: ', sp_old, 'new: ', sp_new)
        if sp_old == sp_new:
            return True
        else:
            self.mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            self.sc = self.sc + 1
            return False

    def finish(self, tbl, dba, prefix):

        sql = '''
            ALTER TABLE {} UPDATE `sp是否进口` = '进口'
            WHERE item_id in ('21877976922') and pkey >='2020-11-01' and pkey < '2021-12-01'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spTOP品牌MODEL` = '非TOP品牌'
            WHERE `spTOP品牌` = '非TOP品牌'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    def join_c(self, uuids):
        sp11_by_uuid = {}
        cdb, ctbl = self.get_c_tbl()
        uuids = list(set(uuids))
        if '' in uuids:
            uuids.remove('')
        sql = '''
        select uuid2, sp11 from {} where uuid2 in ({})
        '''.format(ctbl, ','.join(['\'{}\''.format(str(v)) for v in uuids]))
        cdb = self.cleaner.get_db(cdb)
        for v in cdb.query_all(sql):
            sp11_by_uuid[v[0]] = v[1]
        if 'sp11_by_uuid' in self.cache:
            self.cache['sp11_by_uuid'].update(sp11_by_uuid)
        else:
            self.cache['sp11_by_uuid'] = sp11_by_uuid

    def brush_similarity(self, v1, v2, where, item_ids, fix=False):
        if v1['source'] != 'jd' and str(v1['real_p1']) != str(v2['p1']):
            return 0

        if 'sp11_by_uuid' not in self.cache:
            self.cache['sp11_cache'] = {}

        source = v1['source']
        try:
            prop_alla = ujson.loads(v1['prop_all'])
        except Exception as e:
            prop_alla = eval(v1['prop_all'])
        pk, pv = eval(v2['props.name']), eval(v2['props.value'])
        prop_allb = {k:pv[i] for i,k in enumerate(pk)}

        cmp = [
            ['tb', '型号'],
            ['tmall', '型号'],
            ['jd', '商品名称', '包装清单'],
            ['tuhu', 'oil_series'],
            ['gome', '型号', 'name'],
            ['suning', '型号', '包装清单']
        ]
        clear = False
        # print(v1)
        # print(v2)
        # print(self.cache['sp11_by_uuid'].get(v2['uuid2'], ''))
        # exit()
        if v1['split_pids'][0]['sp1'] != '摩托车机油':
            for v in cmp:
                if source != v[0]:
                    continue
                for name in v[1:]:
                    if name in ['name'] and v1[name].replace(' ', '') == v2[name].replace(' ', ''):
                        continue
                    if name not in prop_alla or name not in prop_allb or prop_alla[name].replace(' ', '') == prop_allb[name].replace(' ', ''):
                        continue
                    clear = True

        if clear == True:
            return 0
        if str(v1['uuid2']) == str(v2['uuid2']):
            return 3
        if str(v1['pkey']) == str(v2['pkey']):
            return 2
        if v2['date'] >= v1['month']:
            return 1.1
        return 1

    def process_exx(self, tbl, prefix, logId=0):
        dba = self.cleaner.get_db('chsop')

        sql = '''
            ALTER TABLE {}
            UPDATE `spTOP品牌MODEL` = transform(`spTOP品牌MODEL`, [
                        '壳牌极净超凡喜力X',
                        '壳牌超凡喜力X',
                        '壳牌先锋超凡喜力亚系(2019新款）', '壳牌先锋超凡喜力亚系（2019新款）', '壳牌先锋超凡喜力欧系(2019新款）', '壳牌先锋超凡喜力欧系（2019新款）', '壳牌先锋超凡喜力亚系-零碳环保', '壳牌先锋超凡喜力欧系-零碳环保',
                        '壳牌先锋超凡喜力亚系-千里江山版', '壳牌先锋超凡喜力欧系-千里江山版', '壳牌先锋超凡喜力欧系千里江山版', '壳牌先锋超凡喜力亚系千里江山版',
                        '壳牌恒护超凡喜力亚系','壳牌恒护超凡喜力欧系','壳牌恒护超凡喜力亚系-零碳环保','壳牌恒护超凡喜力欧系-零碳环保',
                        '壳牌喜力HX5?plus', '壳牌HX5？PLUS',
                        '壳牌超凡喜力-高效动力版', '壳牌超凡喜力-新升级高效动力版'
                    ], [
                        '壳牌极净超凡喜力',
                        '壳牌超凡喜力',
                        '壳牌先锋超凡喜力', '壳牌先锋超凡喜力', '壳牌先锋超凡喜力', '壳牌先锋超凡喜力', '壳牌先锋超凡喜力', '壳牌先锋超凡喜力',
                        '壳牌先锋超凡喜力-千里江山版', '壳牌先锋超凡喜力-千里江山版', '壳牌先锋超凡喜力千里江山版', '壳牌先锋超凡喜力千里江山版',
                        '壳牌恒护超凡喜力','壳牌恒护超凡喜力','壳牌恒护超凡喜力','壳牌恒护超凡喜力',
                        '壳牌HX5 PLUS', '壳牌HX5 PLUS',
                        '壳牌超凡喜力-高效动力', '壳牌超凡喜力-高效动力'
                    ], `spTOP品牌MODEL`)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            SELECT DISTINCT sid FROM {t} WHERE sid NOT IN (
                SELECT DISTINCT sid FROM {t} WHERE toYYYYMM(date) = 202111 AND `source` = 1 AND (shop_type < 20 and shop_type > 10 )
            ) AND toYYYYMM(date) = 202112 AND `source` = 1 AND (shop_type < 20 and shop_type > 10 )
        '''.format(t=tbl)
        ret = dba.query_all(sql)
        sid = ','.join([str(v) for v, in ret])

        if sid != '':
            sql = '''
                ALTER TABLE {} DELETE WHERE sid IN ({}) AND toYYYYMM(date) = 202112 AND `source` = 1 AND (shop_type < 20 and shop_type > 10 )
            '''.format(tbl, sid)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)