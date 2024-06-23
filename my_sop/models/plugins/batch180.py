import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0705(self,smonth,emonth, logId=-1):
        sp5s = ['ETA',
                'CPI',
                'Hydrating Pre-serum',
                'Suncare Reflex',
                'Cleanance ComedoMED',
                'Cicalfate',
                'LRP喷雾',
                'Saint-Gervais Mont Blanc（修护大白喷）',
                'Winona（马齿苋、青刺果喷雾）',
                'Rellet（玻尿酸喷雾）',
                'Biorrier珀芙研（舒缓保湿喷雾）',
                'URIAGE依泉（舒缓保湿喷雾）',
                'Eucerin（玻尿酸保湿喷雾）',
                'Winona（柔润保湿霜）',
                '理肤泉安心霜',
                'First Aid Beauty（Ultra Repair Cream）',
                'CeraVe（补水保湿面霜）',
                'Eucerin（舒安修护面霜）',
                '薇姿温泉矿物修护微精华水',
                'SkinCeuticals（B5保湿精华）',
                'LRP（玻尿酸B5小蓝瓶）',
                'PROYA珀莱雅（修护精华保湿肌底液）',
                '雅诗兰黛肌初赋活原生液',
                '资生堂红色蜜露精华化妆液',
                '理肤泉大哥大',
                'ANESSA（小金瓶防晒霜）',
                'WINONA（清透防晒乳）',
                'ISDIN（清爽防晒霜）',
                '曼秀雷敦（新碧双重防晒霜）',
                'Mistine（小黄帽防晒）',
                'LRP（Effaclar serum, Duo+, K+）',
                'Winona（清痘修复精华液）',
                'DR. WU（Daily Renewel Serum）',
                'B5 Cicaplast baume',
                '玉泽（皮肤屏障修护保湿霜）',
                'Winona（舒敏保湿特护霜）',
                'Medature（修润霜4号）',
                'SkinCeuticals（皮脂膜修护面霜）',
                'HFP玻尿酸原液']
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}
        for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
            for each_sp5 in sp5s:
                where = '{} and uuid2 in (select uuid2 from {} where sp5 = \'{}\')'.format(each_source, ctbl, each_sp5)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.95, where=where)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                        continue
                    uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush_0826(self,smonth,emonth, logId=-1):
        alias_all_bids = '244645,218569,218469,219391,218970,244970,3755964,218521,246337,3755929,1421233,52285,245136,219045,2149687,245844,218550,3237462,883874,130781,390360,76644,245089,106593,244933,51940,218961,219031,245583,245237,3756309,244713,5748635'
        sp1s = ['Skincare Others',
                'Suncreen']
        cname, ctbl = self.get_c_tbl()
        uuids = []
        sales_by_uuid = {}
        for ssmonth, eemonth in [['2019-07-01', '2020-07-01'], ['2020-07-01', '2021-07-01']]:
            for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
                for each_sp1 in sp1s:
                    where = '{} and uuid2 in (select uuid2 from {} where sp1 = \'{}\') and alias_all_bid in ({})'.format(each_source, ctbl, each_sp1, alias_all_bids)
                    if each_sp1 == 'Eyecare':
                        ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=20, where=where)
                    else:
                        ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, limit=30, where=where)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                            continue
                        uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)
        print('add', len(uuids))
        return True

    def brush_0723(self,smonth,emonth, logId=-1):
        item_ids = [43249505908,529338095872,43475557245,58261504869989,505836,567015266775,597585118157,42978367742,611233066987,564464151047,522198475354,578154324998,5205783326891322,567920678838,593056910209,522216409648,42997766509,523082958415,525221086795,522217114977,43540382225,637829385797,564775716641,598261373656,607725457875,618935158929,562476911132,542581458162,553345567288,569436143462,520654260305,578178885920, 582344228159,582884109803,6842948,614933005649,100003021759]
        where = 'item_id in ({})'.format(','.join(['\'{}\''.format(v) for v in item_ids]))
        uuids = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid1)
        return True

    def brush(self, smonth, emonth, logId=-1):



        sales_by_uuid = {}
        cname, ctbl = self.get_all_tbl()


        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):

            ##1
            sp5s = ['ANESSA（小金瓶防晒霜）',	'B5 Cicaplast baume',	'理肤泉B5乳 (B5 Emulsion)',	'Cleanance ComedoMED',	'DR. WU（Daily Renewel Serum）',	'ETA',	'Eucerin（玻尿酸保湿喷雾）',	'ISDIN（清爽防晒霜）',	'LRP B5绷带精华',	'LRP（Effaclar serum, Duo+, K+）',	'LRP喷雾',	'Mistine（小黄帽防晒）',	'PROYA珀莱雅（修护精华保湿肌底液）',	'Rellet（玻尿酸喷雾）',	'SkinCeuticals（皮脂膜修护面霜）',	'SkinCeuticals色修精华',	'Suncare Reflex',	'Winona（马齿苋、青刺果喷雾）',	'WINONA（清透防晒乳）',	'Winona（舒敏保湿特护霜）',	'理肤泉大哥大',	'理肤泉每日调理精华',	'欧莱雅黑精华',	'润百颜白纱布次抛精华',	'薇诺娜焕颜精华',	'薇诺娜乳糖酸精华',	'薇诺娜特护精华',	'雅漾专研速修霜',	'伊肤泉莱菲思壳聚糖舒缓补水喷雾',	'优色林舒安霜',	'玉泽（皮肤屏障修护保湿霜）',	'玉泽大分子防晒(清爽倍护防晒乳)',	'玉泽控油修护面霜(皮肤屏障修护清透保湿霜)']
            sources = ['source = 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
            uuids = []
            for each_source in sources:
                for each_sp5 in sp5s:
                    where = '''
                    c_sp5 = \'{each_sp5}\' and  {each_source}
                    '''.format(ctbl=ctbl, each_sp5=each_sp5, each_source=each_source, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, rate=0.95)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)

            ##2
            sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
            ret = self.cleaner.db26.query_all(sql)
            brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
            data = []
            for snum, tb_item_id, p1, id, visible in ret:
                p1 = brush_p1(snum, p1)
                data.append([snum, tb_item_id, p1, id, visible])
            mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

            alias_bid="'51940','52285','76644','106593','130781','218469','218521','218550','218569','218961','218970','219031','219045','219391','244645','244713','244933','244970','245089','245136','245237','245583','245844','246337','390360','883874','1421233','2149687','3237462','3755929','3755964','3756309','514471','2548829','3077191'"
            sources = ['source = 11', 'source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
            sp1s = ['Eyecare', 'Cleanser', 'Serum', 'Emulsion/Cream', 'Toner', 'Makeup remover', 'Mask', 'Skincare Set', 'Suncreen', 'BodyCare', 'Skincare Others']
            uuids = []
            uuids_update = []
            for each_source in sources:
                for each_sp1 in sp1s:
                    where = '''
                    c_sp1 = \'{each_sp1}\'  and {each_source} and alias_all_bid in ({alias_bid})
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, alias_bid=alias_bid, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=7)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            # uuids_update.append(uuid2)
                            if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                                uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

            print(len(uuids), len(uuids_update))
            if len(uuids_update) > 0:
                sql = 'update {} set visible = {}, created = now() where id in ({}) and cast(created as date)=cast(now() as date)'.format(self.cleaner.get_tbl(), 2, ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()

            self.cleaner.add_brush(uuids, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0805(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        bids_list = '244645,218569,218469,219391,218970,244970,3755964,218521,246337,2972953,1421233,52285,245136,219045,2149687,245844,218550,3237462,883874,130781,390360,76644,245089,106593,244933,51940,218961,219031,245583,245237,3756309,244713,5748635'
        sp1_list = ['Eyecare','Cleanser','Serum','Emulsion/Cream','Toner','Makeup remover','Mask','Skincare Set','Suncreen','BodyCare','Others']
        for ssmonth, eemonth in [['2019-07-01', '2020-07-01'], ['2020-07-01', '2021-07-01']]:
            for each_sp1 in sp1_list:
                for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']:
                    where ='''
                    {} and uuid2 in (select uuid2 from {} where sp1=\'{}\' and alias_all_bid in ({}))
                    '''.format(each_source, ctbl, each_sp1, bids_list)
                    ret, sales_by_uuid1  = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=8)
                    sales_by_uuid.update(sales_by_uuid1 )
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                            continue
                        uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)

        print('------------------------------', len(uuids))


    def brush_0817(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()

        itemid_list1 = "'559772837787', '537267540405'"
        for ssmonth, eemonth in [['2020-10-01', '2021-07-01']]:
            where = '''
            uuid2 in (select uuid2 from {} where  item_id in ({}))
            '''.format(ctbl, itemid_list1)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)

        itemid_list2 = "'632852666703','632852666703','587494423325'"
        for ssmonth, eemonth in [['2021-01-01', '2021-07-01']]:
            where = '''
                    uuid2 in (select uuid2 from {} where  item_id in ({}))
                    '''.format(ctbl, itemid_list2)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)

        itemid_list3 = "'538708149374',	'644034384209',	'643951820309',	'646387766171',	'636245356472',	'643591526117',	'615876308676',	'630553581473',	'624861438168',	'100009040623',	'648260162427',	'592265630469',	'611880845122',	'613582386389',	'641374050707',	'629121208499',	'622981891523',	'594208549807',	'17908625605',	'17908625605',	'647937353612',	'644571939270',	'645318841266',	'644951853666',	'544091476109',	'563020588529',	'644570083990',	'640270957372',	'629466749569',	'646693750062',	'622981891523',	'635393972387',	'558427795001',	'558427795001',	'632852666703',	'622424632368',	'623283019792',	'622414536515',	'639261658671',	'587007070447',	'537140825702',	'66331937058',	'618964018861',	'623080399773',	'612287094647',	'643439867298',	'617378224733',	'630127310382',	'619734179225',	'619734179225',	'638468259865',	'628253669713',	'643439867298',	'646645185676',	'614320067438',	'632342885038',	'622790130721',	'612441658793',	'618809849506',	'643566664481',	'537267540405',	'587169917334',	'581541194821',	'619734179225',	'587169917334',	'630395961445',	'601775389870',	'616136605945',	'637471973105',	'637471973105',	'601775389870',	'584867786473',	'637349744052'"
        for ssmonth, eemonth in [['2021-04-01', '2021-07-01']]:
            where = '''
                    uuid2 in (select uuid2 from {} where  item_id in ({}))
                    '''.format(ctbl, itemid_list3)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)

    def brush_0915(self, smonth, emonth, logId=-1):

        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()

        clean_flag = self.cleaner.last_clean_flag() + 1

        ##1
        sp5s = ['ETA',	'CPI',	'Hydrating Pre-serum',	'Suncare Reflex',	'Cleanance ComedoMED',	'Cicalfate',	'LRP喷雾',	'Saint-Gervais Mont Blanc（修护大白喷）',	'Winona（马齿苋、青刺果喷雾）',	'Rellet（玻尿酸喷雾）',	'Biorrier珀芙研（舒缓保湿喷雾）',	'URIAGE依泉（舒缓保湿喷雾）',	'Eucerin（玻尿酸保湿喷雾）',	'Winona（柔润保湿霜）',	'理肤泉安心霜',	'First Aid Beauty（Ultra Repair Cream）',	'CeraVe（补水保湿面霜）',	'Eucerin（舒安修护面霜）',	'薇姿温泉矿物修护微精华水',	'SkinCeuticals（B5保湿精华）',	'LRP（玻尿酸B5小蓝瓶）',	'PROYA珀莱雅（修护精华保湿肌底液）',	'雅诗兰黛肌初赋活原生液',	'资生堂红色蜜露精华化妆液',	'理肤泉大哥大',	'ANESSA（小金瓶防晒霜）',	'WINONA（清透防晒乳）',	'ISDIN（清爽防晒霜）',	'曼秀雷敦（新碧双重防晒霜）',	'Mistine（小黄帽防晒）',	'LRP（Effaclar serum, Duo+, K+）',	'Winona（清痘修复精华液）',	'DR. WU（Daily Renewel Serum）',	'B5 Cicaplast baume',	'玉泽（皮肤屏障修护保湿霜）',	'Winona（舒敏保湿特护霜）',	'Medature（修润霜4号）',	'SkinCeuticals（皮脂膜修护面霜）',	'HFP玻尿酸原液']
        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        uuids = []
        for each_source in sources:
            for each_sp5 in sp5s:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp5 = \'{each_sp5}\') and {each_source}
                '''.format(ctbl=ctbl, each_sp5=each_sp5, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.95)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                        continue
                    uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)

        ##2
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        alias_bid="'244645','218569','218469','219391','218970','244970','3755964','218521','246337','3755929','1421233','52285','245136','219045','2149687','245844','218550','3237462','883874','130781','390360','76644','245089','106593','244933','51940','218961','219031','245583','245237','3756309','244713','5748635'"
        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sp1s = ['Eyecare', 'Cleanser', 'Serum', 'Emulsion/Cream', 'Toner', 'Makeup remover', 'Mask', 'Skincare Set', 'Suncreen', 'BodyCare', 'Skincare Others']
        uuids = []
        uuids_update = []
        for each_source in sources:
            for each_sp1 in sp1s:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\') and {each_source} and alias_all_bid in ({alias_bid})
                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, alias_bid=alias_bid)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=3)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        # uuids_update.append(uuid2)
                        if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] in (1,):
                            uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        print(len(uuids), len(uuids_update))
        if len(uuids_update) > 0:
            sql = 'update {} set visible = {}, created = now() where id in ({})'.format(self.cleaner.get_tbl(), 2, ','.join([str(v) for v in uuids_update]))
            print(sql)
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()

        self.cleaner.add_brush(uuids, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)


    def brush_modify(self, v, bru_items):
        if v['flag'] != 2 or len(v['split_pids']) == 1:
            return

        same = []
        for vv in v['split_pids']:
            same.append(vv['sp15'])

        if len(list(set(same))) != 1:
            return

        for vv in v['split_pids']:
            vv['sp19'] = same[0]+'套包'


    def hotfix_new(self, tbl, dba, prefix):
        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp功效`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp功效-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp功效`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp功效-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `spFunciton`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'spFunciton-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `spFunciton`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`spFunciton-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))