import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):
    def pre_brush_modify(self, v, products, prefix):
        # Lindt 答题不拆套包
        if prefix.find('Lindt') > 0 and v['flag'] == 2 and v['split'] > 0:
            v['split'] = 0
        elif v['flag'] == 2 and v['split'] > 0:
            props = self.brush_props('1')
            for p in props:
                v['spid{}'.format(p)] = ''
        return v


    # 答题结果特殊修改
    def brush_modify(self, v, bru_items):
        poslist = self.cleaner.get_poslist()

        for vv in v['split_pids']:
            # 优先用属性答题结果 再用sku答题
            for pid in poslist:
                v['spid{}'.format(pid)] = v['spid{}'.format(pid)] or ''
                if v['spid{}'.format(pid)] != '':
                    vv['sp{}'.format(pid)] = v['spid{}'.format(pid)]

            if vv['sp14'].strip() != '':
                sp14 = round(float(vv['sp14'].strip().replace('g', '')), 2)
            else:
                sp14 = 0

            if vv['sp16'].strip() == '':
                sp16 = int(vv['number']) * sp14
                sp16 = round(sp16, 2)
            else:
                # FIXME 答题乱填 try为了测试用 正式用要去掉
                sp16 = eval(vv['sp16'].strip())
                sp16 = round(sp16, 2)

            if sp14 > 0:
                vv['sp14'] = str(sp14 if sp14 % 1 > 0 else int(sp14))# + 'g'
            if sp16 > 0:
                vv['sp16'] = str(sp16 if sp16 % 1 > 0 else int(sp16))# + 'g'

            if vv['pid'] in (1,2,3,4,5):
                vv['sp13'] = '1'
                vv['sp14'] = vv['sp16']

            if vv['sp29'].find('KCR T1') == 0 and vv['number'] in (9,12,16):
                vv['sp29'] = vv['sp29'].replace('KCR T1', 'KCR T1*{}'.format(vv['number'])).replace(vv['sp14'], vv['sp16']).replace('KCR T1*9', 'KCR T9')
                vv['sp13'] = '1'
                vv['sp14'] = vv['sp16']
            elif vv['sp29'].find('KCR T3') == 0 and vv['number'] in (3,4,10):
                vv['sp29'] = vv['sp29'].replace('KCR T3', 'KCR T3*{}'.format(vv['number'])).replace(vv['sp14'], vv['sp16']).replace('KCR T3*3', 'KCR T9')
                vv['sp13'] = '1'
                vv['sp14'] = vv['sp16']
            elif vv['sp29'].find('散装') > -1:
                vv['sp29'] = vv['sp29'].replace(vv['sp14'], vv['sp16'])
                vv['sp13'] = '1'
                vv['sp14'] = vv['sp16']


    def calc_splitrate(self, item, data):
        custom_price = {
            16:820,
            17:2330,
            50:2130,
            51:790,
            52:1460,
            55:1540,
            57:1830,
            58:320,
            72:900,
            81:390,
            83:1550,
            1267:2520,
            1282:3860,
            20051:2200,
            20052:480,
        }
        return self.cleaner.calc_splitrate(item, data, custom_price)


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sp7 = {
            '玛氏-Mars':'Mars',
            '费列罗-Ferrero':'Ferrero',
            '好时-Corp. Hershey':'Corp. Hershey',
            '歌帝梵-Godiva':'Godiva',
            '瑞士莲-Lindt':'Lindt',
            '明治-Meiji':'Meiji',
            '亿滋-MONDELEZ':'MONDELEZ',
            '混合厂商-Mix':'Mix'
        }
        sp7a = ['\'{}\''.format(k.replace('\'', '\\\'')) for k in sp7]
        sp7b = ['\'{}\''.format(sp7[k].replace('\'', '\\\'')) for k in sp7]

        sp8 = {
            '脆香米-CuiXiangMi':'CuiXiangMi',
            '德芙-Dove/品鉴可可-Dark Collection':'Dove',
            '德芙-Dove/德芙心语-Dove Love Heart':'Dove',
            '德芙-Dove/奇思妙感-Dove Sensation':'Dove',
            '德芙-Dove/德芙精心之选-Dove Silk Collection':'Dove',
            '德芙-Dove/德芙星彩-Dove Xin Cai':'Dove',
            '德芙-Dove/德芙酸奶-Dove Yogurt':'Dove',
            '德芙-Dove/德芙榛藏-Dove Zhen Cang':'Dove',
            '德芙-Dove/尊慕-Jewels':'Dove',
            '德芙-Dove/玫瑰物语-Miss Rose':'Dove',
            '德芙-Dove/巧丝-qiao si':'Dove',
            '德芙-Dove/心声-xin sheng':'Dove',
            '德芙-Dove/心随-Xin Sui':'Dove',
            '德芙-Dove/埃丝汀-Esteem':'Dove',
            '德芙-Dove/小巧粒':'Dove',
            '德芙-Dove':'Dove',
            'M & M\'S-M & M\'s':'M & M\'s',
            '麦提莎-Maltesers':'Maltesers',
            '士力架-Snickers/士力架燕麦花生夹心巧克力-Snickers Oat Peanut Chocolate':'Snickers',
            '士力架-Snickers/士力架辣花生夹心巧克力-Snickers Spicy Peanut Chocolate':'Snickers',
            '士力架-Snickers':'Snickers',
            '特趣-Twix':'Twix',
            '玛氏-Mars':'Others',
            '费列罗巧克力精选组合-Ferrero Golden Gallery':'Ferrero Golden Gallery',
            '费列罗臻品-Ferrero Collection':'Ferrero Collection',
            '拉斐尔-Raffaello':'Raffaello',
            '费列罗榛果威化-Rocher':'Rocher',
            '健达排块状-Kinder Chocolate/健达康脆麦-Kinder Chocolate Cereal':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/健达巧克力-Kinder Chocolate':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/健达夹心牛奶巧克力倍多-Maxi':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/迷你-Mini':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate':'Kinder Chocolate',
            '健达缤纷乐-Kinder Bueno/健达缤纷乐普通-Kinder Bueno Regular':'Kinder Bueno',
            '健达缤纷乐-Kinder Bueno/健达缤纷乐白巧克力-Kinder Bueno White':'Kinder Bueno',
            '健达缤纷乐-Kinder Bueno':'Kinder Bueno',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋-Kinder Joy':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋男孩版-KJ Boy':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋女孩版-KJ Girl':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋Max-Kinder Joy Max':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋混合-KJ uni-sex':'Kinder Joy',
            '健达混合-Kinder Mix':'Kinder Mix',
            '巧可蹦脆乐-KSBC':'KSBC',
            '蒙雪莉-Moncheri':'Moncheri',
            '能多益-Nutella':'Nutella',
            '费列罗-Ferrero':'Others',
            '贝客诗-Brookside':'Brookside',
            '锐滋-Reese\'s':'Reese\'s',
            '好时-Hershey\'s/巧珍珠-Drops':'Hershey\'s',
            '好时-Hershey\'s/巧珍珠滑盖装-Globe':'Hershey\'s',
            '好时-Hershey\'s/好时排块-Hershey\'s Bar':'Hershey\'s',
            '好时-Hershey\'s/麦丽素-My Likes':'Hershey\'s',
            '好时-Hershey\'s/巧金砖-Nuggets':'Hershey\'s',
            '好时-Hershey\'s/巧乐园-Snack Mix':'Hershey\'s',
            '好时-Hershey\'s/特浓黑巧克力-Special Dark':'Hershey\'s',
            '好时-Hershey\'s':'Hershey\'s',
            '好时之吻-Kisses/好时臻吻-Kisses Deluxe':'Kisses',
            '好时之吻-Kisses/好时之吻婚庆装-Kisses Wedding':'Kisses',
            '好时之吻-Kisses':'Kisses',
            '好时-Corp. Hershey':'Others',
            '歌帝梵-Godiva':'Godiva',
            '特醇排装-EXCELLENCE':'EXCELLENCE',
            '软心-LINDOR':'LINDOR',
            '精选缤纷-NAPOLITAINS ASSORTIS':'NAPOLITAINS ASSORTIS',
            '瑞士经典排装-SWISS CLASSIC':'SWISS CLASSIC',
            '瑞士经典薄片-SWISS THINS':'SWISS THINS',
            '瑞士莲-Lindt':'Others',
            '明治-Meiji/巴旦木-Almond':'Meiji',
            '明治-Meiji/橡皮糖-GUM':'Meiji',
            '明治-Meiji/Marble 幻彩-HUANCAI':'Meiji',
            '明治-Meiji/钢琴巧克力-Imperiality':'Meiji',
            '明治-Meiji/澳洲坚果-Macadamia':'Meiji',
            '明治-Meiji/雪吻-Meiji Melty Kiss':'Meiji',
            '明治-Meiji':'Meiji',
            '吉百利-CDM':'CDM',
            '吉百利怡口莲-Cadbury Eclairs/金装-Eclairs Golden':'Cadbury Eclairs',
            '吉百利怡口莲-Cadbury Eclairs':'Cadbury Eclairs',
            '克特多金象-Cote d\'Or':'Cote d\'Or',
            '妙卡-Milka/融情-Milka Base':'Milka',
            '妙卡-Milka/妙卡旋妙杯-Milka Magic Cup':'Milka',
            '妙卡-Milka/妙卡奥利奥-Milka Oreo':'Milka',
            '妙卡-Milka':'Milka',
            '三角-Tobleron':'Tobleron',
            '亿滋-MONDELEZ':'Others',
            '混合厂商-Mix':'Others',
            'NA':'Others',
            'na':'Others'
        }
        sp8a = ['\'{}\''.format(k.replace('\'', '\\\'')) for k in sp8]
        sp8b = ['\'{}\''.format(sp8[k].replace('\'', '\\\'')) for k in sp8]

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='出数用厂商', transform(v, [{}], [{}]),
                k='出数用品牌', transform(v, [{}], [{}]),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl, ','.join(sp7a), ','.join(sp7b), ','.join(sp8a), ','.join(sp8b))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='出数用厂商', 'Other Manu',
                k='出数用品牌', 'Others',
            v), c_props.name, c_props.value)
            WHERE c_sku_id = 5 AND `c_props.value`[indexOf(`c_props.name`, '出数用厂商')] IN ['Mars','Ferrero','Lindt','Godiva','Corp. Hershey','Meiji','MONDELEZ']
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='出数用厂商', '每日黑巧',
                k='出数用品牌', '每日黑巧',
            v), c_props.name, c_props.value)
            WHERE sid = 181928791 AND name LIKE '%每日黑巧%'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='出数用厂商', '欧贝拉',
                k='出数用品牌', '欧贝拉',
            v), c_props.name, c_props.value)
            WHERE c_alias_all_bid = 173430 AND c_pid > 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            SELECT toString(groupArray(concat(ss,'_', ssid))), toString(groupArray(stype)) FROM (
                SELECT toString(`source`) ss, toString(sid) ssid, argMin(shop_type, `from`) stype
                FROM sop_e.entity_prod_{}_ALL_ECSHOP WHERE shop_type != '' GROUP BY `source`, sid
            )
        '''.format(self.cleaner.eid)
        ret = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='店铺分类', transform(
                concat(toString(`source`),'_', toString(sid)), {}, {}, ''
            ), v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl, ret[0][0], ret[0][1])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='店铺分类', 'C2C', v), c_props.name, c_props.value)
            WHERE source = 1 AND (shop_type < 20 and shop_type > 10 )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='segment1',
                    multiIf(
                        `c_props.value`[indexOf(`c_props.name`, '是否礼盒')] = '礼盒', 'gift',
                        `c_props.value`[indexOf(`c_props.name`, 'SKU-出数专用')] LIKE '%散装%'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '总克数')])) >= 500.0, 'Bulk',
                        `c_sku_id` = 0 AND `c_props.value`[indexOf(`c_props.name`, '是否散装')] = '散装'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '总克数')])) >= 500.0, 'Bulk',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) > 0.0
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) < 80.0, 'Self-consumption',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 80.0
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) < 500.0, 'Sharing',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 500.0, 'Bulk',
                    'Not Indicated'),
                k='segment2',
                    multiIf(
                        `c_props.value`[indexOf(`c_props.name`, 'SKU-出数专用')] LIKE '%散装%'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '总克数')])) >= 500.0, 'Bulk Pack',
                        `c_sku_id` = 0 AND `c_props.value`[indexOf(`c_props.name`, '是否散装')] = '散装'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '总克数')])) >= 500.0, 'Bulk Pack',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) > 0.0
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) < 80.0, 'Small Pack',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 80.0
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) < 500.0, 'Big Pack',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 500.0, 'Bulk Pack',
                    'Not Indicated'),
                k='pack',
                    multiIf(
                        `c_props.value`[indexOf(`c_props.name`, '包装')] in ['塑料-碗装/桶装/罐装','桶装','碗装'], 'Bowl',
                        `c_props.value`[indexOf(`c_props.name`, '包装')] in ['塑料-盒装','盒装','纸-盒装','罐装','铁'], 'Box',
                        `c_props.value`[indexOf(`c_props.name`, '包装')] in ['塑料-袋装','纸-袋装','袋装'], 'Bag',
                        `c_props.value`[indexOf(`c_props.name`, '包装')] in ['其他','瓶装'], 'Others',
                    ''),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='segment3',
                    multiIf(
                        `c_props.value`[indexOf(`c_props.name`, 'segment2')] = 'Bulk Pack', 'Group Consumption(>500g)',
                        `c_props.value`[indexOf(`c_props.name`, 'segment2')] = 'Small Pack', 'Small Pack(<80g)',
                        `c_props.value`[indexOf(`c_props.name`, 'segment2')] = 'Big Pack'
                            AND `c_props.value`[indexOf(`c_props.name`, 'pack')] = 'Box', 'Big Pack(Tin/Box)',
                        `c_props.value`[indexOf(`c_props.name`, 'segment2')] = 'Big Pack'
                            AND `c_props.value`[indexOf(`c_props.name`, 'pack')] IN ['Bag','Bowl','Others'], 'Big Pack(Plastic/Paper Package)',
                    'Not Indicated'),
                k='segment4',
                    multiIf(
                        `c_props.value`[indexOf(`c_props.name`, 'SKU-出数专用')] LIKE '%散装%'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) > 600.0, 'Group Consumption(>600g)',
                        `c_sku_id` = 0 AND `c_props.value`[indexOf(`c_props.name`, '是否散装')] = '散装'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) > 600.0, 'Group Consumption(>600g)',
                        `c_props.value`[indexOf(`c_props.name`, '出数用品牌')] = 'Rocher'
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 600.0, 'Group Consumption(>600g)',
                        `c_props.value`[indexOf(`c_props.name`, 'pack')] = 'Box'
                            AND ( ( toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 80.0 AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) <= 600.0 ) OR `c_sku_id` IN [1, 2]), 'Big Pack(Tin/Box)',
                        `c_props.value`[indexOf(`c_props.name`, 'pack')] IN ['Bag','Bowl','Others']
                            AND ( ( toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) >= 80.0 AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) <= 600.0 ) OR `c_sku_id` IN [1, 2]), 'Big Pack(Plastic/Paper Package)',
                        toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) > 0.0
                            AND toFloat32OrZero(CONCAT('00', `c_props.value`[indexOf(`c_props.name`, '单包克数')])) < 80.0, 'Small Pack(<80g)',
                    'Not Indicated'),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def finish(self, tbl, dba, prefix):
        jd_sales = [
            [202102,[
                ['730614',5392086],
                ['100001324422',1793776],
                ['967598',1391762],
                ['317651',1020524],
                ['730618',973513],
                ['317653',913099],
                ['317655',832555],
                ['317652',725298],
                ['730617',669147],
                ['1950378',564010],
                ['967625',484426],
                ['1748541',420343],
                ['3503670',381341],
                ['730615',308705],
                ['967615',298178],
                ['100017345426',288396],
                ['100013499280',235712],
                ['1220741',198613],
                ['1220737',197766],
                ['730619',195033],
                ['100009983516',185067],
                ['4414960',167190],
                ['100013499308',166200],
                ['1910569',130066],
                ['730621',113020],
                ['100008212807',112014],
                ['100016186756',99501],
                ['100017264334',90225],
                ['100001833663',82647],
                ['967641',71510],
                ['100016912584',60928],
                ['2241670',50728],
                ['100009628885',43969],
                ['967610',41574],
                ['100001044329',38302],
                ['967592',35507],
                ['100014731550',27949],
                ['1881300',25614],
                ['1220743',20866],
                ['100011280644',14507],
                ['1220740',3018]
            ]]
        ]

        for month, items, in jd_sales:
            for item_id, sales, in items:
                sql = '''
                    SELECT sum(sales) FROM {} WHERE toYYYYMM(pkey) = {} AND item_id = '{}'
                '''.format(tbl, month, item_id)
                ret = dba.query_all(sql)
                if ret[0][0] == 0:
                    continue
                r = sales*100/ret[0][0]

                sql = '''
                    ALTER TABLE {} UPDATE c_sales = c_sales * {r}, c_num = IF(
                        c_sales * {r} / (c_sales / c_num) = 0, 1, c_sales * {r} / (c_sales / c_num)
                    )
                    WHERE toYYYYMM(pkey) = {} AND item_id = '{}'
                '''.format(tbl, month, item_id, r=r)
                dba.execute(sql)

                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp出数用厂商':'String','sp出数用品牌':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp单包克数` = `sp总克数`
            WHERE `sp包数` = '1'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sp7 = {
            '玛氏-Mars':'Mars',
            '费列罗-Ferrero':'Ferrero',
            '好时-Corp. Hershey':'Corp. Hershey',
            '歌帝梵-Godiva':'Godiva',
            '瑞士莲-Lindt':'Lindt',
            '明治-Meiji':'Meiji',
            '亿滋-MONDELEZ':'MONDELEZ',
            '混合厂商-Mix':'Mix'
        }
        sp7a = ['\'{}\''.format(k.replace('\'', '\\\'')) for k in sp7]
        sp7b = ['\'{}\''.format(sp7[k].replace('\'', '\\\'')) for k in sp7]

        sp8 = {
            '脆香米-CuiXiangMi':'CuiXiangMi',
            '德芙-Dove/品鉴可可-Dark Collection':'Dove',
            '德芙-Dove/德芙心语-Dove Love Heart':'Dove',
            '德芙-Dove/奇思妙感-Dove Sensation':'Dove',
            '德芙-Dove/德芙精心之选-Dove Silk Collection':'Dove',
            '德芙-Dove/德芙星彩-Dove Xin Cai':'Dove',
            '德芙-Dove/德芙酸奶-Dove Yogurt':'Dove',
            '德芙-Dove/德芙榛藏-Dove Zhen Cang':'Dove',
            '德芙-Dove/尊慕-Jewels':'Dove',
            '德芙-Dove/玫瑰物语-Miss Rose':'Dove',
            '德芙-Dove/巧丝-qiao si':'Dove',
            '德芙-Dove/心声-xin sheng':'Dove',
            '德芙-Dove/心随-Xin Sui':'Dove',
            '德芙-Dove/埃丝汀-Esteem':'Dove',
            '德芙-Dove/小巧粒':'Dove',
            '德芙-Dove':'Dove',
            'M & M\'S-M & M\'s':'M & M\'s',
            '麦提莎-Maltesers':'Maltesers',
            '士力架-Snickers/士力架燕麦花生夹心巧克力-Snickers Oat Peanut Chocolate':'Snickers',
            '士力架-Snickers/士力架辣花生夹心巧克力-Snickers Spicy Peanut Chocolate':'Snickers',
            '士力架-Snickers':'Snickers',
            '特趣-Twix':'Twix',
            '玛氏-Mars':'Others',
            '费列罗巧克力精选组合-Ferrero Golden Gallery':'Ferrero Golden Gallery',
            '费列罗臻品-Ferrero Collection':'Ferrero Collection',
            '拉斐尔-Raffaello':'Raffaello',
            '费列罗榛果威化-Rocher':'Rocher',
            '健达排块状-Kinder Chocolate/健达康脆麦-Kinder Chocolate Cereal':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/健达巧克力-Kinder Chocolate':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/健达夹心牛奶巧克力倍多-Maxi':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate/迷你-Mini':'Kinder Chocolate',
            '健达排块状-Kinder Chocolate':'Kinder Chocolate',
            '健达缤纷乐-Kinder Bueno/健达缤纷乐普通-Kinder Bueno Regular':'Kinder Bueno',
            '健达缤纷乐-Kinder Bueno/健达缤纷乐白巧克力-Kinder Bueno White':'Kinder Bueno',
            '健达缤纷乐-Kinder Bueno':'Kinder Bueno',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋-Kinder Joy':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋男孩版-KJ Boy':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋女孩版-KJ Girl':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋Max-Kinder Joy Max':'Kinder Joy',
            '健达奇趣蛋-Kinder Joy/健达奇趣蛋混合-KJ uni-sex':'Kinder Joy',
            '健达混合-Kinder Mix':'Kinder Mix',
            '巧可蹦脆乐-KSBC':'KSBC',
            '蒙雪莉-Moncheri':'Moncheri',
            '能多益-Nutella':'Nutella',
            '费列罗-Ferrero':'Others',
            '贝客诗-Brookside':'Brookside',
            '锐滋-Reese\'s':'Reese\'s',
            '好时-Hershey\'s/巧珍珠-Drops':'Hershey\'s',
            '好时-Hershey\'s/巧珍珠滑盖装-Globe':'Hershey\'s',
            '好时-Hershey\'s/好时排块-Hershey\'s Bar':'Hershey\'s',
            '好时-Hershey\'s/麦丽素-My Likes':'Hershey\'s',
            '好时-Hershey\'s/巧金砖-Nuggets':'Hershey\'s',
            '好时-Hershey\'s/巧乐园-Snack Mix':'Hershey\'s',
            '好时-Hershey\'s/特浓黑巧克力-Special Dark':'Hershey\'s',
            '好时-Hershey\'s':'Hershey\'s',
            '好时之吻-Kisses/好时臻吻-Kisses Deluxe':'Kisses',
            '好时之吻-Kisses/好时之吻婚庆装-Kisses Wedding':'Kisses',
            '好时之吻-Kisses':'Kisses',
            '好时-Corp. Hershey':'Others',
            '歌帝梵-Godiva':'Godiva',
            '特醇排装-EXCELLENCE':'EXCELLENCE',
            '软心-LINDOR':'LINDOR',
            '精选缤纷-NAPOLITAINS ASSORTIS':'NAPOLITAINS ASSORTIS',
            '瑞士经典排装-SWISS CLASSIC':'SWISS CLASSIC',
            '瑞士经典薄片-SWISS THINS':'SWISS THINS',
            '瑞士莲-Lindt':'Others',
            '明治-Meiji/巴旦木-Almond':'Meiji',
            '明治-Meiji/橡皮糖-GUM':'Meiji',
            '明治-Meiji/Marble 幻彩-HUANCAI':'Meiji',
            '明治-Meiji/钢琴巧克力-Imperiality':'Meiji',
            '明治-Meiji/澳洲坚果-Macadamia':'Meiji',
            '明治-Meiji/雪吻-Meiji Melty Kiss':'Meiji',
            '明治-Meiji':'Meiji',
            '吉百利-CDM':'CDM',
            '吉百利怡口莲-Cadbury Eclairs/金装-Eclairs Golden':'Cadbury Eclairs',
            '吉百利怡口莲-Cadbury Eclairs':'Cadbury Eclairs',
            '克特多金象-Cote d\'Or':'Cote d\'Or',
            '妙卡-Milka/融情-Milka Base':'Milka',
            '妙卡-Milka/妙卡旋妙杯-Milka Magic Cup':'Milka',
            '妙卡-Milka/妙卡奥利奥-Milka Oreo':'Milka',
            '妙卡-Milka':'Milka',
            '三角-Tobleron':'Tobleron',
            '亿滋-MONDELEZ':'Others',
            '混合厂商-Mix':'Others',
            'NA':'Others',
            'na':'Others'
        }
        sp8a = ['\'{}\''.format(k.replace('\'', '\\\'')) for k in sp8]
        sp8b = ['\'{}\''.format(sp8[k].replace('\'', '\\\'')) for k in sp8]

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用厂商` = transform(`sp出数用厂商`, [{}], [{}]), `sp出数用品牌` = transform(`sp出数用品牌`, [{}], [{}])
            WHERE 1
        '''.format(tbl, ','.join(sp7a), ','.join(sp7b), ','.join(sp8a), ','.join(sp8b))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用厂商` = 'Other Manu', `sp出数用品牌` = 'Others'
            WHERE clean_sku_id = 5 AND `sp出数用厂商` IN ['Mars','Ferrero','Lindt','Godiva','Corp. Hershey','Meiji','MONDELEZ']
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用厂商` = '每日黑巧', `sp出数用品牌` = '每日黑巧'
            WHERE sid = 181928791 AND name LIKE '%每日黑巧%'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用厂商` = '欧贝拉', `sp出数用品牌` = '欧贝拉'
            WHERE clean_alias_all_bid = 173430 AND clean_pid > 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `spsegment1` = multiIf(
                    `sp是否礼盒` = '礼盒', 'gift',
                    `spSKU-出数专用` LIKE '%散装%' AND toFloat32OrZero(CONCAT('00', `sp总克数`)) >= 500.0, 'Bulk',
                    `clean_sku_id` = 0 AND `sp是否散装` = '散装' AND toFloat32OrZero(CONCAT('00', `sp总克数`)) >= 500.0, 'Bulk',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) > 0.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) < 80.0, 'Self-consumption',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 80.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) < 500.0, 'Sharing',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 500.0, 'Bulk',
                    'Not Indicated'),
                `spsegment2` = multiIf(
                    `spSKU-出数专用` LIKE '%散装%' AND toFloat32OrZero(CONCAT('00', `sp总克数`)) >= 500.0, 'Bulk Pack',
                    `clean_sku_id` = 0 AND `sp是否散装` = '散装' AND toFloat32OrZero(CONCAT('00', `sp总克数`)) >= 500.0, 'Bulk Pack',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) > 0.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) < 80.0, 'Small Pack',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 80.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) < 500.0, 'Big Pack',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 500.0, 'Bulk Pack',
                    'Not Indicated'),
                `sppack` = multiIf(
                    `sp包装` in ['塑料-碗装/桶装/罐装','桶装','碗装'], 'Bowl',
                    `sp包装` in ['塑料-盒装','盒装','纸-盒装','罐装','铁'], 'Box',
                    `sp包装` in ['塑料-袋装','纸-袋装','袋装'], 'Bag',
                    `sp包装` in ['其他','瓶装'], 'Others',
                    '')
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `spsegment3` = multiIf(
                    `spsegment2` = 'Bulk Pack', 'Group Consumption(>500g)',
                    `spsegment2` = 'Small Pack', 'Small Pack(<80g)',
                    `spsegment2` = 'Big Pack' AND `sppack` = 'Box', 'Big Pack(Tin/Box)',
                    `spsegment2` = 'Big Pack' AND `sppack` IN ['Bag','Bowl','Others'], 'Big Pack(Plastic/Paper Package)',
                    'Not Indicated'),
                `spsegment4` = multiIf(
                    `spSKU-出数专用` LIKE '%散装%', 'Group Consumption(>600g)',
                    `clean_sku_id` = 0 AND `sp是否散装` = '散装', 'Group Consumption(>600g)',
                    `sp出数用品牌` = 'Rocher' AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 600.0, 'Group Consumption(>600g)',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) > 600.0, 'Group Consumption(>600g)',
                    `sppack` = 'Box' AND ( ( toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 80.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) <= 600.0 ) OR `clean_sku_id` IN [1, 3]), 'Big Pack(Tin/Box)',
                    `sppack` IN ['Bag','Bowl','Others'] AND ( ( toFloat32OrZero(CONCAT('00', `sp单包克数`)) >= 80.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) <= 600.0 ) OR `clean_sku_id` IN [1, 3]), 'Big Pack(Plastic/Paper Package)',
                    toFloat32OrZero(CONCAT('00', `sp单包克数`)) > 0.0 AND toFloat32OrZero(CONCAT('00', `sp单包克数`)) < 80.0, 'Small Pack(<80g)',
                    'Not Indicated')
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        if prefix.find('Lindt') != -1:
            sql = '''
                ALTER TABLE {} UPDATE `sp是否礼盒` = '非礼盒' WHERE `sp是否礼盒` = '礼盒' AND `clean_price` < 4000
            '''.format(tbl)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)


    def finish_new(self, tbl, dba, prefix):
        jd_sales = [
            [202102,[
                ['730614',5392086],
                ['100001324422',1793776],
                ['967598',1391762],
                ['317651',1020524],
                ['730618',973513],
                ['317653',913099],
                ['317655',832555],
                ['317652',725298],
                ['730617',669147],
                ['1950378',564010],
                ['967625',484426],
                ['1748541',420343],
                ['3503670',381341],
                ['730615',308705],
                ['967615',298178],
                ['100017345426',288396],
                ['100013499280',235712],
                ['1220741',198613],
                ['1220737',197766],
                ['730619',195033],
                ['100009983516',185067],
                ['4414960',167190],
                ['100013499308',166200],
                ['1910569',130066],
                ['730621',113020],
                ['100008212807',112014],
                ['100016186756',99501],
                ['100017264334',90225],
                ['100001833663',82647],
                ['967641',71510],
                ['100016912584',60928],
                ['2241670',50728],
                ['100009628885',43969],
                ['967610',41574],
                ['100001044329',38302],
                ['967592',35507],
                ['100014731550',27949],
                ['1881300',25614],
                ['1220743',20866],
                ['100011280644',14507],
                ['1220740',3018]
            ]]
        ]

        for month, items, in jd_sales:
            for item_id, sales, in items:
                sql = '''
                    SELECT sum(sales) FROM {} WHERE toYYYYMM(date) = {} AND item_id = '{}'
                '''.format(tbl, month, item_id)
                ret = dba.query_all(sql)
                if ret[0][0] == 0:
                    continue
                r = sales*100/ret[0][0]

                sql = '''
                    ALTER TABLE {} UPDATE clean_sales = clean_sales * {r}, clean_num = multiIf( clean_num <= 0, clean_num,
                        clean_sales * {r} / (clean_sales / clean_num) = 0, 1, clean_sales * {r} / (clean_sales / clean_num)
                    )
                    WHERE toYYYYMM(date) = {} AND item_id = '{}' AND clean_num > 0
                '''.format(tbl, month, item_id, r=r)
                dba.execute(sql)

                while not self.cleaner.check_mutations_end(dba, tbl):
                    time.sleep(5)


    def test(self):
        # 导旧答题
        # TRUNCATE TABLE artificial_new.`entity_91024_item`;

        # INSERT INTO artificial_new.`entity_91024_item` (
        # id,tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,
        # spid7,spid8,spid14,spid9,spid12,spid10,spid11,spid27,spid1,spid13,spid28,spid15,spid21,spid17,spid18,spid19,spid20,spid22,spid23,spid24,spid25,spid26,spid3,spid4,spid5,spid6,spid2,spid16,
        # pid,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set,created,modified,number
        # ) SELECT id,tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,0,0,0,visible_check,
        # IF(sp1 IS NULL,'',sp1),IF(sp2 IS NULL,'',sp2),spid3,spid4,spid5,spid6,spid7,'','','','','','','','','','','','','','','','','','','','',spid8,
        # pid,batch_id,flag,uid,0,0,1,img,is_set,created,modified,number
        # FROM artificial_new.`entity_90261_item` WHERE flag > 0 AND uid > 0;

        # update artificial_new.`entity_91024_item` set spid7="Other Manu",spid8="Others" where pid in (1,2,3,4,5) and spid7="0" and spid8="其他";
        # update artificial_new.`entity_91024_item` a join artificial_new.entity_90261_maker_brand b on a.spid7=b.id set a.spid7=CONCAT(b.maker_cn,'-',b.maker_en),a.spid8=IF(b.brand_cn IS NULL,b.brand_en,IF(b.sub_brand_cn='',CONCAT(b.brand_cn,'-',b.brand_en),CONCAT(b.brand_cn,'-',b.brand_en,'/',b.sub_brand_cn,'-',b.sub_brand_en))) where pid in (1,2,3,4,5);
        # 只有套包才用属性答题
        # UPDATE artificial_new.`entity_91024_item` SET spid7='',spid8='',spid2='',spid16='',spid9='',spid12='',spid10='',spid11='',spid27='',spid1='',spid13='',spid14='',spid15='',spid21='',spid17='',
        # spid18='',spid19='',spid20='',spid22='',spid23='',spid24='',spid25='',spid26='',spid28='',spid3='',spid4='',spid5='',spid6=''
        # WHERE pid NOT IN (1,2,3,4,5);
        # UPDATE artificial_new.`entity_91024_item` SET spid7 = '' WHERE spid7 = '0';
        # UPDATE artificial_new.`entity_91024_item` SET spid7 = '', spid8 = '' WHERE pid = 5;
        # UPDATE artificial_new.`entity_91024_item` SET spid8 = '德芙-Dove/小巧粒' WHERE spid8 = '德芙-Dove/小巧粒-';
        # UPDATE artificial_new.`entity_91024_item` SET spid8 = '' WHERE spid8 IN ('爱丽莎','尊尼','幕葡罗','金沙','爱丽莎（AiLiSha）');
        # UPDATE artificial_new.`entity_91024_item` SET uid = 167 WHERE uid = 0;
        # UPDATE artificial_new.`entity_91024_item` SET flag = 2 WHERE flag = 1;

        # UPDATE artificial_new.`entity_91024_item` SET spid7='',spid8='',spid2='',spid16='',spid9='',spid12='',spid10='',spid11='',spid27='',spid1='',spid13='',spid14='',spid15='',spid21='',spid17='',
        # spid18='',spid19='',spid20='',spid22='',spid23='',spid24='',spid25='',spid26='',spid28='',spid3='',spid4='',spid5='',spid6=''
        # WHERE pid IN (1,2,3,4,5) AND visible != 1;

        # UPDATE artificial_new.`entity_91024_item` SET spid9='',spid12='',spid10='',spid11='' WHERE pid IN (3) AND visible = 1;

        # truncate TABLE product_lib.product_91024;
        # INSERT into product_lib.product_91024(
        # pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,
        # spid7,spid8,spid14,spid9,spid12,spid10,spid11,spid27,spid1,spid13,spid16,spid15,spid21,spid17,spid18,spid19,spid20,spid22,spid23,spid24,spid25,spid26,spid28,spid29,spid30,spid3,spid4,spid5,spid6,spid2)
        # SELECT pid,name,product_name,full_name,1,alias_all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,
        # spid1,spid2,spid3,spid4,spid5,spid6,spid7,'','','','','','','','','','','','','','','','','','','','','','',''
        # FROM product_lib.product_90261;

        # UPDATE product_lib.product_91024 SET spid8 = '好时-Hershey\'s/好时排块-Hershey\'s Bar' WHERE spid8 = '好时-Hershey\'s/好时排块-Hershey’s Bar';

        # /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_91024_item > /obsfs/91024.sql
        # /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91024.sql
        # /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm product_lib product_91024 > /obsfs/91024p.sql
        # /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91024p.sql

        # CREATE TABLE product_lib_hw.entity_91024_item LIKE product_lib.entity_91024_item;
        # p3 scripts/conver_brush_ali2hw.py

        # UPDATE product_lib_hw.entity_91024_item SET p1 = trade_prop_all ;

        # INSERT INTO product_lib.entity_91024_item (
        # pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,org_bid,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp23,modify_sp24,modify_sp25,modify_sp26,modify_sp27,modify_sp28,modify_sp29,modify_sp3,modify_sp30,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29,sp3,sp30,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid29,spid3,spid30,spid4,spid5,spid6,spid7,spid8,spid9,modify_sp31,spid31,modify_sp32,spid32,modify_sp33,spid33,modify_sp34,spid34,modify_sp35,spid35,modify_sp36,spid36,sp31,sp32,sp33,sp34,sp35,sp36
        # ) SELECT
        # pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp23,modify_sp24,modify_sp25,modify_sp26,modify_sp27,modify_sp28,modify_sp29,modify_sp3,modify_sp30,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29,sp3,sp30,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid29,spid3,spid30,spid4,spid5,spid6,spid7,spid8,spid9,'','','','','','','','','','','','','','','','','',''
        # FROM product_lib_hw.entity_91024_item WHERE (source, tb_item_id, real_p1) NOT IN (SELECT source, tb_item_id, real_p1 FROM product_lib.entity_91024_item);

        # INSERT INTO product_lib.product_91024 (
        #     pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid3,spid4,spid5,spid6,spid7,spid8,spid9,spid29,spid30,spid31,spid32,spid33,spid34,spid35,spid36
        # ) SELECT
        #     pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid3,spid4,spid5,spid6,spid7,spid8,spid9,spid29,spid30,'','','','','',''
        # FROM product_lib_ali.product_91024 WHERE pid >= 8704;

        # SELECT pid, a.alias_all_bid, b.alias_all_bid, a.name, b.name FROM product_lib.product_91024 a RIGHT JOIN product_lib_ali.product_91024 b USING (pid)
        # WHERE a.name != b.name OR a.alias_all_bid IS NULL OR b.alias_all_bid IS NULL;

        # SELECT pid, a.alias_all_bid, b.alias_all_bid, a.name, b.name FROM product_lib.product_91024 a RIGHT JOIN product_lib_ali.product_91024 b USING(pid)
        # WHERE a.name != b.name OR a.alias_all_bid IS NULL OR b.alias_all_bid IS NULL;

        # SELECT name, alias_all_bid, a.pid, b.pid FROM product_lib.product_91024 a RIGHT JOIN product_lib_ali.product_91024 b USING (alias_all_bid, name)
        # WHERE a.pid != b.pid OR a.pid IS NULL OR b.pid IS NULL;

        # INSERT IGNORE INTO brush.product_merge_log
        # SELECT pid, 91024, product_id, merge_to_product_id, uid, created, modified FROM brush.product_merge_log WHERE eid = 90261 AND product_id != merge_to_product_id;
        pass


    def brush_2(self, smonth, emonth):
        where_uuid = 'uuid2 not in (select uuid2 from {} where sp1=\'剔除数据\')'.format(self.get_clean_tbl())
        bids = [1355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640, 131503, 131588, 131643, 131867, 132153, 522178, 1585685, 131694, 5229175, 5780501]
        uuids1 = []
        uuids10 = []
        # where = ['snum = 1 and (shop_type > 20 or shop_type < 10 )',
        #          'snum = 2']
        where = ['{} and source =\'tmall\'', '{} and source = \'jd\'']
        for each_where in where:
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=each_where.format(where_uuid), rate=0.3,if_bid=True)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=True):
                    continue
                print(alias_all_bid)
                if int(alias_all_bid) in bids:
                    uuids1.append(uuid2)
                else:
                    uuids10.append(uuid2)

        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where='{} and source = \'tb\''.format(where_uuid), limit=3000,if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=True):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)
        #KINDER
        where = "(alias_all_bid in (131507,131630,1355260) and (match(name,'奇趣蛋|出奇蛋|JOY') or match(arrayStringConcat(props.value,',') ,'奇趣蛋|出奇蛋|JOY')) " \
                "and snum in (1, 2)) or (alias_all_bid in (131507,131630,1355260) and (match(name,'健达|kinder') and match(name,'牛奶')) and snum in (1, 2)) and {}".format(where_uuid)
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=where,if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=True):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids1, clean_flag, 1)
        # self.cleaner.add_brush(uuids10, clean_flag, 10)
        print('add visibile1:', len(uuids1), 'add visibile 10:',len(uuids10))
        return True

    def skip_helper_12(self, smonth, emonth, uuids):

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # 找出当期之前所有出过的题
        sql = '''select uuid2 from {tbl}  where (snum, tb_item_id, real_p1,month)
                in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}' and length(uuid2)>1 group by snum, tb_item_id, real_p1) and length(uuid2)>1
                '''.format(tbl=self.cleaner.get_tbl(), smonth=smonth)
        ret = self.cleaner.db26.query_all(sql)
        # map = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        map = [v[0] for v in ret]

        # 找出当期前出题的最近一条机洗结果
        sql_result_old_uuid = '''
        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, argMax(alias_all_bid, date) alias,uuid2,c_sp16 FROM {ctbl}
        WHERE pkey  < '{smonth}' and  uuid2 in({uuids})
        GROUP BY source, item_id, p1, uuid2, c_sp16
        '''.format(ctbl=ctbl, smonth=smonth, uuids=','.join(["'" + str(v) + "'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[5]] for v in db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
        WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, alias_all_bid,uuid2,c_sp16 FROM {ctbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0 and uuid2 in ({uuids})
        AND uuid2 NOT IN (SELECT uuid2 FROM {atbl}  WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sign = -1)
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

    ###最新月度默认规则
    def brush_old_monthly(self, smonth, emonth, logId=-1):
        remove = False
        # cdbname, ctbl = self.get_c_tbl()
        cdbname, ctbl = self.get_all_tbl()
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        sql = 'select distinct(cid) from {}'.format(atbl)
        cids = [v[0] for v in dba.query_all(sql)]
        where_uuid = '''uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}')))'''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        bids = [355260,66801,131527,131714,132149,131507,131630,131497,131498,68676,48255,131612,599,40698,50640,131503,131588,131643,131867,132153,522178,1585685,5780501]
        uuids1 = []
        uuids10 = []
        uuid_new = []
        where = ['{} and source = 1 and (shop_type > 20 or shop_type < 10 ) and cid = {}',
                 '{} and source = 2 and cid = {}']
        # where = ['{} and source =\'tmall\'', '{} and source = \'jd\'']
        for each_where in where:
            # 分类目
            for each_cid in cids:
                ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=each_where.format(where_uuid, each_cid), rate=0.8)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_new.append(uuid2)
                    if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                        continue
                    if int(alias_all_bid) in bids:
                        uuids1.append(uuid2)
                    else:
                        uuids10.append(uuid2)

        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where='{} and source = 1 and (shop_type < 20 and shop_type > 10 )'.format(where_uuid), limit=3000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_new.append(uuid2)
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)
        #KINDER
        where = "(alias_all_bid in (131507,131630,1355260) and (match(name,'奇趣蛋|出奇蛋|JOY') or match(arrayStringConcat(props.value,',') ,'奇趣蛋|出奇蛋|JOY')) " \
                "and (source=2 or (source=1 and shop_type>20))) or (alias_all_bid in (131507,131630,1355260) and (match(name,'健达|kinder') and match(name,'牛奶')) " \
                "and (source=2 or (source=1 and shop_type>20))) and {}".format(where_uuid)
        # ali改成tmall
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_new.append(uuid2)
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)


        ###以下为22年1月24日新增需求

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sid_lists = [188167855,1000114421,186608105,192922608,11629904,1000082422,193307761,1000333084,177138369,1000372726,195789500,189079033,1000077643,181934756,1000372082,174983084,69829559,1000094961,1000118521,5554275,944781,1000333003,195740659,1000303482,188178838,947977,187716745,1000118801,188198670,191223922,195213634,192895457,1000333345,195430274,1000100813,10566463,1000342363,62013349,1000118190,12023546,1000078502,20140406,20140461,171209886,14335335,1000081022,176234360,195799908,191299180,1000352022,12083733,1000090791,1000395803,1000300903,10795643,1000072521,162650701,10177910,1000328942,1000089761,188508665,2561061,1000075682,194296439,944768,1000089365,9281929]
        uuids1_new = []
        uuids10_new = []

        for each_source in sources:
            for each_sid in sid_lists:
                if int(each_sid) == 9281929:
                    where = '''
                    uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and uuid2 in (select uuid2 from {ctbl} where c_sp7 != 'Other Manu' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and {each_source} and sid = {each_sid}
                    '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
                    ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_new.append(uuid2)
                        if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                            continue
                        if int(alias_all_bid) in bids:
                            uuids1_new.append(uuid2)
                        else:
                            uuids10_new.append(uuid2)
                else:
                    where = '''
                    uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and {each_source} and sid = {each_sid}
                    '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
                    ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_new.append(uuid2)
                        if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                            continue
                        if int(alias_all_bid) in bids:
                            uuids1_new.append(uuid2)
                        else:
                            uuids10_new.append(uuid2)


        new_uuids, old_uuids_update, map_help_new = self.skip_helper_12(smonth, emonth, uuid_new)
########结果反馈
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, visible_check=1,visible=1)
        self.cleaner.add_brush(uuids10, clean_flag, visible_check=1,visible=10)
        ###以下为22年1月24日新增需求
        self.cleaner.add_brush(uuids1_new, clean_flag, visible_check=1, visible=1)
        self.cleaner.add_brush(uuids10_new, clean_flag, visible_check=1, visible=10)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=111)

        sql_1 = '''update {} set visible = 1 where alias_all_bid in ({}) and visible=111'''.format(self.cleaner.get_tbl(),','.join([str(v) for v in bids]))
        # print(sql)
        self.cleaner.db26.execute(sql_1)
        self.cleaner.db26.commit()

        sql_2 = '''update {} set visible = 10 where alias_all_bid not in ({}) and visible=111'''.format(self.cleaner.get_tbl(), ','.join([str(v) for v in bids]))
        # print(sql)
        self.cleaner.db26.execute(sql_2)
        self.cleaner.db26.commit()

        print('add visibile1:', len(uuids1), 'add visibile 10:',len(uuids10), 'add visibile1_new:', len(uuids1_new), 'add visibile 10_new:',len(uuids10_new),'add visibile 111:', len(old_uuids_update))


        return True



    def brush(self, smonth, emonth, logId=-1):
        remove = False
        # cdbname, ctbl = self.get_c_tbl()
        cdbname, ctbl = self.get_all_tbl()
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        sql = 'select distinct(cid) from {}'.format(atbl)
        cids = [v[0] for v in dba.query_all(sql)]
        where_uuid = '''uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}')))'''.format(
            ctbl=ctbl, smonth=smonth, emonth=emonth)
        bids = [355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599,
                40698, 50640, 131503, 131588, 131643, 131867, 132153, 522178, 1585685, 5780501]
        uuids1 = []
        uuids10 = []
        uuid_new = []
        where = ['{} and source = 1 and (shop_type > 20 or shop_type < 10 ) and cid = {}',
                 '{} and source = 2 and cid = {}']
        # where = ['{} and source =\'tmall\'', '{} and source = \'jd\'']
        for each_where in where:
            # 分类目
            for each_cid in cids:
                ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth,
                                                                   where=each_where.format(where_uuid, each_cid),
                                                                   rate=0.8)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    uuid_new.append(uuid2)


        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth,
                                                           where='{} and source = 1 and (shop_type < 20 and shop_type > 10 )'.format(
                                                               where_uuid), limit=3000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_new.append(uuid2)


        # KINDER
        where = "(alias_all_bid in (131507,131630,1355260) and (match(name,'奇趣蛋|出奇蛋|JOY') or match(arrayStringConcat(props.value,',') ,'奇趣蛋|出奇蛋|JOY')) " \
                "and (source=2 or (source=1 and shop_type>20))) or (alias_all_bid in (131507,131630,1355260) and (match(name,'健达|kinder') and match(name,'牛奶')) " \
                "and (source=2 or (source=1 and shop_type>20))) and {}".format(where_uuid)
        # ali改成tmall
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuid_new.append(uuid2)



        ###以下为22年1月24日新增需求

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 2']
        sid_lists = [188167855, 1000114421, 186608105, 192922608, 11629904, 1000082422, 193307761, 1000333084,
                     177138369, 1000372726, 195789500, 189079033, 1000077643, 181934756, 1000372082, 174983084,
                     69829559, 1000094961, 1000118521, 5554275, 944781, 1000333003, 195740659, 1000303482,
                     188178838, 947977, 187716745, 1000118801, 188198670, 191223922, 195213634, 192895457,
                     1000333345, 195430274, 1000100813, 10566463, 1000342363, 62013349, 1000118190, 12023546,
                     1000078502, 20140406, 20140461, 171209886, 14335335, 1000081022, 176234360, 195799908,
                     191299180, 1000352022, 12083733, 1000090791, 1000395803, 1000300903, 10795643, 1000072521,
                     162650701, 10177910, 1000328942, 1000089761, 188508665, 2561061, 1000075682, 194296439, 944768,
                     1000089365, 9281929]
        uuids1_new = []
        uuids10_new = []

        for each_source in sources:
            for each_sid in sid_lists:
                if int(each_sid) == 9281929:
                    where = '''
                    uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and uuid2 in (select uuid2 from {ctbl} where c_sp7 != 'Other Manu' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and {each_source} and sid = {each_sid}
                    '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
                    ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_new.append(uuid2)

                else:
                    where = '''
                    uuid2 not in (select uuid2 from {ctbl} where c_sp1=\'剔除数据\' and toYYYYMM(date)>=toYYYYMM(toDate('{smonth}')) and toYYYYMM(date)<toYYYYMM(toDate('{emonth}'))) and {each_source} and sid = {each_sid}
                    '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sid=each_sid)
                    ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuid_new.append(uuid2)




        new_uuids, old_uuids_update, map_help_new = self.skip_helper_12(smonth, emonth, uuid_new)

        print (old_uuids_update)
        ########结果反馈
        clean_flag = self.cleaner.last_clean_flag() + 1

        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=55)
        # self.cleaner.add_brush(uuids10, clean_flag, visible_check=1, visible=10)
        # ###以下为22年1月24日新增需求
        # self.cleaner.add_brush(uuids1_new, clean_flag, visible_check=1, visible=1)
        # self.cleaner.add_brush(uuids10_new, clean_flag, visible_check=1, visible=10)
        # self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=111)
        #
        # sql_1 = '''update {} set visible = 1 where alias_all_bid in ({}) and visible=111'''.format(
        #     self.cleaner.get_tbl(), ','.join([str(v) for v in bids]))
        # # print(sql)
        # self.cleaner.db26.execute(sql_1)
        # self.cleaner.db26.commit()
        #
        # sql_2 = '''update {} set visible = 10 where alias_all_bid not in ({}) and visible=111'''.format(
        #     self.cleaner.get_tbl(), ','.join([str(v) for v in bids]))
        # # print(sql)
        # self.cleaner.db26.execute(sql_2)
        # self.cleaner.db26.commit()
        #
        # print('add visibile1:', len(uuids1), 'add visibile 10:', len(uuids10), 'add visibile1_new:',
        #       len(uuids1_new), 'add visibile 10_new:', len(uuids10_new), 'add visibile 111:', len(old_uuids_update))

        return True



    def brush_0124(self, smonth, emonth, logId=-1):
        remove = False
        cdbname, ctbl = self.get_c_tbl()
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        sql = 'select distinct(cid) from {}'.format(atbl)
        cids = [v[0] for v in dba.query_all(sql)]
        where_uuid = 'uuid2 not in (select uuid2 from {} where sp1=\'剔除数据\')'.format(ctbl)
        bids = [1355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640, 131503, 131588, 131643, 131867, 132153, 522178, 1585685, 131694, 5229175, 5780501]
        uuids1 = []
        uuids10 = []
        where = ['{} and source = 1 and (shop_type > 20 or shop_type < 10 ) and cid = {}',
                 '{} and source = 2 and cid = {}']
        # where = ['{} and source =\'tmall\'', '{} and source = \'jd\'']
        for each_where in where:
            # 分类目
            for each_cid in cids:
                ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=each_where.format(where_uuid, each_cid), rate=0.8)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                        continue
                    if int(alias_all_bid) in bids:
                        uuids1.append(uuid2)
                    else:
                        uuids10.append(uuid2)

        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where='{} and source = 1 and (shop_type < 20 and shop_type > 10 )'.format(where_uuid), limit=3000)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)
        #KINDER
        where = "(alias_all_bid in (131507,131630,1355260) and (match(name,'奇趣蛋|出奇蛋|JOY') or match(arrayStringConcat(props.value,',') ,'奇趣蛋|出奇蛋|JOY')) " \
                "and (source=2 or (source=1 and shop_type>20))) or (alias_all_bid in (131507,131630,1355260) and (match(name,'健达|kinder') and match(name,'牛奶')) " \
                "and (source=2 or (source=1 and shop_type>20))) and {}".format(where_uuid)
        # ali改成tmall
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                continue
            if int(alias_all_bid) in bids:
                uuids1.append(uuid2)
            else:
                uuids10.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1, clean_flag, visible_check=1,visible=1)
        self.cleaner.add_brush(uuids10, clean_flag, visible_check=1,visible=10)
        print('add visibile1:', len(uuids1), 'add visibile 10:',len(uuids10))
        return True


    def brush_0401(self, smonth, emonth, logId=-1):

        cdbname, ctbl = self.get_all_tbl()
        dba, atbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)
        sales_by_uuid = {}
        cc = {}

        sql = 'select distinct(cid) from {} where source=11'.format(atbl)
        cids = [v[0] for v in dba.query_all(sql)]

        sql2 = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql2)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        for each_cid in cids:
            # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            for ssmonth, eemonth in [['2021-03-01', '2022-03-01']]:
                uuids = []
                where = '''
                cid = '{each_cid}' and source = 11 and pkey>='{ssmonth}' and pkey<'{eemonth}' and uuid2 in (select uuid2 from {ctbl} where c_sp1 != '剔除数据' and pkey>='{ssmonth}' and pkey<'{eemonth}')
                '''.format(each_cid=each_cid, ssmonth=ssmonth, eemonth=eemonth, ctbl=ctbl)
                ret, sales_by_uuid_1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400000,rate=0.8)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                        uuids.append(uuid2)

                cc['{}+{}+{}'.format(each_cid, ssmonth, eemonth)] = [len(uuids)]

        for i in cc:
            print (i, cc[i])




        ########结果反馈
        clean_flag = self.cleaner.last_clean_flag() + 1


        return True

    # update
    # product_lib.entity_91024_item
    # set
    # visible = 1
    # where
    # created >= '2022-12-02' and snum != 11
    # and alias_all_bid in (
    #     355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640,
    #     131503, 131588, 131643, 131867, 132153, 522178, 1585685, 5780501, 47003, 131786)
    #
    # update
    # product_lib.entity_91024_item
    # set
    # visible = 10
    # where
    # created >= '2022-12-02' and snum != 11
    # and alias_all_bid not in (
    #     355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640,
    #     131503, 131588, 131643, 131867, 132153, 522178, 1585685, 5780501, 47003, 131786)
    #
    # update
    # product_lib.entity_91024_item
    # set
    # visible = 100
    # where
    # created >= '2022-12-02' and snum = 11
    # and alias_all_bid in (
    #     355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640,
    #     131503, 131588, 131643, 131867, 132153, 522178, 1585685, 5780501, 47003, 131786)
    #
    # update
    # product_lib.entity_91024_item
    # set
    # visible = 101
    # where
    # created >= '2022-12-02' and snum = 11
    # and alias_all_bid not in (
    #     355260, 66801, 131527, 131714, 132149, 131507, 131630, 131497, 131498, 68676, 48255, 131612, 599, 40698, 50640,
    #     131503, 131588, 131643, 131867, 132153, 522178, 1585685, 5780501, 47003, 131786)
    #
    # update
    # product_lib.entity_91024_item
    # set
    # visible = 2
    # where
    # created >= '2022-12-02' and tb_item_id in (
    #     '525195023185', '525747335018', '525922468103', '526132887639', '538780382631', '540021843550', '540758571915',
    #     '541236890666', '543832134883', '579498919551', '580837115419', '604064155892', '604064767842', '604319735268',
    #     '608408032998', '613769362991', '614137945139', '614385290266', '627390972131', '627716853652', '627955118315',
    #     '628974251373', '631684578409', '632498192959', '632826469366', '634107505473', '636371674743', '638501718002',
    #     '639662148137', '640229891923', '644460276550', '644975650369', '653195611892', '653352601089', '654405111364',
    #     '656296970366', '656560171550', '658048521773', '658416150409', '659109500757', '660145855315', '660209307402',
    #     '660297131972', '661178976559', '661524717715')
