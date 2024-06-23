import sys
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd
import copy

class main(Batch.main):
    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if v['flag'] == 1:
                vv['sp2'] = '空'

            if 'sku_name' in vv and (vv['sku_name'].find('能多益') > -1 or vv['sku_name'].find('nutella') > -1):
                vv['sp32'] = '能多益'

            vv['sp8'] = vv['sp8'].replace('g', '')
            if vv['sp18'] == '' and (vv['sp8'] == '' or vv['sp9'] == ''):
                continue

            if vv['sp18'] != '':
                sp18 = vv['sp18'].split('+')
                if len(sp18) > 1:
                    vv['sp8'] = vv['sp18'] = str(eval(vv['sp18']))
                    vv['sp9'] = str(1)
                else:
                    vv['sp8'], vv['sp9'] = sp18[0].split('*')
                    vv['sp18'] = str(eval(vv['sp18']))
            else:
                vv['sp18'] = str(round(float(vv['sp8']) * float(vv['sp9']), 1)).replace('.0', '')


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {
            'spSKU-出数专用':'String','sp出数用PRC':'String','sp出数用渠道':'String','sp出数用细分渠道':'String',
            'sp出数用套包类型':'String','sp出数用店铺类型':'String','sp单包克数1':'String','sp总克数1':'String',
            'sp是否答题':'String'
        })

        sql = '''
            ALTER TABLE {} UPDATE `sp是否答题` = '是'
            WHERE clean_brush_id > 0
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `spSKU-出数专用` = '套包'
            WHERE `spSKU-出数专用` = '___套包'
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        self.hotfix_shop(tbl, dba, prefix)
        self.hotfix_pack(tbl, dba, prefix)
        self.hotfix_brand(tbl, dba, prefix)

        if prefix.find('ferrero') != -1:
            self.hotfix_ferrero(tbl, dba, prefix)
        if prefix.find('MDLZ') != -1:
            self.hotfix_mdlz(tbl, dba, prefix)


    def hotfix_shop(self, tbl, dba, prefix):
        sql = '''
            SELECT toString(groupArray(concat(ss,'_', ssid))), toString(groupArray(stype)) FROM (
                SELECT toString(`source`) ss, toString(sid) ssid, argMin(shop_type, `from`) stype
                FROM sop_e.entity_prod_{}_ALL_ECSHOP WHERE shop_type != '' GROUP BY `source`, sid
            )
        '''.format(self.cleaner.eid)
        rr1 = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp店铺分类` = transform(concat(toString(`source`),'_', toString(sid)), {}, {}, `sp店铺分类`)
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl, rr1[0][0], rr1[0][1])
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp店铺分类` = 'C2C'
            WHERE source = 1 AND (shop_type < 20 and shop_type > 10 )
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp店铺分类` = 'EDT'
            WHERE sid = 679642
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp店铺分类2` = IF(`sp店铺分类` = 'EKA_FSS', 'EKA', `sp店铺分类`),
                `sp总克数` = toString(ROUND(toFloat32OrZero(`sp单包克数`) * toFloat32OrZero(`sp包数`), 2))
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def hotfix_pack(self, tbl, dba, prefix):
        etbl = 'sop_e.entity_prod_91081_E_MDLZ_report'

        sql = '''
            ALTER TABLE {} UPDATE `sp包数` = '1'
            WHERE `sp子品类` = '饼干' AND `sp包数` IN ('0','')
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        # 先给所有数据刷 单包克数1 = 0
        sql = '''
            ALTER TABLE {} UPDATE `sp单包克数1` = toString(toFloat32OrZero(`sp单包克数`))
            WHERE `sp单包克数1` IN ('', '0') AND `sp子品类` = '饼干'
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            WITH `sp单包克数` AS a, `sp包数` AS b
            SELECT alias_all_bid, sum(toFloat32OrZero(a)*toInt32OrZero(b)*sales)/sum(num) FROM {}
            WHERE toFloat32OrZero(a) >= 10 AND b != '' AND date >= '2020-01-01' AND date < '2021-01-01' AND `sp子品类` = '饼干'
            GROUP BY alias_all_bid
        '''.format(etbl)
        ret = dba.query_all(sql)

        b, r = [str(v[0]) for v in ret], [str(v[1]) for v in ret]

        sql = '''
            ALTER TABLE {} UPDATE
                `sp单包克数1` = toString(toFloat32(ceil(
                    transform(clean_alias_all_bid,[{b}],[{r}],0) * (clean_sales/clean_num) / toUInt32OrZero(`sp包数`)
                )))
            WHERE `sp单包克数1` = '0' AND `sp子品类` = '饼干'
            SETTINGS mutations_sync = 1
        '''.format(tbl, b=','.join(b), r=','.join(r))
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp单包克数1` = toString(toFloat32OrZero(`sp单包克数`))
            WHERE (toFloat32OrZero(`sp单包克数1`) >= 15000 OR `sp单包克数1` = '0') AND `sp子品类` = '饼干'
            SETTINGS mutations_sync = 1
        '''.format(tbl, b=','.join(b), r=','.join(r))
        dba.execute(sql)

        sql = '''
            WITH CONCAT(toString(`source`), '-', item_id, '-', `sp包数`) AS p1
            SELECT p1, ceil(AVG(toFloat32OrZero(`sp单包克数1`)))
            FROM {} WHERE toFloat32OrZero(`sp单包克数1`) > 0 AND `sp子品类` = '饼干'
            AND p1 IN ( WITH 1 AS _
                SELECT CONCAT(toString(`source`), '-', item_id, '-', `sp包数`) FROM {} WHERE `sp单包克数1` = '0'
            )
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包克数1` = toString(transform(
                        CONCAT(toString(`source`), '-', item_id, '-', `sp包数`), [{p}],[{r}], 0
                    ))
                WHERE `sp单包克数1` = '0' AND `sp子品类` = '饼干'
                SETTINGS mutations_sync = 1
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

        sql = '''
            WITH CONCAT(toString(`source`), '-', item_id) AS p1
            SELECT p1, MIN(toFloat32OrZero(`sp单包克数1`))
            FROM {} WHERE toFloat32OrZero(`sp单包克数1`) > 0 AND `sp子品类` = '饼干'
            AND p1 IN ( WITH 1 AS _
                SELECT CONCAT(toString(`source`), '-', item_id) FROM {} WHERE `sp单包克数1` = '0'
            )
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE `sp单包克数1` = toString(transform(CONCAT(toString(`source`), '-', item_id), [{p}],[{r}], 0))
                WHERE `sp单包克数1` = '0' AND `sp子品类` = '饼干'
                SETTINGS mutations_sync = 1
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

        sql = '''
            WITH CONCAT(toString(`source`), '-', toString(alias_all_bid), '-', `sp包数`) AS p1
            SELECT p1, ceil(AVG(toFloat32OrZero(`sp单包克数1`)))
            FROM {} WHERE toFloat32OrZero(`sp单包克数1`) > 0 AND `sp子品类` = '饼干'
            AND p1 IN ( WITH 1 AS _
                SELECT CONCAT(toString(`source`), '-', toString(clean_alias_all_bid), '-', `sp包数`) FROM {} WHERE `sp单包克数1` = '0'
            )
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包克数1` = toString(transform(
                        CONCAT(toString(`source`), '-', toString(clean_alias_all_bid), '-', `sp包数`), [{p}],[{r}], 0
                    ))
                WHERE `sp单包克数1` = '0' AND `sp子品类` = '饼干'
                SETTINGS mutations_sync = 1
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

        sql = '''
            WITH CONCAT(toString(`source`), '-', toString(alias_all_bid)) AS p1
            SELECT p1, MIN(toFloat32OrZero(`sp单包克数1`))
            FROM {} WHERE toFloat32OrZero(`sp单包克数1`) > 0 AND `sp子品类` = '饼干'
            AND p1 IN ( WITH 1 AS _
                SELECT CONCAT(toString(`source`), '-', toString(clean_alias_all_bid)) FROM {} WHERE `sp单包克数1` = '0'
            )
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包克数1` = toString(transform(CONCAT(toString(`source`), '-', toString(clean_alias_all_bid)), [{p}],[{r}], 0))
                WHERE `sp单包克数1` = '0' AND `sp子品类` = '饼干'
                SETTINGS mutations_sync = 1
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp总克数1` = toString(ROUND(toFloat32OrZero(`sp单包克数1`) * toUInt32OrZero(`sp包数`), 2))
            WHERE `sp总克数1` IN ('0', '') AND `sp子品类` = '饼干'
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def hotfix_brand(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {
            'spname':'String','spname_cn':'String','spname_en':'String','spname_cn_en':'String','spmaker':'String','spmaker_cn':'String','spmaker_en':'String',
            'spname2':'String','spname_cn2':'String','spname_en2':'String','spname_cn_en2':'String','spmaker2':'String','spmaker_cn2':'String','spmaker_en2':'String'
        })

        sql = '''
            SELECT toString(groupArray(IF(toUInt32OrZero(bid)>0,toString(dictGetOrDefault('all_brand', 'alias_bid', tuple(toUInt32(bid)), bid)),bid))),
                toString(groupArray(name)), toString(groupArray(name_cn)), toString(groupArray(name_en)), toString(groupArray(name_cn_en)),
                toString(groupArray(maker)), toString(groupArray(maker_cn)), toString(groupArray(maker_en)),
                toString(groupArray(name2)), toString(groupArray(name_cn2)), toString(groupArray(name_en2)), toString(groupArray(name_cn_en2)),
                toString(groupArray(maker2)), toString(groupArray(maker_cn2)), toString(groupArray(maker_en2))
            FROM artificial.brand_{}
        '''.format(self.cleaner.eid)
        ret = dba.query_all(sql)[0]

        sql = '''
            ALTER TABLE {} UPDATE
                `spname` = transform({c},{b},{},IF(`spname`='',dictGetOrDefault('all_brand', 'name', tuple(toUInt32(clean_alias_all_bid)), ''),`spname`)),
                `spname_cn` = transform({c},{b},{},`spname_cn`),
                `spname_en` = transform({c},{b},{},`spname_en`),
                `spname_cn_en` = transform({c},{b},{},`spname_cn_en`),
                `spmaker` = transform({c},{b},{},`spmaker`),
                `spmaker_cn` = transform({c},{b},{},`spmaker_cn`),
                `spmaker_en` = transform({c},{b},{},`spmaker_en`),
                `spname2` = transform({c},{b},{},`spname2`),
                `spname_cn2` = transform({c},{b},{},`spname_cn2`),
                `spname_en2` = transform({c},{b},{},`spname_en2`),
                `spname_cn_en2` = transform({c},{b},{},`spname_cn_en2`),
                `spmaker2` = transform({c},{b},{},`spmaker2`),
                `spmaker_cn2` = transform({c},{b},{},`spmaker_cn2`),
                `spmaker_en2` = transform({c},{b},{},`spmaker_en2`)
            WHERE 1
            SETTINGS mutations_sync = 1
        '''
        dba.execute(sql.format(
            tbl, ret[1], ret[2], ret[3], ret[4], ret[5], ret[6], ret[7], ret[8], ret[9], ret[10], ret[11], ret[12], ret[13], ret[14],
            b = ret[0], c = '''toString(dictGetOrDefault('all_brand', 'alias_bid', tuple(toUInt32(clean_alias_all_bid)), clean_alias_all_bid))'''
        ))

        dba.execute(sql.format(
            tbl, ret[1], ret[2], ret[3], ret[4], ret[5], ret[6], ret[7], ret[8], ret[9], ret[10], ret[11], ret[12], ret[13], ret[14],
            b = ret[0], c = '`sp品牌`'
        ))


    def hotfix_ferrero(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp饼干分类2':'String'})

        sql = '''
            ALTER TABLE {} DELETE WHERE cid NOT IN (
                124302001,124306001,124604001,124304001,124606001,124300001,124308001,124298001,124310001,124312001,
                124296001,124606002,124312003,124292002,124302002,124320001,124478011,124456021,50008056,50010511,
                124314001,50008055,124312004,124466010,124512008,126474004,124468007,124492008
            ) AND `source` = 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = '非饼干'
            WHERE item_id = '614137945139' AND `source` = 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp饼干分类2` = IF (`sp饼干分类` IN ('Plain Sweet', 'Plain Sweet Digestive'), 'Plain Sweet/甜薄脆', `sp饼干分类`)
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def hotfix_mdlz(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp剔除零食组合后亿滋子品类':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp是否无效链接` = '有效链接'
            WHERE `source` = 1 AND item_id = '602418051419' AND `sp是否无效链接` = '无效链接'
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp饼干分类` = 'Plain Sweet/甜薄脆'
            WHERE `sp饼干分类` IN ('Plain Sweet', 'Plain Sweet Digestive')
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除零食组合后亿滋子品类` = `sp子品类`
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除零食组合后亿滋子品类` = '非饼干'
            WHERE `clean_alias_all_bid` IN (104418,133212) AND `sp辅助_是否零食大礼包` = '零食大礼包' AND toInt32OrZero(`sp总克数1`) >= 800
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)


    def process_exx(self, tbl, prefix, logId=0):
        dba = self.cleaner.get_db('chsop')

        if tbl.find('_MDLZ_report') > 0:
            sql = '''
                ALTER TABLE {}
                UPDATE `spname` = '趣多多', `spname_cn` = '趣多多', `spname_en` = 'Chips Ahoy', `spname_cn_en` = '趣多多/Chips Ahoy',
                        `spmaker` = '亿滋/MDLZ', `spmaker_cn` = '亿滋', `spmaker_en` = 'MDLZ'
                WHERE (`spname` = '王子' AND `sp饼干分类` = 'Cookies')
                   OR (`spname` = '亿滋其他' AND `sp饼干分类` = 'Cookies' AND `sp辅助_标题包含王子` = '包含王子')
                   OR (`spname` = '达能' AND `sp饼干分类` = 'Cookies' AND `sp辅助_标题包含王子` = '包含王子')
                SETTINGS mutations_sync = 1
            '''.format(tbl)
            dba.execute(sql)

        self.cleaner.add_miss_cols(tbl, {'sp亿滋BI&KADIS厂商':'String','sp亿滋BI&KADIS品牌':'String'})

        brand = [
            ['奥利奥', '亿滋MDLZ', '奥利奥Oreo'],
            ['太平', '亿滋MDLZ', '太平Pacific'],
            ['趣多多', '亿滋MDLZ', '趣多多Chips Ahoy'],
            ['焙朗', '亿滋MDLZ', '其他Others'],
            ['露怡', '亿滋MDLZ', '其他Others'],
            ['王子', '亿滋MDLZ', '其他Others'],
            ['优冠', '亿滋MDLZ', '其他Others'],
            ['妙卡', '亿滋MDLZ', '其他Others'],
            ['乐之', '亿滋MDLZ', '乐之Ritz'],
            ['闲趣', '亿滋MDLZ', '闲趣Tuc'],
            ['吉百利', '亿滋MDLZ', '其他Others'],
            ['甜趣', '亿滋MDLZ', '其他Others'],
            ['亿滋其他', '亿滋MDLZ', '其他Others'],
            ['三只松鼠', '三只松鼠Three Squirrels', '三只松鼠Three Squirrels'],
            ['良品铺子', '良品铺子Bestore', '良品铺子Bestore'],
            ['脆脆鲨', '雀巢Nestle', '脆脆鲨CCS'],
            ['奇巧', '雀巢Nestle', '奇巧Kitkat'],
            ['卓脆', '雀巢Nestle', '卓脆Joe'],
            ['Alete', '雀巢Nestle', 'Alete'],
            ['雀巢其他', '雀巢Nestle', '其他Others'],
            ['丽芝士', '纳宝帝Nabati', '丽芝士Richeese'],
            ['丽巧克', '纳宝帝Nabati', '其他Others'],
            ['软心趣', '纳宝帝Nabati', '其他Others'],
            ['我的', '纳宝帝Nabati', '其他Others'],
            ['纳宝帝其他', '纳宝帝Nabati', '其他Others'],
            ['百奇', '格力高Glico', '百奇Pocky'],
            ['百醇', '格力高Glico', '百醇Pejoy'],
            ['百力滋', '格力高Glico', '百力滋Pretz'],
            ['星奇', '格力高Glico', '星奇Sinky'],
            ['菜园小饼', '格力高Glico', '菜园小饼Vegetable Snack'],
            ['奇蒂', '格力高Glico', '奇蒂Kittyland'],
            ['格力高其他', '格力高Glico', '其他Others'],
            ['皇冠', '丹尼诗Danish', '皇冠Danisa'],
            ['滋食', '格尔Geer Food', '滋食Sazy'],
            ['豫吉', '豫吉Yuji', '豫吉Yuji'],
            ['百草味', '百事Pepsi', '百草味Be&Cheery'],
            ['嘉士利', '嘉士利JSL', '嘉士利JSL'],
            ['好吃点', '达利Dali Foods', '好吃点HCD'],
            ['达利园', '达利Dali Foods', '其他Others'],
            ['AJI', 'AJI', 'AJI'],
            ['冬己', '冬己ddung', '冬己ddung'],
            ['江中', '其他Others', '其他Others'],
            ['法丽兹', '丰熙Sunssi', '法丽兹Franzzi'],
            ['丰熙', '丰熙Sunssi', '其他Others'],
            ['其妙', '其妙Kiemeo', '其妙Kiemeo'],
            ['茱蒂丝', '茱蒂丝Julie’s', '茱蒂丝Julie’s'],
            ['蓝罐', '费列罗Ferrero', '蓝罐Kjeldsens'],
            ['快乐河马', '费列罗Ferrero', '其他Others'],
            ['卡尔滋', '费列罗Ferrero', '其他Others'],
            ['能多益', '费列罗Ferrero', '其他Others'],
            ['费列罗其他', '费列罗Ferrero', '其他Others'],
            ['白色恋人', '其他Others', '其他Others'],
            ['奥朗探戈', '奥朗探戈Tango', '奥朗探戈Tango'],
            ['美心', '其他Others', '其他Others'],
            ['康师傅', '其他Others', '其他Others'],
            ['三牛', '三牛San Niu', '三牛San Niu'],
            ['麦维他', '其他Others', '其他Others'],
            ['马奇新新', '马奇新新Munchy‘s', '马奇新新Munchy‘s'],
            ['葡记', '葡记Paulkei', '葡记Paulkei'],
            ['欧贝拉', '欧贝拉Obera', '欧贝拉Obera'],
            ['AKOKO', 'AKOKO', 'AKOKO'],
            ['伟龙', '伟龙Weilong Food', '伟龙Weilong Food'],
            ['疯狂饼干城', '伟龙Weilong Food', '其他Others'],
            ['生活大爆炸', '伟龙Weilong Food', '其他Others'],
            ['莎布蕾', '莎布蕾Sable', '莎布蕾Sable'],
            ['云知道', '莎布蕾Sable', '其他Others']
        ]

        a = ','.join(['\'{}\''.format(v[0]) for v in brand])
        b = ','.join(['\'{}\''.format(v[1]) for v in brand])
        c = ','.join(['\'{}\''.format(v[2]) for v in brand])

        sql = '''
            ALTER TABLE {} UPDATE
                `sp亿滋BI&KADIS厂商` = transform(`spname_cn`, [{}], [{}], '其他Others'),
                `sp亿滋BI&KADIS品牌` = transform(`spname_cn`, [{}], [{}], '其他Others')
            WHERE 1
            SETTINGS mutations_sync = 1
        '''.format(tbl, a, b, a, c)
        dba.execute(sql)


    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        pass

    uuid204044 = []

    def brush_helper(self,smonth,emonth,where, clean_flag, visible, mpp, bids=[], rate=1.0, limit=10000, remove=False):
        uuids1 = []
        uuids2 = []
        uuids3 = []
        delete_id = []
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=rate, limit=limit, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if (int(alias_all_bid) in bids) or (len(bids) == 0):
                key = '{}-{}-{}'.format(source, tb_item_id, p1)
                if key in mpp:
                    uuids1.append(uuid2)
                    # if int(alias_all_bid) == 204044:
                    #     self.uuid204044.append(uuid2)
                    # else:
                    #     uuids1.append(uuid2)
                    delete_id = delete_id + mpp[key]
                if self.skip_brush(source, tb_item_id, p1, remove=remove, if_flag=False):
                    continue
                uuids3.append(uuid2)
                # if int(alias_all_bid) == 204044:
                #     self.uuid204044.append(uuid2)
                # else:
                #   uuids3.append(uuid2)
        print(len(uuids1),len(uuids2),len(uuids3),len(delete_id))
        self.cleaner.add_brush(uuids1+uuids3, clean_flag, visible_check=1, visible=visible, sales_by_uuid=sales_by_uuid1)
        if len(delete_id) > 0:
            sql = 'delete from product_lib.entity_91081_item where id in ({})'.format(','.join(['\'{}\''.format(v) for v in delete_id]))
            self.cleaner.db26.execute(sql)
            self.cleaner.db26.commit()
        return uuids1+uuids3, sales_by_uuid1
        # return uuids1,sales_by_uuid1



    def brush(self, smonth, emonth, logId=-1):
        sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()

        remove = False
        flag = True
        cname, ctbl = self.get_all_tbl()
        # ctbl = 'sop_c.entity_prod_91081_C_210513'
        clean_flag = self.cleaner.last_clean_flag() + 1
        # bids10 = [48219, 106434, 132600, 135194, 139477, 203720, 203727, 203936, 203975, 204061, 204285, 1585517, 1585685, 1677190, 3967398, 5917729, 2436876, 1735871, 955536, 22323, 2990840]
        # bids20 = [20362, 22225, 39508, 104418, 133212, 140329, 203708, 203718, 203896, 204322, 329102, 993596, 2332062, 5978016, 745757, 1065913, 845840, 173430, 3173634, 1262089, 811840, 101352, 5438317, 3961786, 6412376, 5249388, 204044]

        # sql_10 = '''
        # select distinct case when alias_bid = 0 then bid else alias_bid end from brush.all_brand where bid in (845840, 204044, 48219, 106434, 132600, 135194, 139477, 203720, 203727, 203936, 203975, 204061, 204285,1585517, 1585685, 1677190, 3967398, 5917729, 2436876, 1735871, 955536, 22323, 2990840, 204091)
        # '''
        # ret_10 = self.cleaner.db26.query_all(sql_10)
        # bids10 = list(v[0] for v in ret_10)
        # print(bids10)
        #
        # sql_20 = '''
        # select distinct case when alias_bid = 0 then bid else alias_bid end from brush.all_brand where bid in (8422, 5575184, 2436840, 16834, 48255, 87123, 89329, 104374, 108659, 118152, 131501, 131638, 131647,131911, 132283, 176676, 203703, 203757, 204080, 204132, 592982, 1746010, 1854395, 305944, 3433319,3710175, 4493804, 4880816, 133378, 203696, 5881693, 203888, 203691, 204491, 6139761, 5617687, 5838600,6107153, 757428, 204276, 282040, 204137, 60839, 1677190, 5818493, 384, 203893, 5573226,6366821, 131561, 48380, 436292, 38560, 120002, 203773, 17248, 6242395, 20362, 22225, 39508, 104418,133212, 140329, 203708, 203718, 203896, 204322, 329102, 993596, 2332062, 5978016, 745757, 1065913,173430, 3173634, 1262089, 811840, 101352, 5438317, 3961786, 6412376, 5249388)
        # '''
        # ret_20 = self.cleaner.db26.query_all(sql_20)
        # bids20_o = list(v[0] for v in ret_20)
        # # print(bids20_o)
        #
        # bids20 = []
        # for i in bids20_o:
        #     if i in bids10:
        #         continue
        #     else:
        #         bids20.append(i)
        #
        # print(bids20)




        bids10 = [845840, 204044, 48219, 106434, 132600, 135194, 139477, 203720, 203727, 203936, 203975, 204061, 204285,
                  1585517, 1585685, 1677190, 3967398, 5917729, 2436876, 1735871, 955536, 22323, 2990840, 204091]
        bids20 = [8422, 5575184, 2436840, 16834, 48255, 87123, 89329, 104374, 108659, 118152, 131501, 131638, 131647,
                  131911, 132283, 176676, 203703, 203757, 204080, 204132, 592982, 1746010, 1854395, 305944, 3433319,
                  3710175, 4493804, 4880816, 133378, 203696, 5881693, 203888, 203691, 204491, 6139761, 5617687, 5838600,
                  6401313, 5635468, 204276, 38560, 204137, 60839, 1677190, 6702072, 384, 203893, 5573226,
                  6366821, 131561, 48380, 436292, 38560, 120002, 203773, 17248, 6242395, 20362, 22225, 39508, 104418,
                  133212, 140329, 203708, 203718, 203896, 204322, 329102, 993596, 2332062, 131561, 745757, 1065913,
                  173430, 3173634, 1262089, 811840, 101352, 5438317, 3961786, 6412376, 5249388]

        # sql_2177 = 'SELECT snum, tb_item_id, real_p1 id FROM product_lib.entity_91081_item where pid = 2177 and pkey < \'{}\' '.format(smonth)
        # sql_2177 = 'select * from (SELECT snum, tb_item_id, real_p1, max(month) max_month FROM product_lib.entity_91081_item where pid = 2177 group by snum,tb_item_id,real_p1) a where max_month < \'{}\''.format(smonth)
        # ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])


        sales_by_uuid = {}
        where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and ((source !=1 or (source = 1 and (shop_type > 20 or shop_type < 10 ))) and source !=11 )'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids1, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 1, mpp_2177, rate=0.8, bids=bids10, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source = 1 and (shop_type < 20 and shop_type > 10 ) and source !=11'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids101, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 101, mpp_2177, limit=300, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and c_sp2 =\'快乐河马\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source !=11'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids21, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 21, mpp_2177, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = '(source !=1 or (source = 1 and (shop_type > 20 or shop_type < 10 ))) and  uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source !=11'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids3, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 3, mpp_2177, rate=0.3, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        uuids8, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 8, mpp_2177, rate=0.8, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = '(source !=1 or (source = 1 and (shop_type > 20 or shop_type < 10 ))) and uuid2 in (select uuid2 from {ctbl} where c_sp1 != \'饼干\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source !=11'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids1, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 1, mpp_2177, rate=0.8, bids=bids10, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = '(source !=1 or (source = 1 and (shop_type > 20 or shop_type < 10 ))) and uuid2 in (select uuid2 from {ctbl} where c_sp1 != \'饼干\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and source !=11'.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids2, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 2, mpp_2177, rate=0.8, bids=bids20, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        where = 'uuid2 in (select uuid2 from {ctbl} where (c_sp27 = \'\' or c_sp9 = \'\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and alias_all_bid=131561 '.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids2, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 110, mpp_2177, rate=1, limit=50, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        where = 'uuid2 in (select uuid2 from {ctbl} where (c_sp27 = \'\' or c_sp9 = \'\') and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and alias_all_bid=38560 '.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        uuids2, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 110, mpp_2177, rate=1, limit=50, remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'sid in (13566460,244322992,55644404) and source =11 '.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids1, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177, limit=10000,
                                                       bids=bids10, remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11 '.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids1, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177, rate=0.8,
                                                       bids=bids10, remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source = 11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids101, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177, limit=300,
                                                         remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and c_sp2 =\'快乐河马\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids21, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177,
                                                        remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids3, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177, rate=0.3,
                                                       remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids8, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 108, mpp_2177, rate=0.8,
                                                       remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 != \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids1, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 107, mpp_2177, rate=0.8,
                                                       bids=bids10, remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        sql_2177 = 'SELECT snum, tb_item_id, real_p1, id FROM product_lib.entity_91081_item where pid = 2177 and month < \'{}\''.format(
            smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        # mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}
        mpp_2177 = {}
        for v in ret:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp_2177:
                mpp_2177[k] = []
            mpp_2177[k].append(v[-1])

        # for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
        for ssmonth, eemonth in [[smonth, emonth]]:
            where = 'uuid2 in (select uuid2 from {ctbl} where c_sp1 != \'饼干\' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and source =11'.format(
                ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            uuids2, sales_by_uuid1 = self.brush_helper(ssmonth, eemonth, where, clean_flag, 109, mpp_2177, rate=0.8,
                                                       bids=bids20, remove=remove)
            sales_by_uuid.update(sales_by_uuid1)

        # print(len(uuids1), len(uuids2), len(uuids3), len(uuids8), len(uuids21), len(uuids101),len(self.uuid204044))


        return True

    def brush_0528(self, smonth, emonth, logId=-1):
        # sql = 'delete from {} where pkey >= \'{}\' and pkey < \'{}\' and flag = 0'.format(self.cleaner.get_tbl(), smonth, emonth)
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit()

        remove = False
        flag = True
        cname, ctbl = self.get_c_tbl()
        ctbl = 'sop_c.entity_prod_91081_C_210513'
        clean_flag = self.cleaner.last_clean_flag() + 1
        # sql_2177 = 'SELECT snum, tb_item_id, real_p1 id FROM product_lib.entity_91081_item where pid = 2177 and pkey < \'{}\' '.format(smonth)
        sql_2177 = 'select * from (SELECT snum, tb_item_id, real_p1, max(month) max_month FROM product_lib.entity_91081_item where pid = 2177 group by snum,tb_item_id,real_p1) a where max_month < \'{}\''.format(smonth)
        ret = self.cleaner.db26.query_all(sql_2177)
        mpp_2177 = {'{}-{}-{}'.format(v[0], v[1], v[2]): v for v in ret}

        sales_by_uuid = {}

        where = '(source !=1 or (source = 1 and (shop_type > 20 or shop_type < 10 ))) and  uuid2 in (select uuid2 from {} where sp1 = \'饼干\')'.format(ctbl)
        uuids3, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 3, mpp_2177, rate=0.8, bids=[135194,204061,203727], remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        where = 'uuid2 in (select uuid2 from {} where sp1 != \'饼干\')'.format(ctbl)
        uuids1, sales_by_uuid1 = self.brush_helper(smonth, emonth, where, clean_flag, 1, mpp_2177, rate=0.8, bids=[135194,204061,203727,845840], remove=remove)
        sales_by_uuid.update(sales_by_uuid1)

        # print(len(uuids1), len(uuids2), len(uuids3), len(uuids8), len(uuids21), len(uuids101),len(self.uuid204044))
        print(len(uuids1), len(uuids3))
        return True





# 133答题作为属性题导入131 豆子自己会去导
# INSERT INTO product_lib.entity_91081_item (
#     pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,
#     cid,real_cid,region_str,brand,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,
#     promo_price,trade,num,sales,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,
#     b_check_uid,batch_id,flag,split,img,is_set,count,visible,visible_check,clean_flag,all_bid,alias_all_bid,
#     spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,
#     spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9,spid17,
#     spid18,spid19,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp2,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,modify_sp17,modify_sp18,modify_sp19,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,sp17,sp18,sp19,modify_sp20,sp20,spid20,modify_sp21,sp21,spid21
# ) SELECT
#     a.pkey,a.snum,a.ver,a.real_p1,a.real_p2,a.uniq_k,a.uuid2,a.tb_item_id,a.tip,a.source,a.month,a.name,a.sid,a.shop_name,a.shop_type,
#     a.cid,a.real_cid,a.region_str,a.brand,a.super_bid,a.sub_brand,a.sub_brand_name,a.product,a.avg_price,a.price,a.org_price,
#     a.promo_price,a.trade,a.num,a.sales,a.prop_check,a.p1,a.trade_prop_all,a.prop_all,0,0,a.number,a.uid,a.check_uid,
#     a.b_check_uid,a.batch_id,1,0,a.img,a.is_set,a.count,100,0,0,b.alias_all_bid,b.alias_all_bid,
#     b.spid1,b.spid10,b.spid11,b.spid12,b.spid13,b.spid14,b.spid15,b.spid16,
#     b.spid2,b.spid3,b.spid4,b.spid5,b.spid6,b.spid7,b.spid8,b.spid9,b.spid17,
#     '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',''
# FROM product_lib.entity_91088_item a JOIN product_lib.product_91088 b USING(pid)
# WHERE a.flag = 2 AND (a.source, a.tb_item_id, a.real_p1) NOT IN (SELECT source, tb_item_id, real_p1 FROM product_lib.entity_91081_item);

