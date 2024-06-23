import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_2(self, smonth, emonth):
        remove = False
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []

        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, others_ratio=0.05,sp=5)
        for uuid2, source, tb_item_id, p1,cid in ret1:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=2000, random=True, others_ratio=0.05, sp=5)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))

        return True

    def brush_xxxx(self, smonth, emonth):
        remove = False
        rate = 0.75
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        where1 = 'uuid2 in (select uuid2 from {} where sp6=\'是\' or sp5 =\'口香糖\')'.format(self.get_clean_tbl())
        where2 = 'uuid2 in (select uuid2 from {} where sp6=\'是\')'.format(self.get_clean_tbl())
        where3 = 'uuid2 in (select uuid2 from {} where sp5 =\'口香糖\')'.format(self.get_clean_tbl())
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=rate, limit=9999999999, where=where3)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids1.append(uuid2)

        print(rate, len(uuids1))

        return True

    def brush_0202(self, smonth, emonth):
        remove = False
        rate = 0.75
        clean_flag = self.cleaner.last_clean_flag() + 1
        sp5s = ['软糖-太妃糖',
                '软糖-棉花糖',
                '软糖-QQ糖/果汁软糖',
                '软糖-口嚼糖',
                '硬糖-压片糖',
                '硬糖-夹心酥糖',
                '硬糖-硬糖',
                '棒棒糖',
                '口香糖',
                '混合',
                '其它']

        sales_by_uuid = {}

        uuids1 = []
        for sp5 in sp5s:
            where = 'uuid2 in (select uuid2 from {} where sp5 =\'{}\')'.format(self.get_clean_tbl(), sp5)
            ret1, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, limit=200, where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
                if self.skip_brush(source, tb_item_id, p1, remove=remove):
                    continue
                uuids1.append(uuid2)

        uuids2 = []
        where2 = 'uuid2 in (select uuid2 from {} where sp6=\'是\')'.format(self.get_clean_tbl())
        ret1, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, rate=rate, limit=9999999999, where=where2)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids2.append(uuid2)

        uuids3 = []
        where3 = 'uuid2 in (select uuid2 from {} where sp5 =\'口香糖\')'.format(self.get_clean_tbl())
        ret1, sales_by_uuid1= self.cleaner.process_top(smonth, emonth, rate=rate, limit=9999999999, where=where3)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids3.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, visible=2, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids3, clean_flag, visible=3, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):
        remove = False
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        # where = 'sid in (9281929,189139876) and alias_all_bid = 333019 '
        # where = 'sid = 10566463 and alias_all_bid in (333019, 133163)'
        # where = 'sid in (1000100813,704577,597601) and alias_all_bid in (333019, 133163)'
        # where = '(sid in (9281929,189139876) and alias_all_bid = 333019 ) or (sid = 10566463 and alias_all_bid in (333019, 133163)) or (sid in (1000100813,704577,597601) and alias_all_bid in (333019, 133163))'
        where = 'item_id=\'592339944806\''
        ret1, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid,alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids1.append(uuid2)
        self.cleaner.add_brush(uuids1, clean_flag, 1, visible=5, sales_by_uuid=sales_by_uuid)
        print('add: ', len(uuids1))

        return True

    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp出数用是否薄荷糖':'String','sp总重量(千克)1':'String','sp单包重量(g)1':'String'})

        sql = '''
            ALTER TABLE {} UPDATE
                `sp店铺分类` = multiIf(`sp店铺分类` = 'EKA_FSS', 'EKA', `sp店铺分类` = '', 'C2C', `sp店铺分类`),
                `sp品牌` = '其他/Others', `sp厂商` = '其他厂商',
                `sp单包重量(g)` = REPLACE(`sp单包重量(g)`, 'g', ''), `sp单包重量(g)1` = REPLACE(`sp单包重量(g)1`, 'g', ''),
                `sp包数` = REPLACE(`sp包数`, '包', '')
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用是否薄荷糖` = '是'
            WHERE (`sp分类` IN ('软糖-QQ糖/果汁软糖','软糖-口嚼糖','硬糖-硬糖') AND `sp口味` = '薄荷味')
               OR (`sp分类` IN ('硬糖-压片糖'))
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp总重量(千克)` = ROUND(
                    toFloat32(IF(`sp单包重量(g)`='','0',`sp单包重量(g)`)) * toFloat32(IF(`sp包数`='','0',`sp包数`)) / 1000, 3
                )
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp包数` = '1'
            WHERE `sp子品类` = '糖果' AND `sp包数` IN ('0','')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 先给所有数据刷 单包重量(g)1 = 0
        sql = '''
            ALTER TABLE {} UPDATE
                `sp单包重量(g)1` = toString(toFloat32OrZero(`sp单包重量(g)`))
            WHERE `sp单包重量(g)1` IN ('', '0') AND `sp子品类` = '糖果'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        _, etbl = self.get_e_tbl()

        sql = '''
            WITH `sp单包重量(g)` AS a, `sp包数` AS b
            SELECT alias_all_bid, sum(toFloat32(a)*toInt32(b)*sales)/sum(num) FROM {}
            WHERE toFloat32OrZero(a) >= 5 AND b != '' AND date >= '2020-01-01' AND date < '2021-01-01'
            AND `sp子品类` = '糖果'
            GROUP BY alias_all_bid
        '''.format(etbl)
        ret = dba.query_all(sql)

        b, r = [str(v[0]) for v in ret], [str(v[1]) for v in ret]

        sql = '''
            ALTER TABLE {} UPDATE
                `sp单包重量(g)1` = toString(toFloat32(ceil( transform(clean_alias_all_bid,[{b}],[{r}],0) * (clean_price) / toUInt32OrZero(`sp包数`) )))
            WHERE `sp单包重量(g)1` = '0' AND `sp子品类` = '糖果'
        '''.format(tbl, b=','.join(b), r=','.join(r))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp单包重量(g)1` = toString(toFloat32OrZero(`sp单包重量(g)`))
            WHERE (toFloat32OrZero(`sp单包重量(g)1`) >= 15000 OR `sp单包重量(g)1` = '0')
              AND `sp子品类` = '糖果'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            WITH CONCAT(toString(`source`), '-', item_id, '-', `sp包数`) AS p1
            SELECT p1, ceil(AVG(toFloat32OrZero(`sp单包重量(g)1`)))
            FROM {} WHERE p1 IN (
                SELECT CONCAT(toString(`source`), '-', item_id, '-', `sp包数`) FROM {} WHERE `sp单包重量(g)1` = '0'
            )
            AND toFloat32OrZero(`sp单包重量(g)1`) > 0 AND `sp子品类` = '糖果'
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包重量(g)1` = toString(transform( CONCAT(toString(`source`), '-', item_id, '-', `sp包数`), [{p}],[{r}], 0 ))
                WHERE `sp单包重量(g)1` = '0' AND `sp子品类` = '糖果'
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            WITH CONCAT(toString(`source`), '-', item_id) AS p1
            SELECT p1, toFloat32(MIN(toFloat32OrZero(`sp单包重量(g)1`)))
            FROM {} WHERE p1 IN (
                WITH CONCAT(toString(`source`), '-', item_id) AS p1
                SELECT p1 FROM {} WHERE `sp单包重量(g)1` = '0'
            )
            AND toFloat32OrZero(`sp单包重量(g)1`) > 0 AND `sp子品类` = '糖果'
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]
            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包重量(g)1` = toString(transform(CONCAT(toString(`source`), '-', item_id), [{p}],[{r}], 0))
                WHERE `sp单包重量(g)1` = '0' AND `sp子品类` = '糖果'
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            WITH CONCAT(toString(`source`), '-', toString(alias_all_bid), '-', `sp包数`) AS p1
            SELECT p1, ceil(AVG(toFloat32OrZero(`sp单包重量(g)1`)))
            FROM {} WHERE p1 IN (
                SELECT CONCAT(toString(`source`), '-', toString(clean_alias_all_bid), '-', `sp包数`)
                FROM {} WHERE `sp单包重量(g)1` = '0'
            )
            AND toFloat32OrZero(`sp单包重量(g)1`) > 0
            AND `sp子品类` = '糖果'
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]

            sql = '''
                ALTER TABLE {} UPDATE
                    `c_props.name` = arrayConcat(
                        arrayFilter((x) -> x NOT IN ['单包重量(g)1'], `c_props.name`),
                        ['单包重量(g)1']
                    ),
                    `c_props.value` = arrayConcat(
                        arrayFilter((k, x) -> x NOT IN ['单包重量(g)1'], `c_props.value`, `c_props.name`),
                        [
                            toString(transform(
                                CONCAT(toString(`source`), '-', toString(clean_alias_all_bid), '-', `sp包数`),
                                [{p}],[{r}], 0
                            ))
                        ]
                    )
                WHERE `sp单包重量(g)1` = '0'
                  AND `sp子品类` = '糖果'
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            WITH CONCAT(toString(`source`), '-', toString(alias_all_bid)) AS p1
            SELECT p1, toFloat32(MIN(toFloat32OrZero(`sp单包重量(g)1`)))
            FROM {} WHERE p1 IN (
                WITH CONCAT(toString(`source`), '-', toString(clean_alias_all_bid)) AS p1
                SELECT p1 FROM {} WHERE `sp单包重量(g)1` = '0'
            )
            AND toFloat32OrZero(`sp单包重量(g)1`) > 0 AND `sp子品类` = '糖果'
            GROUP BY p1
        '''.format(etbl, tbl)
        ret = dba.query_all(sql)
        if len(ret) > 0:
            a = ['\'{}\''.format(v[0]) for v in ret]
            b = [str(v[1]) for v in ret]

            sql = '''
                ALTER TABLE {} UPDATE
                    `sp单包重量(g)1` = toString(transform(CONCAT(toString(`source`), '-', toString(clean_alias_all_bid)), [{p}],[{r}], 0))
                WHERE `sp单包重量(g)1` = '0' AND `sp子品类` = '糖果'
            '''.format(tbl, p=','.join(a), r=','.join(b))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp总重量(千克)1` = toString(ROUND(toFloat32OrZero(`sp单包重量(g)1`) * toUInt32OrZero(`sp包数`) / 1000, 3))
            WHERE `sp总重量(千克)1` IN ('0', '') AND `sp子品类` = '糖果'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp单包重量(g)1` = IF(toFloat32OrZero(`sp单包重量(g)1`)=0, '', CONCAT(toString(ROUND(toFloat32OrZero(`sp单包重量(g)1`), 2)), 'g')),
                `sp总重量(千克)1` = IF(toFloat32OrZero(`sp总重量(千克)1`)=0, '', CONCAT(toString(ROUND(toFloat32OrZero(`sp总重量(千克)1`), 3)), 'kg'))
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def finish(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp出数用拼sku':'String','sp出数用-是否奶糖':'String'})

        sql = '''
            ALTER TABLE {} UPDATE
                `sp出数用拼sku` = CONCAT( `sp品牌`,'+',`sp口味`,'+',`sp细化口味`,'+',`sp单包重量(g)1`,'+',`sp包数` )
            WHERE `clean_brush_id` > 0 AND `sp品牌` IN ('炫迈/Stride','荷氏/Halls','益达/Extra','绿箭/DM')
              AND `sp子品类` = '糖果'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp出数用-是否奶糖` = multiIf(
                    `sp分类` = '硬糖-硬糖' AND `sp口味` = '奶味', '是',
                    `sp分类` = '软糖-口嚼糖' AND `sp口味` = '奶味', '是',
                    `sp分类` = '软糖-太妃糖', '是',
                    '否'
                )
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'SELECT min(toYYYYMM(date)), max(toYYYYMM(date)) FROM {}'.format(tbl)
        rrr = dba.query_all(sql)

        sql = '''
            SELECT uuid2 FROM {} WHERE `sp子品类`='糖果' AND (`source`, sid, item_id, `date`, trade_props_arr) IN (
                SELECT `source`, sid, item_id, `date`, trade_props_arr FROM sop_e.entity_prod_91024_E
                WHERE `sp子品类`='巧克力' AND toYYYYMM(date) >= {} AND toYYYYMM(date) <= {}
            )
        '''.format(tbl, rrr[0][0], rrr[0][1])
        rrr = dba.query_all(sql)
        rrr = [str(v) for v, in rrr]

        if len(rrr) > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp子品类` = '巧克力'
                WHERE uuid2 IN ({})
            '''.format(tbl, ','.join(rrr))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)