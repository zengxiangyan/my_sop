import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def get_extend(self, item):
        if 'rbid' not in self.cache:
            cols = ['rbid', 'parent_rbid', 'name', 'name_front', 'name_cn', 'name_en']

            aname, atbl = self.get_a_tbl()
            adba = self.cleaner.get_db(aname)
            db18 = self.cleaner.get_db('196_apollo')

            sql = 'select distinct(toUInt32(brand)) from {}'.format(atbl)
            ret = adba.query_all(sql)

            sql = '''
                select b.bid, if(c.parent_rbid>0, c.parent_rbid, b.root_brand)
                from dw_entity.root_brand as c
                INNER JOIN (select bid, root_brand from dw_entity.brand where bid in ({})) as b
                on c.rbid = b.root_brand
            '''.format(','.join([str(bid) for bid, in ret]))
            rbid_data = db18.query_all(sql)

            self.cache['brand_rbid'] = {str(info[0]): info[1] for info in rbid_data}

            sql = '''
                SELECT {} FROM dw_entity.root_brand WHERE rbid IN ({})
            '''.format(','.join(cols), ','.join([str(info[1]) for info in rbid_data]))
            ret = db18.query_all(sql)

            mpp = {}
            for v in ret:
                v = {k: v[i] for i, k in enumerate(cols)}
                mpp[v['rbid']] = v

            prbids = [str(mpp[k]['parent_rbid']) for k in mpp if mpp[k]['parent_rbid'] > 0 and mpp[k]['parent_rbid'] != mpp[k]['rbid'] and mpp[k]['parent_rbid'] not in mpp]
            if len(prbids) > 0:
                sql = 'SELECT {} FROM dw_entity.root_brand WHERE rbid IN ({})'.format(','.join(cols), ','.join(prbids))
                ret = db18.query_all(sql)

                for v in ret:
                    v = {k: v[i] for i, k in enumerate(cols)}
                    mpp[v['rbid']] = v

            self.cache['rbid'] = mpp

        if self.cache['brand_rbid'].get(item['brand']) == None or self.cache['rbid'].get(self.cache['brand_rbid'][item['brand']]) == None:
            return {'rbrand':None}

        v = self.cache['rbid'][self.cache['brand_rbid'][item['brand']]]
        if v['parent_rbid'] in self.cache['rbid']:
            v = self.cache['rbid'][v['parent_rbid']]
        return {'rbrand':v}


    def brush(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids2 = []
        ret2, ret_sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))
        # print(len(uuids2))

        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE cid IN (50023745,124994004) AND (
                clean_sales/clean_num <= 3000 OR (`spbrand_name` = 'OMRON/欧姆龙' AND clean_sales/clean_num < 10000)
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE (
                cid IN (350210,50010409,50018399,50018403,50018404,50018410,50018411,122362004,122320003)
                OR (cid = 124130011 AND `sp子品类` = '低频治疗仪')
            ) AND (
                (clean_sales/clean_num <= 500 OR clean_sales/clean_num >= 400000)
             OR (`spbrand_name` = 'OMRON/欧姆龙' AND clean_sales/clean_num < 20000)
             OR (`spbrand_name` IN ('攀高','盛阳康','六合','COFOE/可孚') AND clean_sales/clean_num < 6000)
             OR (`spbrand_name` IN ('诺嘉','BEURER','益博士','LERAVAN/乐范','健得龙') AND clean_sales/clean_num < 10000)
             OR (name LIKE '%电源线%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%导线%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%插头%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%贴片%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%电极片%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%替换%' AND clean_sales/clean_num < 3000)
             OR (name LIKE '%配件%' AND clean_sales/clean_num < 3000)
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        _, atbl = self.get_a_tbl()
        sql = '''
            SELECT uuid2 FROM {} WHERE sign = 1 AND num > 0 AND cid IN (122372004) AND sales/num > 5000 AND sales/num <= 10000 AND (
                arrayStringConcat(arrayMap((x,y) -> IF(x='颜色分类',y,''), props.name, props.value)) LIKE '%成人面罩%'
             OR arrayStringConcat(arrayMap((x,y) -> IF(x='颜色分类',y,''), props.name, props.value)) LIKE '%儿童面罩%'
             OR arrayStringConcat(arrayMap((x,y) -> IF(x='颜色分类',y,''), props.name, props.value)) LIKE '%成人咬嘴%'
            )
        '''.format(atbl)
        ret = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE uuid2 IN ({})
        '''.format(tbl, ','.join(['\'{}\''.format(v) for v, in ret]))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp雾化器类型` = '配件'
            WHERE cid IN (122372004) AND clean_sales/clean_num <= 5000 AND name LIKE '%面罩%'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE cid IN (122372004) AND clean_sales/clean_num <= 5000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            WHERE cid IN (50009104,122384001,122320002) AND clean_sales/clean_num <= 50000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp制氧机升数` = multiIf(
                    `sp辅助-是否便携` = '便携' AND clean_sales/clean_num < 300000, '',
                    `sp辅助-制氧机升数4` = '便携' AND clean_sales/clean_num >= 300000 AND `sp辅助-升数-注册证号`!='含械', '保健型便携式',
                    `sp辅助-制氧机升数4` = '便携' AND clean_sales/clean_num >= 300000 AND `sp辅助-升数-注册证号`='含械', '医用型便携式',
                    `sp辅助-制氧机升数1`='10' AND clean_sales/clean_num >= 300000, '10',
                    `sp辅助-制氧机升数1`='9' AND clean_sales/clean_num >= 300000, '9',
                    `sp辅助-制氧机升数1`='8' AND clean_sales/clean_num >= 300000, '8',
                    `sp辅助-制氧机升数1`='7' AND clean_sales/clean_num >= 300000, '7',
                    `sp辅助-制氧机升数1`='6' AND clean_sales/clean_num >= 300000, '6',
                    `sp辅助-制氧机升数1`='5' AND clean_sales/clean_num >= 190000, '5',
                    `sp辅助-制氧机升数1`='3' AND clean_sales/clean_num >= 80000, '3',
                    `sp辅助-制氧机升数1`='2' AND clean_sales/clean_num >= 80000, '2',
                    `sp辅助-制氧机升数1`='1' AND `sp辅助-升数-注册证号`='含械', '医用1L机',
                    `sp辅助-制氧机升数1`='1' AND `sp辅助-升数-注册证号`='不含械', '保健1L机',
                    `sp辅助-制氧机升数2`='10' AND clean_sales/clean_num >= 300000, '10',
                    `sp辅助-制氧机升数2`='9' AND clean_sales/clean_num >= 300000, '9',
                    `sp辅助-制氧机升数2`='8' AND clean_sales/clean_num >= 300000, '8',
                    `sp辅助-制氧机升数2`='7' AND clean_sales/clean_num >= 300000, '7',
                    `sp辅助-制氧机升数2`='6' AND clean_sales/clean_num >= 300000, '6',
                    `sp辅助-制氧机升数2`='5' AND clean_sales/clean_num >= 190000, '5',
                    `sp辅助-制氧机升数2`='3' AND clean_sales/clean_num >= 80000, '3',
                    `sp辅助-制氧机升数2`='2' AND clean_sales/clean_num >= 80000, '2',
                    `sp辅助-制氧机升数2`='1' AND `sp辅助-升数-注册证号`='含械', '医用1L机',
                    `sp辅助-制氧机升数2`='1' AND `sp辅助-升数-注册证号`='不含械', '保健1L机',
                    `sp辅助-制氧机升数3`='10' AND clean_sales/clean_num >= 300000, '10',
                    `sp辅助-制氧机升数3`='9' AND clean_sales/clean_num >= 300000, '9',
                    `sp辅助-制氧机升数3`='8' AND clean_sales/clean_num >= 300000, '8',
                    `sp辅助-制氧机升数3`='7' AND clean_sales/clean_num >= 300000, '7',
                    `sp辅助-制氧机升数3`='6' AND clean_sales/clean_num >= 300000, '6',
                    `sp辅助-制氧机升数3`='5' AND clean_sales/clean_num >= 190000, '5',
                    `sp辅助-制氧机升数3`='3' AND clean_sales/clean_num >= 80000, '3',
                    `sp辅助-制氧机升数3`='2' AND clean_sales/clean_num >= 80000, '2',
                    `sp辅助-制氧机升数3`='1' AND `sp辅助-升数-注册证号`='含械', '医用1L机',
                    `sp辅助-制氧机升数3`='1' AND `sp辅助-升数-注册证号`='不含械', '保健1L机',
                    `sp制氧机升数` = '', '其它',
                `sp制氧机升数`)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # sql = '''
        #     ALTER TABLE {} UPDATE `sp剔除` = '是'
        #     WHERE (
        #             cid IN (50012442,50023746)
        #         OR (cid = 124130011 AND `sp子品类` = '体温计')
        #     ) AND (
        #            (name LIKE '%额温%' AND clean_sales/clean_num < 4000)
        #         OR (name LIKE '%耳温%' AND clean_sales/clean_num < 4000)
        #     )
        # '''.format(tbl)
        # dba.execute(sql)

        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spalias_all_bid brandname` = IF(clean_sales/clean_num>10000,'小米/米家iHealth','小米/米家非iHealth')
            WHERE (
                    cid IN (50012442,50023746)
                OR (cid = 124130011 AND `sp子品类` = '体温计')
            ) AND clean_alias_all_bid = 990
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp中频治疗仪是否剔除` = '是'
            WHERE `sp是否中频治疗仪` = '中频治疗仪' AND clean_sales/clean_num <= 3000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp剔除` = '是'
            where cid = 122372004 and (`spr_bid` in ('2299','3189','544278','391568','547902')) and clean_sales/clean_num <= 10000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
