import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):
    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
            SELECT toString(groupArray(concat(ss,'_', ssid))), toString(groupArray(stype)) FROM (
                SELECT toString(`source`) ss, toString(sid) ssid, argMin(shop_type, `from`) stype
                FROM sop_e.entity_prod_{}_ALL_ECSHOP WHERE shop_type != '' GROUP BY `source`, sid
            )
        '''.format(self.cleaner.eid)
        rr1 = dba.query_all(sql)

        dbb = self.cleaner.get_db('18_apollo')
        sql = '''
            SELECT IF(b.alias_bid=0,bid,b.alias_bid), a.maker
            FROM all_site.all_brand_maker a JOIN all_site.all_brand b USING (bid)
            ORDER BY a.maker_id
        '''
        rr2 = dbb.query_all(sql)
        rr2a = [str(k) for k,v, in rr2 if v is not None]
        rr2b = ['\'{}\''.format(v.replace('\'', '\\\'')) for k,v, in rr2 if v is not None]

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='店铺分类', transform(concat(toString(`source`),'_', toString(sid)), {}, {}, v),
                k='厂商', transform(c_alias_all_bid, [{}], [{}], v),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl, rr1[0][0], rr1[0][1], ','.join(rr2a), ','.join(rr2b))
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
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                k='厂商', multiIf(
                    v='纳宝帝', 'Nabati/纳宝帝',
                    v='亿滋', 'MONDELEZ/亿滋',
                    c_alias_all_bid=2332062, 'Yu Ji/豫吉',
                    c_alias_all_bid=5978016, 'Zhi Shi/滋食',
                    c_alias_all_bid=22225, 'ddung/冬己',
                    c_alias_all_bid=5887350, '不多言',
                v),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    def brush(self, smonth, emonth):
        uuids_hema = []
        # where = 'uuid2 in (select uuid2 from {} where sp1 = \'饼干-开心河马\')'.format(self.get_clean_tbl())
        where = "(source in ('tmall','tb') and cid in (50008055,50008056,124302002,124312003,124312004,124320001,124456021,124466010,124468007,124478011,124492008,124512008,126474004)) " \
                "or (source = 'jd' and cid in (1594,5021)) " \
                "or (source = 'kaola' and cid = 2153) " \
                "or (source = 'suning' and cid in (500402,500506)) " \
                "or (source = 'gome' and cid in (16035551,16035560)) " \
                "or (source = 'jumei' and cid in (465,1880))"

        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=where, if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids_hema.append(uuid2)

        # 淘宝每月top300
        uuids = []
        where = 'source = \'tb\' and uuid2 in (select uuid2 from {} where sp1=\'饼干\')'.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=300, where=where,if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids.append(uuid2)

        # 其他平台 前30和前80分开建
        uuids30 = []
        where = 'source != \'tb\' and uuid2 in (select uuid2 from {} where sp1=\'饼干\')'.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.3, where=where,if_bid=True)
        for uuid2, source, tb_item_id, p1,alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            uuids30.append(uuid2)

        uuids3080 = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where,if_bid=True)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=False):
                continue
            if uuid2 not in uuids30:
                uuids3080.append(uuid2)

        # uuids105 = []
        # where = "uuid2 in (select uuid2 from {} where sp1=\'非饼干\') and alias_all_bid in (455298,203770,955539,757556,1473654,20362,106434,131647,204878," \
        #         "3627113,48219,135194,43057,203936,4880816,204044,139477,1065913,599,6004220,87508,21179,176612,131612,5490576,2527726,1676988,1677190," \
        #         "5573226,133212,203720,89329,203896,203727,204061,204080,109115,2823006)".format(self.get_c_tbl()[1])
        # # where = "uuid2 in (select uuid2 from {} where sp1=\'非饼干\')".format(self.get_c_tbl()[1])
        # ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
        # for uuid2, source, tb_item_id, p1, cid in ret:
        #     if self.skip_brush(source, tb_item_id, p1, remove=False):
        #         continue
        #     uuids105.append(uuid2)

        # print(len(uuids_hema))
        # print(min(sales_by_uuid.values()))
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, visible=101)
        # self.cleaner.add_brush(uuids30, clean_flag, visible=3)
        # self.cleaner.add_brush(uuids3080, clean_flag, visible=8)
        # self.cleaner.add_brush(uuids_hema, clean_flag, visible=21)
        # self.cleaner.add_brush(uuids105, clean_flag, visible=105)
        print('visible 101 ', len(uuids), 'vsible 3 ',len(uuids30), 'visible 8 ',len(uuids3080),'visible 21 ', len(uuids_hema))
        # print('visible105:', len(uuids105))
        return True


    def test(self):
        # 导旧答题
        # TRUNCATE TABLE artificial_new.`entity_91088_item`;

        # INSERT INTO artificial_new.`entity_91088_item` (
        # id,tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,
        # spid1,spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9,spid10,spid11,spid12,spid13,spid14,spid15,spid16,
        # pid,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set,created,modified,number
        # ) SELECT id,tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,0,0,0,visible_check,
        # '','','','','','','','','','','','','','','','',
        # pid,batch_id,flag,uid,0,0,1,img,is_set,created,modified,number
        # FROM artificial_new.`entity_90319_item` WHERE flag > 0 AND uid > 0;

        # truncate TABLE product_lib.product_91088;
        # INSERT into product_lib.product_91088(
        # pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,
        # spid5,spid6,spid10,spid8,spid1,spid2,spid7,spid4,spid9,spid3,spid11,spid12,spid13,spid14,spid15,spid16)
        # SELECT pid,name,product_name,full_name,1,alias_all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,
        # spid1,spid2,spid3,spid4,'','','','','','','','','','','',''
        # FROM product_lib.product_90319;

        # /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_91088_item > /obsfs/91088.sql
        # /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91088.sql
        # /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm product_lib product_91088 > /obsfs/91088p.sql
        # /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91088p.sql

        # CREATE TABLE product_lib_hw.entity_91088_item LIKE product_lib.entity_91088_item;
        # p3 scripts/conver_brush_ali2hw.py

        # INSERT INTO product_lib.entity_91088_item (
        # pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp2,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9
        # ) SELECT
        # pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp2,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp2,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9
        # FROM product_lib_hw.entity_91088_item WHERE (source, tb_item_id, real_p1) NOT IN (SELECT source, tb_item_id, real_p1 FROM product_lib.entity_91088_item);

        # SELECT pid, a.alias_all_bid, b.alias_all_bid, a.name, b.name FROM product_lib.product_91088 a RIGHT JOIN product_lib_ali.product_91088 b USING (pid)
        # WHERE a.name != b.name OR a.alias_all_bid IS NULL OR b.alias_all_bid IS NULL;

        # INSERT INTO product_lib.product_91088 (
        # pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,
        # sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
        # spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9,spid17
        # ) SELECT
        # pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,
        # sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
        # spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid2,spid3,spid4,spid5,spid6,spid7,spid8,spid9,''
        # FROM product_lib_ali.product_91088 WHERE pid >= 4319;

        pass