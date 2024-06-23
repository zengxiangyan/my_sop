import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def pre_brush_modify(self, v, products, prefix):
        if v['visible'] in [1]:
            v['flag'] = 0

    def finish_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `spcategory` = '排除', `splv1category` = '排除', `sp剔除` = '是'
            WHERE clean_brush_id = 0 AND (
                   (clean_sales/clean_num <= 1000)
                OR (clean_alias_all_bid IN (218502,51962,97604,52567) AND name LIKE '%尿素%' AND clean_sales/clean_num <= 3000)
                OR (source = 2 AND name NOT LIKE '%小样%' AND clean_sales/clean_num <= 5000)
                OR (clean_sales/clean_num >= 888800 AND clean_sales/clean_num <= 888899)
                OR (clean_sales/clean_num >= 999900 AND clean_sales/clean_num <= 999999)
                OR (`spcategory` IN ('Body care','Face','Makeup Accessories') AND clean_sales/clean_num > 400000)
                OR (`spcategory` = 'Cleansing' AND clean_sales/clean_num > 200000)
                OR (`spcategory` = 'Creams' AND clean_sales/clean_num > 1500000)
                OR (`spcategory` = 'Essences & Serums' AND clean_sales/clean_num > 2000000)
                OR (`spcategory` = 'Eye & Lip care' AND clean_sales/clean_num > 900000)
                OR (`spcategory` = 'Skincare sets' AND clean_sales/clean_num > 3000000)
                OR (`spcategory` = 'Suncare，' AND clean_sales/clean_num > 300000)
                OR (`spcategory` = 'Fragrance，' AND clean_sales/clean_num > 4000000)
                OR (`spcategory` IN ('Masks & Exfoliators','Lotions & Toners','Eyes') AND clean_sales/clean_num > 500000)
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    def brush(self, smonth, emonth, logId=-1):

        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        chsop = self.cleaner.get_db(aname)
        # bids = [26,3756783,27721,218458,218563,4787,5586,15740,47943,51945,51965,51966,52494,106578,106760,106863,146399,218039,218459,218531,218576,218811,218827,218834,218896,218970,219031,219377,219404,222713,244890,245046,245527,245590,246544,246888,392660,404633,405992,1006756,1058246,1063304,1143854,1836211,1941326,2095996,2377835,2496034,2598698,3161611,3250023,3250027,3949400,4637524,4687080,4890199,5172868,5227427,5432489,5531913,5620133,5781566,5781674,5781710]
        bids = [26,4787,5586,15740,27721,47943,51945,51965,51966,52494,106578,106760,106863,113650,218039,218458,15740,218531,218563,218576,218811,218827,218834,218896,218970,219031,219377,219404,222713,244890,245046,245527,245590,246544,246888,392660,404633,405992,1006756,1058246,1063304,1052052,1836211,1941326,2095996,2377835,2496034,2598698,3161611,3250023,3250027,245140,3949400,4637524,4786804,4890199,5172868,5227427,455585,5531913,5620133,5781566,5781674,5781710,218450,4873]
        # sql = 'select if(alias_bid=0,bid,alias_bid) from all_site.all_brand where bid in ({})'.format(','.join([str(v) for v in bids]))
        sql = 'select if(alias_bid=0,bid,alias_bid) from brush.all_brand where bid in ({})'.format(','.join([str(v) for v in bids]))
        ret = self.cleaner.db26.query_all(sql)
        # db  = self.cleaner.get_db('18_all_site')
        # where_bid = [v[0] for v in db.query_all(sql)]
        where_bid = [v[0] for v in ret]

        sql = 'select distinct(cid) from {}'.format(atbl)
        cids = [v[0] for v in chsop.query_all(sql)]
        sql = 'select distinct(alias_all_bid) from {}'.format(atbl)
        bids = [v[0] for v in chsop.query_all(sql)]
        cc = {}
        dd = {}
        for ssmonth,eemonth in self.cleaner.each_month(smonth,emonth):
            # c = 0
            for each_cid in cids:
                where = 'cid = {} and source = 1 and (shop_type > 20 or shop_type < 10 ) and alias_all_bid not in ({})'.format(each_cid, ','.join([str(v) for v in where_bid]))
                ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, rate=0.8)
                sales_by_uuid.update(sbs)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)

            for each_bid in bids:
                if each_bid not in where_bid:
                    where = 'alias_all_bid = {} and source = 1 and (shop_type > 20 or shop_type < 10 )'.format(each_bid)
                    ret, sbs = self.cleaner.process_top_anew(ssmonth,eemonth, where=where, rate=0.8)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        print('add brush', len(uuids))
        # for i in cc:
        #     print(i,cc[i])
        # for i in dd:
        #     print(i,dd[i])
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        return True


# TRUNCATE TABLE artificial_new.entity_91171_item;
# INSERT INTO artificial_new.entity_91171_item (tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,spid1,spid2,spid3,spid4,spid5,spid6,spid7,pid,number,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set,created)
# SELECT tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,avg_price,trade,num,sales,visible,p1,0,0,0,0,'','','', '', '','','',pid,number,batch_id,flag,uid,0,0,tip,img,is_set,created
# FROM artificial_new.entity_90374_item WHERE flag > 0;
# TRUNCATE TABLE product_lib.product_91171;
# INSERT into product_lib.product_91171 SELECT `pid`,`name`,`product_name`,`full_name`,`tip`,`check_flag`,`all_bid`,`alias_all_bid`,`brand_name`,`img`,`market_price`,`source`,`item_id`,`month`,`alias_pid`,`type`,`visible`,`flag`,`cid`,`category`,`sub_category`,`category_manual`,`sub_category_manual`,`trade_cid`,`uid`,`kid1`,`kid2`,`kid3`,`kid4`,`kid5`,`spid1`,'','','','','','',`spid2`,`created`,`modified`,`id`,`maker_en`,`brand_en`,`sub_brand_en`,`benefit` FROM product_lib.product_90374;

# /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_91171_item > /obsfs/91171.sql
# /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm product_lib product_91171 > /obsfs/91171p.sql
# /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91171.sql
# /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91171p.sql

# DELETE FROM product_lib_ali.entity_91171_item WHERE pid NOT IN (SELECT pid FROM product_lib_ali.product_91171) AND pid > 0 AND flag = 2;

# SELECT * FROM product_lib.product_91171 LIMIT 100;
# UPDATE product_lib_ali.product_91171 SET spid2 = '香水|香水套装/Fragrance', spid7 = '香水|香水套装/Fragrance' WHERE spid1 = '1';
# UPDATE product_lib_ali.product_91171 SET spid2 = '彩妆（男女）/Make Up', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '2';
# UPDATE product_lib_ali.product_91171 SET spid2 = '美容护肤/Skincare', spid7 = '美容护肤/Skincare' WHERE spid1 = '3';
# UPDATE product_lib_ali.product_91171 SET spid2 = '香水|香水套装/Fragrance', spid7 = '香水|香水套装/Fragrance' WHERE spid1 = '4';
# UPDATE product_lib_ali.product_91171 SET spid2 = '眼部彩妆|眉部彩妆|睫毛彩妆/Eyes', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '5';
# UPDATE product_lib_ali.product_91171 SET spid2 = '脸部彩妆/Face', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '6';
# UPDATE product_lib_ali.product_91171 SET spid2 = '唇部彩妆/Lips', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '7';
# UPDATE product_lib_ali.product_91171 SET spid2 = '彩妆工具|彩妆刷/Makeup Accessories', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '8';
# UPDATE product_lib_ali.product_91171 SET spid2 = '美甲产品|指甲油/Nails', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '9';
# UPDATE product_lib_ali.product_91171 SET spid2 = '彩妆套装/彩妆盘/Palettes & Sets', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '10';
# UPDATE product_lib_ali.product_91171 SET spid2 = '护肤工具|美容仪|洁面仪/Accessories', spid7 = '美容护肤/Skincare' WHERE spid1 = '11';
# UPDATE product_lib_ali.product_91171 SET spid2 = '身体护理|护手霜|护甲油|足贴|脱毛膏|止汗露/Body care', spid7 = '美容护肤/Skincare' WHERE spid1 = '12';
# UPDATE product_lib_ali.product_91171 SET spid2 = 'Creams（面部使用，需要根据包装英文判断）', spid7 = '美容护肤/Skincare' WHERE spid1 = '13';
# UPDATE product_lib_ali.product_91171 SET spid2 = '洁面|卸妆/Cleansing', spid7 = '美容护肤/Skincare' WHERE spid1 = '14';
# UPDATE product_lib_ali.product_91171 SET spid2 = 'Emulsions & Fluids（面部使用，需要根据包装英文判断）', spid7 = '美容护肤/Skincare' WHERE spid1 = '15';
# UPDATE product_lib_ali.product_91171 SET spid2 = 'Essences & Serums（面部使用，需要根据包装英文判断）', spid7 = '美容护肤/Skincare' WHERE spid1 = '16';
# UPDATE product_lib_ali.product_91171 SET spid2 = '眼唇护理|润唇膏/Eye & Lip Care', spid7 = '美容护肤/Skincare' WHERE spid1 = '17';
# UPDATE product_lib_ali.product_91171 SET spid2 = 'Lotions & Toners（面部使用，需要根据包装英文判断）', spid7 = '美容护肤/Skincare' WHERE spid1 = '18';
# UPDATE product_lib_ali.product_91171 SET spid2 = '面膜|脸部去角质|脸部磨砂膏/Masks & Exfoliators', spid7 = '美容护肤/Skincare' WHERE spid1 = '19';
# UPDATE product_lib_ali.product_91171 SET spid2 = '男士护理/Men skincare', spid7 = '美容护肤/Skincare' WHERE spid1 = '20';
# UPDATE product_lib_ali.product_91171 SET spid2 = '其他（香薰蜡烛放这里）/Others', spid7 = '美容护肤/Skincare' WHERE spid1 = '21';
# UPDATE product_lib_ali.product_91171 SET spid2 = '护肤套装/Skincare sets', spid7 = '美容护肤/Skincare' WHERE spid1 = '22';
# UPDATE product_lib_ali.product_91171 SET spid2 = '防晒|晒后修复/Suncare', spid7 = '美容护肤/Skincare' WHERE spid1 = '23';
# UPDATE product_lib_ali.product_91171 SET spid2 = '删除', spid7 = '删除' WHERE spid1 = '24';
# UPDATE product_lib_ali.product_91171 SET spid2 = '身体彩妆/body', spid7 = '彩妆（男女）/Make Up' WHERE spid1 = '25';
# UPDATE product_lib_ali.product_91171 SET spid1 = '';
# UPDATE product_lib_ali.entity_91171_item SET visible = 1 WHERE visible = 0;
# UPDATE product_lib_ali.`entity_91171_item` SET uid = 171 WHERE uid = 0;

# INSERT INTO product_lib.product_91171
# SELECT pid,name,product_name,full_name,tip,check_flag,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,spid1,spid2,spid3,spid4,spid5,spid6,spid7,created,modified,id,maker_en,brand_en,sub_brand_en,benefit,'','','','','','',spid16,'','','','','','','','',''
# FROM product_lib_ali.product_91171 WHERE pid NOT IN (SELECT pid FROM product_lib.product_91171);

# INSERT INTO product_lib.entity_91171_item (pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9,modify_sp23,sp23,spid23)
# SELECT pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9,modify_sp23,sp23,spid23
# FROM product_lib_hw.entity_91171_item WHERE (snum,tb_item_id,real_p1) NOT IN (SELECT snum,tb_item_id,real_p1 FROM product_lib.entity_91171_item) GROUP BY snum, tb_item_id, real_p1;

# UPDATE product_lib.product_91171 a JOIN product_lib_ali.product_91171 b USING(pid) SET a.name = b.name WHERE a.name != b.name;
