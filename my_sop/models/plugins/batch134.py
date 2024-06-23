import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        cname,ctbl = self.get_c_tbl()
        where = 'uuid2 in (select uuid2 from {} where sp4=\'瑞士莲冰山巧克力\')'.format(ctbl)
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100, where=where)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret1:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print(len(uuids1))

        return True

# INSERT INTO product_lib.product_91130x (
# name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,
# sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
# spid2,spid8,spid12,spid10,spid14,spid15,spid20,spid38,spid39
# )
# SELECT
# name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,0,type,visible,flag,cid,category,sub_category,category_manual,
# sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
# spid1,spid3,spid4,spid5,spid6,spid7,spid8,spid17,spid18
# FROM product_lib.product_90591;
# INSERT INTO product_lib.product_91130x (
# name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,
# sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
# spid1,spid3,spid5,spid4,spid9,spid11,spid13,spid14,spid16,spid17,spid29,spid30,spid31,spid18,spid19
# )
# SELECT
# name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,0,type,visible,flag,cid,category,sub_category,category_manual,
# sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,
# spid1,spid2,spid5,spid7,spid9,spid10,spid11,spid12,spid13,spid14,spid27,spid28,spid29,concat(spid30,spid31),spid35
# FROM product_lib.product_90996;
# UPDATE product_lib.product_91130x SET spid1 = '' WHERE spid1 IS NULL;
# UPDATE product_lib.product_91130x SET spid2 = '' WHERE spid2 IS NULL;
# UPDATE product_lib.product_91130x SET spid3 = '' WHERE spid3 IS NULL;
# UPDATE product_lib.product_91130x SET spid4 = '' WHERE spid4 IS NULL;
# UPDATE product_lib.product_91130x SET spid5 = '' WHERE spid5 IS NULL;
# UPDATE product_lib.product_91130x SET spid6 = '' WHERE spid6 IS NULL;
# UPDATE product_lib.product_91130x SET spid7 = '' WHERE spid7 IS NULL;
# UPDATE product_lib.product_91130x SET spid8 = '' WHERE spid8 IS NULL;
# UPDATE product_lib.product_91130x SET spid9 = '' WHERE spid9 IS NULL;
# UPDATE product_lib.product_91130x SET spid10 = '' WHERE spid10 IS NULL;
# UPDATE product_lib.product_91130x SET spid11 = '' WHERE spid11 IS NULL;
# UPDATE product_lib.product_91130x SET spid12 = '' WHERE spid12 IS NULL;
# UPDATE product_lib.product_91130x SET spid13 = '' WHERE spid13 IS NULL;
# UPDATE product_lib.product_91130x SET spid14 = '' WHERE spid14 IS NULL;
# UPDATE product_lib.product_91130x SET spid15 = '' WHERE spid15 IS NULL;
# UPDATE product_lib.product_91130x SET spid16 = '' WHERE spid16 IS NULL;
# UPDATE product_lib.product_91130x SET spid17 = '' WHERE spid17 IS NULL;
# UPDATE product_lib.product_91130x SET spid18 = '' WHERE spid18 IS NULL;
# UPDATE product_lib.product_91130x SET spid19 = '' WHERE spid19 IS NULL;
# UPDATE product_lib.product_91130x SET spid20 = '' WHERE spid20 IS NULL;
# UPDATE product_lib.product_91130x SET spid21 = '' WHERE spid21 IS NULL;
# UPDATE product_lib.product_91130x SET spid22 = '' WHERE spid22 IS NULL;
# UPDATE product_lib.product_91130x SET spid23 = '' WHERE spid23 IS NULL;
# UPDATE product_lib.product_91130x SET spid24 = '' WHERE spid24 IS NULL;
# UPDATE product_lib.product_91130x SET spid25 = '' WHERE spid25 IS NULL;
# UPDATE product_lib.product_91130x SET spid26 = '' WHERE spid26 IS NULL;
# UPDATE product_lib.product_91130x SET spid27 = '' WHERE spid27 IS NULL;
# UPDATE product_lib.product_91130x SET spid28 = '' WHERE spid28 IS NULL;
# UPDATE product_lib.product_91130x SET spid29 = '' WHERE spid29 IS NULL;
# UPDATE product_lib.product_91130x SET spid30 = '' WHERE spid30 IS NULL;
# UPDATE product_lib.product_91130x SET spid31 = '' WHERE spid31 IS NULL;
# UPDATE product_lib.product_91130x SET spid32 = '' WHERE spid32 IS NULL;
# UPDATE product_lib.product_91130x SET spid33 = '' WHERE spid33 IS NULL;
# UPDATE product_lib.product_91130x SET spid34 = '' WHERE spid34 IS NULL;
# UPDATE product_lib.product_91130x SET spid35 = '' WHERE spid35 IS NULL;
# UPDATE product_lib.product_91130x SET spid36 = '' WHERE spid36 IS NULL;
# UPDATE product_lib.product_91130x SET spid37 = '' WHERE spid37 IS NULL;
# UPDATE product_lib.product_91130x SET spid38 = '' WHERE spid38 IS NULL;
# UPDATE product_lib.product_91130x SET spid39 = '' WHERE spid39 IS NULL;

# UPDATE product_lib.product_91130 SET spid19 = '' WHERE spid19 = '不知道';
# UPDATE product_lib.product_91130
# SET spid20 = REPLACE(concat(round(REPLACE(REPLACE(REPLACE(REPLACE(spid18,'瓶',''),'袋',''),'片',''),'粒','')*REPLACE(REPLACE(spid19,'ml',''),'g',''), 1), IF(LOCATE('g',spid19)>0,'g','ml')),'.0','')
# WHERE spid20 = '' AND spid19 != '';

# INSERT INTO product_lib.entity_91130_itemx (
# pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,
# all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,
# clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,number,uid,batch_id,flag,split,img,is_set,count,created
# )
# SELECT a.pkey,a.snum,a.ver,a.real_p1,a.real_p2,a.uniq_k,a.uuid2,a.tb_item_id,a.tip,a.source,a.month,a.name,a.sid,a.shop_name,a.shop_type,a.cid,a.real_cid,a.region_str,a.brand,
# a.all_bid,a.alias_all_bid,a.super_bid,a.sub_brand,a.sub_brand_name,a.product,a.avg_price,a.price,a.org_price,a.promo_price,a.trade,a.num,a.sales,a.visible,a.visible_check,
# a.clean_flag,a.prop_check,a.p1,a.trade_prop_all,a.prop_all,c.pid,a.number,a.uid,a.batch_id,a.flag,a.split,a.img,a.is_set,a.count,a.created
# FROM product_lib.product_90996 b
# JOIN product_lib.product_91130 c ON ((b.name=REPLACE(c.name,'___套包（维生素）','套包')) AND b.alias_all_bid=c.alias_all_bid)
# JOIN product_lib.entity_90996_item a ON (a.pid=b.pid)
# WHERE a.flag = 2;
# FROM product_lib.product_90591 b
# JOIN product_lib.product_91130 c ON ((b.name=REPLACE(c.name,'___套包（钙）','套包')) AND b.alias_all_bid=c.alias_all_bid)
# JOIN product_lib.entity_90591_item a ON (a.pid=b.pid)
# WHERE a.flag = 2;

# update product_lib.entity_91130_itemx set modify_sp1='';
# update product_lib.entity_91130_itemx set modify_sp10='';
# update product_lib.entity_91130_itemx set modify_sp11='';
# update product_lib.entity_91130_itemx set modify_sp12='';
# update product_lib.entity_91130_itemx set modify_sp13='';
# update product_lib.entity_91130_itemx set modify_sp14='';
# update product_lib.entity_91130_itemx set modify_sp15='';
# update product_lib.entity_91130_itemx set modify_sp16='';
# update product_lib.entity_91130_itemx set modify_sp17='';
# update product_lib.entity_91130_itemx set modify_sp18='';
# update product_lib.entity_91130_itemx set modify_sp19='';
# update product_lib.entity_91130_itemx set modify_sp2='';
# update product_lib.entity_91130_itemx set modify_sp20='';
# update product_lib.entity_91130_itemx set modify_sp21='';
# update product_lib.entity_91130_itemx set modify_sp22='';
# update product_lib.entity_91130_itemx set modify_sp23='';
# update product_lib.entity_91130_itemx set modify_sp24='';
# update product_lib.entity_91130_itemx set modify_sp25='';
# update product_lib.entity_91130_itemx set modify_sp26='';
# update product_lib.entity_91130_itemx set modify_sp27='';
# update product_lib.entity_91130_itemx set modify_sp28='';
# update product_lib.entity_91130_itemx set modify_sp29='';
# update product_lib.entity_91130_itemx set modify_sp3='';
# update product_lib.entity_91130_itemx set modify_sp30='';
# update product_lib.entity_91130_itemx set modify_sp31='';
# update product_lib.entity_91130_itemx set modify_sp32='';
# update product_lib.entity_91130_itemx set modify_sp33='';
# update product_lib.entity_91130_itemx set modify_sp4='';
# update product_lib.entity_91130_itemx set modify_sp5='';
# update product_lib.entity_91130_itemx set modify_sp6='';
# update product_lib.entity_91130_itemx set modify_sp7='';
# update product_lib.entity_91130_itemx set modify_sp8='';
# update product_lib.entity_91130_itemx set modify_sp9='';
# update product_lib.entity_91130_itemx set sp1='';
# update product_lib.entity_91130_itemx set sp10='';
# update product_lib.entity_91130_itemx set sp11='';
# update product_lib.entity_91130_itemx set sp12='';
# update product_lib.entity_91130_itemx set sp13='';
# update product_lib.entity_91130_itemx set sp14='';
# update product_lib.entity_91130_itemx set sp15='';
# update product_lib.entity_91130_itemx set sp16='';
# update product_lib.entity_91130_itemx set sp17='';
# update product_lib.entity_91130_itemx set sp18='';
# update product_lib.entity_91130_itemx set sp19='';
# update product_lib.entity_91130_itemx set sp2='';
# update product_lib.entity_91130_itemx set sp20='';
# update product_lib.entity_91130_itemx set sp21='';
# update product_lib.entity_91130_itemx set sp22='';
# update product_lib.entity_91130_itemx set sp23='';
# update product_lib.entity_91130_itemx set sp24='';
# update product_lib.entity_91130_itemx set sp25='';
# update product_lib.entity_91130_itemx set sp26='';
# update product_lib.entity_91130_itemx set sp27='';
# update product_lib.entity_91130_itemx set sp28='';
# update product_lib.entity_91130_itemx set sp29='';
# update product_lib.entity_91130_itemx set sp3='';
# update product_lib.entity_91130_itemx set sp30='';
# update product_lib.entity_91130_itemx set sp31='';
# update product_lib.entity_91130_itemx set sp32='';
# update product_lib.entity_91130_itemx set sp33='';
# update product_lib.entity_91130_itemx set sp4='';
# update product_lib.entity_91130_itemx set sp5='';
# update product_lib.entity_91130_itemx set sp6='';
# update product_lib.entity_91130_itemx set sp7='';
# update product_lib.entity_91130_itemx set sp8='';
# update product_lib.entity_91130_itemx set sp9='';
# update product_lib.entity_91130_itemx set spid1='';
# update product_lib.entity_91130_itemx set spid10='';
# update product_lib.entity_91130_itemx set spid11='';
# update product_lib.entity_91130_itemx set spid12='';
# update product_lib.entity_91130_itemx set spid13='';
# update product_lib.entity_91130_itemx set spid14='';
# update product_lib.entity_91130_itemx set spid15='';
# update product_lib.entity_91130_itemx set spid16='';
# update product_lib.entity_91130_itemx set spid17='';
# update product_lib.entity_91130_itemx set spid18='';
# update product_lib.entity_91130_itemx set spid19='';
# update product_lib.entity_91130_itemx set spid2='';
# update product_lib.entity_91130_itemx set spid20='';
# update product_lib.entity_91130_itemx set spid21='';
# update product_lib.entity_91130_itemx set spid22='';
# update product_lib.entity_91130_itemx set spid23='';
# update product_lib.entity_91130_itemx set spid24='';
# update product_lib.entity_91130_itemx set spid25='';
# update product_lib.entity_91130_itemx set spid26='';
# update product_lib.entity_91130_itemx set spid27='';
# update product_lib.entity_91130_itemx set spid28='';
# update product_lib.entity_91130_itemx set spid29='';
# update product_lib.entity_91130_itemx set spid3='';
# update product_lib.entity_91130_itemx set spid30='';
# update product_lib.entity_91130_itemx set spid31='';
# update product_lib.entity_91130_itemx set spid32='';
# update product_lib.entity_91130_itemx set spid33='';
# update product_lib.entity_91130_itemx set spid4='';
# update product_lib.entity_91130_itemx set spid5='';
# update product_lib.entity_91130_itemx set spid6='';
# update product_lib.entity_91130_itemx set spid7='';
# update product_lib.entity_91130_itemx set spid8='';
# update product_lib.entity_91130_itemx set spid9='';
# update product_lib.entity_91130_itemx set modify_sp34='';
# update product_lib.entity_91130_itemx set modify_sp35='';
# update product_lib.entity_91130_itemx set modify_sp36='';
# update product_lib.entity_91130_itemx set modify_sp37='';
# update product_lib.entity_91130_itemx set sp34='';
# update product_lib.entity_91130_itemx set sp35='';
# update product_lib.entity_91130_itemx set sp36='';
# update product_lib.entity_91130_itemx set sp37='';
# update product_lib.entity_91130_itemx set spid34='';
# update product_lib.entity_91130_itemx set spid35='';
# update product_lib.entity_91130_itemx set spid36='';
# update product_lib.entity_91130_itemx set spid37='';

# INSERT INTO product_lib.entity_91130_item (
# pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp23,modify_sp24,modify_sp25,modify_sp26,modify_sp27,modify_sp28,modify_sp29,modify_sp3,modify_sp30,modify_sp31,modify_sp32,modify_sp33,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29,sp3,sp30,sp31,sp32,sp33,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid29,spid3,spid30,spid31,spid32,spid33,spid4,spid5,spid6,spid7,spid8,spid9,modify_sp34,modify_sp35,modify_sp36,modify_sp37,sp34,sp35,sp36,sp37,spid34,spid35,spid36,spid37
# ) SELECT pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp23,modify_sp24,modify_sp25,modify_sp26,modify_sp27,modify_sp28,modify_sp29,modify_sp3,modify_sp30,modify_sp31,modify_sp32,modify_sp33,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp23,sp24,sp25,sp26,sp27,sp28,sp29,sp3,sp30,sp31,sp32,sp33,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,spid29,spid3,spid30,spid31,spid32,spid33,spid4,spid5,spid6,spid7,spid8,spid9,modify_sp34,modify_sp35,modify_sp36,modify_sp37,sp34,sp35,sp36,sp37,spid34,spid35,spid36,spid37
# FROM product_lib.entity_91130_itemx ORDER BY source, tb_item_id, real_p1, real_p2;

# UPDATE product_lib.entity_91130_item SET visible = 100 WHERE (source, tb_item_id, real_p1) IN (
# SELECT source, tb_item_id, real_p1 FROM product_lib.entity_91130_itemx GROUP BY source, tb_item_id, real_p1 HAVING count(*) > 1
# );