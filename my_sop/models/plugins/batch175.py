import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        pass
        # sql = '''
        #         #     ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
        #         #         k='Category' AND c_sales/c_num/100<=438 AND v='Bottoms (men\\'s)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=350 AND v='Dress & bottoms (women’s)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=680 AND v='Outerwear: Jackets & knitwear (men’s)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=680 AND v='Outerwear: Jackets & knitwear (women’s)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=350 AND v='Other tops not specified as outwear (men’s and women’s mixed for this category)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=350 AND v='Bottoms(unisex)', '其它',
        #         #         k='Category' AND c_sales/c_num/100<=680 AND v='Outwear(unisex)', '其它',
        #         #     v), c_props.name, c_props.value)
        #         #     WHERE 1
        #         # '''.format(tbl)
        # sql = '''
        #     ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
        #         k='Category' AND c_sales/c_num/100<=438 AND v='Dress & bottoms (women’s)', '其它',
        #         k='Category' AND c_sales/c_num/100<=760 AND v='Outerwear: Jackets & knitwear (men’s)时', '其它',
        #         k='Category' AND c_sales/c_num/100<=760 AND v='Outerwear: Jackets & knitwear (women’s)', '其它',
        #         k='Category' AND c_sales/c_num/100<=388 AND v='Other tops not specified as outwear (men’s and women’s mixed for this category)', '其它',
        #         k='Category' AND c_sales/c_num/100<=438 AND v='Bottoms(unisex)', '其它',
        #         k='Category' AND c_sales/c_num/100<=760 AND v='Outwear(unisex)', '其它',
        #     v), c_props.name, c_props.value)
        #     WHERE 1
        # '''.format(tbl)
        # dba.execute(sql)
        # while not self.cleaner.check_mutations_end(dba, tbl):
        #     time.sleep(5)


# INSERT INTO product_lib.product_91357 (
#     pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9
# ) SELECT
#     pid+10000,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,'','','','','','','','','',spid2,'','','',spid3,spid4,spid5,spid6,spid7,spid8,spid9
# FROM product_lib.product_90902;

# INSERT INTO product_lib.product_91357 (
#     pid,name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9
# ) SELECT
#     pid,IF(name='___其它','___其它2',name),product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,created,modified,start_time,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9
# FROM product_lib.product_90778;

# INSERT INTO product_lib.entity_91357_item (
#     pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9
# ) SELECT
#     pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid+10000,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,'','','','','','','','','',modify_sp2,'','','',modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,'','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',''
# FROM product_lib.entity_90902_item WHERE flag = 2 AND pid > 0;

# INSERT INTO product_lib.entity_91357_item (
#     pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,modify_sp11,modify_sp12,modify_sp13,modify_sp14,modify_sp15,modify_sp16,modify_sp17,modify_sp18,modify_sp19,modify_sp2,modify_sp20,modify_sp21,modify_sp22,modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,sp1,sp10,sp11,sp12,sp13,sp14,sp15,sp16,sp17,sp18,sp19,sp2,sp20,sp21,sp22,sp3,sp4,sp5,sp6,sp7,sp8,sp9,spid1,spid10,spid11,spid12,spid13,spid14,spid15,spid16,spid17,spid18,spid19,spid2,spid20,spid21,spid22,spid3,spid4,spid5,spid6,spid7,spid8,spid9
# ) SELECT
#     pkey,snum,ver,real_p1,real_p2,uniq_k,uuid2,tb_item_id,tip,source,month,name,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,avg_price,price,org_price,promo_price,trade,num,sales,visible,visible_check,clean_flag,prop_check,p1,trade_prop_all,prop_all,pid,alias_pid,number,uid,check_uid,b_check_uid,batch_id,flag,split,img,is_set,count,created,modified,modify_sp1,modify_sp10,'','','','','','','','','',modify_sp2,'','','',modify_sp3,modify_sp4,modify_sp5,modify_sp6,modify_sp7,modify_sp8,modify_sp9,'','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',''
# FROM product_lib.entity_90778_item WHERE flag = 2 AND pid > 0;

# CREATE TABLE product_lib.entity_91357_itemx LIKE product_lib.entity_91357_item;
# INSERT INTO product_lib.entity_91357_itemx SELECT * FROM product_lib.entity_91357_item;

# UPDATE product_lib.entity_91357_item SET flag = 3 WHERE (source, tb_item_id, real_p1) IN (
#     SELECT source, tb_item_id, real_p1 FROM product_lib.entity_91357_itemx a GROUP BY source, tb_item_id, real_p1 HAVING count(*) > 1
# );