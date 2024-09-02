import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def b(self):
        db26 = self.cleaner.get_db('26_apollo')
        sql = '''
            SELECT pid, name FROM product_lib.product_91269
        '''
        ret = db26.query_all(sql)
        pids = {v[1]:v[0] for v in ret}

        sql = '''
            SELECT id, spid4, tb_item_id, source, p1, month, tip FROM product_lib.entity_91269_item WHERE spid4 LIKE '%,%'
        '''
        ret = db26.query_all(sql)
        for id, sp4, tb_item_id, source, p1, month, tip, in ret:
            # source, itemid, p1, splitid
            sp4 = sp4.split(',')
            for pname in sp4:
                pid = pids[pname]

                sql = 'INSERT INTO product_lib.entity_91269_item_split VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s)'
                db26.execute(sql, (id, pid, 1, tb_item_id, source, p1, month, tip,))
            sql = 'UPDATE product_lib.entity_91269_item SET split = 1 WHERE id = {}'.format(id)
            db26.execute(sql)
        exit()


    def finish_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE
                `clean_all_bid` = 26, `clean_alias_all_bid` = 26, `clean_pid` = 0, `clean_sku_id` = 0, `sp是否药妆` = '否'
            WHERE clean_alias_all_bid = 245089 AND (clean_sales/clean_num < 8000 OR clean_sales/clean_num > 100000)
              AND name NOT LIKE '%CeraVe%' AND name NOT LIKE '%适乐肤%'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def calc_splitrate(self, item, data):
        if len(data) <= 1:
            return data

        pids = [str(v['pid']) for v in data]

        dba, atbl = self.get_all_tbl()
        _, stbl = self.get_sku_stat_tbl()
        dba = self.cleaner.get_db(dba)

        sql = '''
            WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
            SELECT b_pid, median(price/`b_number`) FROM {}
            WHERE b_split = 0 AND b_id > 0 AND sales > 0 AND num > 0 AND b_pid IN ({})
              AND uuid2 IN (SELECT uuid2 FROM {} WHERE sid = 170846517)
            GROUP BY b_pid
        '''.format(stbl, ','.join(pids), atbl)
        ret = dba.execute(sql)

        if len(ret) != len(pids):
            total = sum([v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less
        else:
            ret = {v[0]: v[1] for v in ret}
            total = sum([ret[v['pid']]*v['number'] for v in data])
            less  = 1
            for v in data:
                split_rate = ret[v['pid']]*v['number'] / total
                less -= split_rate
                v['split_rate'] = split_rate
            data[-1]['split_rate'] += less

        return data

    def brush_0521(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1
        where = 'alias_all_bid = 52285'
        # uuids = []
        count = {}
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids = []
            ret, sales_by_uuid = self.cleaner.process_top_anew(ssmonth, eemonth, where=where)
            for uuid2, source, tb_item_id, p1, cid, bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
            count[ssmonth] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, 1)
        for i in count:
            print(i, count[i])


        return True

    def brush_0524(self, smonth, emonth, logId=-1):

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        clean_flag = self.cleaner.last_clean_flag() + 1
        where = 'alias_all_bid = 52285'
        count = {}
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids_update = []
            uuids = []
            ret, sales_by_uuid = self.cleaner.process_top_anew(ssmonth, eemonth, where=where)
            for uuid2, source, tb_item_id, p1, cid, bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] == 0:
                        uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,-1]
            count[ssmonth] = len(uuids)
            print(len(uuids),len(uuids_update))
            if len(uuids_update) > 0:
                sql = 'update {} set visible = 1, visible_check=1, created = now() where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
                print(sql)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=6, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {} where visible = 2'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        clean_flag = self.cleaner.last_clean_flag() + 1
        where = '''
        uuid2 in (select uuid2 from {} where sp3 = 'Kits') and alias_all_bid in (244645,245244,218521,1421233,52436,218469,51981,52285,245844,3755964,245089,3077191,218569,218970,219049,246337,218961,218679,218550,106593,130885,52222,245038,245072,94398,219391,245583,245136,4686598,3756310,244668,2548829,883874,244762,91704,52143,130781,245015,5435577,3755955,2149687,2663833,219309,3132219,4743526,54358,219031,244679,218694,1411131,219045,946894,1683650,245237,3132220,1170125,2186048,245800,496082,3756309,51940,1485599,3237462,487741,245278,76644,2972953,3755929,5829651,245214,3756555,2149710,4517105,4732498,6265200,5748635,3756670)
        '''.format(ctbl)
        count = {}
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids_update = []
            uuids = []
            ret, sales_by_uuid = self.cleaner.process_top_anew(ssmonth, eemonth, rate=0.8, where=where)
            for uuid2, source, tb_item_id, p1, cid, bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    pass
                    # if mpp['{}-{}-{}'.format(source, tb_item_id, p1)][1] == 2:
                    #     uuids_update.append(mpp['{}-{}-{}'.format(source, tb_item_id, p1)][0])
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0,-1]
            count[ssmonth] = len(uuids)
            print(len(uuids),len(uuids_update))
            # if len(uuids_update) > 0:
            #     sql = 'update {} set visible = 1, visible_check=1, created = now() where id in ({})'.format(self.cleaner.get_tbl(), ','.join([str(v) for v in uuids_update]))
            #     print(sql)
            #     self.cleaner.db26.execute(sql)
            #     self.cleaner.db26.commit()
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        return True
# INSERT INTO artificial_new.`entity_91269_item` (
# tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,spid1,spid2,spid3,spid4,pid,number,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set
# )
# SELECT
# tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,avg_price,trade,num,sales,visible,p1,0,0,0,0,sp1,sp2,sp3,sp4,pid,1,0,2,233,0,0,tip,'',is_set
# FROM artificial_new.entity_90545_sp WHERE flag = 0;

# INSERT INTO artificial_new.`entity_91269_item` (
# tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,spid1,spid2,spid3,spid4,pid,number,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set
# )
# SELECT
# tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,trade_prop_all,prop_all,sum(sales)/sum(num),trade,sum(num),sum(sales),visible,p1,0,0,0,0,sp1,sp2,sp3,group_concat(sp4),pid,1,0,2,233,0,0,tip,'',is_set
# FROM artificial_new.entity_90545_sp WHERE flag = 3 GROUP BY tb_item_id, source, `month`, p1;

# INSERT INTO product_lib.`product_91269`(
# name,product_name,full_name,tip,all_bid,alias_all_bid,brand_name,img,market_price,source,item_id,month,alias_pid,type,visible,flag,cid,category,sub_category,category_manual,
# sub_category_manual,trade_cid,uid,kid1,kid2,kid3,kid4,kid5,start_time,spid1,spid2,spid3,spid4
# )
# SELECT
# sp4,'','',1,alias_all_bid,alias_all_bid,'','',0,'',0,NULL,0,'',0,0,0,'','','','',0,0,'','','','','',NULL,'','','',''
# FROM artificial_new.`entity_90545_sp` WHERE sp4 != '' GROUP BY alias_all_bid, sp4;

# UPDATE artificial_new.`entity_91269_item` SET pid = 0, flag = 2, number = 1;
# UPDATE artificial_new.`entity_91269_item` a JOIN all_site.all_brand b ON (a.alias_all_bid=b.bid) SET a.alias_all_bid = IF(b.alias_bid>0,b.alias_bid,b.bid);
# UPDATE product_lib.`product_91269` a JOIN all_site.all_brand b ON (a.alias_all_bid=b.bid) SET a.alias_all_bid = IF(b.alias_bid>0,b.alias_bid,b.bid);

# /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_91269_item > /obsfs/91269.sql
# /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91269.sql
# /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm product_lib product_91269 > /obsfs/91269p.sql
# /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/91269p.sql