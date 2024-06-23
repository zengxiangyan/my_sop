import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):

    def xxxx(self):
        # 搬答题
        db26 = self.cleaner.get_db('26_apollo')
        dba = self.cleaner.get_db('chsop')

        db26.execute('drop TABLE product_lib.entity_91808_item')
        db26.execute('drop TABLE product_lib.product_91808')

        sql = '''
            SELECT b_id FROM sop_c.entity_prod_91130_ALL WHERE (`source`, item_id, trade_props_arr) IN (
                SELECT `source`, item_id, trade_props_arr FROM sop_c.entity_prod_91808_ALL
            ) AND b_id > 0 AND b_similarity = 0 LIMIT 1 BY b_id
        '''
        ret = dba.query_all(sql)
        bids = [str(v) for v, in ret]

        sql = 'CREATE TABLE product_lib.entity_91808_item LIKE product_lib.entity_91808_item_old'
        db26.execute(sql)

        colsa = self.cleaner.get_cols('product_lib.entity_91130_item', db26)
        cols = self.cleaner.get_cols('product_lib.entity_91808_item', db26)
        cols = {c:cols[c] for c in cols if c in colsa}

        sql = '''
            INSERT INTO product_lib.entity_91808_item ( {c}, visible )
            SELECT {c}, 127 FROM product_lib.entity_91130_item
            WHERE flag > 0 AND id IN ({}) AND visible NOT IN (1,3,4,10)
        '''.format(','.join(bids), c=','.join(['`{}`'.format(c) for c in cols if c != 'visible']))
        db26.execute(sql)

        sql = '''
            INSERT INTO product_lib.entity_91808_item ( {c}, id, pid )
            SELECT {c}, `id`+10000000, IF(pid>0,`pid`+1000000,0) FROM product_lib.entity_91808_item_old
        '''.format(c=','.join(['`{}`'.format(c) for c in cols if c not in ['id','pid']]))
        db26.execute(sql)

        sql = 'CREATE TABLE product_lib.product_91808 LIKE product_lib.product_91808_old'
        db26.execute(sql)

        colsa = self.cleaner.get_cols('product_lib.product_91130', db26)
        cols = self.cleaner.get_cols('product_lib.product_91808', db26)
        cols = {c:cols[c] for c in cols if c in colsa}

        sql = '''
            INSERT INTO product_lib.product_91808 ( {c}, `spid1`, `spid6` )
            SELECT {c}, `spid1`, `spid14` FROM product_lib.product_91130
            WHERE pid IN (SELECT pid FROM product_lib.entity_91808_item WHERE visible = 127)
        '''.format(c=','.join(['`{}`'.format(c) for c in cols if c not in ['spid1','spid2','spid3','spid4','spid5','spid6','spid7']]))
        db26.execute(sql)

        sql = '''
            INSERT INTO product_lib.product_91808 ( {c}, pid )
            SELECT {c}, `pid`+1000000 FROM product_lib.product_91808_old
            WHERE (alias_all_bid,name) NOT IN (SELECT alias_all_bid,name FROM product_lib.product_91808)
        '''.format(c=','.join(['`{}`'.format(c) for c in cols if c != 'pid']))
        db26.execute(sql)

        sql = '''
            INSERT IGNORE INTO product_lib.entity_91808_item_split SELECT * FROM product_lib.entity_91130_item_split
            WHERE entity_id IN ({})
        '''.format(','.join(bids))
        db26.execute(sql)

        db26.commit()


    def hotfix_new(self, tbl, dba, prefix):
        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {}_set ENGINE = Set AS
            SELECT (source, item_id, date, trade_props_arr) FROM sop_e.entity_prod_91130_E_jiangzhong WHERE `sp子品类` = '益生菌'
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp品类`='剔除数据', `sp子品类`='剔除数据', `clean_sku_id`=0, `spSKU（不出数）` = ''
            WHERE `sp品类`='胃肠道疾病用药' AND b_id=0 AND (source, item_id, date, trade_props_arr) IN {t}_set
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
        dba.execute(sql)