import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import models.plugins.batch as Batch
import scripts.muying_shoptype as muying_shoptype
import application as app


class main(Batch.main):

    def hotfix_new(self, tbl, dba, prefix):

        muying_shoptype.main()

        time.sleep(5)

        colname = 'sp店铺分类'

        self.cleaner.add_miss_cols(tbl, {colname:'String'})

        db26 = self.cleaner.get_db('26_apollo')

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.cleaner.eid))

        sql = '''
            CREATE TABLE default.ecshop_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT `source`, sid, transform(type_meizhuang,[1,2,3,4,5,6,7],['品牌旗舰店','集团旗舰店','集合店','自营店铺','专营店','专卖店','其他分销店铺'],'') AS s
            FROM sop_e.shop_92232_type
        '''.format(self.cleaner.eid)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `{}` = IF(
                `source`=1 AND (shop_type<20 and shop_type>10), 'C2C', ifNull(joinGet('default.ecshop_{}', 's', toUInt64(source), sid), '')
            )
            WHERE 1
        '''.format(tbl, colname, self.cleaner.eid)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.ecshop_{}'.format(self.cleaner.eid))

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.cleaner.eid))

        sql = '''
            CREATE TABLE default.fixshopbid_{} ENGINE = Join(ANY, LEFT, source, sid) AS
            SELECT source, sid, any(clean_alias_all_bid) b FROM {} WHERE `{}` = '品牌旗舰店' AND clean_alias_all_bid > 0
            GROUP BY source, sid HAVING countDistinct(clean_alias_all_bid) = 1
        '''.format(self.cleaner.eid, tbl, colname)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_all_bid`),
                `clean_alias_all_bid` = ifNull(joinGet('default.fixshopbid_{e}', 'b', source, sid), `clean_alias_all_bid`)
            WHERE not isNull(joinGet('default.fixshopbid_{e}', 'b', source, sid))
        '''.format(tbl, colname, e=self.cleaner.eid)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS default.fixshopbid_{}'.format(self.cleaner.eid))