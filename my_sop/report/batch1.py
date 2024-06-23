import sys
import re
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
imimport sys
import re
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as appport application as app

class main(Batch.main):

    def add_bids(self, eid, tbl,care_alias_all_bid=None):
        db26 = self.cleaner.get_db('26_apollo')
        dba = self.cleaner.get_db('chsop')
        ret = dba.query_all('SELECT DISTINCT all_bid FROM {}'.format(tbl))
        data = [[eid, bid] for bid, in ret]
        db26.batch_insert('insert ignore into dataway.`entity_topbrands` (eid,all_bid) values','(%s, %s)', data)
        db26.execute('UPDATE dataway.`entity_topbrands` a JOIN brush.all_brand b ON (a.all_bid=b.bid) SET a.alias_all_bid=if(b.alias_bid=0,b.bid,b.alias_bid)')
        db26.execute('UPDATE dataway.`entity_topbrands` a JOIN brush.all_brand b ON (a.alias_all_bid=b.bid) SET a.name=b.name')
        db26.
        if care_alias_all_bid:
            db26.execute('UPDATE dataway.`entity_topbrands` a SET a.follow=1 WHERE eid = {} AND alias_all_bid in {}'.format(eid,care_alias_all_bid))


    def check_shop(self, eid, tbl):
        dba = self.cleaner.get_db('chsop')
        sql = '''
            INSERT INTO artificial.entity_etbl_shopinfo (tbl, `source`, sid, name, nick, date, isNew)
            SELECT '{t}', `source`, sid, dictGet('all_shop', 'title', tuple(`source`,sid)), dictGet('all_shop', 'nick', tuple(`source`,sid)), now(), toYYYYMM(now())=toYYYYMM(min(created))
            FROM {t} GROUP BY `source`, sid
        '''.format(t=tbl)
        dba.execute(sql)


    def get_top_bids(self, eid):
        db26 = self.cleaner.get_db('26_apollo')
        if eid in [91137,91164,91128,91863]:
            sql = 'SELECT alias_all_bid FROM dataway.`entity_topbrands` WHERE eid = {}'.format(eid)
        else:
            sql = 'SELECT alias_all_bid FROM dataway.`entity_topbrands` WHERE eid = {} AND follow = 1'.format(eid)
        ret = db26.query_all(sql)
        bids = ['dictGet(\'all_brand\',\'alias_bid\',tuple({}))'.format(bid) for bid, in ret]
        return bids
    def get_brand_str(self,brand):
        if brand!='':
            return '|'.join(re.split(r"[ /]", brand))
        else:
            return ''

    def get_sid_brand(self,eid,tbl,bids,source_sid):
        dba = self.cleaner.get_db('chsop')
        sql = """
            SELECT case
                when source = 1 then 'ali'
                when source = 2 then 'jd'
                when source = 3 then 'gome'
                when source = 4 then 'jumei'
                when source = 5 then 'kaola'
                when source = 6 then 'suning'
                when source = 7 then 'vip'
                when source = 8 then 'pdd'
                when source = 9 then 'jiuxian'
                when source = 11 then 'dy'
                when source = 12 then 'cdf'
                when source = 14 then 'dw'
                when source = 15 then 'hema'
                when source = 24 then 'ks'
                else '其他' end as `平台`,AAA.sid,AAA.alias_all_bid,
                case when dictGet('all_brand','alias_bid',tuple(alias_all_bid)) IN ({bids_str}) THEN '是' ELSE '否' END AS "是否关注品牌",
                BBB.title `shopname`,BBB.url FROM(
                    SELECT Distinct source,sid,alias_all_bid FROM {tbl}
                    WHERE (`source`, sid) IN ({source_sid}) AAA
                LEFT JOIN (select * from artificial.all_shop) BBB
                ON AAA.source = BBB.source and AAA.sid = BBB.sid
        """

    def get_fss_shop_all(self,eid,tbl,source):
        dba = self.cleaner.get_db('chsop')
        bids = self.get_top_bids(eid)
        brand_str=''
        brand = dba.query_all("""SELECT Distinct dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') 
                                        FROM {} WHERE source in ({}) and alias_all_bid in ({})""".format(tbl,','.join(source),','.join(bids)))
        for b in brand:
            s = self.get_brand_str(b[0])
            if s == '':
                pass
            elif brand_str=='':
                brand_str += s
            else:
                brand_str = brand_str+'|' + s
        print(brand_str)
        sql = '''
                SELECT DISTINCT `source`, sid FROM artificial.entity_etbl_shopinfo
                WHERE tbl = '{t}' AND source in ({source})
                AND (
                    (`source`, sid) IN (
                        SELECT toUInt8(`source`), sid FROM sop_e.shop_91783_type WHERE type_meizhuang IN (1,2) AND sid > 0
                    ) OR
                    (`source`, sid) IN (
                        SELECT toUInt8(transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'dy2', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0)), toUInt64(sid)
                        FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm') WHERE chtype_h IN (1,2) AND chtype_m IN (1,2) AND IF(chtype_h > 0, chtype_h, chtype_m) IN (1,2) AND sid > 0
                    ) OR
                    (`source`, sid) IN (
                        SELECT toUInt8(`source`),sid from (
                            SELECT a.*,b.title `shopname` FROM (
                                SELECT DISTINCT source,sid FROM sop_e.entity_prod_91171_E_1112JD4 WHERE source in ({source}) and alias_all_bid in ({bids_str})
                                )a
                            LEFT JOIN (select * from artificial.all_shop) b
                            ON a.source = b.source and a.sid = b.sid)
                        WHERE `shopname` LIKE '%旗舰%' AND match(`shopname`, '{brand_str}')
                    )
                ) AND (`source`, sid) IN (
                    SELECT DISTINCT `source`, sid FROM {t} WHERE dictGet('all_brand','alias_bid',tuple(alias_all_bid)) IN ({bids_str})
                )
        '''.format(source=','.join(source), t=tbl,brand_str=brand_str,bids_str=','.join(bids))
        source_sid = dba.query_all(sql)
        # print(source_sid)
        sql = """
                SELECT case
                    when source = 1 then 'ali'
                    when source = 2 then 'jd'
                    when source = 3 then 'gome'
                    when source = 4 then 'jumei'
                    when source = 5 then 'kaola'
                    when source = 6 then 'suning'
                    when source = 7 then 'vip'
                    when source = 8 then 'pdd'
                    when source = 9 then 'jiuxian'
                    when source = 11 then 'dy'
                    when source = 12 then 'cdf'
                    when source = 14 then 'dw'
                    when source = 15 then 'hema'
                    when source = 24 then 'ks'
                    else '其他' end as `平台`,AAA.sid,AAA.alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(AAA.alias_all_bid)), '')`品牌名`,
                    case when dictGet('all_brand','alias_bid',tuple(alias_all_bid)) IN ({bids_str}) THEN '是' ELSE '否' END AS "是否关注品牌",
                    BBB.title `shopname`,BBB.url FROM(
                        SELECT Distinct source,sid,alias_all_bid FROM {tbl}
                        WHERE (`source`, sid) IN {source_sid}) AAA
                    LEFT JOIN (select * from artificial.all_shop) BBB
                    ON AAA.source = BBB.source and AAA.sid = BBB.sid
                """.format(tbl=tbl,brand_str=brand_str,bids_str=','.join(bids),source_sid=source_sid)
        rr0 = dba.query_all(sql)
        return rr0

    def get_fss_shop_info(self, eid, tbl, date='toYYYYMM(now())'):
        dba = self.cleaner.get_db('chsop')
        bids = self.get_top_bids(eid)
        sql = '''
            SELECT case
                when source = 1 then 'ali'
                when source = 2 then 'jd'
                when source = 3 then 'gome'
                when source = 4 then 'jumei'
                when source = 5 then 'kaola'
                when source = 6 then 'suning'
                when source = 7 then 'vip'
                when source = 8 then 'pdd'
                when source = 9 then 'jiuxian'
                when source = 11 then 'dy'
                when source = 12 then 'cdf'
                when source = 14 then 'dw'
                when source = 15 then 'hema'
                when source = 24 then 'ks'
                else '其他' end as `平台`,AAA.sid,BBB.title `shopname`,BBB.url FROM(
                SELECT DISTINCT `source`, sid FROM artificial.entity_etbl_shopinfo
                WHERE tbl = '{t}' AND toYYYYMM(date) = {} AND isNew
                AND (
                    (`source`, sid) IN (
                        SELECT toUInt8(`source`), sid FROM sop_e.shop_91783_type WHERE type_meizhuang IN (1,2) AND sid > 0
                    ) OR
                    (
                        (`source`, sid) NOT IN (
                            SELECT toUInt8(`source`), sid FROM sop_e.shop_91783_type WHERE sid > 0
                        ) AND
                        (`source`, sid) IN (
                            SELECT toUInt8(transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'dy2', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0)), toUInt64(sid)
                            FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm') WHERE chtype_h IN (1,2) AND chtype_m IN (1,2) AND IF(chtype_h > 0, chtype_h, chtype_m) IN (1,2) AND sid > 0
                        )
                    )
                ) AND (`source`, sid) IN (
                    SELECT DISTINCT `source`, sid FROM {t} WHERE dictGet('all_brand','alias_bid',tuple(alias_all_bid)) IN ({})
                )
            ) AAA 
            LEFT JOIN (select * from artificial.all_shop) BBB
            ON AAA.source = BBB.source and AAA.sid = BBB.sid
        '''.format(date, ','.join(bids), t=tbl)
        rr1 = dba.query_all(sql,as_dict=True)

        sql = '''
            SELECT case
                when source = 1 then 'ali'
                when source = 2 then 'jd'
                when source = 3 then 'gome'
                when source = 4 then 'jumei'
                when source = 5 then 'kaola'
                when source = 6 then 'suning'
                when source = 7 then 'vip'
                when source = 8 then 'pdd'
                when source = 9 then 'jiuxian'
                when source = 11 then 'dy'
                when source = 12 then 'cdf'
                when source = 14 then 'dw'
                when source = 15 then 'hema'
                when source = 24 then 'ks'
                else '其他' end as `平台`,AAA.sid,AAA.name,AAA.new_name,BBB.url FROM (
                SELECT DISTINCT `source`, sid, name, dictGet('all_shop', 'title', tuple(`source`,sid)) AS new_name
                FROM artificial.entity_etbl_shopinfo
                WHERE tbl = '{t}' AND toYYYYMM(date) = {} AND name != new_name
                AND (
                    (`source`, sid) IN (
                        SELECT toUInt8(`source`), sid FROM sop_e.shop_91783_type WHERE type_meizhuang IN (1,2) AND sid > 0
                    ) OR
                    (
                        (`source`, sid) NOT IN (
                            SELECT toUInt8(`source`), sid FROM sop_e.shop_91783_type WHERE sid > 0
                        ) AND
                        (`source`, sid) IN (
                            SELECT toUInt8(transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'dy2', 'cdf', 'lvgou', 'dewu', 'hema', 'sunrise', 'test17', 'test18', 'test19', 'ks', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19, 24, 999], 0)), toUInt64(sid)
                            FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm') WHERE chtype_h IN (1,2) AND chtype_m IN (1,2) AND IF(chtype_h > 0, chtype_h, chtype_m) IN (1,2) AND sid > 0
                        )
                    )
                ) AND (`source`, sid) IN (
                    SELECT DISTINCT `source`, sid FROM {t} WHERE dictGet('all_brand','alias_bid',tuple(alias_all_bid)) IN ({})
                )
            ) AAA
            LEFT JOIN (select * from artificial.all_shop) BBB
            ON AAA.source = BBB.source and AAA.sid = BBB.sid
        '''.format(date, ','.join(bids), t=tbl)
        rr2 = dba.query_all(sql,as_dict=True)
        return rr1, rr2