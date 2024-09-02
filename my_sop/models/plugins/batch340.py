import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def calc_splitrate(self, item, data, custom_price={}):
        if len(data) <= 1:
            return data

        source = str(0 if item['snum']==1 and item['pkey'].day<20 else item['snum'])

        follow_pids = [1,4,6,9,11,13,16,18,21,23,26,28,30,33,35,37,40,42,46,47,48]

        if 'sku_price2' not in self.cleaner.cache:
            dba, atbl = self.get_all_tbl()
            _, stbl = self.get_sku_price_tbl()
            dba = self.cleaner.get_db(dba)
            db26 = self.cleaner.get_db('26_apollo')
            tbl = self.cleaner.get_product_tbl()

            pids = {
                '1': 159000,
                '4': 79000,
                '6': 85000,
                '9': 26000,
                '11': 16000,
                '13': 38000,
                '16': 43000,
                '18': 48000,
                '21': 42000,
                '23': 64000,
                '26': 38000,
                '30': 198000,
                '34': 48000,
                '35': 88000,
                '37': 53000,
                '40': 53000,
                '46': 29000,
                '47': 29000,
                '48': 98000
            }

            sql = '''
                WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
                SELECT toString(b_pid) k, price/`b_number` FROM {}
                WHERE b_similarity = 0 AND b_pid IN ({}) ORDER BY transform(s,[1,2],[999,998],s) DESC
                LIMIT 1 BY k
            '''.format(stbl, ','.join([str(v) for v in follow_pids]))
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            sql = '''
                WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
                SELECT concat(toString(b_pid),'_',toString(s),'_',toString(toYYYYMM(date))) k, median(price/`b_number`) FROM {}
                WHERE b_similarity = 0 AND b_pid IN ({})
                GROUP BY k
            '''.format(stbl, ','.join([str(v) for v in follow_pids]))
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            self.cleaner.cache['sku_price2'] = pids

        month = item['month'].strftime("%Y%m")
        pids = {v['pid']: self.cleaner.cache['sku_price2'][str(v['pid'])+'_'+str(source)+'_'+month] if str(v['pid'])+'_'+str(source)+'_'+month in self.cleaner.cache['sku_price2'] else self.cleaner.cache['sku_price2'][str(v['pid'])] for v in data if v['pid'] in follow_pids}
        if len(pids) == 0:
            return False

        item['price'] = item['sales'] / max(item['num'],1)
        item['price'] = max(item['price'],1)

        total = sum([pids[v['pid']]*v['number'] for v in data if v['pid'] in pids])
        item['split_flag'] = 2
        rate, vv = 1, None
        for v in data:
            if v['pid'] in pids:
                r = pids[v['pid']]*v['number']/total
                r = int(r*100)/100
                rate -= r
                v['split_rate'] = r
                if vv is None or v['split_rate'] > vv['split_rate']:
                    vv = v
            else:
                v['split_rate'] = 0

        return data


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp是否套包':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp是否套包` = '是' WHERE clean_split > 0 AND clean_brush_id > 0
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        _, stbl = self.get_sku_price_tbl()
        follow_pids = [1,4,6,9,11,13,16,18,21,23,26,28,30,33,35,37,40,42,46,47,48]

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))

        dba.execute('''
            CREATE TABLE {}_join (`pid` UInt32,`s` UInt8,`m` UInt32,`p` Float64) ENGINE = Join(ANY, LEFT, pid, `s`, `m`) AS
            WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
            SELECT b_pid AS pid, 0, 0 AS m, price/`b_number` AS p FROM {}
            WHERE b_similarity = 0 AND b_pid IN ({}) ORDER BY transform(s,[1,2],[999,998],s) DESC
            LIMIT 1 BY b_pid
        '''.format(tbl, stbl, ','.join([str(p) for p in follow_pids])))

        dba.execute('''
            INSERT INTO {}_join
            WITH IF(`source`=1 AND shop_type IN (11,12),0,`source`) AS s
            SELECT b_pid AS pid, s, toYYYYMM(date) AS m, median(price/`b_number`) AS p FROM {}
            WHERE b_similarity = 0 AND b_pid IN ({})
            GROUP BY pid, s, m
        '''.format(tbl, stbl, ','.join([str(p) for p in follow_pids])))

        sql = '''
            CREATE TABLE {t}_join2 ENGINE = Join(ANY, LEFT, uuid2, clean_sku_id) AS
            WITH ifNull(joinGet('{t}_join', 'p', clean_sku_id,
                IF(`source`=1 AND shop_type IN (11,12),0,`source`),toYYYYMM(date)),
                    joinGet('{t}_join', 'p', clean_sku_id,0,toUInt32(0))
            ) AS mprice
            SELECT uuid2, clean_sku_id, mprice FROM {t} UPDATE
            WHERE clean_split > 0 AND clean_brush_id > 0 AND clean_price/clean_number > mprice AND uuid2 > 0
              AND NOT isNull(joinGet('{t}_join', 'p', clean_sku_id,0,toUInt32(0)))
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `clean_sales` = joinGet('{t}_join2', 'mprice', uuid2, clean_sku_id) * clean_number * clean_num
            WHERE NOT isNull(joinGet('{t}_join2', 'mprice', uuid2, clean_sku_id))
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {t} UPDATE clean_price = `clean_sales`/clean_num, clean_split_rate=clean_sales/sales
            WHERE NOT isNull(joinGet('{t}_join2', 'mprice', uuid2, clean_sku_id))
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
        dba.execute('DROP TABLE IF EXISTS {}_join2'.format(tbl))


    # 出数数据检查
    def check(self, tbl, dba, logId):
        sql = 'SELECT sum(s) FROM (SELECT any(sales) s FROM {} GROUP BY uuid2)'.format(tbl)
        rr1 = dba.query_all(sql)

        sql = 'SELECT sum(clean_sales) FROM {}'.format(tbl)
        rr2 = dba.query_all(sql)

        if rr1[0][0] != rr2[0][0]:
            self.cleaner.set_log(logId, {'warn':'扣除销售额{:.02f}'.format(rr1[0][0]-rr2[0][0])})
