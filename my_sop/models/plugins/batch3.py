import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush(self, smonth, emonth, logId=-1):
        if self.cleaner.check(smonth, emonth) == False:
            return True

        clean_flag = self.cleaner.last_clean_flag() + 1

        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=500)
        self.cleaner.add_brush([v[0] for v in ret1], clean_flag, 1, sales_by_uuid=sales_by_uuid)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=500)
        self.cleaner.add_brush([v[0] for v in ret2], clean_flag, 2)
        print('top:', len(ret1), 'rand:', len(ret2))
        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        # 名称出现可啦啦的但是品牌填了爱尔康（或者合并品牌之一）的都改成可啦啦
        sql = '''
            ALTER TABLE {} UPDATE `c_all_bid` = 243071, `c_alias_all_bid` = 243071, `c_props.value` = arrayMap((k, v) -> IF(k='品牌', '可啦啦', v), c_props.name, c_props.value)
            WHERE name like '%可啦啦%' AND c_alias_all_bid = 201237
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 爱尔康没年抛，半年抛 sp1=年抛,半年抛 且 sp4=爱尔康 ，品牌刷0，系列刷其它
        sql = '''
            ALTER TABLE {} UPDATE `c_all_bid` = 0, `c_alias_all_bid` = 0, `c_props.value` = arrayMap((k, v) -> IF(k='系列', '其它', v), c_props.name, c_props.value)
            WHERE c_alias_all_bid = 201237 AND c_props.value[indexOf(c_props.name, '佩戴周期')] IN ('年抛', '半年抛')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 爱尔康彩片 只有日抛 sp1!=日抛 且 sp4=爱尔康 且 sp6=彩色隐形眼镜 ，品牌刷0，系列刷其它
        sql = '''
            ALTER TABLE {} UPDATE `c_all_bid` = 0, `c_alias_all_bid` = 0, `c_props.value` = arrayMap((k, v) -> IF(k='系列', '其它', v), c_props.name, c_props.value)
            WHERE c_alias_all_bid = 201237
              AND c_props.value[indexOf(c_props.name, '佩戴周期')] != '日抛'
              AND c_props.value[indexOf(c_props.name, '子品类')] = '彩色隐形眼镜'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 爱尔康白片 只有日抛月抛 sp1!=日抛 且 sp1!=月抛 且 sp4=爱尔康 且 sp6=普通隐形眼镜 ，品牌刷0，系列刷其它
        sql = '''
            ALTER TABLE {} UPDATE `c_all_bid` = 0, `c_alias_all_bid` = 0, `c_props.value` = arrayMap((k, v) -> IF(k='系列', '其它', v), c_props.name, c_props.value)
            WHERE c_alias_all_bid = 201237
              AND c_props.value[indexOf(c_props.name, '佩戴周期')] NOT IN ('日抛', '月抛')
              AND c_props.value[indexOf(c_props.name, '子品类')] = '普通隐形眼镜'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def market(self, smonth, emonth):
        mid = 757
        self.cleaner.market(mid, smonth, emonth, 'clean_props.value[6] NOT IN (\'其它\')')

        db14 = app.get_db('14_apollo')
        db14.connect()
        db18 = app.get_db('18_apollo')
        db18.connect()

        sql = '''
            INSERT ignore INTO `artificial_new`.`entity_{mid}_product` (`alias_all_bid`, `brand`, `name`)
            select alias_all_bid, brand, sp9 from `artificial_new`.`entity_{mid}`
            where alias_all_bid > 0 and sp9 != '' and sp9 != '其它' and date >= '{}' group by alias_all_bid, sp9;
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        sql = '''
            UPDATE `artificial_new`.`entity_{mid}` set pid = 0 where date >= '{}'
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        sql = '''
            UPDATE `artificial_new`.`entity_{mid}` a
            JOIN `artificial_new`.`entity_{mid}_product` b on (a.alias_all_bid=b.alias_all_bid and a.sp9=b.name)
            SET a.pid = b.pid where a.date >= '{}'
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        db18.commit()

        sql = 'UPDATE dataway.entity SET micro_flag = 0 WHERE id = {}'.format(mid)
        db14.execute(sql)
        db14.commit()