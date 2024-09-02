# coding=utf-8
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))

import application as app
import time
import arrow
from dateutil.parser import parse
import pandas as pd
from models.report import common
import re

db_sop = app.get_clickhouse('chsop')
db_sop.connect()
re_spid = re.compile("([\.0-9]+[gG克]{1})")
re_num = re.compile("([0-9]+)")

# 将结果输出为csv
def rlt2csv(rlt, clm, name):
    # rlt是个元组，clm是列名
    # name特异化文件命名
    tmp = pd.DataFrame(columns=clm, data=list(rlt))
    name = map(str, name)
    name_merge = '_'.join(name)
    # tmp.head()
    tmp.to_csv('./{}.csv'.format(name_merge), encoding='utf-8', index=False)


# e表回填数据检查
# 将b表答题结果和c表机洗结果对比
def backfillDataCheck(db_, eid, date_s, date_e):
    error_list = set()
    date_s = parse(date_s)
    date_e = parse(date_e)

    for r in arrow.Arrow.span_range('month', date_s, date_e):
        ms = r[0].format('YYYY-MM-01')
        me = r[1].shift(months=1).format('YYYY-MM-01')
        start_time = time.time()
        sql = '''
        SELECT b.pkey, b.sp18, c.sp18, a.name,a.`trade_props.value`, a.img, a.all_bid, a.alias_all_bid, a.sales, a.shop_type, a.item_id, a.source 
        FROM (SELECT name, `trade_props.value`, img,  shop_type, item_id, source FROM sop.entity_prod_{eid}_A WHERE pkey>='{ms}' and pkey<'{me}')
        AS a 
        JOIN (SELECT pkey, sp18 FROM sop_b.entity_prod_{eid}_B WHERE similarity >= 1 and pkey>='{ms}' and pkey<'{me}') AS b
        ON a.uuid=b.uuid
        JOIN (SELECT sp18 FROM sop_c.entity_prod_{eid}_C WHERE pkey>='{ms}' and pkey<'{me}') AS c 
        ON a.uuid=c.uuid
        '''.format(eid=eid, ms=ms, me=me)
        data = db_.query_all(sql)
        end_time = time.time()
        print('select one month time used:', (end_time - start_time))
        for item in data:
            if item[1] and item[2] and item[1] != item[2]:
                if re_spid.findall(item[3]) or re_spid.findall(str(item[4])):
                    if re_num.match(item[1])[0] in re_num.findall(item[3]) or re_num.match(item[1])[0] in re_num.findall(str(item[4])):
                        continue
                    else:
                        tmp = list(item[:4])
                        tmp.append(str(item[4]))
                        tmp.append(item[5:9])
                        if item[-1] == 1:
                            source_ = 'tb' if item[-3] < 20 else 'tmall'
                        else:
                            source_ = common.source_name_ref[item[-1]]
                        tmp.append(common.get_link(source_, item[-2]))

                        error_list.add(tuple(tmp))
    columns = (
        'pkey', '答题回填规格', '机洗规格', 'name', 'trade_props.value', 'img', 'all_bid', 'alias_all_bid', '销售额', 'item_link')
    return error_list, columns


if __name__ == '__main__':
    eid = 91357
    t_s = '2021-01-01'
    t_e = '2021-07-01'
    sop_error, result_clm = backfillDataCheck(db_sop, eid, t_s, t_e)
    f_name = ('sop_error', '91357')
    rlt2csv(sop_error, result_clm, f_name)