#coding=utf-8
from os import listdir
import sys
import re
import csv
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app

def eid_by_bid(dba, bid):
    # check bid exsits
    sql = 'SELECT eid, status FROM cleaner.clean_batch where batch_id = {}'.format(bid)
    ret = dba.query_all(sql)
    if len(ret) == 0:
        raise Exception("bid not exists")

    return ret[0][0], ret[0][1]  # eid,status =


def create_table(dba):
    sql = '''
        CREATE TABLE IF NOT EXISTS `artificial_new`.`entity_sp_modify` (
            `batch_id` int(11) NOT NULL,
            `tb_item_id` varchar(40) NOT NULL,
            `source` varchar(10) NOT NULL,
            `month` varchar(10) NOT NULL,
            `p1` varchar(255) NOT NULL,
            `sp_category` varchar(100) NOT NULL,
            `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY `index` (`batch_id`,`tb_item_id`,`source`,`month`,`p1`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
    '''
    dba.execute(sql)


def load_csv(csvdir):
    files = listdir(csvdir)
    data = []
    for file in files:
        if re.match('[a-z]+\d+.*\.csv', file) is not None:
            input_file = open(csvdir+'/'+file, 'r', encoding='gbk', errors='ignore')
            reader = csv.reader(input_file)
            ret = [row for row in reader]
            k, d = ret[0], ret[1:]
            for v in d:
                data.append([v[k.index('batch_id')], v[k.index('tb_item_id')],v[k.index('source')], datetime.datetime.strptime(v[k.index('month')], "%Y/%m/%d").strftime('%Y-%m-%d'), v[k.index('p1')], v[k.index('new_spid_category')]])
    return data


def upload_dir(csvdir):
    db_26 = app.get_db('26_apollo')
    db_26.connect()

    create_table(db_26)
    data = load_csv(csvdir)
    upload_data(db_26, data)

    db_26.commit()
    db_26.close()


def upload_data(dba, data):
    sql = "REPLACE INTO `artificial_new`.`entity_sp_modify` (`batch_id`,`tb_item_id`,`source`,`month`,`p1`,`sp_category`) VALUES (%s,%s,%s,%s,%s,%s)"
    dba.execute_many(sql, data)
