# coding=utf-8
import arrow
import argparse
import csv
import collections
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import json
import logging
logger = logging.getLogger('')
import re
import sys
import time
import pysolr
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
from extensions import utils
default_limit = 1000


def create(db, core_name, sql, id_key, key_list, key_check_ignore=[], start=0, limit=default_limit, check_exist=1, limit_insert=default_limit, solr_use='default'):
    '''
    把品牌导入到solr中
    :return:
    '''
    sep = ' | '
    solr = app.get_solr(core_name, solr_use=solr_use)
    id_idx = key_list.index(id_key)
    global g_error
    g_error = []
    def add_solr(data):
        global g_error
        h_exist = {}
        if check_exist == 1:
            for data2 in utils.list_split(data, 1000):
                l_add = []
                for row in data2:
                    id = row[id_idx]
                    l_add.append('{}:{}'.format(id_key, id))
                r = solr.search(" OR ".join(l_add), rows=default_limit*10)
                for row2 in r.docs:
                    h_exist[str(row2[id_key])] = row2

        l_add = []
        for row in data:
            id = row[id_idx]
            id = str(id)

            doc = {}
            for i in range(len(key_list)):
                doc[key_list[i]] = row[i]

            if id in h_exist:
                # print('exist: ', id, h_exist[id])
                row2 = h_exist[id]
                flag = True
                for i in range(len(key_list)):
                    key = key_list[i]
                    if key in key_check_ignore:
                        continue
                    val_new = row[i]
                    val_old = row2[key] if key in row2 else ''
                    if str(val_new) != val_old:
                        print('exist:', id, 'key:', key, 'new:', val_new, 'old:', val_old)
                        flag = False
                        break
                if flag:
                    continue
                else:
                    print(row, row2)
                    doc['id'] = row2['id']
                    # exit()

            l_add.append(doc)
            if len(l_add) > limit_insert:
                try:
                    solr.add(l_add)
                    solr.commit()
                except Exception as e:
                    print('l_add:', l_add, e)
                    for k in l_add:
                        g_error.append(k[id_key])
                print('add to solr count:', len(l_add), 'ids:', ','.join([str(x[id_key]) for x in l_add]))
                l_add = []

        if len(l_add) > 0:
            try:
                solr.add(l_add)
                solr.commit()
            except Exception as e:
                print('l_add:', l_add, e)
                for k in l_add:
                    g_error.append(k[id_key])
            print('add to solr count:', len(l_add), 'ids:', ','.join([str(x[id_key]) for x in l_add]))
            l_add = []

    utils.easy_traverse(db, sql, one_callback=add_solr, start=start, limit=limit)
    print('l_error:', g_error)

def check(db, core_name, sql, id_key, key_list, start=0, limit=default_limit, limit_delete=1000, solr_use='default'):
    '''
    检查solr中已经导入的数据，把重复的删除掉
    :return:
    '''
    utils.easy_call([db], 'connect')
    sep = ' | '
    solr = app.get_solr(core_name, solr_use=solr_use)
    id_idx = key_list.index(id_key)
    def add_solr(data2):
        for data in utils.list_split(data2, 1000):
            l_add = []
            for row in data:
                id = row[id_idx]
                l_add.append('{}:{}'.format(id_key, id))
            r = solr.search(" OR ".join(l_add), rows=10000)
            h_exist = {}
            h_count = {}
            for row2 in r.docs:
                bid = row2[id_key]
                id = row2['id']
                if bid not in h_count:
                    h_count[bid] = []
                h_count[bid].append(id)
                h_exist[bid] = row2
            ids_del = []
            for bid in h_count:
                if len(h_count[bid]) > 1:
                    print(bid, 'count:', len(h_count[bid]))
                    for id in h_count[bid][1:]:
                        ids_del.append(id)
                        if len(ids_del) > limit_delete:
                            solr.delete(id=ids_del, commit=True)

            if len(ids_del) > 0:
                solr.delete(id=ids_del, commit=True)

    utils.easy_traverse(db, sql, one_callback=add_solr, start=start, limit=limit)

def create_brand():
    db = app.get_db('default')
    core_name = 'brand'
    utils.easy_call([db], 'connect')
    sql = 'select bid, name, alias_bid, sales from brush.all_brand where bid>%d and bid not in (0, 26) and alias_bid not in (26) limit %d'
    id_key = 'bid'
    key_list = ['bid', 'bname', 'alias_bid', 'sales']
    create(db, core_name, sql, id_key, key_list)



def main(args):
    action = args.action
    eval(action)()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params for maintain f table')
    parser.add_argument('--action', type=str, default='create_brand', help='action')


    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    print('all done used:', (end_time - start_time))