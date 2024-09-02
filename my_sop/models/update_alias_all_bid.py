
import sys, getopt, os, io, re
from os import listdir
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import csv
import time
import application as app
from models import market
import collections

db_cleaner = app.get_db('default')

def update_brand_alias_all_bid(batch_id, data):
    db_cleaner.connect()
    try:
        eid = db_cleaner.query_all('SELECT eid FROM cleaner.clean_batch WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id = batch_id))[0][0]
    except:
        return None, '该batch_id在clean_batch表里不存在，输入错误！请检查文件名是否为清洗项目id'
    else:
        if not eid:
            return None, '该batch_id在clean_batch表里未填写eid，请补充后再上传需求表！'
    # 检查新字段是否已经添加成功
    modify_table = 'artificial_new.entity_' + str(eid) + '_item'
    new_column = 'artificial_alias_all_bid'
    check_column_sql = 'SHOW COLUMNS FROM ' + modify_table + ';'
    column_list = [info[0] for info in db_cleaner.query_all(check_column_sql)]
    if new_column not in column_list:
        add_column_sql = 'ALTER TABLE {} ADD COLUMN {} INT(11)'.format(modify_table, new_column)
        warn = db_cleaner.execute(add_column_sql)
        if warn:
            return eid, '创建新字段时出错'
        else:
            return eid, '正在创建新字段, 请稍后重试'
    '''
    update_dict = {
        (base_id, rank, change_id): [
            同一个base_id, rank对应的不同dict要都成立才可以修改
            {
                'pkey': {
                    'alone'对应=> ','  'overall'对应=> '+'
                    'inquiry_mode': 'alone' / 'overall',
                    'pkey_list': [str, str, ...]
                },
                需要在对应pkey字段全部找到才生效
                'and_word': [str, str, ...]
                只要存在就可以
                'or_word': [str, str, ...]
                如果存在就不可以
                'not_word': [str, str, ...]
                先进行忽略再进行词语的匹配, 防止名字的包含关系
                'ignore_word': [str, str, ...]
            },
            {
                ...
            }
        ]
    }
    '''
    # 'inquiry_mode': 'alone' / 'overall',
    update_dict = collections.defaultdict(lambda: [])
    alias_bid_list = set()
    all_pkey_list = set()
    now_key = None
    for info in data:
        if info["base bid"] != None and info["base bid"] != 0 and info["base bid"] != '':
            alias_bid_list.add(info["base bid"])
        if len(info["base bid"]) != 0:
            now_key = (int(info["base bid"]), int(info['rank']), int(info['change bid']))
        pkey = dict()
        and_word = info['and word'].split(',') if len(info['and word']) > 0 else []
        or_word = info['or word'].split(',') if len(info['or word']) > 0 else []
        not_word = info['not word'].split(',') if len(info['not word']) > 0 else []
        ignore_word = info['ignore word'].split(',') if len(info['ignore word']) > 0 else []

        if len(info['pKey']) != 0:
            if ',' in info['pKey']:
                pkey_list = str(info['pKey']).split(',')
                pkey = {
                    'inquiry_mode': 'alone',
                    'pkey_list': pkey_list
                }
                all_pkey_list.update(pkey_list)
            elif '+' in info['pKey']:
                pkey_list = str(info['pKey']).split('+')
                pkey = {
                    'inquiry_mode': 'overall',
                    'pkey_list': pkey_list
                }
                all_pkey_list.update(pkey_list)
            else:
                pkey = {
                    'inquiry_mode': 'alone',
                    'pkey_list': [info['pKey']]
                }
                all_pkey_list.add(info['pKey'])
        else:
            pkey = {
                'inquiry_mode': 'alone',
                'pkey_list': []
            }
        
        update_dict[now_key].append(
            {
                'pkey': pkey,
                'and_word': and_word,
                'or_word': or_word,
                'not_word': not_word,
                'ignore_word': ignore_word
            }
        )
    
    # 检查alias_all_bid和pkey的存在性
    check_alias_bid_sql = 'SELECT alias_all_bid FROM {table_name} where alias_all_bid in ({alias_bid_list}) group by alias_all_bid;'.format(table_name=modify_table, alias_bid_list=','.join(alias_bid_list))
    useful_alias_bid_list = [info[0] for info in db_cleaner.query_all(check_alias_bid_sql)]
    not_useful_alias_bid_list = [i for i in alias_bid_list if i not in useful_alias_bid_list]

    check_pkey_sql = 'SELECT name, pos_id FROM cleaner.clean_pos where deleteFlag = 0 AND batch_id = {batch_id}'.format(batch_id=batch_id)
    useful_pkey_posid_dict = {x[0]: x[1] for x in db_cleaner.query_all(check_pkey_sql)}
    not_useful_pkey_list = [i for i in all_pkey_list if i not in useful_pkey_posid_dict.keys()]
    if len(not_useful_pkey_list) > 0:
        return eid, '存在无效key值, 请检查后再上传!'

    '''
    final_update_dict = {
        'id': {
            'base_id': str,
            'change_id': str,
            'rank': int
        }
    }
    '''
    final_update_dict = {}
    # 一组数据更新设定
    for base_rank_change_tuple in update_dict.keys():
        if base_rank_change_tuple[0] in useful_alias_bid_list:
            all_pkey = set()
            pro_number_dict = dict()
            now_number = 1
            for info in update_dict[base_rank_change_tuple]:
                for pkey in info['pkey']['pkey_list']:
                    if pkey not in all_pkey:
                        all_pkey.add(pkey)
                        pro_number_dict[pkey] = now_number
                        now_number = now_number + 1
            
            search_item_sql = 'SELECT id{split} {pos_list} FROM {modify_table} where alias_all_bid = {base_id}'.format(split=',' if len(all_pkey) > 0 else '',pos_list=','.join(['sp' + str(useful_pkey_posid_dict.get(pkey)) for pkey in all_pkey]), modify_table=modify_table, base_id=base_rank_change_tuple[0])
            item_info = list(db_cleaner.query_all(search_item_sql))

            update_info_list = update_dict[base_rank_change_tuple]
            # 判断这个item能否符合所有要求
            for item in item_info:
                can_update = True
                update_id = item[0]
                # 单行进行检测
                for update_info in update_info_list:
                    if len(update_info['pkey']['pkey_list']) == 0:
                        break
                    pos_str_list = []
                    prop_value_list = []
                    # 单行取需要的属性进行比对
                    for prop_name in update_info['pkey']['pkey_list']:
                        prop_value_list.append(item[pro_number_dict[prop_name]])
                    if update_info['pkey']['inquiry_mode'] == 'alone':
                        pos_str_list = prop_value_list
                    else:
                        pos_str_list = [''.join(prop_value_list)]
                    for prop in pos_str_list:
                        if len(update_info['ignore_word']) > 0:
                            for word in update_info['ignore_word']:
                                prop = prop.replace(word, '')
                        if len(update_info['and_word']) > 0:
                            for word in update_info['and_word']:
                                if word not in prop:
                                    can_update = False
                                    break
                        if len(update_info['or_word']) > 0:
                            contain_one = False
                            for word in update_info['or_word']:
                                if word in prop:
                                    contain_one = True
                                    break
                            if not contain_one:
                                can_update = False
                                break
                        if len(update_info['not_word']) >0:
                            for word in update_info['not_word']:
                                if word in prop:
                                    can_update = False
                                    break
                    # 行和行之间是and关系
                    if not can_update:
                        break

                if can_update:
                    if final_update_dict.get(update_id) == None or final_update_dict.get(update_id).get('rank') < base_rank_change_tuple[1]:
                        final_update_dict[update_id] = {
                            'base_id': base_rank_change_tuple[0],
                            'change_id': base_rank_change_tuple[2],
                            'rank': base_rank_change_tuple[1]
                        }

    # 根据final_update_dict更新表
    update_sql = 'UPDATE {modify_table} SET {new_column} = %s WHERE id = %s;'.format(modify_table=modify_table, new_column=new_column)
    value = []
    print(len(final_update_dict.keys()))
    for only_id in final_update_dict.keys():
        value.append([final_update_dict[only_id]['change_id'], only_id])
    warn = db_cleaner.execute_many(update_sql, tuple(value))
    if warn:
        return eid, '更新数据时出错'
    db_cleaner.commit()

    return_str = ''
    if len(not_useful_alias_bid_list) > 0:
        return_str = '上传数据中以下alias_all_bid在数据中没有存在:' + ','.join(not_useful_alias_bid_list)
    if len(return_str) > 0:
        return eid, return_str
    else:
        return eid, '更新已成功'