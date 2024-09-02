#coding=utf-8

"""
This script is to read csv file of request form and insert into db as settings for a clean batch. Also provide copy and delete functions.
"""

import sys, getopt, os, io, re
from os import listdir
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import csv
import application as app
#from models import market

db_cleaner = app.get_db('default')

def sop_status3(eid, status3):
    if status3 not in ('未开始', '进行中', '失败'):
        status3 = '进行中'
    if eid:
        sql = "UPDATE dataway.entity SET status_3 = '{status3}' WHERE id = {eid};".format(eid=eid, status3=status3)
        print(sql)
        warn = db_cleaner.execute(sql)
        db_cleaner.commit()

def update_sop_status(func):
    def wrapper(*args, **kwargs):
        eid, s = func(*args, **kwargs)
        if s.endswith('上传成功。') or s.endswith('迁移成功。'):
            sop_status3(eid, '进行中')
        elif s.endswith('删除成功。'):
            sop_status3(eid, '未开始')
        else:
            sop_status3(eid, '失败')
        return s
    return wrapper

def get_local_input(input_file_name):
    input_file = open(app.output_path(input_file_name), 'r', encoding = 'utf-8-sig', errors = 'ignore')
    reader = csv.reader(input_file)
    data = [row for row in reader]
    input_file.close()
    return data

@update_sop_status
def add_request(batch_id, data):
    default_other = '其它'

    db_cleaner.connect()
    try:
        eid = db_cleaner.query_scalar('SELECT eid FROM clean_batch WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id=batch_id))
    except:
        return None, '该batch_id在clean_batch表里不存在，输入错误！'
    else:
        if not eid:
            return None, '该batch_id在clean_batch表里未填写eid，请补充后再上传需求表！'
        # try:
        #     is_mixed = db_cleaner.query_scalar('SELECT is_mixed FROM dataway.entity WHERE id = {eid};'.format(eid=eid))
        # except:
        #     return None, '该eid在dataway.entity表里不存在！'
        # else:
        #     if is_mixed == 0:
        #         sop = db_cleaner.query_all('SELECT CONCAT(source, cid) FROM dataway.entity_tip_category WHERE eid = {eid};'.format(eid=eid))
        #     elif is_mixed == 1:
        #         sop = db_cleaner.query_all('SELECT CONCAT(source, cid) FROM dataway.entity_tip_category2 WHERE eid = {eid};'.format(eid=eid))
        #     else:
        #         return None, '该eid在dataway.entity表里的is_mixed字段范围错误！'
        #     sc_item = {i[0] for i in sop}

    first_row = [x.strip() for x in data[0][1:]]
    pos_len = first_row.index('')
    data = data[1:]
    first_column = [default_other if x[0].strip() in ['其他','其她'] else x[0].strip() for x in data if x[0] != '']
    sb_len = len(first_column)

    already_exist = db_cleaner.query_scalar('SELECT COUNT(1) FROM clean_sub_batch WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id=batch_id))
    if already_exist:
        return eid, '因batch{batch_id}已上传过sub_batch，本次需求表上传失败。'.format(batch_id=batch_id)
    sb_dict = dict()
    sub_batch_id = db_cleaner.query_scalar('SELECT MAX(sub_batch_id) FROM clean_sub_batch;')
    last = ''
    sql = "INSERT INTO clean_sub_batch(sub_batch_id, name, batch_id, createTime) VALUES "
    for i in range(sb_len):
        x = first_column[i]
        if x == '有效子品类' or x == '所有子品类':
            continue
        if x == '' or x == default_other:
            break
        if x != last:
            sub_batch_id += 1
            last = x
            sb_dict[x] = sub_batch_id
            sql += '({sub_batch_id}, "{name}", {batch_id}, UNIX_TIMESTAMP()), '.format(sub_batch_id=sub_batch_id, name=x, batch_id=batch_id)
    sb_dict['有效子品类'] = -1
    sb_dict['所有子品类'] = -2
    sql = sql[:-2] + ';'
    print(sql, sb_dict, sep = '\n')
    try:
        warn = db_cleaner.execute(sql)
        if warn and warn.args[0] == 1265:
            db_cleaner.rollback()
            return eid, '因字段超过clean_sub_batch表限定长度，已撤回本次需求表上传。'
    except Exception as e:
        db_cleaner.rollback()
        if e.args[0] == 1205:
            return eid, '因clean_sub_batch表数据库写入错误，已撤回本次需求表上传。请等待锁表状态解除！'
        else:
            return eid, '因clean_sub_batch表数据库写入错误，已撤回本次需求表上传。请检查填写格式！'

    already_exist = db_cleaner.query_scalar('SELECT COUNT(1) FROM clean_pos WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id=batch_id))
    if already_exist:
        return eid, '因batch{batch_id}已上传过pos，本次需求表上传失败。'.format(batch_id=batch_id)
    sql = "INSERT INTO clean_pos(pos_id, name, batch_id, createTime) VALUES "
    for i in range(pos_len):
        sql += '({pos_id}, "{name}", {batch_id}, UNIX_TIMESTAMP()), '.format(pos_id=i+1, name=first_row[i], batch_id=batch_id)
    sql = sql[:-2] + ';'
    print(sql, sep = '\n')
    try:
        warn = db_cleaner.execute(sql)
        if warn and warn.args[0] == 1265:
            db_cleaner.rollback()
            return eid, '因字段超过clean_pos表限定长度，已撤回本次需求表上传。'
    except Exception as e:
        db_cleaner.rollback()
        if e.args[0] == 1205:
            return eid, '因clean_pos表数据库写入错误，已撤回本次需求表上传。请等待锁表状态解除！'
        else:
            return eid, '因clean_pos表数据库写入错误，已撤回本次需求表上传。请检查填写格式！'

    sql = "INSERT INTO clean_target(name, batch_id, sub_batch_id, pos_id, mark, rank, createTime) VALUES "
    for i in range(sb_len):
        for j in range(pos_len):
            if data[i][j+1].strip():
                name, mark, rank = data[i][j+1].strip().split(',')
                sql += '("{name}", {batch_id}, {sub_batch_id}, {pos_id}, {mark}, {rank}, UNIX_TIMESTAMP()), '.format(name=default_other if name in ['其他','其她'] else name, batch_id=batch_id, sub_batch_id=sb_dict.get(first_column[i],0), pos_id=j+1, mark=mark, rank=rank)
    sql = sql[:-2] + ';'
    print(sql)
    try:
        warn = db_cleaner.execute(sql)
        if warn and warn.args[0] == 1265:
            db_cleaner.rollback()
            return eid, '因字段超过clean_target表限定长度，已撤回本次需求表上传。'
    except Exception as e:
        db_cleaner.rollback()
        if e.args[0] == 1205:
            return eid, '因clean_target表数据库写入错误，已撤回本次需求表上传。请等待锁表状态解除！'
        else:
            return eid, '因clean_target表数据库写入错误，已撤回本次需求表上传。请检查填写格式！'

    already_exist = db_cleaner.query_scalar('SELECT COUNT(1) FROM clean_category WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id=batch_id))
    if already_exist:
        return eid, '因batch{batch_id}已上传过category，本次需求表上传失败。'.format(batch_id=batch_id)
    # sc_request = set()
    sql = "INSERT INTO clean_category(source, batch_id, sub_batch_id, filters, and_word, or_word, not_word, ignore_word, rank, createTime) VALUES "
    for row in data:
        x = [i.strip() for i in row[pos_len+2 : pos_len+11]]
        source = x[0]
        if not source:
            break
        filters = x[1]
        sub_batch_id = sb_dict.get(x[2], 0)
        rank = x[3]
        and_word = "'"+x[4]+"'" if x[4] else 'NULL'
        or_word = "'"+x[5]+"'"  if x[5] else 'NULL'
        not_word = "'"+x[6]+"'" if x[6] else 'NULL'
        ignore_word = "'"+x[7]+"'" if x[7] else 'NULL'
        sql += "('{source}', {batch_id}, {sub_batch_id}, '{filters}', {and_word}, {or_word}, {not_word}, {ignore_word}, {rank}, UNIX_TIMESTAMP()), ".format(source=source, batch_id=batch_id, sub_batch_id=sub_batch_id, filters=filters, and_word=and_word, or_word=or_word, not_word=not_word, ignore_word=ignore_word, rank=rank)
        # if source in ('tmall', 'tb'):
        #     sc_request.add('ali' + cid)
        # else:
        #     sc_request.add(source + cid)
    sql = sql[:-2] + ';'
    print(sql)
    try:
        warn = db_cleaner.execute(sql)
        if warn and warn.args[0] == 1265:
            db_cleaner.rollback()
            return eid, '因字段超过clean_category表限定长度，已撤回本次需求表上传。'
    except Exception as e:
        db_cleaner.rollback()
        if e.args[0] == 1205:
            return eid, '因clean_category表数据库写入错误，已撤回本次需求表上传。请等待锁表状态解除！'
        else:
            return eid, '因clean_category表数据库写入错误，已撤回本次需求表上传。请检查填写格式！'
    # if sc_item != sc_request:
    #     db_cleaner.rollback()
    #     msg = '\n导数系统有但需求表没有：' + str(sc_item.difference(sc_request)) + '\n需求表有但导数系统没有：' + str(sc_request.difference(sc_item))
    #     return eid, '因clean_category表与导数系统里勾选的cid不一致，已撤回本次需求表上传。' + msg

    # try:
    #     customized_category = dict()
    #     lv_name_set = [set(), set(), set(), set(), set()]
    #     for row in data:
    #         rl = [i.strip() for i in row[pos_len+12 : pos_len+17]]
    #         d_now = customized_category
    #         bend = False
    #         for i in range(5):
    #             if rl[i]:
    #                 if bend == True:
    #                     db_cleaner.rollback()
    #                     return eid, '客制化品类每行填写只能靠后留空，已撤回本次需求表上传。'
    #                 lv_name_set[i].add(rl[i])
    #                 d_now[rl[i]] = d_now.get(rl[i], dict())
    #                 d_now = d_now[rl[i]]
    #             else:
    #                 bend = True
    #     for i1 in range(5):
    #         for i2 in range(i1):
    #             if lv_name_set[i1].intersection(lv_name_set[i2]):
    #                 db_cleaner.rollback()
    #                 return eid, '客制化品类不同级别禁止同名，已撤回本次需求表上传。'
    # except:
    #     db_cleaner.rollback()
    #     return eid, '请检查客制化品类填写格式，已撤回本次需求表上传。'
    # try:
    #     market.add_category(batch_id, customized_category)      # from zhou.wenjun 20191015
    # except:
    #     db_cleaner.rollback()
    #     return eid, '客制化品类上传接口调用报错，已撤回本次需求表上传。'

    db_cleaner.commit()

    sql = "INSERT INTO clean_correct_rate(clean_flag, sp_no, sp_name, batch_id, kind) VALUES (2, -2, '总正确率', {batch_id}, 'all');".format(batch_id=batch_id)
    print(sql)
    try:
        warn = db_cleaner.execute(sql)
        db_cleaner.commit()
    except:
        pass

    return eid, 'Batch{batch_id}需求表上传成功。'.format(batch_id=batch_id)

@update_sop_status
def delete_request(batch_id):
    db_cleaner.connect()

    try:
        eid = db_cleaner.query_scalar('SELECT eid FROM clean_batch WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(batch_id=batch_id))
    except:
        eid = None
    else:
        if not eid:
            eid = None

    max_del = db_cleaner.query_scalar('SELECT MAX(deleteFlag) FROM clean_pos WHERE batch_id = {batch_id};'.format(batch_id=batch_id))
    del_f = 10 + max_del if max_del else 10

    sql = 'UPDATE clean_sub_batch SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_pos SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_target SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_category SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_unify_result SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_classify_result SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_multi_units SET deleteFlag = {del_f} WHERE deleteFlag = 0 AND batch_id = {batch_id};'.format(del_f=del_f, batch_id=batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)

    db_cleaner.commit()

    return eid, 'Batch{batch_id}需求表配置删除成功。'.format(batch_id=batch_id)

@update_sop_status
def clone_batch_config(old_batch_id, new_batch_id):
    if old_batch_id <= 0 or new_batch_id <= 0 or old_batch_id == new_batch_id:
        return None, 'Batch_id输入错误！'

    delete_request(new_batch_id)

    try:
        eid = db_cleaner.query_scalar('SELECT eid FROM clean_batch WHERE deleteFlag = 0 AND batch_id = {new_batch_id};'.format(new_batch_id=new_batch_id))
    except:
        return None, '该batch_id在clean_batch表里不存在，输入错误！'
    else:
        if not eid:
            return None, '该batch_id在clean_batch表里未填写eid，请补充后再迁移需求表！'
        # try:
        #     is_mixed = db_cleaner.query_scalar('SELECT is_mixed FROM dataway.entity WHERE id = {eid};'.format(eid=eid))
        # except:
        #     return None, '该eid在dataway.entity表里不存在！'
        # else:
        #     if is_mixed == 0:
        #         sop = db_cleaner.query_all('SELECT CONCAT(source, cid) FROM dataway.entity_tip_category WHERE eid = {eid};'.format(eid=eid))
        #     elif is_mixed == 1:
        #         sop = db_cleaner.query_all('SELECT CONCAT(source, cid) FROM dataway.entity_tip_category2 WHERE eid = {eid};'.format(eid=eid))
        #     else:
        #         return None, '该eid在dataway.entity表里的is_mixed字段范围错误！'
        #     new_sc_item = {i[0] for i in sop}

    sql = 'INSERT INTO clean_sub_batch(deleteFlag, name, batch_id, createTime) SELECT -sub_batch_id, name, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_sub_batch WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sub_batch_data = db_cleaner.query_all('SELECT sub_batch_id, deleteFlag FROM clean_sub_batch WHERE deleteFlag < 0 AND batch_id = {new_batch_id};'.format(new_batch_id=new_batch_id))
    old_map_new_sub_batch = {0: 0}
    for tup in sub_batch_data:
        sub_batch_id, old_inverse = tup
        old_map_new_sub_batch[-old_inverse] = sub_batch_id
    sql = 'UPDATE clean_sub_batch SET deleteFlag = 0 WHERE deleteFlag < 0 AND batch_id = {new_batch_id};'.format(new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)

    sql = 'INSERT INTO clean_pos(pos_id, name, type, calculate_sum, p_no, p_key, ignore_word, remove_word, default_quantifier, only_quantifier, prefix, unit_conversion, from_multi_sp, from_multi_spid, read_from_right, pre_unit_in, pure_num_in, pure_num_out, num_range, as_model, target_no_rank, in_question, single_rate, all_rate, in_market, split_in_e, output_type, output_case, sku_default_val, batch_id, createTime) \
                          SELECT pos_id, name, type, calculate_sum, p_no, p_key, ignore_word, remove_word, default_quantifier, only_quantifier, prefix, unit_conversion, from_multi_sp, from_multi_spid, read_from_right, pre_unit_in, pure_num_in, pure_num_out, num_range, as_model, target_no_rank, in_question, single_rate, all_rate, in_market, split_in_e, output_type, output_case, sku_default_val, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_pos WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)

    old_target_data = db_cleaner.query_all('SELECT name, batch_id, sub_batch_id, pos_id, mark, rank FROM clean_target WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id))
    sql = 'INSERT INTO clean_target(name, batch_id, sub_batch_id, pos_id, mark, rank, createTime) VALUES '
    for tup in old_target_data:
        name, batch_id, sub_batch_id, pos_id, mark, rank = tup
        sql += '("{name}", {batch_id}, {sub_batch_id}, {pos_id}, {mark}, {rank}, UNIX_TIMESTAMP()), '.format(name=name, batch_id=new_batch_id, sub_batch_id=old_map_new_sub_batch.get(sub_batch_id, sub_batch_id) if sub_batch_id <0 else old_map_new_sub_batch.get(sub_batch_id, 0), pos_id=pos_id, mark=mark, rank=rank)
    sql = sql[:-2] + ';'
    print(sql)
    warn = db_cleaner.execute(sql)
    target_data = db_cleaner.query_all('SELECT target_id, name, batch_id, sub_batch_id, pos_id, mark, rank FROM clean_target WHERE deleteFlag = 0 AND batch_id IN ({old_batch_id}, {new_batch_id});'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id))
    target_dict = dict()
    for tup in target_data:
        target_id, name, batch_id, sub_batch_id, pos_id, mark, rank = tup
        key = (name, pos_id, mark, rank, old_map_new_sub_batch.get(sub_batch_id, sub_batch_id))
        target_dict[key] = target_dict.get(key, dict())
        target_dict[key][batch_id] = target_id
    old_map_new_target = dict()
    for key in target_dict:
        old_map_new_target[target_dict[key][old_batch_id]] = target_dict[key][new_batch_id]

    sql = 'INSERT INTO clean_category(source, filters, sub_batch_id, rank, and_word, or_word, not_word, ignore_word, batch_id, createTime) \
                               SELECT source, filters, sub_batch_id, rank, and_word, or_word, not_word, ignore_word, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_category WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    vals = [(old_map_new_sub_batch.get(old_sub_batch_id, 0), old_sub_batch_id) for old_sub_batch_id in old_map_new_sub_batch]
    sql = 'UPDATE clean_category SET sub_batch_id = %s WHERE batch_id = {new_batch_id} AND sub_batch_id = %s;'.format(new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute_many(sql, tuple(vals))

    sql = 'INSERT INTO clean_keyword(name, target_id, and_name, not_name, ignore_name, createTime, deleteFlag) \
                            SELECT k.name, k.target_id, k.and_name, k.not_name, k.ignore_name, UNIX_TIMESTAMP(), -1 FROM \
                            clean_keyword k LEFT JOIN clean_target t ON k.target_id = t.target_id WHERE k.deleteFlag = 0 AND t.deleteFlag = 0 AND t.batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    vals = [(old_map_new_target.get(old_target_id, 0), old_target_id) for old_target_id in old_map_new_target]
    sql = 'UPDATE clean_keyword SET target_id = %s, deleteFlag = 0 WHERE target_id = %s AND deleteFlag = -1;'
    print(sql)
    warn = db_cleaner.execute_many(sql, tuple(vals))
    sql = 'INSERT INTO clean_price(target_id, min_amount, max_amount, createTime, deleteFlag) \
                            SELECT k.target_id, k.min_amount, k.max_amount, UNIX_TIMESTAMP(), -1 FROM \
                            clean_price k LEFT JOIN clean_target t ON k.target_id = t.target_id WHERE k.deleteFlag = 0 AND t.deleteFlag = 0 AND t.batch_id = {new_batch_id};'.format(new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'UPDATE clean_price SET target_id = %s, deleteFlag = 0 WHERE target_id = %s AND deleteFlag = -1;'
    print(sql)
    warn = db_cleaner.execute_many(sql, tuple(vals))

    sql = 'INSERT INTO clean_unify_result(base_pos_id, base_sp_value, base_sp_not_value, change_pos_id, change_sp_value, rank, batch_id, createTime) \
                                   SELECT base_pos_id, base_sp_value, base_sp_not_value, change_pos_id, change_sp_value, rank, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_unify_result WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'INSERT INTO clean_classify_result(base_pos_id, base_min_value, base_max_value, base_unit, change_pos_id, change_value, rank, batch_id, createTime) \
                                      SELECT base_pos_id, base_min_value, base_max_value, base_unit, change_pos_id, change_value, rank, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_classify_result WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)
    sql = 'INSERT INTO clean_multi_units(pos_id, base_min_value, base_max_value, base_unit, change_unit, unit_conversion, batch_id, createTime) \
                                  SELECT pos_id, base_min_value, base_max_value, base_unit, change_unit, unit_conversion, {new_batch_id}, UNIX_TIMESTAMP() FROM clean_multi_units WHERE deleteFlag = 0 AND batch_id = {old_batch_id};'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)
    print(sql)
    warn = db_cleaner.execute(sql)

    db_cleaner.commit()
    # market.clone_category(old_batch_id, new_batch_id)      # from zhou.wenjun 20191015

    # new_sc_request = set()
    # new_category_data = db_cleaner.query_all('SELECT source, cid FROM clean_category WHERE deleteFlag = 0 AND batch_id = {new_batch_id};'.format(new_batch_id=new_batch_id))

    # for tup in new_category_data:
    #     source, cid = tup
    #     if source in ('tmall', 'tb'):
    #         new_sc_request.add('ali' + str(cid))
    #     else:
    #         new_sc_request.add(source + str(cid))
    # if new_sc_item != new_sc_request:
    #     msg = '\n导数系统有但需求表没有：' + str(new_sc_item.difference(new_sc_request)) + '\n需求表有但导数系统没有：' + str(new_sc_request.difference(new_sc_item))
    #     return eid, '因clean_category表与导数系统里勾选的cid不一致，请额外调整batch{new_batch_id}配置。'.format(new_batch_id=new_batch_id) + msg
    # else:
    return eid, 'Batch{new_batch_id}需求表已从batch{old_batch_id}迁移成功。'.format(old_batch_id=old_batch_id, new_batch_id=new_batch_id)

def usage():
    print('''Run clean_read_request_csv.py following by below command-line arguments:
    -a <add/delete/clone> -b <batch_id(positive integer)> -o <old_batch_id(positive integer)> --loadcsv="***.csv"
    -h <help>''')

def main():
    options, args = getopt.getopt(sys.argv[1:], 'a:b:o:h', ["loadcsv="])
    help = False
    action = ''
    batch_id = 0
    old_batch_id = 0
    input_file_name = None
    for name, value in options:
        if name in ('--loadcsv'):
            input_file_name = value
        if name in ('-a', '-action'):
            action = value
        if name in ('-b', '-batch_id'):
            batch_id = int(value)
        if name in ('-o', '-old_batch_id'):
            old_batch_id = int(value)
        if name in ('-h', '-help'):
            help = True
    if action == '' or batch_id == 0:
        help = True
    if help:
        return usage()
    if action == 'add' and input_file_name:
        csv_data = get_local_input(input_file_name)
        print(csv_data)
        msg = add_request(batch_id, csv_data)
        print(msg)
    elif action == 'delete':
        msg = delete_request(batch_id)
        print(msg)
    elif action == 'clone':
        msg = clone_batch_config(old_batch_id, batch_id)
        print(msg)