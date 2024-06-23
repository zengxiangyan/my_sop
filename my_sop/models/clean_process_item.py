#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import random
import pandas as pd
import application as app
from models import common
from models import entity_manager
from models.clean import Clean
from models.clean_batch import CleanBatch
from models.plugin_manager import PluginManager

db_cleaner = app.get_db('default')

def sp_distribution(batch_id, csv_out = False):
    batch_now = CleanBatch(batch_id)
    assert batch_now.item_table_name is not None, '该项目已从MySQL迁移到ClickHouse，本功能不支持使用。'
    first_row, data, id_count = batch_now.item_distribution()
    if csv_out:
        output_file_name = 'Batch{id}_{name} 各sp取值分布 {t}.csv'.format(id=batch_now.batch_id, name=batch_now.batch_name, t=time.strftime('%Y-%m-%d %H-%M', time.localtime()))
        common.write_csv(output_path_name = app.output_path(output_file_name), data = data, first_row = first_row)
    return first_row, data

def search_sp(batch_id, pos_id, sp_value, num = 0, csv_out = False):
    batch_now = CleanBatch(batch_id)
    assert batch_now.item_table_name is not None, '该项目已从MySQL迁移到ClickHouse，本功能不支持使用。'
    batch_now.check_lack_fields()
    sql = "SELECT {fields} FROM {item_table} WHERE sp{pos_id} = %s {limit_num};".format(fields=', '.join(batch_now.item_fields_use), item_table=batch_now.item_table_name, pos_id=pos_id, limit_num='LIMIT {n}'.format(n=num) if num > 0 else '')
    data = batch_now.db_artificial_new.query_all(sql, (sp_value, ))
    if csv_out:
        output_file_name = 'Batch{id}_{name} sp{i}定值 前{n}条 {t}.csv'.format(id=batch_id, name=batch_now.batch_name, i=pos_id, n=num, t=time.strftime('%Y-%m-%d %H-%M', time.localtime()))
        common.write_csv(output_path_name = app.output_path(output_file_name), data = data, first_row = batch_now.item_fields_use)
    return batch_now.item_fields_use, data

def get_clean_log(batch_id, id, comments = None, txt_out = False):
    batch_now = CleanBatch(batch_id, store_log=True)
    batch_now.comments = comments
    batch_now.get_config()
    if batch_now.is_item_table_mysql:
        id = int(id)
        batch_now.check_lack_fields()
        data = batch_now.read_items(condition='id = {id}'.format(id=id))
        result_dict = batch_now.process_given_items(data)
    else:
        id = str(id)
        cln = entity_manager.get_clean(batch_id)
        cache = cln.get_plugin().get_cachex()
        batch_now = cln.batch_now(cache)
        batch_now.store_log = True
        batch_now.print_out = True
        cln.sample(None, where = f"uuid2 IN ('{id}')", limit = 'LIMIT 1')
    if txt_out:
        output_file_name = 'Batch{id}_{name} id {i} 机洗过程 {t}.txt'.format(id=batch_id, name=batch_now.batch_name, i=id, t=time.strftime('%Y-%m-%d %H-%M', time.localtime()))
        with open(app.output_path(output_file_name), mode = 'w') as f:
            for row in batch_now.log:
                f.write(row + '\n')
    return batch_now.log

def csv_format(batch_now, l):
    if not l:
        return ['已完成'], []

    p_no_list = ['p1', 'p2']
    all_used_fields = ['id', 'snum', 'tb_item_id', 'source', 'month', 'name', 'sid', 'shop_name', 'shopkeeper', 'shop_type', 'shop_type_ch', 'cid', 'real_cid', 'region_str', 'brand', 'all_bid', 'alias_all_bid', 'sub_brand_name', 'product', 'prop_all', 'trade_prop_all'] + p_no_list + ['avg_price', 'price', 'trade', 'num', 'sales', 'visible', 'visible_check', 'clean_flag', 'prop_check', 'clean_type', 'clean_ver', 'all_bid_sp', 'alias_all_bid_sp']
    pos_id_list = sorted(batch_now.pos_id_list)
    for i in pos_id_list:
        all_used_fields.extend(['mp'+str(i), 'sp'+str(i)])
    all_used_fields += ['pid', 'img', 'created', 'modified']

    part_keys = []
    not_in_field_keys = []
    for i in l[0].keys():
        if i in all_used_fields:
            part_keys.append(i)
        else:
            not_in_field_keys.append(i)

    keys = sorted(part_keys, key = lambda x:all_used_fields.index(x)) + sorted(not_in_field_keys)

    keys.remove('id')
    keys.insert(0, 'id')
    rows = []
    for d in l:
        now = [str(d[k]) for k in keys]
        rows.append(now)
    return keys, rows

def random_test(batch_id, num, id_list = None, quick = False, ans_expand = False, csv_out = False, sop_sampling = False):
    cln = entity_manager.get_clean(batch_id)
    cache = cln.get_plugin().get_cachex()
    batch_now = cln.batch_now(cache)

    if batch_now.item_table_name is None:
        batch_now.store_log = True
        batch_now.print_out = True
        suffix = '_sample'
        data = []
        if not id_list:
            if num > 0:
                if quick:
                    data = cln.sample(suffix, limit = f'LIMIT {num}')
                else:
                    data = cln.sample(suffix, limit = f'LIMIT {num} BY snum, cid', bycid = True)
            else:
                suffix = ''
                ans_num = 1 if ans_expand else 2
                bname, btbl = cln.get_plugin().get_b_tbl()
                condition = f'uuid2 IN (SELECT uuid2 FROM {btbl} WHERE similarity >= {ans_num})'
                cln.mainprocess('2001-01-01', '2040-01-01', 3, condition, '', force=True)
        else:
            if not sop_sampling:
                id_list = "'" + id_list.replace(",", "','") + "'"
                data = cln.sample(suffix, limit = f'LIMIT 10000', where = f"uuid2 IN ({id_list})")
            else:
                id_list = id_list.split(',')
                id_info_list = [id_info.split('|') for id_info in id_list]
                id_info_list = ["(" + id_info[0] + ",'" + id_info[1] + "','" + id_info[2] + "')" for id_info in id_info_list]
                id_info_list = ','.join(id_info_list)
                data = cln.sample(suffix, limit = f'LIMIT 10000', where = f"(source, pkey, uuid2) IN ({id_info_list})")
        header, data_rows = csv_format(batch_now, data)
    else:
        batch_now.get_config()
        status, table_fields = batch_now.table_alter_status(batch_now.item_table_name)
        assert status >= 0, '该batch缺少item表，请先导数。'
        assert status == 0, '该batch的数据表正在修改表结构，请等候一段时间重新点击，或联系开发人员协助。'
        batch_now.check_lack_fields()
        print(batch_now)

        if not id_list:
            if num > 0:
                fix_result_cond = 'WHERE id > {last_id}'.format(last_id=batch_now.last_id) if batch_now.last_id > 0 else ''
                sql = 'SELECT MIN(id), MAX(id), COUNT(id) FROM {item_table} {wh};'.format(item_table=batch_now.item_table_name, wh=fix_result_cond)
                min_id, max_id, count_id = batch_now.db_artificial_new.query_all(sql)[0]
                assert count_id > 0, '该batch现有数据已全部交付，如确有配置变更及全量重洗需求，请修改clean_batch表对应的last_id解除数据锁定。'
                if not quick and (max_id - min_id + 1 == count_id):
                    quick = True
                if quick:
                    ids = range(max(min_id, batch_now.last_id), max_id + 1)
                else:
                    sql = 'SELECT id FROM {item_table} {wh};'.format(item_table=batch_now.item_table_name, wh=fix_result_cond)
                    data = batch_now.db_artificial_new.query_all(sql)
                    ids = [r[0] for r in data]
                smp = random.sample(ids, num) if num < len(ids) else ids
                id_list = ','.join(map(str, smp))
            else:
                sql = 'SELECT entity_id FROM product_lib.entity_{eid}_item_merge WHERE similarity >= {s};'.format(eid=batch_now.eid, s=1 if ans_expand else 2)   # zhou.wenjun: 答题不回填到item表 2020-05-08
                try:
                    data = batch_now.db_artificial_new.query_all(sql)
                    id_list = ','.join([str(row[0]) for row in data])
                finally:
                    assert id_list, '该batch无出题宝贝或未计算正确率，如有疑问请联系出题人员。'

        cond = 'id IN ({id_list})'.format(id_list=id_list)
        data = batch_now.read_items(condition=cond)
        result_dict = batch_now.process_given_items(data)
        batch_now.update_given_items(result_dict)

        sql = 'SELECT {fu} FROM {item_table} WHERE {cond} ORDER BY id ASC;'.format(fu=', '.join(batch_now.item_fields_use), item_table=batch_now.item_table_name, cond=cond)
        data_rows = batch_now.db_artificial_new.query_all(sql)
        header = batch_now.item_fields_use

    if csv_out:
        output_file_name = 'Batch{id}_{name} 机洗抽样测试 {n}条 {t}.csv'.format(id=batch_id, name=batch_now.batch_name, n=num, t=time.strftime('%Y-%m-%d %H-%M', time.localtime()))
        common.write_csv(output_path_name = app.output_path(output_file_name), data = data_rows, first_row = header)

    return id_list, header, data_rows

def analyze_item(batch_id, start = None, end = None, condition = '', mul_pro = 0, distri = False):
    db_cleaner.connect()
    is_item_table_mysql = db_cleaner.query_scalar('SELECT is_item_table_mysql FROM clean_batch WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=batch_id))
    db_cleaner.close()

    if is_item_table_mysql:
        assert (start is None or '-' not in start) and (end is None or '-' not in end), '机洗item表起止id参数传递错误！'
        plugin = PluginManager.getPlugin('analyze_item{batch_id}'.format(batch_id=batch_id), defaultPlugin='analyze_item', args=CleanBatch(batch_id, print_out=False))
        plugin.start(start, end, condition, mul_pro, distri)
    else:
        plugin = PluginManager.getPlugin('analyze_a{batch_id}'.format(batch_id=batch_id), defaultPlugin='analyze_a', args=entity_manager.get_clean(batch_id))
        plugin.start(start, end, condition, mul_pro)

# def check_batch_overlap(batch_id_list_s = None):
#     not_batch_id_list = [27, 49]    # batch 27 & 49 only for test
#     use_batch_id_list = list(map(int, batch_id_list_s.split(','))) if batch_id_list_s else []
#     cid_batch_dict, df_overlap_dict = CleanBatch.batch_overlap_within_same_cid(not_batch_id_list = not_batch_id_list, use_batch_id_list = use_batch_id_list)

#     save_path = app.mkdirs('overlap')
#     app.cleardir(save_path)
#     with open(join(save_path, '不同子品类有重合宝贝的cid列表.txt'), mode = 'w') as f:
#         s = 'batch ' + batch_id_list_s if batch_id_list_s else '所有batch'
#         f.write('于{t}已检查{s}，不同子品类有重合宝贝的cid列表如下：\n'.format(s=s, t=time.strftime('%Y-%m-%d %H:%M', time.localtime())))
#         for key in df_overlap_dict:
#             source, cid = key
#             f.write('{source}{cid}: {list}\n'.format(source=source, cid=cid, list=str(cid_batch_dict[key])[1:-1]))
#             output_file_name = '{source}{cid}_不同子品类有重合宝贝.csv'.format(source=source, cid=cid)
#             df_overlap_dict[key].to_csv(join(save_path, output_file_name), encoding = 'utf-8-sig', index = False)

'''

def get_ali_cid_list(sub_batch_id):
    data = db_cleaner.query_all('SELECT DISTINCT cid FROM clean_category WHERE sub_batch_id = {sub_batch_id} and deleteFlag = 0;'.format(sub_batch_id=sub_batch_id))
    cid_list = [i[0] for i in data]
    return cid_list

def analyze_props(batch_id, all_mode):
    global all_units, keywords_dict, target_dict, period_start, period_end, pos, sub_batch, eid, pos_id_list, sub_batch_id_list

    db_clickhouse.connect()
    db_cleaner.connect()

    get_config(batch_id)

    row_columns = ["category_id", "prop_name", "value_name", "tb_item_id", "name", "totalnum"]
    col_map = {}
    for i in range(len(row_columns)):
        col_map[row_columns[i]] = i

    for pos_id in pos_id_list:
        kind = pos[pos_id]['name']
        for sub_batch_id in sub_batch_id_list:
            print(sub_batch[sub_batch_id] + '_' + kind)
            if not prop[sub_batch_id][pos_id]:
                continue
            cid_list = tuple(get_ali_cid_list(sub_batch_id))
            if len(cid_list)==1:
                cid_list = "({c_l})".format(c_l=cid_list[0])
            prop_list = tuple(prop[sub_batch_id][pos_id]) if len(prop[sub_batch_id][pos_id])>1 else "('"+prop[sub_batch_id][pos_id][0]+"')"
            sql = """SELECT category_id, prop_name, value_name, tb_item_id, name, sum(num) as totalnum
                    FROM props_sum_value
                    WHERE category_id in {c}
                        AND (month BETWEEN {s} AND {e})
                        AND prop_name in {p}
                    GROUP BY category_id, prop_name, value_name, tb_item_id, name
                    ORDER BY totalnum DESC
                ; """.format(c=cid_list, s=period_start, e=period_end, p=prop_list)
            data = db_clickhouse.query_all(sql)

            if all_mode:
                output_file_name = sub_batch[sub_batch_id] + '_' + kind + ".csv"
            else:
                output_file_name = sub_batch[sub_batch_id] + '_need_review_' + kind + ".csv"
            output_file = open(app.output_path(output_file_name), 'w', encoding = 'gbk', errors = 'ignore', newline = '')
            writer = csv.writer(output_file)
            writer.writerow(row_columns+['中间结果','Determined',kind])
            for d in data:
                item = list(d)
                if pos[pos_id]['type'] == 1: # 有限种类
                    mp, result, length, other = get_result_value(item[col_map['value_name']], keywords_dict[sub_batch_id][pos_id], target_dict[sub_batch_id][pos_id])
                    mp = ' + '.join(mp) if mp else ''
                    item.extend([mp, result])
                elif pos[pos_id]['type'] >= 200: # 品牌、品类
                    pass
                elif pos[pos_id]['type'] == 100: # 型号
                    mp, result = get_model([item[col_map['value_name']],item[col_map['name']]], '', [])
                elif pos[pos_id]['type'] >= 3: # 提取量词
                    mp, result = quantify_num(pos[pos_id], [item[col_map['value_name']], item[col_map['name']]])
                    item.extend([mp, result])
                if all_mode:
                    writer.writerow(item)    # 输出全部
                elif (length != 1):      # 只输出需要人工检查的
                    writer.writerow(item)
            output_file.close()

def analyze_words():
    global all_units, gh_words
    gh_words = dict()
    db_1_apollo.connect()
    db_18_apollo.connect()
    global all_units, gh_category
    gh_category = common.get_category_last_name(db_1_apollo, cid_list)
    sql = 'select id, source, cid, name, product'
    for idx in range(1, 9):
        sql += ',p' + str(idx)
    sql += ' from artificial_new.entity_90253_item where cid id>%d limit %d'
    utils.easy_traverse(db_18_apollo, sql, process_one_for_analyze_words, start=0, limit=10, test=-1)
    l = []
    for w in gh_words:
        l.append((w, gh_words[w][0]))
    sorted_list = sorted(l, key=itemgetter(1), reverse=True)
    i = 0
    for row in sorted_list:
        i += 1
        if i > 1000:
            break
        print(row)

def process_one_for_analyze_words(data):
    global all_units, gh_words
    for row in data:
        s = ''
        for k in row:
            s += str(k) + ','
        s = pnlp.unify_character(s)
        word_list = jieba_text.convert_doc_to_wordlist(s, True)
        #文字中带有某个字的出现
        # for w in word_list:
        #     if '糖' in w:
        #         h_sub = utils.get_or_new(gh_words, [w], [0])
        #         h_sub[0] += 1

'''