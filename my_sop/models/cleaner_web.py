import os
import sys
import json
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.entity_manager as entity_manager
import application as app


def clean_batch():
    db26 = app.get_db('26_apollo')
    db26.connect()
    sql = '''
        SELECT eid, `sort` FROM dataway.eid_update_set WHERE eid IN (SELECT eid FROM cleaner.clean_batch WHERE eid > 0)
    '''
    ret = db26.query_all(sql)
    aaa = {v[0]: v[1] for v in ret}

    sql = '''
        SELECT batch_id, user_id FROM cleaner.clean_batch_task_actor WHERE actor_type = 3 AND delete_flag = 0
    '''
    ret = db26.query_all(sql)
    bbb = {v[0]: v[1] for v in ret}

    sql = '''
        SELECT batch_id, eid, name, comments, batch_type, has_client, update_alias_bid, is_item_table_mysql, report_month, status, estatus, updateTime
        FROM cleaner.clean_batch ORDER BY batch_id
    '''
    ret = db26.query_all(sql)
    data = []
    for v in ret:
        link = '''
            <button type="button" class="btn btn-success" onclick="window.open('/cleaner/index.php?r=admin%2Fdashboard%2Findex&amp;id={id}');">dashboard</button>
        '''.format(id=v[0])
        v = [link] + list(v) + [aaa[v[1]] if v[1] in aaa else 0, bbb[v[0]] if v[0] in bbb else '']
        data.append([str(vv) for vv in v])
    return data


def batch_insert(sql, cols, data):
    db26 = app.get_db('26_apollo')
    db26.connect()

    d = [data[c] if c in data else '' for c in cols]
    s = ('%s',) * len(cols)
    sql = sql.format(','.join(['`{}`'.format(c) for c in cols]), ','.join(s))
    print(sql, d)
    return 1
    db26.execute(sql, tuple(d))
    last_id = db26.con.insert_id()
    db26.commit()
    return last_id


def batch_update(sql, cols, data):
    db26 = app.get_db('26_apollo')
    db26.connect()

    cols = [c for c in cols if c in data]
    if len(cols) > 0:
        d = [data[c] for c in cols]
        s = ['`{}`=%s'.format(c) for c in cols]
        sql = sql.format(','.join(s))
        db26.execute(sql, tuple(d))
    db26.commit()


def clean_pos(batch_id):
    db26 = app.get_db('26_apollo')
    db26.connect()
    sql = '''
        SELECT pos_id,name,type,calculate_sum,p_no,p_key,ignore_word,remove_word,default_quantifier,only_quantifier,prefix,
               unit_conversion,from_multi_sp,from_multi_spid,read_from_right,pre_unit_in,pure_num_in,pure_num_out,num_range,
               as_model,target_no_rank,in_question,single_rate,all_rate,in_market,split_in_e,output_type,output_case,
               sku_default_val,createTime
        FROM cleaner.clean_pos WHERE batch_id = {} AND deleteFlag = 0
    '''.format(batch_id)
    ret = db26.query_all(sql)

    data = []
    for v in ret:
        if v[2] == 900:
            link = '''
                <a href="/cleaner/index.php?r=admin%2Fclean-category%2Findex2&amp;id={}" target="_blank">＞target</a><br />
            '''.format(batch_id)
        else:
            link = '''
                <a href="/cleaner/index.php?r=admin%2Fclean-target%2Findex2&amp;id={}&amp;pid={}" target="_blank">＞target</a><br />
            '''.format(batch_id, v[0])
        v = list(v)
        v[11] = json_decode(v[11])
        v[11] = json.dumps(v[11], ensure_ascii=False)
        v = [link] + v
        data.append([str(vv) for vv in v])
    return data


def clean_category(batch_id):
    db26 = app.get_db('26_apollo')
    db26.connect()
    sql = '''
        SELECT id,source,filters,sub_batch_id,rank,and_word,or_word,not_word,ignore_word,createTime
        FROM cleaner.clean_category WHERE batch_id = {} AND deleteFlag = 0
    '''.format(batch_id)
    ret = db26.query_all(sql)

    data = []
    for v in ret:
        data.append([str(vv) for vv in v])
    return data


def clean_target(batch_id, pos_id):
    db26 = app.get_db('26_apollo')
    db26.connect()
    sql = '''
        SELECT target_id,name,sub_batch_id,pos_id,mark,rank,createTime
        FROM cleaner.clean_target WHERE batch_id = {} AND pos_id = {} AND deleteFlag = 0
    '''.format(batch_id, pos_id)
    ret = db26.query_all(sql)

    data = []
    for v in ret:
        data.append([str(vv) for vv in v])
    return data


def json_decode(v):
    try:
        return json.loads(v)
    except:
        try:
            return eval(v)
        except:
            pass
    return None
