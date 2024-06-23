import time
import json
import  application as app

table = 'clean_import_tb_item'

#status 0: 初始状态 1 导完ali 2 导完jd 4 导完 suning 7 导完所有
def add_task(db, eid):
    sql = 'select status from {table} where eid=%s'.format(table=table)
    row = db.query_one(sql, (eid, ))
    if row is None:
        now = time.time()
        sql = 'insert into {table} (eid, create_time) values(%s, %s)'.format(table=table)
        db.execute(sql, (eid, now))
        db.commit()
        return 1

    status = row[0]
    if status < 7:
        print('task eid:{eid} already exist'.format(eid=eid))
        return 0

    sql = 'update {table} set status=0, ali_used=0, jd_used=0, suning_used=0 where eid=%s'.format(table=table)
    db.execute(sql, (eid,))
    db.commit()
    return 2

def get_wait_task(db):
    sql = 'select eid from {table} where status=0'.format(table=table)
    return db.query_all(sql)

def update_status(db, eid, source, start_time, end_time, delta, id_list):
    h_status_add = {'tmall': 1, 'jd': 2, 'suning': 4}
    status_add = h_status_add[source]
    h_used_key = {'tmall': 'ali_used', 'jd': 'jd_used', 'suning': 'suning_used'}
    used_key = h_used_key[source]
    h_source_str = {'tmall': 0, 'jd': 1, 'suning': 2}
    source_str = h_source_str[source]
    sql = 'update {table} set status=status ^ {status_add},{used_key}={delta} where eid={eid}'.format(table=table, status_add=status_add, used_key=used_key, delta=delta, eid=eid)
    db.execute(sql)

    now = time.time()
    sql = 'insert into clean_import_tb_item_log(eid, source, start_time, end_time, id_list, create_time) values(%s, %s, %s, %s, %s, %s)'
    db.execute(sql, (eid, source_str, start_time, end_time, json.dumps(id_list, ensure_ascii=False), now))
    db.commit()