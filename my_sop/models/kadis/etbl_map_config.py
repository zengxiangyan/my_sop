table_etabl_map_config = 'etbl_map_config'

def get_one(db, eid):
    sql = 'select id, eid, tb, name from kadis.{table} where eid=%s'.format(table=table_etabl_map_config)
    data = db.query_one(sql, (eid,))
    return data

#取eid对应的keid记录，没有的话插入一条
def get_or_insert(db, eid):
    data = get_one(db, eid)
    if data is not None:
        return data

    sql = 'select id, name from dataway.entity where eid=%s'
    data = db.query_one(sql, (eid,))
    if data is None:
        return
    name = data[1]
    tb = '' #默认E表
    sql = "insert into kadis.{table} (eid, tb, name) value (%s,%s,%s)".format(table=table_etabl_map_config)
    db.execute(sql,(eid, tb, name))
    db.commit()

    data = get_one(db, eid)
    return data


