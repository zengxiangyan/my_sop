#coding=utf-8
from os import listdir, mkdir, chmod
import sys
import re
import json
import csv
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

def export_csv(d, file, limit, result):
    try:
        mkdir(app.output_path(d), 0o777)
    except Exception as e:
        print(e.args)

    file = d+'/'+file
    print('******************WRITE csv for {} limit{}******************'.format(file, limit))
    with open(app.output_path(file), 'w', encoding = 'gbk', errors = 'ignore', newline = '') as output_file:
        writer = csv.writer(output_file)
        for row in result:
            writer.writerow(row)
    output_file.close()
    chmod(app.output_path(file), 0o777)

def format_csv(data):
    result = []
    first_row = data[0]
    for i, r in enumerate(first_row):
        for pos_mapped in data[2][i].split(","):
            for key in pos_mapped.split(','):
                if key != '':
                    key = re.match('(.*?)(\d*)$', key)
                    order = key.group(2) if key.group(2) != '' else '0'
                    result.append({'key': r, 'name': data[1][i], 'pos_mapped': key.group(1), 'order': order})
    return result

def upload(dba, bid, data):
    sql = "UPDATE cleaner.clean_pos set p_key = NULL where batch_id = %s"
    dba.execute(sql, (bid,))
    for name in data:
        kval = data[name]
        for cid in kval:
            for k in kval[cid]:
                kval[cid][k] = '+'.join(kval[cid][k])
            kval[cid] = ','.join([kval[cid][k] for k in sorted(kval[cid].keys())])
            # TODO: 小五 第三行填好导pKey的时候。非“型号”pos，每个cid的pkey后面默认要加,name  “型号”pos，每个cid的pkey后面默认要加, sub_brand_name, product
            if name == '型号':
                kval[cid] += ',sub_brand_name,product'
            else:
                kval[cid] += ',name'
        sql = "UPDATE cleaner.clean_pos set p_key = %s where name = %s and batch_id = %s"
        dba.execute(sql, (json.dumps(kval, ensure_ascii=False), name, bid))


def load_csv(csvdir):
    files = listdir(csvdir)
    data = {}
    for file in files:
        m = re.match('([a-z]+\d+)\.csv', file)
        if m != None:
            cid = m.group(1).replace(' ', '')

            input_file = open(csvdir+'/'+file, 'r', encoding='gbk', errors='ignore')
            reader = csv.reader(input_file)
            data[cid] = [row for row in reader][0:4]
    return data


def sample(bid, limit=10000):
    db_26 = app.get_db('26_apollo')
    db_26.connect()

    eid, status = eid_by_bid(db_26, bid)

    sql = "SELECT a.name, a.p_key FROM cleaner.clean_pos a join cleaner.clean_batch b using(batch_id) where a.deleteFlag = 0 and b.eid = %s"
    data = db_26.query_all(sql, (eid,))
    props = {}
    for v in data:
        keys = json.loads(v[1]) if v[1] != None else []
        for cid in keys:
            for i, prop in enumerate(keys[cid].split(',')):
                for p in prop.split('+'):
                    if str(cid)+p not in props:
                        props[str(cid)+p] = []
                    props[str(cid)+p].append(v[0]+str(i if i > 0 else ''))
    for v in props:
        props[v] = ','.join(props[v])

    db_26.close()

    dbch = app.get_clickhouse('chsop')
    dbch.connect()

    tbl = 'sop.entity_prod_{}_A'.format(eid)
    cols = ['uuid2', 'sign', 'ver', 'pkey', 'date', 'cid', 'real_cid', 'item_id', 'sku_id', 'name', 'sid', 'shop_type', 'brand', 'rbid', 'all_bid', 'alias_all_bid', 'sub_brand', 'region', 'region_str', 'price', 'org_price', 'promo_price', 'trade', 'num', 'sales', 'img', 'source', 'trade_props.name', 'trade_props.value', 'props.name', 'props.value']

    sql = '''
        SELECT cid, any(source),
            transform(IF(source=1 AND (shop_type < 20 and shop_type > 10 ), 0, source), [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,24,999], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu','dy','cdf','lvgou','dewu','hema','sunrise','test17','test18','test19','ks',''], '') ss
        , count(*) FROM {} group by cid, ss
    '''.format(tbl)
    cids = dbch.query_all(sql)

    for cid, snum, source, count, in cids:
        sql = '''
            SELECT {} FROM {t}
            WHERE (source, pkey, cid, sid, uuid2) IN (
                WITH sum(sales*sign) AS ss
                SELECT argMax(source, uuid2), argMax(pkey, uuid2), argMax(cid, uuid2), argMax(sid, uuid2), max(uuid2)
                FROM {t} WHERE cid = {} AND source = {} {} GROUP BY item_id, trade_props.value
                ORDER BY ss DESC LIMIT {}
            )
        '''.format(
            ','.join(['toString({})'.format(col) for col in cols]), cid, snum,
            'AND (shop_type < 20 and shop_type > 10 )' if source=='tb' else ('AND (shop_type > 20 or shop_type < 10 )' if source=='tmall' else ''), limit, t=tbl
        )
        data = dbch.query_all(sql)
        result = [cols[0:-4], [], [], []]
        tnames, tvals, pnames, pvals, vals = [], [], [], [], []
        for v in data:
            vals.append(v[0:-4])

            tname, tval = eval(v[-4]), eval(v[-3])
            pname, pval = eval(v[-2]), eval(v[-1])

            for i, k in enumerate(tname):
                if k not in tnames:
                    tnames.append(k)

            tmp = ['' for i in range(len(tnames))]
            for i, k in enumerate(tname):
                tmp[tnames.index(k)] = tval[i]
            tvals.append(tmp)

            for i, k in enumerate(pname):
                if k not in pnames and k not in tnames:
                    pnames.append(k)

            tmp = ['' for i in range(len(pnames))]
            for i, k in enumerate(pname):
                if k in pnames:
                    tmp[pnames.index(k)] = pval[i]
            pvals.append(tmp)

        result[3] = ['' for k in result[0]] + ['交易属性' for k in tnames] + ['' for k in pnames]
        result[1] = ['' for k in result[0]] + tnames + pnames
        result[2] = [props['{}{}{}'.format(source, cid, k)] if '{}{}{}'.format(source, cid, k) in props else '' for k in result[1]]
        result[0]+= ['p{}'.format(i+1) for i in range(len(tnames))] + ['p{}'.format(i+1+len(tnames)) for i in range(len(pnames))]

        for i, v in enumerate(vals):
            result.append(list(v) + [tvals[i][ii] if ii < len(tvals[i]) else '' for ii in range(len(tnames))] + [pvals[i][ii] if ii < len(pvals[i]) else '' for ii in range(len(pnames))])

        export_csv("batch{}".format(bid), "{}{}.csv".format(source, cid), len(result), result)

    dbch.close()


def upload_dir(bid, csvdir):
    data = load_csv(csvdir)
    upload_data(bid, data)


def upload_data(bid, csvdata):
    db_26 = app.get_db('26_apollo')
    db_26.connect()

    pos_mapped = {}
    for cid in csvdata:
        d = format_csv(csvdata[cid])
        for v in d:
            if v['pos_mapped'] not in pos_mapped:
                pos_mapped[v['pos_mapped']] = {}
            if cid not in pos_mapped[v['pos_mapped']]:
                pos_mapped[v['pos_mapped']][cid] = {}
            if v['order'] not in pos_mapped[v['pos_mapped']][cid]:
                pos_mapped[v['pos_mapped']][cid][v['order']] = []
            pos_mapped[v['pos_mapped']][cid][v['order']].append(v['name'])

    upload(db_26, bid, pos_mapped)
    db_26.commit()
    db_26.close()

    return 'succ'
