import sys
import csv
from os import listdir, mkdir, chmod
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app

def get_e_tbl(eid):
    return 'chsop', 'sop_e.entity_prod_{}_E'.format(eid)


def get_cols(eid):
    ename, etbl = get_e_tbl(eid)

    edba = app.get_clickhouse(ename)
    edba.connect()

    sql = 'DESC {}'.format(etbl)
    ret = edba.query_all(sql)

    return [v[0] for v in ret]


def get_props(eid):
    ename, etbl = get_e_tbl(eid)

    edba = app.get_clickhouse(ename)
    edba.connect()

    sql = 'SELECT clean_props.name FROM {} LIMIT 1'.format(etbl)
    ret = edba.query_all(sql)

    return ret[0][0]


def export_csv(d, file, result):
    d = '/nas/' + d

    try:
        mkdir(app.output_path(d), 0o777)
    except Exception as e:
        print(e.args)

    file = d+'/'+file
    print('******************WRITE csv for {}******************'.format(file))
    with open(app.output_path(file), 'w', encoding = 'gbk', errors = 'ignore', newline = '') as output_file:
        writer = csv.writer(output_file)
        for row in result:
            writer.writerow(row)
    output_file.close()
    chmod(app.output_path(file), 0o777)

    return file


def compare_ab(aid, acol, bid, bcol, smonth, emonth, limit=100):
    ename, atbl = get_e_tbl(aid)
    ename, btbl = get_e_tbl(bid)

    edba = app.get_clickhouse(ename)
    edba.connect()

    acols = get_cols(aid)
    bcols = get_cols(bid)

    sql = '''
        WITH a.`clean_props.value`[indexOf(a.`clean_props.name`, '{acol}')] AS acol,
             b.`clean_props.value`[indexOf(b.`clean_props.name`, '{bcol}')] AS bcol
        SELECT acol, bcol, '---------', a.*, '---------', b.*
        FROM (SELECT * FROM {} WHERE pkey>='{smonth}' AND pkey<'{emonth}') a
        JOIN (SELECT * FROM {} WHERE pkey>='{smonth}' AND pkey<'{emonth}') b
        USING (item_id, date) WHERE acol != bcol LIMIT {} BY item_id
    '''.format(atbl, btbl, limit, acol=acol, bcol=bcol, smonth=smonth, emonth=emonth)
    ret = edba.query_all(sql)

    res = [[acol,bcol,'---------']+acols+['---------']+bcols] + ret
    return export_csv('compare', 'compare {}-{} {}-{} month {}~{} limit {}.csv'.format(aid,acol,bid,bcol,smonth,emonth,limit), res)