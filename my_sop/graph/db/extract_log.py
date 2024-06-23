import sys
from os.path import abspath, join, dirname
from extensions import utils
import numpy as np
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))

def add_log(conn, sheet, gid_by_clean_name, now):
    # brand根据all_bid区分
    # category和propname按name区分
    tbl_map = {'brand': 'graph.extract_cleaner_brand',
               'category': 'graph.extract_cleaner_category',
               'propname': 'graph.extract_cleaner_propname'}
    insert = []
    for name in gid_by_clean_name:
        insert.append([name, gid_by_clean_name[name], now])
    if len(insert) > 0:
        utils.easy_batch(conn, tbl_map[sheet], ['name', 'gid', 'createTime'], np.array(insert))


def read_log(conn, sheet):
    # brand根据all_bid区分
    # category和propname按name区分
    tbl_map = {'brand': 'graph.extract_cleaner_brand',
               'category': 'graph.extract_cleaner_category',
               'propname': 'graph.extract_cleaner_propname'}
    sql = 'select name, gid from {}'.format(tbl_map[sheet])
    gid_by_clean_name = {v[0]: v[1] for v in conn.query_all(sql)}
    return gid_by_clean_name