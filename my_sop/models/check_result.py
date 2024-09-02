#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

import time, collections
from models.clean import Clean

class CheckResult:

    def __init__(self, batch_id):
        self.clean_obj = Clean(batch_id)
        self.batch_now = self.clean_obj.batch_now()
        self.value_dict = collections.defaultdict(list)
        for k, v in self.batch_now.unify_classify_dict.items():
            self.value_dict[v[1]].append(v[2])
        for i in self.batch_now.pos:
            if self.batch_now.pos[i]['type'] == 1:
                for j in list(self.batch_now.sub_batch.keys()) + [0]:
                    self.value_dict[i].extend([self.batch_now.tid_map_name[tid] for tid in self.batch_now.target_dict.get((j, i), dict())])
        for k in self.value_dict:
            self.value_dict[k] = set(self.value_dict[k] + [self.batch_now.default_other])
        self.all_sub_batch_names = self.value_dict[self.batch_now.pos_id_category].union(set([self.batch_now.sub_batch[i]['name'] for i in self.batch_now.sub_batch] + [self.batch_now.default_other]))     

    def check_sp_value(self, pos_id, val):
        ret = True
        if self.batch_now.pos[pos_id]['type'] >= 2 and self.batch_now.pos[pos_id]['type'] <= 99:
            if not val:
                ret = True
            elif self.batch_now.num_in_range(val, self.batch_now.pos[pos_id]['num_range']) is None:
                ret = False
            elif self.batch_now.pos[pos_id]['pure_num_out'] and not val.replace('.', '').isdigit():
                ret = False
            elif not self.batch_now.pos[pos_id]['pure_num_out'] and not val.endswith(self.batch_now.pos[pos_id]['default_quantifier']):
                ret = False
            elif self.batch_now.quantifier[self.batch_now.pos[pos_id]['type']]['only_int'] and '.' in val:
                ret = False
        elif self.batch_now.pos[pos_id]['type'] == 900:
            if val not in self.all_sub_batch_names:
                ret = False
        elif self.batch_now.pos[pos_id]['type'] == 1:
            if val not in self.value_dict[pos_id]:
                ret = False
        return ret

if __name__ == '__main__':
    start_time = time.time()

    cr = CheckResult(131)
    print(cr.value_dict)
    all_sp = ', '.join(['sp' + str(i) for i in cr.batch_now.pos_id_list])
    bname, btbl = cr.clean_obj.get_plugin().get_b_tbl()
    bdba = cr.clean_obj.get_db(bname)
    sql = f'SELECT {all_sp} FROM {btbl} WHERE similarity >= 2 LIMIT 100;'
    ret = bdba.query_all(sql)
    print(ret)
    for r in ret:
        for i, j in zip(cr.batch_now.pos_id_list, r):
            if not cr.check_sp_value(i, j):
                print('wrong sp', i, ':', j)

    end_time = time.time()
    print('All done used: {t:.2f} min'.format(t=(end_time - start_time)/60))
    print(time.strftime("%Y-%m-%d %H:%M", time.localtime()))