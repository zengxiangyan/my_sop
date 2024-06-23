
import getopt
import sys, io
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import collections
from extensions import utils
import application as app



# 词条预处理函数
def data_pre_process(name):
    if name:
        if utils.is_chinese(name):
            name = utils.keyword_process(name, lang='cn')
            name = ','.join(name)
        elif utils.is_eng(name):
            name = utils.keyword_process(name, lang='en')
            name = ','.join(name)
        else:
            name = name
    else:
        name = ''
    return name

# 获取所有的batch_id和属性id、属性名
def get_all_propname(db):
    batchid_id_name = collections.defaultdict(lambda: [])

    sql = "select batch_id, id, name from cleaner.clean_pos where deleteFlag=0 and name != '' "
    data = db.query_all(sql)
    for row in data:
        batch_id0, id, name = list(row)
        batch_id0 = int(batch_id0)
        batchid_id_name[batch_id0].append(row)  # batchid_id_name—— {batch_id:[(batch_id0, id, name)]}

    return batchid_id_name

# 获取id和原始属性名
def get_propname(batch_id, propname, batchid_id_name):
    batch_id = int(batch_id)
    prop_name = collections.defaultdict(lambda: [])
    tmp = batchid_id_name[batch_id] # tmp——[(batch_id0, id, name)]
    for item in tmp:
        if propname not in item:
            id = int(item[1])
            name = item[2]
            prop_name[id] = data_pre_process(name) # prop_name——{id:name}
    return prop_name


# 获取所有的关联的id和属性名
def get_name_pnid(db, name):
    h = {}
    name_pnid = collections.defaultdict(lambda: [])
    sql1 = "select pnid, name from graph.propName where confirmType in (0,1) and deleteFlag=0 and name != '' and name in (%s)"
    data1 = utils.easy_get(db, sql1, list(name), h, is_return=True)
    for row1 in data1:
        pnid, name1 = list(row1)
        name_pnid[data_pre_process(name1)] = row1 #name_pnid—— {name1:[pnid, name1]}
    return name_pnid

def get_name_pid(db, name):
    h = {}
    name_pid = collections.defaultdict(lambda: [])
    sql = "select pid, name from graph.part where mode=6 and confirmType in (0,1) and deleteFlag=0 and name != '' and name in (%s)"
    data = utils.easy_get(db, sql, list(name), h, is_return=True)
    for row in data:
        pid, name = list(row)
        name_pid[data_pre_process(name)] = row #name_pid—— {name:[pid, name]}
    return name_pid


def read_log(conn, sheet):
    # brand根据all_bid区分
    # category和propname按name区分
    tbl_map = {'brand': 'graph.extract_cleaner_brand',
               'category': 'graph.extract_cleaner_category',
               'propname': 'graph.extract_cleaner_propname'}
    sql = 'select name, gid from {}'.format(tbl_map[sheet])
    gid_by_clean_name = {v[0]: v[1] for v in conn.query_all(sql)}
    return gid_by_clean_name

'''获取属性关联语义单元id'''
def get_relative_part_id(db, pnid_list):

    pnid_pid = collections.defaultdict()
    sql = '''select pnid, pid from graph.partpropname where pnid in {} '''.format(tuple(pnid_list))
    data = db.query_all(sql)

    for row in data:
        pnid, pid = list(row)
        pnid_pid[pnid] = pid
    return pnid_pid


# 根据原始属性名，从graph.propName中拉取关联的属性id和属性名
def get_pnid_pid(db, prop_name): # prop_name——{pid: name}
    name = [i for i in prop_name.values()]

    name_pnid = get_name_pnid(db, name) # name_pnid——{name1:[pnid, name1]}
    name_pid = get_name_pid(db, name) # name_pid——{name:[pid, name]}

    pnid_list = [name_pnid[i][0] for i in name_pnid]
    '''获取属性关联语义单元id'''
    pnid_pid_dict = get_relative_part_id(db, pnid_list)

    output = []
    for i in prop_name: #prop_name—— {pid: name}
        index = 0
        name1, name2, pnid, pid = list(['']*4)
        # 获取关联的id和属性名
        name = prop_name[i]
        if name in name_pnid:
            name1 = name
            pnid = name_pnid[name1][0]

        if name in name_pid:
            name2 = name
            pid = name_pid[name2][0]

        relative_pid = pnid_pid_dict.get(pnid, '')

        gid_by_clean_name = read_log(db, 'propname')
        if name in gid_by_clean_name:
            if index == 0:
                output.append(['★', i, name, pnid, name1, pid, name2, relative_pid, ''])
            else:
                output.append(['★', '', '', pnid, name1, pid, name2, relative_pid, ''])
            index += 1
        else:
            if index == 0:
                output.append(['',i, name, pnid, name1, pid, name2, relative_pid, ''])
            else:
                output.append(['', '', '', pnid, name1, pid, name2, relative_pid, ''])
            index += 1

    return output

def main(h_db, batch_id, propname, date):
    batchid_id_name = get_all_propname(h_db['mixdb'])  # 获取所有的batch_id和属性id、属性名
    Propname = get_propname(batch_id, propname, batchid_id_name)  #Propname——{pid: name}

    result = get_pnid_pid(h_db['mixdb'], Propname)
    col_names = ['是否已被处理', '属性id', '属性名', '关联属性id', '关联属性名', '语义单元id', '语义单元属性名', '属性关联语义单元id', '是否是新属性']

    # with open(app.output_path('recommended_propname_for_{}_{}_{}.csv'.format(batch_id, propname, date)), 'w',
    #           newline='', encoding='gb18030') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(col_names)
    #     for i in result:
    #         writer.writerow(i)

    result.insert(0, col_names)
    return result

if __name__ == '__main__':
    options, args = getopt.getopt(sys.argv[1:], 'd:c:b:')
    for name, value in options:
        if name in ('-d'):  # 自定义输出文件的日期
            date = value
        if name in ('-c'):  # 需要的品类
            propname = value
        if name in ('-b'):  # batch_id
            batch_id = value
    c_195 = app.get_clickhouse('chmaster')
    chslave = app.get_clickhouse('chslave')
    mixdb = app.get_db('default')
    # mixdb = app.get_db('26_apollo')
    h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
    utils.easy_call([c_195, chslave, mixdb], 'connect')
    main(h_db, batch_id, propname, date)

# batch_id = 54
# propname = '子品类'
# date = '0805_2'
# c_195 = app.get_clickhouse('chmaster')
# chslave = app.get_clickhouse('chslave')
# mixdb = app.get_db('26_apollo')
# h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
# utils.easy_call([c_195, chslave, mixdb], 'connect')
# main(h_db, batch_id, propname, date)