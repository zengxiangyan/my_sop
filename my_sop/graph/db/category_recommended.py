import collections
import getopt
import sys,io

from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')

from extensions import utils
import application as app
import re
import csv


col_names = ['是否已被处理','关联现有品类','sub_batch_id', 'name', 'cid', 'info', 'tmall_cid', 'tmall_name', 'tmall_url', 'jd_cid', 'jd_name', 'jd_url']

not_name = ['delete','Not Indicated','待配置','待配置1','待配置2','待配置3','待配置4','暂不清洗','JD','Others']

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

# 定义函数：对原始品类进行切分、删除操作
def get_all_batch_id_name(h_db, batch_id, pos_name):
    pos_name = pos_name.split(',')
    sub_id_name = collections.defaultdict(lambda: [])
    i = 0
    for tmp in pos_name:
        if tmp == '子品类':
            sql = "select sub_batch_id, name from cleaner.clean_sub_batch where deleteFlag = 0 and batch_id = {};".format(batch_id)
        else:
            sql = " select target_id, name from cleaner.clean_target where batch_id={id} and pos_id in (select pos_id from cleaner.clean_pos where batch_id={id} and name='{name}'); ".format(id=batch_id, name=tmp)
        data = h_db['mixdb'].query_all(sql)
        if data != ():
            for row in data:
                sub_batch_id, name = list(row)
                sub_batch_id = int(sub_batch_id)
                sub_id_name[sub_batch_id].append(name)  # sub_id_name——{sub_batch_id: name}
        else:
            # 补充clean_target中没有的sp
            sql1 = "select eid from cleaner.clean_batch where batch_id = {}".format(batch_id)
            eid = h_db['mixdb'].query_all(sql1)[0][0]
            sql2 = "SELECT distinct clean_props.value[indexOf(clean_props.name, '{a}')] from sop_e.entity_prod_{b}_E epe ;".format(a=tmp, b=eid)
            # print(sql2)
            data0 = h_db['chmaster'].query_all(sql2)
            for row in data0:
                if row[0] != '':
                    sub_id_name[i].append(row[0])  # sub_id_name——{sub_batch_id: name}
                    i += 1
    return sub_id_name

# 对原始品类进行切分、删除操作，得到最终的品类
def format_data(dict):
    # 切分
    final_cat = collections.defaultdict(lambda: [])
    for j, k in dict.items():
        name = re.split(r"[-/ ]", k[0])
        for k2 in name:
            if k2 != '':
                k2 = k2.lower()
                if k2 not in not_name:
                    final_cat[j].append(k2) # final_cat——{sub_batch_id:[[name]]}
    return final_cat

def get_tmall_jd_cid_name_url(db):
    tm_info_all = collections.defaultdict(lambda: [])
    sql11 = "select categoryId, cid, name, url from cleaner.tmall_category"
    data11 = db.query_all(sql11)
    for tmp1 in data11:
        tm_categoryId, tmcid, tmname, tmurl = list(tmp1)
        tm_categoryId = int(tm_categoryId)
        if tm_categoryId != 0 and tmcid != 0:
            tm_info_all[tm_categoryId].append(tmp1) # tm_info_all——{tm_categoryId:[(tm_categoryId, tmcid, tmname, tmurl)]}

    jd_info_all = collections.defaultdict(lambda: [])
    sql12 = 'select categoryId, cid, name, url from cleaner.jd_category'
    data12 = db.query_all(sql12)
    for tmp2 in data12:
        jd_categoryId, jdcid, jdname, jdurl = list(tmp2)
        jd_categoryId = int(jd_categoryId)
        if jd_categoryId != 0 and jdcid != 0:
            jd_info_all[jd_categoryId].append(tmp2)  #jd_info_all——{jd_categoryId:[(jd_categoryId, jdcid, jdname, jdurl)]}

    return tm_info_all, jd_info_all

# 输出对应的天猫、京东cid，name，url
def search_source_info(tmall_cid, jd_cid,  tm_info_all, jd_info_all):

    tm_info = [['','','','']]
    if tmall_cid != '0' :
        tmall_cid_new = re.split(r",", tmall_cid)
        for i in tmall_cid_new:
            i = int(i)
            tmp1 = tm_info_all[i]
            for i1 in tmp1: # tmp1——[(tm_categoryId, tmcid, tmname, tmurl)]
                tm_info.append(i1)

    jd_info = [['','','','']]
    if jd_cid != '0':
        jd_cid_new = re.split(r",", jd_cid)
        for j in jd_cid_new:
            j = int(j)
            tmp2 = jd_info_all[j]
            for j1 in tmp2:
                jd_info.append(j1)

    return tm_info, jd_info

def get_name_from_category(db):
    name_cid_to_jdcid = collections.defaultdict(lambda: [])
    # sql = "select c.name, cp.cid, c.lv1name, c.lv2name, c.lv3name, c.lv4name, c.cid1, c.cid2 from graph.category c left join graph.categoryParent cp on cp.cid=c.cid where c.confirmType in (0,1) and c.deleteFlag=0 and c.name != '' and cp.confirmType in (0,1) and cp.deleteFlag=0;"
    sql = "select c.name, gc.cid, c.lv1name, c.lv2name, c.lv3name, c.lv4name,c.cid1, c.cid2 from graph.categoryparent gc left join graph.category c on gc.cid=c.cid where c.confirmType in (0,1) and c.deleteFlag=0 and c.name != '' and gc.confirmType in (0,1) and gc.deleteFlag=0;"
    data = db.query_all(sql)
    for row in data:
        name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid = list(row)
        name = name.lower()
        name_cid_to_jdcid[name].append(row)   # name_cid_to_jdcid—— {name:[(name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid)]}
    return name_cid_to_jdcid

def read_log(conn, sheet):
    # brand根据all_bid区分
    # category和propname按name区分
    tbl_map = {'brand': 'graph.extract_cleaner_brand',
               'category': 'graph.extract_cleaner_category',
               'propname': 'graph.extract_cleaner_propname'}
    sql = 'select name, gid from {}'.format(tbl_map[sheet])
    gid_by_clean_name = {v[0]: v[1] for v in conn.query_all(sql)}
    return gid_by_clean_name

def inner_func(db, sub_id, tmp0, index, item0, name_cid_to_jdcid, tm_info_all, jd_info_all):
    output = []
    gid_by_clean_name = read_log(db, 'category')
    tmp11 = name_cid_to_jdcid[item0] # tmp11——[(name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid)]
    if tmp11 != []:
        for item11 in tmp11:
            cid1 = int(item11[1])
            tm_cid1 = item11[6]
            jd_cid1 = item11[7]

            name1 = item11[0]
            lv1name1 = item11[2]
            lv2name1 = item11[3]
            lv3name1 = item11[4]
            lv4name1 = item11[5]


            if lv1name1 == '':
                info1 = name1 + ','
            else:
                if lv2name1 == '':
                    info1 = name1 + ',' + lv1name1
                else:
                    if lv3name1 == '':
                        info1 = name1 + ',' + lv1name1 + ' -> ' + lv2name1
                    else:
                        if lv4name1 == '':
                            info1 = name1 + ',' + lv1name1 + ' -> ' + lv2name1 + ' -> ' + lv3name1
                        else:
                            info1 = name1 + ',' + lv1name1 + ' -> ' + lv2name1 + ' -> ' + lv3name1 + ' -> ' + lv4name1

            # 输出对应的天猫、京东cid，name，url
            tm_info1, jd_info1 = search_source_info(tmall_cid=tm_cid1, jd_cid=jd_cid1, tm_info_all=tm_info_all,
                                                    jd_info_all=jd_info_all)  # [(tm_categoryId, tmcid, tmname, tmurl)]
            tmcid1 = []
            tmname1 = []
            tmurl1 = []
            for item12 in tm_info1:  # item12=(tm_categoryId, tmcid, tmname, tmurl)
                if item12[1] not in ['',0, 'None']:
                    tmcid1.append(item12[1])
                    tmname1.append(item12[2])
                    tmurl1.append(item12[3])

            jdcid1 = []
            jdname1 = []
            jdurl1 = []
            for item13 in jd_info1:
                if item13[1] not in ['',0, 'None']:
                    jdcid1.append(item13[1])
                    jdname1.append(item13[2])
                    jdurl1.append(item13[3])

            name_new = '/'.join(tmp0)
            tmcid_new = '\n'.join([str(i) for i in tmcid1])
            tmname_new = '\n'.join(tmname1)
            tmurl_new = '\n'.join(tmurl1)
            jdcid_new = '\n'.join([str(i) for i in jdcid1])
            jdname_new = '\n'.join(jdname1)
            jdurl_new = '\n'.join(jdurl1)


            if name_new in gid_by_clean_name:
                if index == 0:
                    output.append(['★', '', sub_id, name_new, cid1, info1, tmcid_new, tmname_new, tmurl_new, jdcid_new, jdname_new, jdurl_new])
                else:
                    output.append(['★', '', '', '', cid1, info1, tmcid_new, tmname_new, tmurl_new, jdcid_new, jdname_new, jdurl_new])
                index += 1
            else:
                if index == 0:
                    output.append(['','', sub_id, name_new, cid1, info1, tmcid_new, tmname_new, tmurl_new, jdcid_new, jdname_new, jdurl_new])
                else:
                    output.append(['', '', '', '', cid1, info1, tmcid_new, tmname_new, tmurl_new, jdcid_new, jdname_new, jdurl_new])
                index += 1
    else:
        for kk in tmp0:
            if kk in gid_by_clean_name:
                if index == 0:
                    output.append(['★', '', sub_id, kk, '','','','','','','',''])
                index += 1
            else:
                output.append(['','', sub_id, kk, '','','','','','','',''])

    return output

# 精确查找，模糊查找
def exact_fuzzy_search(db, final_cat, name_cid_to_jdcid): # final_cat——{sub_batch_id:[[name]]}   # name_cid_to_jdcid——{name:[(name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid)]}
    tm_info_all, jd_info_all = get_tmall_jd_cid_name_url(db)
    # tm_info_all——{tm_categoryId:[(tm_categoryId, tmcid, tmname, tmurl)]}
    # jd_info_all——{jd_categoryId:[(jd_categoryId, jdcid, jdname, jdurl)]}
    output = []

    for sub_id in final_cat: # 对final_cat中的每个batch_id进行遍历 final_cat {sub_batch_id:[[name]]}
        index = 0
        index1 = 0
        tmp0 = final_cat[sub_id]
        for item0 in tmp0:
            # 首先精确查找，找不到再模糊查找
            if item0 in name_cid_to_jdcid:  # name_cid_to_jdcid——{name:[(name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid)]}
                output.append(inner_func(db, sub_id, tmp0, index, item0, name_cid_to_jdcid, tm_info_all, jd_info_all))
                index += 1
                index1 = 1

            else: # 模糊查找
                for item21 in name_cid_to_jdcid:
                    if item0 in item21:
                        output.append(inner_func(db, sub_id, tmp0, index, item21, name_cid_to_jdcid, tm_info_all, jd_info_all))
                        index += 1
                        index1 = 1
            if index1 == 0:
                output.append(inner_func(db, sub_id, tmp0, index, '', name_cid_to_jdcid, tm_info_all, jd_info_all))
                index1 += 1

    result = []
    for tmp in output:
        for tmp0 in tmp:
            result.append(tmp0)
    return result

def main(h_db, batch_id, date, pos_name):
    batch_id = int(batch_id)
    origin_batch_name = get_all_batch_id_name(h_db, batch_id, pos_name)  # origin_batch_name——{sub_batch_id: name}
    final_cat = format_data(origin_batch_name) # 最终的品类 # final_cat——{sub_batch_id:[[name]]}
    # 获取category中所有的name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid信息
    name_cid_to_jdcid = get_name_from_category(h_db['mixdb'])  # name_cid_to_jdcid——{name:[(name, cid, lv1name, lv2name, lv3name, lv4name, tmall_cid, jd_cid)]}
    result = exact_fuzzy_search(h_db['mixdb'], final_cat, name_cid_to_jdcid)
    print(result)

    # # # save_path: C:\Users\DELL\Desktop\DataCleaner\src\output
    # with open(app.output_path('recommended_cat_for_{}_{}.csv'.format(batch_id, date)), 'w', newline='', encoding='gb18030') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(col_names)
    #     for i in result:
    #         writer.writerow(i)

    result.insert(0, col_names)
    return result

if __name__ == '__main__':
    options, args = getopt.getopt(sys.argv[1:], 'd:b:c:')
    for name, value in options:
        if name in ('-d'):  # 自定义输出文件的日期
            date = value
        if name in ('-b'):  # batch_id
            batch_id = value
        if name in ('-c'): # 清洗字段
            pos_name = [str(t) for t in value.split(',')]
    c_195 = app.get_clickhouse('chmaster')
    chslave = app.get_clickhouse('chslave')
    mixdb = app.get_db('default')
    # mixdb = app.get_db('26_apollo')
    h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
    utils.easy_call([c_195, chslave, mixdb], 'connect')
    main(h_db, batch_id, date, pos_name)
# -d 0907 -b 50 -c "品牌,货号,是否有效"

# if __name__ == '__main__':
#     batch_id = 93
#     date = '0907'
#     pos_name = '品牌,系列,是否进口'
#     c_195 = app.get_clickhouse('chmaster')
#     chslave = app.get_clickhouse('chslave')
#     mixdb = app.get_db('26_apollo')
#     h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
#     utils.easy_call([c_195, chslave, mixdb], 'connect')
#     print(h_db)
#     main(h_db, batch_id, date, pos_name)
