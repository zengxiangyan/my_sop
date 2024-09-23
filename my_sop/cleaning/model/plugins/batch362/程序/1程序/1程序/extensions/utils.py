#coding=utf-8
import sys
import os, time, signal, platform
from os.path import abspath, join, dirname, exists, isfile
import datetime
from operator import itemgetter
import json
import re
import socket
from html.parser import HTMLParser
import requests
from sklearn.feature_extraction.text import TfidfTransformer
from zhon.hanzi import punctuation
import math
import numpy as np
import urllib
import uuid
import pnlp

def get_timestamp_with_format(str, format='%Y-%m-%d'):
    arr = time.strptime(str, format)
    timestamp = int(time.mktime(arr))
    return timestamp

def get_month_time(now=0):
    if now == 0:
        now = time.time()

    arr = time.localtime(now)
    arr_time = (arr.tm_year, arr.tm_mon, 1, 0, 0, 0, 0, 0, 0)
    timestamp = int(time.mktime(arr_time))
    return timestamp

def get_day_time(now=0):
    if now == 0:
        now = time.time()

    arr = time.localtime(now)
    arr_time = (arr.tm_year, arr.tm_mon, arr.tm_mday, 0, 0, 0, 0, 0, 0)
    timestamp = int(time.mktime(arr_time))
    return timestamp

def get_next_month_time(now=0):
    if now == 0:
        now = time.time()

    arr = time.localtime(now)
    year = arr.tm_year
    mon = arr.tm_mon
    mon += 1
    if mon == 13:
        year += 1
        mon = 1
    arr_time = (year, mon, 1, 0, 0, 0, 0, 0, 0)
    timestamp = int(time.mktime(arr_time))
    return timestamp

def get_month_str(now=0):
    if type(now) == str:
        print(now.split('-',2)[0:2])
        return ''.join(now.split('-')[0:2])

    if now == 0:
        now = time.time()

    arr = time.localtime(now)
    month_str = '%(tm_year)04d%(tm_mon)02d'%{'tm_year':arr.tm_year, 'tm_mon':arr.tm_mon}
    return month_str

def get_month_date_str(now=0, is_end=False):
    if type(now) == str:
        year, month, day = list(now.split('-',2))
        year, month, day = int(year), int(month), int(day)
    else:
        if now == 0:
            now = time.time()

        arr = time.localtime(now)
        year = int(arr.tm_year)
        month = int(arr.tm_mon)
        day = int(arr.tm_mday)
    if is_end and day != 1:
        month += 1
        if month == 13:
            month = 1
            year += 1
    month_str = '%(year)04d-%(month)02d-01'%{'year':year, 'month':month}
    return month_str

def  get_last_month(d=0):
    dayscount =  datetime.timedelta(days=d.day)
    dayto =  d -  dayscount
    arr_time = (dayto.year, dayto.month, 1, 0, 0, 0, 0, 0, 0)
    timestamp = int(time.mktime(arr_time))
    return timestamp

def  get_last_month_str(d=0):
    dayscount =  datetime.timedelta(days=d.day)
    dayto =  d -  dayscount
    month_str = '%(tm_year)04d%(tm_mon)02d'%{'tm_year':dayto.year, 'tm_mon':dayto.month}
    return month_str

def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    return dates

def monthRange(beginDate, endDate):
    monthSet = set()
    for date in dateRange(beginDate, endDate):
        monthSet.add(date[0:7])
    monthList = []
    for month in monthSet:
        monthList.append(month)
    return sorted(monthList)

def monthTimestampRange(beginDate, endDate):
    monthSet = set()
    for date in dateRange(beginDate, endDate):
        monthSet.add(date[0:7])
    monthList = []
    for month in monthSet:
        monthList.append(get_timestamp_with_format(month, format='%Y-%m'))
    return sorted(monthList)

def is_python_3():
    return sys.version_info[0] >= 3

def isNum2(value):
    try:
        x = float(value)
    except TypeError:
        return False
    except ValueError:
        return False
    except Exception as e:
        return False
    else:
        return True

def read_all_from_file(file, encoding='utf-8'):
    f = open(file, "r", encoding=encoding)
    str = f.readlines()
    f.close()
    return json.loads(''.join(str))

def write_all_to_file(file, str):
    fileObject = open(file, 'w+', encoding='utf-8')
    fileObject.write(json.dumps(str, ensure_ascii=False))
    fileObject.close()

def write_file_for_reading(file, data):
    file = open(file, 'a', encoding='utf-8')
    for info in data:
        file.write(','.join([str(row) for row in info]) + '\n')
    file.close()

# 不对输入str做任何处理
def easy_write_str_to_file(file, str):
    fileObject = open(file, 'w+', encoding='utf-8')
    fileObject.write(str)
    fileObject.close()

def is_windows():
    import platform
    sysstr = platform.system()
    return sysstr =="Windows"

#use_row=False simple=True 返回{row[0]：1}
#use_row=True simple=True 返回{row[0]: row}
def transfer_list_to_dict(data, use_row=False, simple=False):
    h = dict()
    for row in data:
        k1 = row[0]
        if simple:
            if use_row:
                h[k1] = row
            else:
                h[k1] = row[1] if len(row) > 1 else 1
            continue
        if k1 in h:
            l = h[k1]
        else:
            l = []
            h[k1] = l
        if use_row:
            l.append(row[1:])
        else:
            l.append(row[1])
    return h

def transfer_list_to_dict_level2(data, use_row=False, use_list = False):
    h = dict()
    for row in data:
        k1 = row[0]
        k2 = row[1]
        if k1 in h:
            h_sub = h[k1]
        else:
            h_sub = dict()
            h[k1] = h_sub
        if not use_list:
            if use_row:
               h_sub[k2] = row[2:]
            else:
                h_sub[k2] = row[2]
            continue
        if k2 in h_sub:
            l = h_sub[k2]
        else:
            l = []
            h_sub[k2] = l
        if use_row:
            l.append(row[2:])
        else:
            l.append(row[2])
    return h

def easy_query(db, sql, l_id, toCloseDb=False):
    if len(l_id) == 0:
        return []
    where_sql = ','.join(('%s',) * len(l_id))
    sql = sql %(where_sql,)
    data = db.query_all(sql, tuple(l_id))
    if toCloseDb:
        db.close()
    return data

def get_dict_from_db(db, sql, l_id, level=1):
    h = dict()
    if len(l_id) == 0:
        return h
    data = easy_query(db, sql, l_id)
    if level == 1:
        return transfer_list_to_dict(data)
    return transfer_list_to_dict_level2(data)

def easy_sort(h, level=1):
    l = []
    for k in h:
        if level == 2:
            for kk in h[k]:
                l.append((k, h[k], h[k][kk]))
        else:
            l.append((k, h[k]))
    l = sorted(l, key=itemgetter(level), reverse=True)
    return l

def get_or_new(h, value_list, default):
    length = len(value_list)
    for i in range(length):
        value = value_list[i]
        if value not in h:
            if i == length - 1:
                h[value] = default
            else:
                h[value] = dict()
        h = h[value]
    return h

def easy_batch(db, table, key_list, item_vals, sql_dup_update='', batch=1000, commit=True, clickhouse=False):
    sql_key_str = ','.join(key_list)
    sql_bind_str = ','.join(('%s',) * len(key_list))
    try:
        sql = 'insert into ' + table + ' (' + sql_key_str + ') values '
        if clickhouse:
            sql_val = ''
        else:
            sql_val = '(' + sql_bind_str + ')'
        db.batch_insert(
            sql, sql_val, item_vals, sql_dup_update, batch)
        if commit and not clickhouse:
            db.commit()
        # print('insert into ', table, ' success count:', len(item_vals))
    except Exception as e:
        print('batch insert error:', e)
        if not clickhouse:
            db.rollback()

def get_list_by_key(data, key):
    l = []
    for item in data:
        l.append(item[key])
    return l

def easy_traverse(db, sql, one_callback, start=0, limit= 10000, test=-1, as_dict=False, check_key='id'):
    start_time = time.time()
    origin_start = start
    c = 0
    while True:
        temp_sql = sql %(start, limit)
        data = db.query_all(temp_sql, as_dict=as_dict)
        if one_callback is not None:
            one_callback(data)
        length = len(data)
        if length < limit:
            break
        if as_dict:
            start = int(data[length-1][check_key])
        else:
            start = int(data[length-1][0])
        end_time = time.time()
        t = end_time - start_time
        print('start:', start, ' total:', t, ' average:', (t/(start-origin_start)))
        c += 1
        if test >= 0 and c >= test:
            break

def easy_call(l, func):
    for obj in l:
        getattr(obj, func)()

def sig_handler(sig, frame):
    global g_pid_file
    if os.path.exists(g_pid_file):
        os.remove(g_pid_file)
    sys.exit(0)

def is_locked(pid_file, script_file):
    global g_pid_file
    g_pid_file = pid_file
    s = platform.system()
    if s != 'Windows':
        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGQUIT, sig_handler)

    try:
        pf = open(pid_file, 'a')
        s = pf.read().strip()
        s = 0 if s == '' else s
        pid = int(s)
        pf.close()
    except IOError:
        pid = None
        return True

    if pid:
        shell = "/bin/ps -p " + str(pid) + " -o cmd= | grep " + script_file
        out = os.system(shell)
        print('shell:', shell, ' out:', out)
        if out == 0:
            sys.stdout.write('instance is running...\n')
            return True

    pf = open(pid_file, 'w+')
    pf.write('%s\n' % os.getpid())
    pf.close()
    return False

def check_or_create_table(db, prefix, all, base=''):
    for table in all:
        new_table = table + prefix
        sql = 'create table if not exists %s like %s' %(new_table, table + base)
        db.execute(sql)

def easy_get(db, sql, l, h, batch=10000, is_return=False):
    size3 = len(l)
    r = []
    if size3 > 0:
        for i in range(int(size3/batch) + 1):
            start = i*batch
            end = start + batch - 1 if start + batch - 1 < size3 else size3
            if start >= size3:
                break
            l_ids = l[start:end+1]
            where_sql = ','.join(('%s',) * len(l_ids))
            real_sql = sql %(where_sql,)
            data3 = db.query_all(real_sql, l_ids)
            if is_return:
                r += data3
                continue
            for row in data3:
                h[str(row[0])] = row
    return r

def easy_get_with_key_list(db, sql, l, key_list, batch=10000):
    size3 = len(l)
    r = []
    if size3 > 0:
        for i in range(int(size3/batch) + 1):
            start = i*batch
            end = start + batch - 1 if start + batch - 1 < size3 else size3
            if start >= size3:
                break
            l_ids = l[start:end+1]
            p = {key_list: l_ids}
            where_sql = common.check_where(p)
            where_sql = ' and '.join(where_sql)
            real_sql = sql %(where_sql,)
            data3 = db.query_all(real_sql)
            r += data3
    return r

def get_from_cache(filename, db, sql, force=False):
    if not force and os.path.exists(filename):
        return read_all_from_file(filename)
    data = db.query_all(sql)
    write_all_to_file(filename, data)
    return data

def is_chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False

def is_only_chinese(word):
    for ch in word:
        if not '\u4e00' <= ch <= '\u9fff':
            return False
    return True

def easy_try(obj, func, params=None, try_max=3, time_sleep=0):
    try_count = 0
    while try_count < try_max:
        try_count += 1
        try:
            r = getattr(obj, func)()
            if try_count != 1:
                print('success at try_count:', try_count)
            return r
        except Exception as e:
            if try_count == try_max:
                print('try_count:', try_count, 'error:', e)
    return False

def parse_category(p={}):
    if 'category' in p and len(p['category']) > 0:
        return p['category'][0]
    i = 5
    while i >= 0:
        key = 'lv{}Selector'.format(i)
        if key in p and int(p[key]) != 0:
            return int(p[key])
        i -= 1
    return 0

def parse_subtype(sub_type,front_type_ref):
    list = []
    for i in sub_type:
        j = front_type_ref[i] if i in front_type_ref else i
        list.append(j)
    return list

def isnum(s):
    if(s == ''):
        return False
    for i in s:
        if(i>'9' or i <'0'):
            return False
    return True

def parse_id(id):
    l = []
    j = id.split(',')
    for i in j:
        i = i.strip()
        if(isnum(i)):
            l.append(int(i))
    l = list(set(l))
    return l

def easy_run_with_batch(l, func, batch, *args):
    size3 = len(l)
    if size3 > 0:
        for i in range(int(size3/batch) + 1):
            start = i*batch
            end = start + batch - 1 if start + batch - 1 < size3 else size3
            if start >= size3:
                break
            l_sub = l[start:end+1]
            func(l_sub, args)

def get_next_id(db, name, count=1, table='graph.max_id'):
    ip = get_local_ip()
    #if not re.match(re.compile(r'192.168.0.138'), ip):
    #    print('use max_id_bak')
    #    table = 'max_id_bak'
    sql = 'select id, max_id from {table} where name=%s for update'.format(table=table)
    data = db.query_one(sql, (name,))
    id, max_id = list(data)
    sql = 'update {table} set max_id=max_id+%s where id={id}'.format(table=table, id=id)
    db.execute(sql, (count,))
    db.commit()
    return max_id+1

def get_local_ip():
    return socket.gethostbyname(socket.getfqdn(socket.gethostname()))

def standard_name(name):
    name = pnlp.unify_character(name)
    name = re.sub('\s+', '', name)
    name = HTMLParser().unescape(name)
    return(name)

def keyword_process(name,lang):
    #将中英文字符串处理成关键词，返回关键词数组 包含去除标点的操作
    kk = []
    name = str(name)
    r = "[-\(\){}\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）\、\/；\，\。\、\’·.\\\\\:]+".format(punctuation)
    #r = "[\\-\(\){}\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）\、\/；\，\。\、\’]+".format(punctuation)
    if lang == 'cn':
        name.strip()
        name = pnlp.unify_character(name)
        name = re.sub('\s+', '', name)
        name = HTMLParser().unescape(name)
        name = re.sub(r, '', name)
        k = [name]
    elif lang == 'en':
        name.strip()
        name = pnlp.unify_character(name)
        k1 = name
        name = re.sub('\s+', '', name)
        name = HTMLParser().unescape(name)
        k2 = re.sub(r, '', name)
        if k1 != k2:
            k = [k1,k2]
        else:
            k = [k1]
    for i in k:
        if i.strip() not in list(str(a) for a in range(10)) + ['新', '旧'] + list(chr(i) for i in range(97, 123)):
            kk.append(i.strip())
    return (kk)

def name_split(name):
    #拆分name
    #r = "[-{}\s+\.\!_,$%^*+\"\']+|[+——！，。？、~@#￥%……&*\、；\，\。\、\’]+".format(punctuation)
    name = pnlp.unify_character(name)
    name_list_1 = re.split(r'[/（(+]', name)
    name_list = []
    if len(name_list_1) > 1:
        for i in name_list_1:
            ii = re.sub(r'[)）]', '', i)
            # ii = re.sub(r, '', ii)
            # ii = HTMLParser().unescape(ii)
            if ii.strip() not in list(str(a) for a in range(10))+['新','旧']+list(chr(i) for i in range(97,123)):#'新',‘旧’,'a','3',
                name_list.append(ii.strip())
    else:
        name_list.append(name_list_1[0].strip())
    #print('name split')
    return (name_list)

def is_eng(word):
    for ch in word:
        #print(ch)
        if ch.encode('UTF-8').isalpha():
            return True
    return False

def easy_spider_url(db, table_url, url, base_dir, create_time=None, encoding='utf-8', sleep_time=0, head=(), s=None, return_list=False, force=False):
    if create_time is None:
        create_time = time.time()
    sql = 'select id, url from {table} where url=%s'.format(table=table_url)
    row = db.query_one(sql, (url, ))
    need_request = False
    if not row:
        sql = 'insert into {table} (url, createTime) values(%s, {create_time})'.format(table=table_url, create_time=create_time)
        db.execute(sql, (url,))
        db.commit()
        sql = 'select max(id) from {table}'.format(table=table_url)
        id = db.query_scalar(sql)
        need_request = True
    else:
        id = row[0]
    print('id:', id, 'url:', url)
    filename = join(base_dir, str(id) + '.txt')
    if not need_request and not exists(filename):
        need_request = True
    if force:
        need_request = True
    if need_request:
        data = simple_get(url, head, encoding, s=s)
        # print(type(data))
        data2 = data.encode(encoding)
        with open(filename, "wb") as f:
            f.write(data2)
        if sleep_time > 0:
            time.sleep(sleep_time)
    else:
        with open(filename, 'r', encoding=encoding) as f:
            data = f.read()
    return [data, filename] if return_list else data

def simple_get(url, head, encoding, sleep_time=0, s=requests):
    # print(url)
    r = s.get(url, headers=head)
    r.encoding = encoding
    data = r.text
    if sleep_time > 0:
        time.sleep(sleep_time)
    return data

def get_rand_filename(now=0):
    if type(now) == str:
        print(now.split('-',2)[0:2])
        return ''.join(now.split('-')[0:2])

    if now == 0:
        now = time.time()

    arr = time.localtime(now)
    month_str = '%(tm_year)04d%(tm_mon)02d%(tm_day)02d%(tm_hour)02d%(tm_min)02d%(tm_sec)02d'%{'tm_year':arr.tm_year, 'tm_mon':arr.tm_mon, 'tm_day': arr.tm_mday, 'tm_hour':arr.tm_hour, 'tm_min': arr.tm_min, 'tm_sec': arr.tm_sec}
    return month_str


def chunks(arr, m):
    n = int(math.ceil(len(arr) / float(m)))
    return [arr[i:i + n] for i in range(0, len(arr), n)]

def chunks_by(arr, n):
    return [arr[i:i + n] for i in range(0, len(arr), n)]

def die(msg):
    raise Exception(msg)

def merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res

def get_month_with_next(v):
    v = str(v)
    y, m, d = [int(k) for k in v.split('-')]
    month = '%(year)04d-%(mon)02d-01' %{'year':y, 'mon':m}
    m += 1
    if m == 13:
        m = 1
        y += 1
    next_month = '%(year)04d-%(mon)02d-01' %{'year':y, 'mon':m}
    return [month, next_month]

def list_split(items, n):
    return [items[i:i+n] for i in range(0, len(items), n)]

def escape_solr(word):
    return re.sub('(\\\|\+|-|&|\|\||!|\(|\)|\{|}|\[|]|\^|"|~|\*|\?|:|;|/|\~)','\\\1', word )

def isset(v):
    try:
        type (eval(v))
    except:
        return False
    else:
        return True

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return str(obj, encoding='utf-8');
        elif isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        return json.JSONEncoder.default(self, obj)

def get_uuid():
    return uuid.uuid1()

def download_image(url, filename):
    if re.match('\/\/*', url):
        url = 'http:' + url
    try:
        urllib.request.urlretrieve(url, filename)
    except urllib.error.HTTPError as e:
        print(e)
        return False
    return True


def ksort(d):
    h = {}
    for k in sorted(d.keys()):
        h[k] = d[k]
    return h

def vsort(d, reverse=False):
    return sorted(d.items(), key=lambda kv: (kv[1], kv[0]), reverse=reverse)

punc_pattern = re.compile(r"[\s+\.\!\/_,$%^*\(\+\-\"\'\)\[\]]+|[+——()?【】“”！，。？、~@#￥%……&*（）]+")
int_pattern = re.compile(r"^[\*\-\.\+0-9]+$")
def parse_keyword(name, with_split=False):
    name = pnlp.unify_character(name)
    h = {}
    if name == '':
        return h.keys()
    h[name] = 1
    if re.match(pnlp.en_word_pattern, name) is not None:
        temp = re.sub('\s+', '', name)
        h[temp] = 1
        temp = re.sub("'", '', name)
        h[temp] = 1
        temp = re.sub("'s", '', name)
        h[temp] = 1
        temp = punc_pattern.sub('', name)
        h[temp] = 1
    if with_split:
        m = re.findall(r'([a-z\.\']+)', name)
        for w in m:
            if len(w) > 1:
                h[w] = 1
            name = re.sub(w, ' ', name)
        words = name.split(' ')
        for w in words:
            w = w.strip()
            if re.match(int_pattern, w) is not None:
                continue
            h[w] = 1
    return h.keys()

#检查是否英文子串
def is_valid_word(name, word, idx):
    length = len(name)
    length2 = len(word)
    if word[0] >= 'a' and word[0] <= 'z':
        if idx != 0:
            word2 = name[idx-1]
            if word2 >= 'a' and word2 <= 'z':
                return False
    if word[-1] >= 'a' and word[-1] <= 'z':
        if idx + length2 < length - 1:
            word2 = name[idx + length2]
            if word2 >= 'a' and word2 <= 'z':
                return False
    return True

def get_tfidf_matrix(l_all, l_keys_all):
    l = []
    for row in l_all:
        temp = []
        for bid in l_keys_all:
            temp.append((row[bid] if bid in row else 0))
        l.append(temp)

    transformer = TfidfTransformer()
    # 将词频矩阵X统计成TF-IDF值
    tfidf = transformer.fit_transform(l)
    # 查看数据结构 tfidf[i][j]表示i类文本中的tf-idf权重
    return tfidf.toarray()

def parse_keyword_all(name='vita-sedds', h_useless={}, with_split=False):
    """
    拆分关键字 对使用/分割，或者爱唯乐(aivilor)这种形式的
    :param
    :return:
    """
    min_length = 2
    name_origin = name
    name = pnlp.unify_character(name)
    name = name.split('/')
    name = expand_keyword(name)
    h = {}
    for k in name:
        for kk in parse_keyword(k, with_split=with_split):
            if len(kk) < min_length or (kk != name and kk in h_useless):
                continue
            h[kk] = 1
    l = h.keys()
    if len(l) > 1:
        print('name:', name_origin, 'words:', l)
    return l

def expand_keyword(name):
    h = {}
    for k in name:
        m = re.match('^(.+)\((.+)\)$', k)
        if m:
            w = m[1]
            h[w] = 1
            w = m[2]
            if is_only_chinese(w):
                print('warning: ', w, 'name: ', name)
            else:
                h[w] = 1
        else:
            h[k] = 1
    return h.keys()