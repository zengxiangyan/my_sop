import time
import re
import pandas as pd
from colorama import Fore
from pathlib import Path
from datetime import datetime
import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))
import application as app
from extensions import utils


def printpath(path):
    """打印知识图谱path路径"""
    count = -1
    for index, p in enumerate(path):
        if index == 0:
            print('┌', Fore.YELLOW, p, Fore.RESET, sep='')
        elif index == len(path) - 1:
            print(' ' * (count * 2), '└──', Fore.RED, p, Fore.RESET, sep='')
        elif isinstance(path[index]['id'], int):
            print(' ' * (count * 2), '└─┬', p, Fore.RESET, sep='')
        else:
            continue
        count += 1


def get_cached_data(filename, db='', sql='', dtype=None, columns=[]):
    """
    由sql, db获取数据

    :param dtype:
    :param columns:
    :param db:
    :param sql:
    :param filename:
    :return:
    """
    if not exists(filename):
        if db == 'graph':
            print(sql)
            graph_db = app.connect_db('graph')
            data = graph_db.query_all(sql)
        elif db == 'chmaster':
            chmaster = app.connect_clickhouse('chmaster')
            data = chmaster.execute(sql)
        else:
            raise ValueError("Choose database which is authorized: graph / chmaster")

        # 自动辨别表头
        if not columns:
            def backtracking_sql(string):
                def backtrack(p):
                    sub_string, delete = '(', False
                    p += 1
                    while string[p] != ')':
                        if string[p] == '(':
                            _add, p = backtrack(p)
                            sub_string += _add
                        else:
                            sub_string += string[p]
                            if string[p] == ',':
                                delete = True
                        p += 1
                    return '' if delete else sub_string + ')', p

                res, pos = '', 0
                while pos < len(string):
                    if string[pos] == '(':
                        add, pos = backtrack(pos)
                        res += add
                    else:
                        res += string[pos]
                    pos += 1
                return res

            pre_columns = re.sub('[ ]+', ' ', re.sub(r'\n|\t', '', sql[7: sql.index('from')].lower()))
            pre_columns = backtracking_sql(pre_columns)
            columns = [i.split(' as ')[1] if ' as ' in i
                       else re.search(r'(?<=\()\S+(?=\))', i).group() if '(' in i else i.strip()
                       for i in pre_columns.split(',')]

        # 修复name中的回车, 避免混淆, 并修改tuple(tuple()) -> list[list[]]
        name_indexes = []
        if 'name' in columns:
            name_indexes.append(columns.index('name'))
        if 'trade' in columns:
            name_indexes.append(columns.index('trade'))

        def fix_data(line):
            line = list(line)
            for index in name_indexes:
                line[index] = re.sub(r'[\r\n\t]', ' ', line[index])
            return line

        data = list(map(lambda row: fix_data(row), data))

        assert len(columns) == len(data[0]), "columns have different length from data..."

        df = pd.DataFrame(data, columns=columns)
        print(f'生成临时文件... {filename}')
        df.to_csv(filename, index=False)
    else:
        print(f'读取临时文件... {filename}')
        if utils.is_chinese(str(filename)):
            df = pd.read_csv(filename, engine='python', encoding='utf-8')
        else:
            df = pd.read_csv(filename, dtype=dtype) if dtype else pd.read_csv(filename)
    return df


def get_cached_txt(filename, data=None):
    if not exists(filename):
        data = [line.strip() + '\n' for line in data]
        with open(filename, 'w', encoding='utf-8') as f:
            f.writelines(data)
    else:
        f = open(filename, 'r', encoding='utf-8')
        data = f.readlines()
        f.close()
        data = [line.strip() for line in data]
    return data


def get_columns_from_sql(sql):
    def backtracking_sql(string):
        def backtrack(p):
            sub_string, delete = '(', False
            p += 1
            while string[p] != ')':
                if string[p] == '(':
                    _add, p = backtrack(p)
                    sub_string += _add
                else:
                    sub_string += string[p]
                    if string[p] == ',':
                        delete = True
                p += 1
            return '' if delete else sub_string + ')', p

        res, pos = '', 0
        while pos < len(string):
            if string[pos] == '(':
                add, pos = backtrack(pos)
                res += add
            else:
                res += string[pos]
            pos += 1
        return res

    pre_columns = re.search(r'(?<=^select ).*(?=from)', re.sub(r'\n|\t', ' ', sql.lower())).group()
    pre_columns = re.sub('[ ]+', ' ', re.sub('distinct ', ' ', pre_columns))
    pre_columns = backtracking_sql(pre_columns.strip())
    columns = [i.split(' as ')[1] if ' as ' in i
               else re.search(r'(?<=\()\S+(?=\))', i).group() if '(' in i else i.strip()
               for i in pre_columns.split(',')]
    return columns


def used_time(func):
    def wrapper(*args, **kwargs):
        print('#' * 10 + f'task <{func.__name__.upper()}>' + '#' * 10)
        start_time = time.time()
        f = func(*args, **kwargs)
        end_time = time.time()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{current_time}] task <{func.__name__.upper()}> done used:{end_time - start_time}s\n')
        return f

    return wrapper


class Logger:
    def __init__(self, eid, log_dir, clear=True):
        self.filename = Path(log_dir) / f"{eid}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        if clear:
            self.clear()
        print(self.filename)

    def write(self, string):
        with open(self.filename, 'a') as outfile:
            outfile.write(string)

    def clear(self):
        outfile = open(self.filename, 'w')
        outfile.close()


if __name__ == '__main__':
    sub_cat_sql = f"select cid, substringUTF8(name, 8) as category from artificial.category_123" \
                  f"\nwhere name like '运动鞋new-%';"
    print(get_columns_from_sql(sub_cat_sql))
