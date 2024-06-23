import sys
from os.path import abspath, join, dirname, exists
# from tqdm import tqdm
from typing import List
import time

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import csv  
import xlrd
from os import listdir, mkdir, chmod
import application as app
from extensions import utils

'''
将数据导出为csv的通用方法
导出文件位置为output文件夹下的d文件夹中, 如果d文件夹不存在则会创建
d: 导出文件存储的文件夹名, 如果传入值是空则会将文件储存到default文件夹下
file: 文件名, 只需要文件名字, 不需要添加.csv文件后缀
result: 写入文件内容, 格式为二维数组 [[], [], ...], 每一个数组元素为文件的一行
'''
def export_csv(d, file, result, real_path=False):
    try:
        if d is None or d == '':
            d = 'default'
        mkdir(app.output_path(d))
    except Exception as e:
        print(e.args)

    file = d + '/' + file + '.csv'
    if not real_path:
        file = app.output_path(file)
    with open(file, 'a', encoding='gbk', errors='ignore', newline='') as output_file:
        writer = csv.writer(output_file)
        for row in result:
            writer.writerow(row)
    output_file.close()
    chmod(file, 0o777)

'''
针对一些临时查询查看结果的接口, 直接打印查询结果
'''
def show_query_info(db, sql):
    print(db.query_all(sql))


# def write_to_file(filepath: str, filename: str, data: List[List[str]],
#                   head=None, encoding='gbk', real_path=False) -> None:
#     """
#     缓存data到文件

#     example:
#             data = db.query_all('select pid, name from graph.product_lib_product limit 1;') \n
#             write_to_file(filepath, filename, data, head=('pid', 'name'), real_path=True, encoding='utf-8')

#     :param filepath: str
#         文件路径，当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
#     :param filename: str
#         文件名称，支持类型为.csv/.tsv/和其他文件以.txt打开    分隔符分别为,/\t/\t
#     :param data: List[List[str]]
#         mysql查询下来的数据
#     :param head: Any=None
#         是否添加首行(表头)， 断言：需要len(line)==len(head)，表头数等于表列数
#     :param encoding: Any='gbk'
#         修改编码
#     :param real_path: Any=False  当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
#     :return: None
#     """
#     if head is None:
#         head = []
#     if filepath is None or filepath == '':
#         filepath = 'default'
#     path = filepath if real_path else app.output_path(filepath)
#     app.mkdirs(path)

#     if head:
#         assert (len(head) == len(data[0]))

#     file = join(path, filename)
#     print("saving %s data into %s" % (len(data), file))
#     with open(file, 'w', encoding=encoding, newline='') as output_f:
#         time.sleep(0.1)
#         if filename[-4:] in ['.csv', '.tsv']:
#             delimiter = '\t' if filename[-4:] == '.tsv' else ','
#             csv_w = csv.writer(output_f, delimiter=delimiter)
#             if head:
#                 csv_w.writerow(head)
#             for line in tqdm(data):
#                 csv_w.writerow(line)
#             time.sleep(0.1)
#         else:
#             if head:
#                 output_f.write('\t'.join('%s' % element for element in head) + '\n')
#             for line in tqdm(data):
#                 if isinstance(line, str):
#                     output_f.write(line + '\n')
#                 elif isinstance(line, list):
#                     output_f.write('\t'.join('%s' % element for element in line) + '\n')


def cache_huge_db_to_file(db, sql: str, filepath: str, filename: str, head=None, test=-1, encoding='gbk') -> None:
    """
    缓存大批量在mysql中的文件

    :param db:  Any         数据库
    :param sql: str         查询语句
    :param filepath: str    文件路径，DataCleaner/src/output/ + filepath
    :param filename: str    文件名称，支持类型为.csv/.tsv/和其他文件以.txt打开    分隔符分别为,/\t/\t
    :param head: =None      是否添加首行(表头)，
                            断言：需要len(line)==len(head)，表头数等于表列数
    :param encoding: ='gbk' 修改编码
    :param test: =-1        当默认test=-1时，读取全表，否则每个test，读取10000行
    :return: None
    """
    if head is None:
        head = []
    if filepath is None or filepath == '':
        filepath = 'default'
    app.mkdirs(app.output_path(filepath))

    def single_item_process(data):
        if head:
            assert (len(head) == len(data[0]))
        with open(file, 'a+', encoding=encoding, newline='') as f:
            if filename[-4:] in ['.csv', '.tsv']:
                delimiter = '\t' if filename[-4:] == '.tsv' else ','
                csv_w = csv.writer(f, delimiter=delimiter)
                for line in data:
                    csv_w.writerow(line)
            else:
                for line in data:
                    f.write('\t'.join('%s' % element for element in line) + '\n')

    file = join(app.output_path(filepath), filename)
    f = open(file, 'w', encoding=encoding, newline='')
    if head:
        if filename[-4:] in ['.csv', '.tsv']:
            delimiter = '\t' if filename[-4:] == '.tsv' else ','
            csv_w = csv.writer(f, delimiter=delimiter)
            csv_w.writerow(head)
        else:
            f.write('\t'.join('%s' % element for element in head) + '\n')
    f.close()
    utils.easy_traverse(db, sql, single_item_process, test=test)


def read_from_file(filepath: str, filename: str, hashead=True, encoding='gbk', real_path=False) -> List[List]:
    """
    从.csv/.tsv/其他文件以.txt打开的文件中，读取数据

    :param filepath: str
        文件路径，当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
    :param filename: str
        文件名称，支持类型为.csv/.tsv/和其他文件以.txt打开    分隔符分别为,/[tab]/[tab]
    :param hashead: List[List[str]]
        是否跳过首行(表头)
    :param encoding: Any='gbk'
        修改编码
    :param real_path: Any=False
        当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
    :return:
        输出格式为二维数组[[str1, str2, ...], [], ...], 每一个数组元素为一行
    """
    data = []
    if filepath is None or filepath == '':
        filepath = 'default'
    path = filepath if real_path else app.output_path(filepath)

    file = join(path, filename)

    with open(file, 'r', encoding=encoding, newline='') as input_f:
        if hashead:
            next(input_f)
        if filename[-4:] in ['.csv', '.tsv']:
            delimiter = '\t' if filename[-4:] == '.tsv' else ','
            csv_r = csv.reader(input_f, delimiter=delimiter)
            for line in csv_r:
                data.append(line)
        else:
            d_f = input_f.readlines()
            for line in d_f:
                data.append([element.strip() for element in line.split('\t')])
    return data

def read_from_xlsx(filepath: str, filename: str, ignorehead=True, sheet_num=0, encoding='gbk', real_path=False) -> List[List]:
    """
    从.xlsx文件中，读取单页数据

    :param filepath: str
        文件路径，当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
    :param filename: str
        文件名称，支持类型为.xlsx类型的文件
    :param ignorehead: boolean
        是否跳过首行(表头)
    :param sheet_num: int Any=0
        指明需要的数据表页数, 从0号编码开始
    :param encoding: Any='gbk'
        修改编码
    :param real_path: Any=False
        当默认real_path=False时，为DataCleaner/src/output/下的目录，否则为真实路径
    :return:
        输出格式为二维数组[[str1, str2, ...], [], ...], 每一个数组元素为一行
    """
    data = []
    if filepath is None or filepath == '':
        filepath = 'default'
    path = filepath if real_path else app.output_path(filepath)

    file_path = join(path, filename)

    excel_file = xlrd.open_workbook(file_path)
    sheet = excel_file.sheets()[sheet_num]
    file_data = []
    for row_num in range(sheet.nrows):
        if row_num == 0 and ignorehead:
            continue
        row_info = []
        for col_num in range(sheet.ncols):
            row_info.append(sheet.cell(row_num, col_num).value)
        file_data.append(row_info)

    return file_data