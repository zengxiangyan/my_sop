# coding=utf-8
import re
import sys, getopt, os, io
from os.path import abspath, join, dirname

from dateutil.relativedelta import relativedelta

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
import collections
import datetime
import copy
from models.report import common
import pandas as pd

chmaster = app.get_clickhouse('chsop')
db = app.get_db('graph')
chsql = app.connect_clickhouse_http('chsql')
apollo = app.get_db('39_apollo')

class CompareData():
    id_name_1o1 = {'item_id': 'name', 'clean_cid': '子品类', 'sid': '店铺名', 'sku_id': 'sku', 'alias_all_bid': '品牌名',
                   'num_yoy': 'num_yoy', 'num_mom': 'num_mom', 'sales_yoy': 'sales_yoy', 'sales_mom': 'sales_mom'}

    date_type_dict = {'day': 'gDate()', 'month': 'gMonth()', 'quarter': 'gQuarter()', 'year': 'gYear()'}

    # 输出结果时，具体比较的字段的表头
    arrCols_dict = {'num': ['num1', 'num2', 'num3'],
                    'sales': ['sales1', 'sales2', 'sales3'],
                    'price': ['price1', 'price2', 'price3'],
                    'num_yoy': ['numyoy1', 'numyoy2'],
                    'num_mom': ['nummom1', 'nummom2'],
                    'sales_yoy': ['salesyoy1', 'salesyoy2'],
                    'sales_mom': ['salesmom1', 'salesmom2']}

    # 输出结果时显示的比较维度的表头
    comd_name_dict = {'子品类': 'subCategory', 'alias_all_bid': 'brand', 'sid': 'shop', 'item_id': 'goods'}

    # chsql中platformsIn的参数
    plt_dict = {'天猫': 'ali', '淘宝': 'ali', '京东': 'jd', '苏宁': 'suning', '考拉': 'kaola', '国美': 'gome', '聚美': 'jumei',
                '酒仙': 'jx', '途虎': 'tuhu'}

    # 平台中英文映射
    plt_ch_en = {
        '阿里': 'ali',
        '天猫': 'tmall',
        '淘宝': 'tb',
        '京东': 'jd',
        '苏宁': 'suning',
        '考拉': 'kaola',
        '国美': 'gome',
        '聚美': 'jumei',
        '酒仙': 'jx',
        '途虎': 'tuhu'
    }

    shop_link_map_dict = {
        'jd': 'http://mall.jd.com/index-{}.html',
        'tmall': 'https://shop{}.taobao.com/',
        'tb': 'https://shop{}.taobao.com/',
        'kaola': 'https://mall.kaola.com/{}',
        'suning': 'http://shop.suning.com/{}/index.html'
    }

    plt_str = '''transform(multiIf(source!=1,source,(shop_type<20 and shop_type>10),0,1), [0,1,2,3,4,5,6,7,8,9,10],
                          ['淘宝','天猫','京东','国美','聚美','考拉','苏宁','唯品会','拼多多','酒仙','途虎'],'' ) as source2 '''
    gb_sp_info_dict = {'item_id': {'value': ['item_id'], 'group_by': ['item_id']},
                       'sid': {'value': ['gSid() as g_sid'], 'group_by': ['g_sid']},
                       'alias_all_bid': {'value': ["gAliasBid() as g_alias_bid"],
                                         'group_by': ["g_alias_bid"]},
                       '子品类': {'value': ["prop_value('子品类') as sp_name"],
                               'group_by': ['sp_name']},
                       'sku_id':{'value':['sku_id'],
                                 'group_by':['sku_id']}}

    gb_wh_id_dict = {'item_id': 'item_id',
                     'sid': 'g_sid',
                     'alias_all_bid': 'g_alias_bid',
                     '子品类': 'sp_name',
                     'sku_id':'sku_id'}

    # 获取指定字段target在列表gb中的位置索引
    def get_index(self, target, gb):
        for i, j in enumerate(gb):
            if target in j:
                return i

    # '''get all info needed from log table'''
    def get_log_info(self, f_name):
        sql = '''select id, eid, group_by, date_type, check_columns from kadis.entity_prod_common_E_log
                 where deleteFlag = 0 and name='{}' '''.format(f_name)
        data = db.query_all(sql)
        id, eid, group_by, date_type, check_columns = data[0]

        group_by_list = group_by.split(',')
        check_columns_list = check_columns.split(',')

        res = {'id': id, 'eid': eid, 'group_by': group_by_list, 'date_type': date_type,
               'check_columns': check_columns_list}
        return res

    # 获取从E表中所需的select字段、条件字段及group_by字段
    def get_some_where_list(self):
        gb_select = ['{}'.format(self.plt_str)]
        gb_where_name = ['source2']
        gb_groupby = ['source2']

        tmp_index = 0
        for gb in self.group_by:
            if '**' not in gb:
                gb_select.append(','.join(self.gb_sp_info_dict[gb]['value']))
                gb_where_name.append(self.gb_wh_id_dict[gb])
                gb_groupby.append(','.join(self.gb_sp_info_dict[gb]['group_by']))
            else:
                new_gb = gb.replace('**','')
                gb_select.append("prop_value('{}') as clean_sp{}".format(new_gb, tmp_index))
                gb_where_name.append('clean_sp{}'.format(tmp_index))
                gb_groupby.append('clean_sp{}'.format(tmp_index))
            tmp_index += 1

        return gb_select, gb_where_name, gb_groupby

    # 根据group_by里面的多个值，获取多个clean_props.value--common_E表
    def get_more_clean_props(self):
        gb_id = ['source']
        gb_name = []

        for i in self.group_by:
            if i == '子品类' or '**' in i:
                gb_id.append("`clean_props.value` [indexOf(`clean_props.name`, '{}')]".format(i))
            else:
                gb_id.append(i)
                gb_name.append("`clean_props.value` [indexOf(`clean_props.name`, '{}')]".format(self.id_name_1o1.get(i, '')))

        return gb_id, gb_name

    # 从common_E表中获取数据
    def get_all_info_from_common_e(self):
        gb_id, gb_name = self.get_more_clean_props()

        sql_front = 'select date, num, sales, price, clean_props.name, clean_props.value, '
        if gb_name:
            sql_mid = ' {a}, {b}'.format(a=','.join(gb_id), b=','.join(gb_name))
        else:
            sql_mid = ' {b}'.format(b=','.join(gb_id))
        sql_back = ' from sop_e.entity_prod_common_E where id={c} and eid={d}'.format(c=self.id_ip, d=self.eid)
        sql = sql_front + sql_mid + sql_back

        data = chmaster.query_all(sql)

        id_info_common_e_dict = collections.defaultdict(dict)
        m_date_list = []  # 存储日期列表
        names_dict = collections.defaultdict(lambda: [])  # 存储index与name

        for row in data:
            m_date, num, sales, price, clean_name, clean_values = list(row[:6])
            m_date = m_date.strftime('%Y-%m-%d')
            m_date_list.append(m_date)

            rest_value = row[6:]
            index = rest_value[:len(self.group_by)+1]

            bol_list = ['**' in i for i in self.group_by]

            if True in bol_list:
                if False in bol_list:
                    names_dict[index] = rest_value[::-1][:2]
                else:
                    names_dict[index] = rest_value[1:]
            else:
                names_dict[index] = row[(len(self.group_by) + 6) + 1:]

            clean_props = {j: clean_values[i] for i, j in enumerate(clean_name)}
            if index not in id_info_common_e_dict[m_date]:
                id_info_common_e_dict[m_date][index] = {}
            id_info_common_e_dict[m_date][index] = {'num': num, 'sales': sales / 100, 'price': price / 100,
                                                    'clean_props': clean_props}  # 将销售额和均价的单位转换为千元

        return min(m_date_list), max(m_date_list), names_dict, id_info_common_e_dict

    # 当tuple中只有一个元素时，删除tuple中的逗号
    def del_comma_1(self, l):
        if len(l) == 1:
            return l[0]
        else:
            return l

    # 根据条件，从E表里取数据
    def get_all_info_from_new_table(self, gs, gw, gg, id_list):
        if self.date_type == 'day':
            delta = relativedelta(days=1)
        elif self.date_type == 'month':
            delta = relativedelta(months=1)
        elif self.date_type == 'quarter':
            delta = relativedelta(months=3)
        else:
            delta = relativedelta(years=1)

        new_end = (datetime.datetime.strptime(self.end_date, '%Y-%m-%d') + delta).strftime('%Y-%m-%d')

        # 获取平台的种类
        new_plt_set = pd.DataFrame(list(id_list)).iloc[:,0].unique()

        sql_0 = '''select {d} as g_date, num_total,sales_total,tb('last_num') as tb_num, hb('last_num') as hb_num,
                           tb('last_sales') as tb_sales, hb('last_sales') as hb_sales,
                '''.format(d=self.date_type_dict[self.date_type],gs=','.join(gs))

        if '天猫' not in new_plt_set and '淘宝' not in new_plt_set:
            sql_1 = ''' num_review_rate_info('num_review_rate') as num_review_rate, {gs}
                    '''.format(gs=','.join(gs))
            sql_3 = ''' and with_num_review_rate('num_review_rate_info')'''
        else:
            sql_1 = ''' {gs}'''.format(gs=','.join(gs))
            sql_3 = ''

        sql_2 = ''' from {table}
                    where {where_eid} and w_start_date('{start_date}') and w_end_date_exclude('{end_date}') and
                          platformsIn('{plt}') and with_tb('tb') and with_hb('hb')
            '''.format(table=self.one_table, where_eid=self.get_table_where_by_eid(),start_date=self.start_date,
                       end_date=new_end,plt=self.plt_dict[list(new_plt_set)[0]] if len(new_plt_set)==1 else 'all')

        if isinstance(self.del_comma_1(id_list), str):
            sql_4 = ''' and {gw}='{id_list}' group by g_date, {gg}
                    '''.format(gw=gw[0], id_list=self.del_comma_1(id_list), gg=','.join(gg))
        else:
            sql_4 = ''' and ({gw}) in {id_list} group by g_date, {gg}
                    '''.format(gw=','.join(gw), id_list=tuple(self.del_comma_1(id_list)), gg=','.join(gg))

        sql = sql_0 + sql_1 + sql_2 + sql_3 + sql_4

        data = chsql.query_all(sql)
        res_from_sop_e = collections.defaultdict(dict)
        if data:
            for row in data:
                res_tmp = list(row.values())

                if '天猫' not in list(new_plt_set) and '淘宝' not in list(new_plt_set):
                    month, num_t, sales_t, tb_num, hb_num, tb_sales, hb_sales, rate = res_tmp[:8]
                    index0 = tuple(res_tmp[8:])
                else:
                    month, num_t, sales_t, tb_num, hb_num, tb_sales, hb_sales = res_tmp[:7]
                    rate = '-'
                    index0 = tuple(res_tmp[7:])

                mm = month.strftime('%Y-%m-%d')
                price = int(sales_t / num_t) if num_t else 0
                num_yoy = round(num_t / tb_num - 1, 4) if tb_num else 0
                num_mom = round(num_t / hb_num - 1, 4) if hb_num else 0
                sales_yoy = round(sales_t / tb_sales - 1, 4) if tb_sales else 0
                sales_mom = round(sales_t / hb_sales - 1, 4) if hb_sales else 0

                if index0 not in res_from_sop_e[mm]:
                    res_from_sop_e[mm][index0] = {}
                res_from_sop_e[mm][index0] = {'num': num_t, 'sales': sales_t / 100, 'price': price / 100,
                                             'num_yoy': num_yoy, 'num_mom': num_mom,
                                             'sales_yoy': sales_yoy, 'sales_mom': sales_mom, 'rate': rate}
            return res_from_sop_e
        else:
            raise Exception('数据表中没有对应的数据哦！')

    # 格式化输出比较的暂时结果
    def format_res_dict(self, compare_dict):
        tmp_dict = collections.defaultdict(lambda:[])
        for col in self.check_columns:
            com_res = compare_dict[col]
            m_index = 0
            for k in com_res:
                the_key = self.arrCols_dict[col][m_index]
                tmp_dict[the_key] = com_res[k]
                m_index += 1
        return tmp_dict

    # 获取输出结果时，比较维度的id
    def get_out_id(self, sp_id_list):
        gb_bk = copy.deepcopy(self.group_by)
        if '子品类' in gb_bk:
            gb_bk.remove('子品类')
        D = {}
        gb_index = 0
        for i in gb_bk:
            if '**' not in i:
                if i not in D:
                    D[i] = ''
                D[i] = sp_id_list[gb_index]
                gb_index += 1
        return D

    # 获取输出结果时，比较维度的name
    def get_out_name(self, l):
        out = {}
        if l:
            try:
                for i, j in enumerate(self.group_by):
                    if '**' not in j:
                        name = self.comd_name_dict[j]
                    else:
                        name = j.replace('**','')
                    if name not in out:
                        out[name] = ''
                    out[name] = l[i]
            except:
                out = {}
        return out

    # 标准化输出
    def format_out(self, tmp_output):
        out_dict = {}
        for my_key in tmp_output:
            if isinstance(tmp_output[my_key], dict):
                for inner_key in tmp_output[my_key]:
                    if inner_key not in out_dict:
                        out_dict[inner_key] = ''
                    out_dict[inner_key] = tmp_output[my_key][inner_key]
            else:
                if my_key not in out_dict:
                    out_dict[my_key] = ''
                out_dict[my_key] = tmp_output[my_key]
        return out_dict

    # 获取grop_by的名字
    def get_groupby_name(self):
        gb_list = []
        for gb0 in self.group_by:
            if '**' not in gb0:
                db = self.comd_name_dict.get(gb0, '{}'.format(gb0))
            else:
                db = gb0.replace('**','')
            gb_list.append(db)
        return gb_list

    # 展开嵌套列表
    def flat_list(self, l):
        l1 = []
        for a in l:
            if isinstance(a, list):
                for b in a:
                    l1.append(b)
            else:
                l1.append(a)
        return l1

    # 通过eid（列表）确定从哪张表里面取数据
    def get_table_where_by_eid(self):
        if '' in self.where_eid:
            where = 'w_eid({})'.format(self.where_eid[0])
        else:
            where = 'w_eid({})'.format(self.where_eid[0]) + " and w_eid_tb('{}')".format(self.where_eid[1])
        return where

    # 如果是“天猫”平台，需要根据sid去查找对应的tb_sid
    def get_tbsid_by_sid(self, sid):
        if self.en_plt in ['tmall', 'taobao']:
            sql = '''select tb_sid from apollo.shop where sid={} '''.format(sid)
            tb_sid = apollo.query_all(sql)[0][0]
        else:
            tb_sid = sid
        return tb_sid

    def main_compare_process(self, param):

        self.com_type = param.get('type', 1)  # com_type = 1代表了 common-E 和 普通E表对比  com_type = 0 代表两张普通E表对比
        self.raw_t = param.get('raw', 'sop_e.entity_prod_common_E')
        self.new_t = param.get('new', '')
        self.date_type = param.get('date', 'day')
        self.file_name_list = param.get('name', '')

        all_res_list = []  # 存储最终结果的列表

        for index, file_name in enumerate(self.file_name_list):
            log_res = self.get_log_info(file_name)
            self.id_ip = log_res['id']
            self.eid = log_res['eid']
            self.group_by = log_res['group_by']
            self.check_columns = log_res['check_columns']

            output = []
            gb_select, gb_where_name, gb_groupby = self.get_some_where_list()
            # com_type = 1代表了 common-E 和 普通E表对比
            if self.com_type == 1:
                self.start_date, self.end_date, id_dict, id_info_common_e_dict = self.get_all_info_from_common_e()

                new_id_list = tuple(id_dict.keys())

                if '_A' in self.new_t:
                    self.one_table = 'sop.a'
                    self.where_eid = [self.eid, self.new_t[23:]]
                else:
                    self.one_table = self.new_t[:5].replace('_', '.')
                    self.where_eid = [self.eid, self.new_t[26:]]

                res_from_sop_e = self.get_all_info_from_new_table(gb_select, gb_where_name, gb_groupby, new_id_list)

                for month in id_info_common_e_dict:
                    all_data_of_month = id_info_common_e_dict[month]

                    for index in all_data_of_month:
                        self.en_plt = self.plt_ch_en[index[0]]  # 平台的英文

                        if 'item_id' in self.group_by:
                            if len(self.group_by) == 1:
                                id_index = 0
                                other_index = 1
                            else:
                                id_index = self.get_index('item_id', self.group_by)
                                other_index = len(self.group_by)-id_index-1
                            link = common.get_link(self.plt_ch_en[index[id_index]], index[other_index])
                        elif 'sid' in self.group_by:
                            if self.get_index('sid',self.group_by) == 1:
                                tb_sid = self.get_tbsid_by_sid(index[2])
                            else:
                                tb_sid = self.get_tbsid_by_sid(index[1])
                            link = self.shop_link_map_dict[self.en_plt].format(tb_sid)
                        else:
                            link = ''

                        names = id_dict.get(index, '')
                        compare_dict = collections.defaultdict(lambda: [])
                        item = all_data_of_month[index]
                        if index in res_from_sop_e[month]:
                            for column in self.check_columns:
                                new = res_from_sop_e[month][index][column]
                                if 'yoy' in column or 'mom' in column:
                                    value = item['clean_props'][self.id_name_1o1[column]]
                                    if '%' in value:
                                        raw = float(value.strip("%")) / 100 if value else 0
                                    else:
                                        raw = float(value) if value else 0
                                    compare_dict[column] = {'raw': raw, 'new': new}
                                else:
                                    raw = item[column]
                                    pickup = '{}%'.format(round((new / raw) * 100, 2)) if raw else 'Inf'
                                    compare_dict[column] = {'raw': raw, 'new': new, 'rate': pickup}
                            rate = res_from_sop_e[month][index]['rate']
                            whether = '是'
                        else:
                            for column in self.check_columns:
                                if 'yoy' in column or 'mom' in column:
                                    value = item['clean_props'][self.id_name_1o1[column]]
                                    if '%' in value:
                                        raw = float(value.strip("%")) / 100 if value else 0
                                    else:
                                        raw = float(value) if value else 0
                                    compare_dict[column] = {'raw': raw, 'new': '-'}
                                else:
                                    raw = item[column]
                                    compare_dict[column] = {'raw': raw, 'new': '-', 'rate': '-'}
                            rate = '-'
                            whether = '否'

                        tmp_dict = self.format_res_dict(compare_dict)
                        regex = re.compile('\d+')
                        sp_id_list = re.findall(regex, str(index))

                        id_out_dict = self.get_out_id(sp_id_list)
                        name_out_dict = self.get_out_name(names)
                        if not name_out_dict:
                            for gb in self.group_by:
                                cc = self.comd_name_dict[gb]
                                if gb == '子品类':
                                    name_out_dict[cc] = index[self.get_index('子品类', self.group_by)+1]
                                else:
                                    name_out_dict[cc] = id_dict[index][0]

                        self.tmp_output = {'date': month, 'name': name_out_dict, 'link': link, 'id': id_out_dict,
                                      'compare_dimension': tmp_dict,'isCatch': whether, 'ringDownRatio': rate}
                        output.append(self.format_out(self.tmp_output))
            else:
                pass

            res_final = {}
            res_final['groupby'] = self.get_groupby_name()
            res_final['data'] = output

            self.tmp_output.pop('compare_dimension')
            l01 = list(self.format_out(self.tmp_output).keys())
            l01.insert(-2, self.check_columns)

            l1 = self.flat_list(l01)  # ['date', 'subCategory', 'shop', 'sid', 'num', 'sales', 'price', 'isCatch', 'ringDownRatio']
            l1.remove('link')

            l2 = [j for i in self.check_columns for j in self.arrCols_dict[i]]
            res_final['header'] = [l1, l2]

            all_res_list.append(res_final)

        return all_res_list

def main_process(param):
    utils.easy_call([chmaster, db, apollo], 'connect')

    ComData = CompareData()
    output = ComData.main_compare_process(param)

    return output


if __name__ == '__main__':
    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅-店铺.csv'],
    #          'date':'day'}  # checked_new 04.21

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅-子品类+店铺.csv'],
    #          'date':'day'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅-子品类.csv'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅-品牌.csv'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90261_E',
    #          'name':['巧克力-itemid.csv'],
    #          'date':'month'}   # checked_new  04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅-店铺+品牌.csv'],
    #          'date':'day'} # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_80261_E',
    #          'name':['瑞士莲item.csv'],
    #          'date':'day'} # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_90513_E',
    #          'name':['康师傅同一宝贝不同日期test.csv'],
    #          'date':'day'} # checked_new 04.09

    # param = {'type': 1,
    #         'raw': 'sop_e.entity_prod_common_E',
    #         'new': 'sop.entity_prod_90513_A',
    #         'name': ['康师傅_link_test.csv'],
    #         'date': 'day'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #          'name':['亿滋E表.csv//上传数据模板'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #          'name':['亿滋E表.csv//上传数据模板'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop.entity_prod_91081_A',
    #          'name':['亿滋21年.csv//品牌维度'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #        'raw':'sop_e.entity_prod_common_E',
    #         'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #         'name':['mdlz_test_0408.csv'],
    #         'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #          'name':['mdlz_test_double_clean_sps.csv'],
    #          'date':'month'}  # checked_new 04.09

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #          'name':['模板—数据对比_最新版-亿滋2.csv//上传数据模板'],
    #          'date':'month'}  # checked_new 04.14

    # param = {'type':1,
    #          'raw':'sop_e.entity_prod_common_E',
    #          'new':'sop_e.entity_prod_91081_E_MDLZ_report',
    #          'name':['亿滋21年品牌维度.csv//宝贝维度'],
    #          'date':'month'}  # checked_new 04.21

    param = {'type':1,
             'raw':'sop_e.entity_prod_common_E',
             'new':'sop_e.entity_prod_91081_E_ferrero_report',
             'name':['21年4月top50宝贝检查.csv//Sheet1'],
             'date':'day'}
    output = main_process(param)
    print(output)