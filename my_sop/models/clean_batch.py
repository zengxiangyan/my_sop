#coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import re
import json
import ujson
import time
import pandas as pd
import application as app
from html import unescape
from models import common
from models.trie import Trie
from models.entity import Entity
from models.plugin_manager import PluginManager
from models.all_brand import AllBrand
from models.time_monitor import TimeMonitor
from nlp.sku_match import SkuMatch, Sku, Item

class CleanBatch:
    db_clickhouse = app.get_clickhouse('default')
    db_chsop = app.get_clickhouse('chsop')
    db_cleaner = app.get_db('default')
    # db_1_apollo = app.get_db('1_apollo')
    db_18_apollo = app.get_db('18_apollo')
    db_artificial_new = app.get_db('26_artificial_new')
    # db_artificial_new_write = app.get_db('26_artificial_new')
    line = '-' * 50
    dline = ':' * 50
    default_limit = 1000
    default_other = '其它'
    decimal_place = 2
    chn_numbers = ('零', '半', '整', '一', '单', '二', '两', '双', '三', '四', '五', '六', '七', '八', '九', '十', '百') # 千以下，避免和千克等单位识别冲突
    chn_number_map = {
        '零':0,
        '半':0.5,
        '整':1,
        '一':1,
        '单':1,
        '二':2,
        '两':2,
        '双':2,
        '三':3,
        '四':4,
        '五':5,
        '六':6,
        '七':7,
        '八':8,
        '九':9,
    }
    chn_unit_map = {
        '十':10,
        '百':100,
        '千':1000,
        '万':10000,
    }
    range_indicator_middle = ('-', '到', '至')
    range_indicator_post = ('以上', '以下', '及以上', '及以下')
    separator = '|'
    model_separators = ('、', ',', ';', ':', '+', '|')
    model_need_trans = {'~': '-', '\\': '/'}
    model_strip = ''.join({' ', '|', ',', '.', '?', '!', '~', '-', '_', 'ˉ', '=', ';', ':', '/', '*', '^', '`', '·', '、'})
    model_no_duplicate = {' ', '|', ',', '.', '?', '!', '~', '-', '_', 'ˉ', '=', ';', ':', '/', '*', '^', '`', '·', '、', '+'}
    model_should_remove = {'()', '( )', '[]', '[ ]', '{}', '{ }', '<>', '< >'}
    model_recent_days = 31 * 6      # recent 6 months
    jdzy_suffix = '_jdzy'
    old_suffix = '_old'
    cmpl_alnum = re.compile(r'([0-9a-z])', re.I)
    pure_num = re.compile(r'(\d+(\.\d+)?)')
    invisible = re.compile(r'[\x00-\x1F\x7F]', re.S) # name.strip().replace(u'\u3000', u' ').replace(u'\xa0', u' ')
    parenthesis_before_num = re.compile(r'(\S)[()](\d|{c_n})'.format(c_n='|'.join(chn_numbers)))

    def __init__(self, batch_id, store_log = False, print_out = True, cache = None, entity = None):
        print('{line}Load clean_batch {i} at {t}{line}'.format(line=self.dline, i=batch_id, t=time.strftime('%Y-%m-%d %H:%M', time.localtime())))
        self.db_clickhouse.connect()
        self.db_chsop.connect()
        self.db_cleaner.connect()
        # self.db_1_apollo.connect()
        # self.db_18_apollo.connect() # 因连接问题临时禁用
        # self.db_artificial_new_write.connect()
        self.db_an_conn = self.db_artificial_new.connect()
        self.db_an_engine = self.db_artificial_new.get_engine()

        self.batch_id = batch_id
        self.store_log = store_log
        self.print_out = print_out
        self.log = []
        self.error_msg = []
        self.warning_id = {'id_list': set()}
        self.brand_obj = AllBrand()
        self.ref_models = dict()
        self.entity = entity
        self.cache = cache
        self.allow_unfold = True if self.batch_id <= 190 else False    # request from zhou.wenjun 2021-09-10
        self.__get_info()

    def __del__(self):
        #self.db_cleaner.close()
        #self.db_1_apollo.close()
        #self.db_18_apollo.close()
        #self.db_artificial_new.close()
        print('{line}Release clean_batch {i} at {t}{line}'.format(line=self.dline, i=self.batch_id, t=time.strftime('%Y-%m-%d %H:%M', time.localtime())))

    def __str__(self):
        print('{line}Printing Object{line}'.format(line=self.line))
        for key in self.__dict__:
            print(key, ':', self.__dict__[key], '\n')
        return '>>> Instance Attributes: ' + ', '.join(self.__dict__.keys())

    def print_log(self, *vartuple):
        s = ' '.join([str(v) for v in vartuple])
        if self.print_out:
            print(s)
        if self.store_log:
            self.log.append(s)

    def __get_info(self):
        self.comments = None
        self.config_max_update_time = 0
        self.config_total_count = 0
        self.clean_ver = 0

        data = self.db_cleaner.query_all('SELECT eid, last_id, last_pid, name, update_alias_bid, is_item_table_mysql, clean_sku, dy_status FROM clean_batch WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=self.batch_id))
        self.eid, self.last_id, self.last_pid, self.batch_name, self.is_update_alias_bid, self.is_item_table_mysql, self.clean_sku, self.dy_status = data[0]

        self.clean_table_name = 'entity_{eid}_clean'.format(eid=self.eid) if self.is_item_table_mysql else None
        self.item_table_name = 'entity_{eid}_item'.format(eid=self.eid) if self.is_item_table_mysql else None
        self.distribution_table_name = 'entity_{eid}_distribution'.format(eid=self.eid)
        self.props_table_name = 'entity_{eid}_model_all_props'.format(eid=self.eid)
        self.model_table_name = 'entity_{eid}_model'.format(eid=self.eid)
        self.model_table_name_old = self.model_table_name + self.old_suffix
        self.model_table_name_jdzy = self.model_table_name + self.jdzy_suffix
        self.time_table_name = 'entity_{eid}_time'.format(eid=self.eid)
        self.time_monitor = TimeMonitor(db=self.db_artificial_new, table_name=self.time_table_name)

        if self.dy_status != 0:
            data = self.db_cleaner.query_one('select id from tiktok_log where batch_id = {batch_id} and delete_flag = 0;'.format(batch_id=self.batch_id))
            if data == None:
                assert '抖音模型数据没有刷新完成，暂时无法进行清洗。请联系相关负责人。'

        self.sub_batch = dict()
        data = self.db_cleaner.query_all('SELECT sub_batch_id, name, UNIX_TIMESTAMP(updateTime) FROM clean_sub_batch WHERE batch_id = {batch_id} AND deleteFlag = 0 ORDER BY sub_batch_id ASC;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)
        count = 0
        for row in data:
            sub_batch_id, name, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            self.sub_batch[sub_batch_id] = {'clean_type': count, 'name': name}
            count += 1

        self.sc_category = dict()
        self.category_filter_keys = ['all_bid', 'alias_all_bid', 'cid', 'sid', 'item_id']
        data = self.db_cleaner.query_all('SELECT id, source, filters, sub_batch_id, and_word, or_word, not_word, ignore_word, rank, UNIX_TIMESTAMP(updateTime) FROM clean_category WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)

        if self.batch_id not in [259,270]:
            dddd = self.db_cleaner.query_all('SELECT is_mixed FROM dataway.entity WHERE id = {eid}'.format(eid=self.eid))
            dddd = self.db_cleaner.query_all('SELECT source, cid FROM dataway.entity_tip_category{mix} WHERE eid = {eid}'.format(mix='2' if dddd[0][0] else '', eid=self.eid))
            dddd = {(a,b):1 for a,b, in dddd}
        else:
            dddd = {}

        for row in data:
            id, source, filters_str, sub_batch_id, and_word, or_word, not_word, ignore_word, rank, u_tmp = row
            if sub_batch_id not in list(self.sub_batch.keys()) + [0]:
                self.error_msg.append('clean_category表id={id}所填写的sub_batch_id不在本项目子品类范围内。'.format(id=id))
                continue
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            source = source.lower().strip()
            try:
                filters = json.loads(filters_str) if filters_str else dict()
            except:
                try:
                    filters = eval(filters_str) if filters_str else dict()
                except:
                    self.error_msg.append('clean_category表id={id}的filters解析失败。'.format(id=id))
                    filters = dict()
            for fkey in filters:
                if fkey not in self.category_filter_keys:
                    self.error_msg.append('clean_category表id={id}的filters填写错误。'.format(id=id))
                elif self.batch_id in [259,270] and fkey == 'cid' and self.entity: # 兼容旧逻辑 不允许直接new clean_batch
                    sss = self.entity.get_plugin().get_source(source)

                    cat = []
                    for check in filters[fkey]:
                        if not isinstance(check, int):
                            self.error_msg.append('clean_category表id={id}的filters里数字列表{fkey}格式类型错误。'.format(id=id, fkey=fkey))
                            break
                        cat += self.entity.get_plugin().get_category_info(sss, check)['child_cids_with_self'] or []

                    filters[fkey] = list(set(cat))
                    for check in filters[fkey]:
                        if ('ali' if source in ('tb','tmall') else source, check) in dddd:
                            dddd[('ali' if source in ('tb','tmall') else source, check)] = 0
                elif fkey == 'item_id':
                    for check in filters[fkey]:
                        if not isinstance(check, str):
                            self.error_msg.append('clean_category表id={id}的filters里字符串列表{fkey}格式类型错误。'.format(id=id, fkey=fkey))
                            break
                else:
                    for check in filters[fkey]:
                        if not isinstance(check, int):
                            self.error_msg.append('clean_category表id={id}的filters里数字列表{fkey}格式类型错误。'.format(id=id, fkey=fkey))
                            break
                        if ('ali' if source in ('tb','tmall') else source, check) in dddd:
                            dddd[('ali' if source in ('tb','tmall') else source, check)] = 0
                if not filters[fkey]:
                    for (s, c) in dddd:
                        if 'ali' if source in ('tb','tmall') else source == s:
                            dddd[(s, c)] = 0
            if not filters:
                dddd = {}

            and_words = and_word.upper().split(',') if and_word else []     # 大小写不敏感，标准化为全大写
            or_words = or_word.upper().split(',') if or_word else []        # 大小写不敏感，标准化为全大写
            not_words = not_word.upper().split(',') if not_word else []     # 大小写不敏感，标准化为全大写
            ignore_words = sorted(ignore_word.upper().split(','), key = lambda x:(len(x),x), reverse = True) if ignore_word else []    # 大小写不敏感，标准化为全大写
            self.sc_category[source] = self.sc_category.get(source, {})
            # 把filter里面用的比较多的cid拆分出来,单独做字典
            cid_list = filters.get('cid', [])
            for cid in cid_list:
                self.sc_category[source][cid] = self.sc_category[source].get(cid, [])
                self.sc_category[source][cid].append((filters, sub_batch_id, and_words, or_words, not_words, ignore_words, rank))
            # filter里面没有cid的，归入-1中
            if len(cid_list) == 0:
                if self.sc_category[source].get(-1) == None:
                    self.sc_category[source][-1] = []
                self.sc_category[source][-1].append((filters, sub_batch_id, and_words, or_words, not_words, ignore_words, rank))

        if self.batch_id not in [259,270]:
            for (fkey, check) in dddd:
                if dddd[(fkey, check)] == 1:
                    self.error_msg.append('{}平台cid{}，缺少能够涵盖整个cid的配置逻辑，请确认配置后再采样/清洗'.format(fkey, check))
                    break

        for key in self.sc_category:
            for cid in self.sc_category[key]:
                self.sc_category[key][cid] = sorted(self.sc_category[key][cid], key = lambda x:x[-1], reverse = True)     # 优先级rank从高到低排序

        self.quantifier = dict()
        all_unit_set = set()
        pre_unit_set = set()
        data = self.db_cleaner.query_all('SELECT type, pre, middle, post, calculate, only_int, UNIX_TIMESTAMP(updateTime) FROM clean_quantifier WHERE deleteFlag = 0;')
        self.config_total_count += len(data)
        q_type_list = [r[0] for r in data if r[0] > 2]
        for row in data:
            q_type, pre, middle, post, calculate, only_int, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            self.quantifier[q_type] = {
                'pre':       pre.upper().split(',') if pre else [],             # 大小写不敏感，标准化为全大写
                'middle':    middle.upper().split(',') if middle else [],       # 大小写不敏感，标准化为全大写
                'post':      post.upper().split(',') if post else [],           # 大小写不敏感，标准化为全大写
                'calculate': calculate.split(',') if calculate else [],
                'only_int':  bool(int(only_int)),
            }
            pre_unit_set.update(set(self.quantifier[q_type]['pre']))
            all_unit_set.update(set(self.quantifier[q_type]['post']))
        self.all_units = tuple(sorted(all_unit_set, key = lambda x:(len(x),x), reverse = True))
        self.pattern_units = r'([0-9{c_n}]+(\.\d+)?)({all_u})?(\*|\+|±|{r_i_m})?(\d*(\.\d+)?)({all_u})?({r_i_p})?'.format(all_u='|'.join(self.all_units), c_n=''.join(self.chn_numbers), r_i_m = '|'.join(self.range_indicator_middle), r_i_p = '|'.join(self.range_indicator_post))
        self.cmpl_units = re.compile(self.pattern_units, re.I)
        self.cmpl_extract = re.compile(r'((\.|\/|\+|\*|{r_i_p}|各|共|\d|{cn_u})+)'.format(cn_u='|'.join(self.chn_numbers + self.all_units), r_i_p = '|'.join(self.range_indicator_post)), re.I)

        self.pos_id_category, self.pos_id_brand, self.pos_id_as_model, self.pos_id_model, self.pos_id_sku, self.pos_id_price = (None, None, None, None, None, None)
        data = self.db_cleaner.query_all('SELECT pos_id, name, type, p_no, default_quantifier, read_from_right, prefix, p_key, only_quantifier, unit_conversion, num_range, pre_unit_in, pure_num_in, pure_num_out, calculate_sum, from_multi_sp, ignore_word, remove_word, as_model, target_no_rank, output_type, UNIX_TIMESTAMP(updateTime) FROM clean_pos WHERE batch_id = {batch_id} AND deleteFlag = 0 ORDER BY type DESC;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)
        self.pos_id_list = tuple([i[0] for i in data])
        self.pos = dict()
        for row in data:
            pos_id, name, q_type, p_no, default_quantifier, read_from_right, prefix, p_key, only_quantifier, unit_conversion, num_range, pre_unit_in, pure_num_in, pure_num_out, calculate_sum, from_multi_sp, ignore_word, remove_word, as_model, target_no_rank, output_type, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            self.pos[pos_id] = {
                'name': name,
                'type': q_type,
                'p_no': p_no,
                'output_type': output_type,
                'default_quantifier': default_quantifier,       # 大小写敏感
                'read_from_right': read_from_right,       # 0从左读，1从右读，2最小值，3最大值
                'prefix': prefix.split(',') if prefix else [],
                'only_quantifier': only_quantifier.replace(' ','').upper().split(',') if only_quantifier else [],   # 大小写不敏感，标准化为全大写
                'unit_conversion': dict(),       # key的大小写敏感接下来分别处理
                'multi_units': dict(),     # 内容接下来从另一个表读
                'num_range': [float(x) for x in num_range.replace(' ','').split(',')] if num_range else None,
                'pre_unit_in': bool(int(pre_unit_in)),
                'pure_num_in': bool(int(pure_num_in)),
                'pure_num_out': bool(int(pure_num_out)),
                'calculate_sum': bool(int(calculate_sum)),
                'as_model': bool(int(as_model)),
                'target_no_rank': target_no_rank,       # 1从左读，2从右读
                'from_multi_sp': [i.split('+') for i in from_multi_sp.replace(' ','').split(',')] if from_multi_sp else [],
                'ignore_word': sorted(ignore_word.upper().split(','), key = lambda x:(len(x),x), reverse = True) if ignore_word else [],    # 大小写不敏感，标准化为全大写
                'remove_word': sorted(remove_word.upper().split(','), key = lambda x:(len(x),x), reverse = True) if remove_word else [],    # 大小写不敏感，标准化为全大写
            }
            if unit_conversion:
                try:
                    tmp_uc_dict = eval(unit_conversion)
                except:
                    tmp_uc_dict = dict()
                    self.error_msg.append('clean_pos表pos_id={pos_id}单位换算格式错误。'.format(pos_id=pos_id))
                u_c_err = False
                for k in tmp_uc_dict:
                    t_k = k.split(',')
                    new_k = t_k[0].upper()
                    if self.pos[pos_id]['unit_conversion'].get(new_k):
                        self.error_msg.append('clean_pos表pos_id={pos_id}单位{q}重复换算。'.format(pos_id=pos_id, q=new_k))
                    self.pos[pos_id]['unit_conversion'][new_k] = (tmp_uc_dict[k], t_k[1])    # key的,后为目标转换单位（大小写敏感，不能标准化为全大写）
                    if type(tmp_uc_dict[k]) == list:
                        for i in tmp_uc_dict[k]:
                            if type(i) not in [int, float]:
                                u_c_err = True
                                break
                    elif type(tmp_uc_dict[k]) not in [int, float]:
                        u_c_err = True
                if u_c_err:
                    self.error_msg.append('clean_pos表pos_id={pos_id}单位换算错误。'.format(pos_id=pos_id))
            try:
                self.pos[pos_id]['p_key'] = json.loads(p_key) if p_key else dict()
            except:
                try:
                    self.pos[pos_id]['p_key'] = eval(p_key) if p_key else dict()
                except:
                    self.error_msg.append('clean_pos表pos_id={pos_id}的p_key解析失败。'.format(pos_id=pos_id))
                    self.pos[pos_id]['p_key'] = dict()
            if self.pos[pos_id]['type'] < 0: # 不参与机洗
                continue
            elif self.pos[pos_id]['type'] == 1000: # 品牌
                self.pos_id_brand = pos_id
            elif self.pos[pos_id]['type'] == 900: # 子品类
                self.pos_id_category = pos_id
            elif self.pos[pos_id]['type'] == 800: # 价格段
                self.pos_id_price = pos_id
            elif self.pos[pos_id]['type'] == 100: # 型号
                self.pos_id_model = pos_id
            elif self.pos[pos_id]['type'] == 101: # SKU
                self.pos_id_sku = pos_id
            elif self.pos[pos_id]['type'] in q_type_list:
                self.pos[pos_id]['q_post'] = list(set.intersection(set(self.pos[pos_id]['only_quantifier']), set(self.quantifier[self.pos[pos_id]['type']]['post']))) if self.pos[pos_id]['only_quantifier'] else self.quantifier[self.pos[pos_id]['type']]['post']
                self.pos[pos_id]['q_pre'] = list(set.intersection(set(self.pos[pos_id]['only_quantifier']), set(self.quantifier[self.pos[pos_id]['type']]['pre']))) if self.pos[pos_id]['only_quantifier'] else self.quantifier[self.pos[pos_id]['type']]['pre']
                if self.pos[pos_id]['calculate_sum']:
                    q_volume = sorted(set(self.pos[pos_id]['q_post']), key = lambda x:(len(x),x), reverse = True)
                    q_count = sorted(set(self.quantifier[4]['post']).difference(set(q_volume)), key = lambda x:(len(x),x), reverse = True)
                    q_others = sorted(set(self.all_units).difference(set(q_volume + q_count)), key = lambda x:(len(x),x), reverse = True)
                    self.pos[pos_id]['cmpl_v'] = re.compile(r'({q_v})'.format(q_v='|'.join(q_volume)), re.I)
                    self.pos[pos_id]['cmpl_c'] = re.compile(r'({q_c})'.format(q_c='|'.join(q_count)), re.I)
                    self.pos[pos_id]['cmpl_o'] = re.compile(r'({q_o})'.format(q_o='|'.join(q_others)), re.I)
                    self.pos[pos_id]['cmpl_n'] = re.compile(r'([0-9*+]|{c_n})'.format(c_n='|'.join(self.chn_numbers)), re.I)
                    pattern = r'([0-9{c_n}]+)?({q_c})?(\*|各|共)?(\d*(\.\d+)?)({q_v})?({r_i_p})?(\*|各|共)?([0-9{c_n}]+)?({q_c})?(\/({q_c}))?(\*|各|共)?([0-9{c_n}]+)?({q_c})?'.format(c_n=''.join(self.chn_numbers), q_c='|'.join(q_count), q_v='|'.join(q_volume), r_i_p = '|'.join(self.range_indicator_post))
                    self.pos[pos_id]['cmpl'] = re.compile(pattern, re.I)
                else:
                    self.pos[pos_id]['cmpl'] = re.compile(r'({pre})*'.format(pre='|'.join(self.pos[pos_id]['prefix'])) + self.pattern_units, re.I) if prefix else self.cmpl_units
                    if self.pos[pos_id]['q_pre']:
                        multiply_pattern = r"({pre})([0-9{c_n}]+)(?![0-9{c_n}]|{all_u})".format(pre='|'.join(map(re.escape, self.pos[pos_id]['q_pre'])), all_u='|'.join(self.all_units), c_n=''.join(self.chn_numbers))
                        self.pos[pos_id]['cmpl_m'] = re.compile(multiply_pattern, re.I)
            if self.pos[pos_id]['type'] not in [0,1,100,101,700,701,800,900,1000,1100,1101,1102] + q_type_list:
                self.error_msg.append('clean_pos表pos_id={pos_id}类型type不存在。'.format(pos_id=pos_id))
            else:
                if not self.pos[pos_id]['p_key'] and not self.pos[pos_id]['p_no'] and self.pos[pos_id]['type'] not in (0,1000,800,1100,1101,1102): # 空字段、品牌、价位和抖音模型不读属性字段
                    self.error_msg.append('clean_pos表pos_id={pos_id}缺少p_key或p_no。'.format(pos_id=pos_id))
                if self.pos[pos_id]['from_multi_sp'] and self.pos[pos_id]['type'] > 0:
                    self.error_msg.append('clean_pos表pos_id={pos_id}为拼接字段，type应置0。'.format(pos_id=pos_id))
                if self.pos[pos_id]['num_range'] and (self.pos[pos_id]['num_range'][0] > self.pos[pos_id]['num_range'][1] or self.pos[pos_id]['type'] not in q_type_list):
                    self.error_msg.append('clean_pos表pos_id={pos_id}限定数值范围num_range错误。'.format(pos_id=pos_id))
                if self.pos[pos_id]['ignore_word'] and (self.pos[pos_id]['type'] not in q_type_list):
                    self.error_msg.append('clean_pos表pos_id={pos_id}忽略词ignore_word在该type下无效。'.format(pos_id=pos_id))
                if self.pos[pos_id]['remove_word'] and (self.pos[pos_id]['type'] not in q_type_list):
                    self.error_msg.append('clean_pos表pos_id={pos_id}移除词remove_word在该type下无效。'.format(pos_id=pos_id))
                if self.pos[pos_id]['calculate_sum'] and self.pos[pos_id]['type'] not in q_type_list:
                    self.error_msg.append('clean_pos表pos_id={pos_id}的calculate_sum与type不匹配。'.format(pos_id=pos_id))
                if self.pos[pos_id]['read_from_right'] not in (0, 1, 2, 3):
                    self.error_msg.append('clean_pos表pos_id={pos_id}的read_from_right错误。'.format(pos_id=pos_id))
                elif self.pos[pos_id]['read_from_right'] > 0 and self.pos[pos_id]['type'] not in q_type_list:
                    self.error_msg.append('clean_pos表pos_id={pos_id}的read_from_right与type不匹配。'.format(pos_id=pos_id))
                elif self.pos[pos_id]['read_from_right'] > 0 and self.pos[pos_id]['calculate_sum']:
                    self.error_msg.append('clean_pos表pos_id={pos_id}的read_from_right与calculate_sum无法同时生效。'.format(pos_id=pos_id))
                if self.pos[pos_id]['target_no_rank'] not in (0, 1, 2):
                    self.error_msg.append('clean_pos表pos_id={pos_id}的target_no_rank错误。'.format(pos_id=pos_id))
                elif self.pos[pos_id]['target_no_rank'] > 0 and self.pos[pos_id]['type'] != 1:
                    self.error_msg.append('clean_pos表pos_id={pos_id}的target_no_rank与type不匹配。'.format(pos_id=pos_id))
                if self.pos[pos_id]['as_model']:
                    if self.pos_id_as_model:
                        self.error_msg.append('clean_pos表不允许存在多于一个的as_model。'.format(pos_id=pos_id))
                    else:
                        self.pos_id_as_model = pos_id
        # if not self.pos_id_category or not self.pos_id_brand:
        #     self.error_msg.append('clean_pos表的type不允许缺少900子品类和1000品牌。')

        sort_pos_id_list = []
        header = -1
        for pos_id in self.pos_id_list:
            if self.pos[pos_id]['type'] == 900:
                header = pos_id
            else:
                sort_pos_id_list.append(pos_id)
        sort_pos_id_list = [header] + sorted(sort_pos_id_list)
        self.pos_id_list = sort_pos_id_list

        data = self.db_cleaner.query_all('SELECT id, pos_id, base_min_value, base_max_value, base_unit, change_unit, unit_conversion, UNIX_TIMESTAMP(updateTime) FROM clean_multi_units WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)
        for row in data:
            m_id, pos_id, base_min_value, base_max_value, ori_base_unit, change_unit, unit_conversion, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            base_unit = ori_base_unit.upper()       # 大小写不敏感，标准化为全大写
            unit_conv = dict()
            tmp_uc_dict = eval(unit_conversion)
            u_c_err = False
            for k in tmp_uc_dict:
                t_k = k.split(',')
                new_k = t_k[0].upper()
                if unit_conv.get(new_k):
                    self.error_msg.append('clean_multi_units表id={id}单位“{q}”重复换算。'.format(id=m_id, q=new_k))
                unit_conv[new_k] = (tmp_uc_dict[k], t_k[1])    # key的,后为目标转换单位（大小写敏感，不能标准化为全大写）
                if type(tmp_uc_dict[k]) == list:
                    for i in tmp_uc_dict[k]:
                        if type(i) not in [int, float]:
                            u_c_err = True
                            break
                elif type(tmp_uc_dict[k]) not in [int, float]:
                    u_c_err = True
                if new_k != base_unit or t_k[1] != change_unit:     # fix for multi base units
                    u_c_err = True
            if len(unit_conv) != 1 or u_c_err:      # fix for multi base units
                self.error_msg.append('clean_multi_units表id={id}单位换算错误。'.format(id=m_id))
            self.pos[pos_id]['multi_units'][base_unit] = self.pos[pos_id]['multi_units'].get(base_unit, []) + [(base_min_value, base_max_value, change_unit, unit_conv)]
            if base_min_value != None and base_max_value != None and base_min_value > base_max_value:
                self.error_msg.append('clean_multi_units表id={id}数值范围错误。'.format(id=m_id))

        self.unify_classify_dict = dict()
        data = self.db_cleaner.query_all('SELECT id, base_pos_id, base_sp_value, base_sp_not_value, change_pos_id, change_sp_value, rank, UNIX_TIMESTAMP(updateTime) FROM clean_unify_result WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)
        for row in data:
            un_id, base_pos_id_s, base_sp_value_s, base_sp_not_value_s, change_pos_id_s, change_sp_value, rank, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            try:
                base_pos_id = [int(i) if i.isdigit() else i for i in base_pos_id_s.replace(' ','').split(',')]
                base_sp_value = eval(base_sp_value_s) if base_sp_value_s else dict()
                base_sp_not_value = eval(base_sp_not_value_s) if base_sp_not_value_s else dict()
                change_pos_id = int(change_pos_id_s) if change_pos_id_s.isdigit() else change_pos_id_s
            except:
                self.error_msg.append('clean_unify_result表id={id}格式错误。'.format(id=un_id))
            else:
                if not base_sp_value and not base_sp_not_value:
                    self.error_msg.append('clean_unify_result表id={id}的base_sp_value和base_sp_not_value不可都为空。'.format(id=un_id))
                elif type(base_sp_value) is not dict or type(base_sp_not_value) is not dict:
                    self.error_msg.append('clean_unify_result表id={id}的base_sp_value或base_sp_not_value格式有误。'.format(id=un_id))
                    base_sp_value, base_sp_not_value = dict(), dict()
                elif set(base_pos_id) != set(list(base_sp_value.keys()) + list(base_sp_not_value.keys())):
                    self.error_msg.append('clean_unify_result表id={id}的base_pos_id不一致。'.format(id=un_id))
                else:
                    for i in base_pos_id:
                        if i not in list(self.pos_id_list) + ['all_bid', 'alias_all_bid', 'source', 'sid', 'cid']:
                            self.error_msg.append('clean_unify_result表id={id}的base字段不在规定范围内。'.format(id=un_id))
                            break
                    if change_pos_id == 'all_bid_sp':
                        change_sp_value = int(change_sp_value)
                        if self.search_all_brand(change_sp_value):    # for cache
                            alias_bid = self.search_all_brand(change_sp_value)[-1]
                            if alias_bid > 0:
                                self.search_all_brand(alias_bid)    # for cache
                                self.error_msg.append('clean_unify_result表id={id}的all_bid_sp字段{b}为{a}的子品牌。'.format(id=un_id, b=change_sp_value, a=alias_bid))
                        else:
                            self.error_msg.append('clean_unify_result表id={id}的change值不在all_brand表的bid范围内。'.format(id=un_id))
                    elif change_pos_id not in list(self.pos_id_list):
                        self.error_msg.append('clean_unify_result表id={id}的change字段不在规定范围内。'.format(id=un_id))
                if rank in self.unify_classify_dict:
                    self.error_msg.append('clean_unify_result表或clean_classify_result表里rank={rank}冲突。'.format(rank=rank))
                else:
                    base_dict = dict()
                    for k in base_sp_value:
                        base_dict[k] = tuple(base_sp_value[k].split(','))       # 大小写敏感，和target保持一致
                        if k == 'alias_all_bid':
                            for bid in base_dict[k]:
                                if self.search_all_brand(bid):    # for cache
                                    alias_bid = self.search_all_brand(bid)[-1]
                                    if alias_bid > 0:
                                        self.search_all_brand(alias_bid)    # for cache
                                        self.error_msg.append('clean_unify_result表id={id}的base_sp_value字段里alias_all_bid里的{b}为{a}的子品牌。'.format(id=un_id, b=bid, a=alias_bid))
                                else:
                                    self.error_msg.append('clean_unify_result表id={id}的base_sp_value字段里alias_all_bid里的{b}不在all_brand表的bid范围内。'.format(id=un_id))
                    base_not_dict = dict()
                    for k in base_sp_not_value:
                        base_not_dict[k] = tuple(base_sp_not_value[k].split(','))       # 大小写敏感，和target保持一致
                        if k == 'alias_all_bid':
                            for bid in base_not_dict[k]:
                                if self.search_all_brand(bid):    # for cache
                                    alias_bid = self.search_all_brand(bid)[-1]
                                    if alias_bid > 0:
                                        self.search_all_brand(alias_bid)    # for cache
                                        self.error_msg.append('clean_unify_result表id={id}的base_sp_not_value字段里alias_all_bid里的{b}为{a}的子品牌。'.format(id=un_id, b=bid, a=alias_bid))
                                else:
                                    self.error_msg.append('clean_unify_result表id={id}的base_sp_not_value字段里alias_all_bid里的{b}不在all_brand表的bid范围内。'.format(id=un_id))
                    self.unify_classify_dict[rank] = ('unify', change_pos_id, change_sp_value, base_dict, base_not_dict)

        data = self.db_cleaner.query_all('SELECT id, base_pos_id, base_min_value, base_max_value, base_unit, change_pos_id, change_value, rank, UNIX_TIMESTAMP(updateTime) FROM clean_classify_result WHERE batch_id = {batch_id} AND deleteFlag = 0;'.format(batch_id=self.batch_id))
        self.config_total_count += len(data)
        for row in data:
            cl_id, base_pos_id, base_min_value, base_max_value, ori_base_unit, change_pos_id, change_value, rank, u_tmp = row
            self.config_max_update_time = max(self.config_max_update_time, u_tmp)
            if base_min_value != None and base_max_value != None and base_min_value > base_max_value:
                self.error_msg.append('clean_classify_result表id={id}数值范围错误。'.format(id=cl_id))
            if rank in self.unify_classify_dict:
                self.error_msg.append('clean_unify_result表或clean_classify_result表里rank={rank}冲突。'.format(rank=rank))
            else:
                base_unit = ori_base_unit.upper().strip()       # 大小写不敏感，标准化为全大写
                self.unify_classify_dict[rank] = ('classify', change_pos_id, change_value, base_pos_id, base_unit, base_min_value, base_max_value)

        if self.clean_sku:
            try:
                cols = ['spid{}'.format(pos) for pos in self.pos]+['alias_all_bid']
                # 豆子要剔除的
                cols = [c for c in cols if not (self.batch_id==204 and c=='spid10')]
                ppid = [pos for pos in self.pos if self.pos[pos]['output_type']==1][0]
                sql = 'SELECT spid{}, name, {} FROM product_lib.product_{}'.format(ppid, ','.join(cols), self.eid)
                ret = self.db_artificial_new.query_all(sql)
                self.clean_sku = {vv[0] or vv[1]:{col:str(vv[ii+2]) for ii, col in enumerate(cols)} for vv in ret}
                self.clean_sku_pos = ppid
            except:
                self.clean_sku = {}
                self.clean_sku_pos = None
        else:
            self.clean_sku = {}
            self.clean_sku_pos = None

        #self.item_p_map_name = {1:'交易属性'}
        #data = self.db_cleaner.query_all('SELECT pos, prop_name FROM dataway.entity_props_name WHERE eid = {eid} and tip = 1;'.format(eid=self.eid))
        #for row in data:
        #   self.item_p_map_name[row[0]] = row[1]

        self.set_batch_status()
        assert not self.error_msg, ''.join(self.error_msg)

    def get_config(self):
        self.tid_map_name = dict()
        self.target_dict = dict()
        tid_map_s_p = dict()
        sql = '''
            SELECT sub_batch_id, pos_id, target_id, name, mark, rank, UNIX_TIMESTAMP(updateTime)
            FROM clean_target WHERE batch_id = {batch_id} AND deleteFlag = 0 AND sub_batch_id >= 0
        '''.format(batch_id=self.batch_id)
        d1 = self.db_cleaner.query_all(sql)
        sql = '''
            SELECT b.sub_batch_id, a.pos_id, a.target_id, a.name, a.mark, a.rank, UNIX_TIMESTAMP(a.updateTime)
            FROM clean_target a JOIN clean_sub_batch b USING (batch_id)
            WHERE batch_id = {batch_id} AND a.deleteFlag = 0 AND b.deleteFlag = 0 AND a.sub_batch_id < 0
        '''.format(batch_id=self.batch_id)
        d2 = self.db_cleaner.query_all(sql)
        sql = '''
            SELECT 0, pos_id, target_id, name, mark, rank, UNIX_TIMESTAMP(updateTime)
            FROM clean_target WHERE batch_id = {batch_id} AND deleteFlag = 0 AND sub_batch_id = -2
        '''.format(batch_id=self.batch_id)
        d3 = self.db_cleaner.query_all(sql)
        data = list(d1)+list(d2)+list(d3)
        self.config_total_count += len(data)
        for row in data:
            sub_batch_ids, pos_id, target_id, name, mark, rank, u_tmp = row
            if str(sub_batch_ids).find(',') > -1:
                sub_batch_ids = sub_batch_ids.split(',')
            else:
                sub_batch_ids = [sub_batch_ids]

            for sub_batch_id in sub_batch_ids:
                sub_batch_id = int(sub_batch_id)
                self.config_max_update_time = max(self.config_max_update_time, u_tmp)
                if pos_id not in self.pos_id_list or self.pos[pos_id]['type'] not in (1,800) or sub_batch_id not in list(self.sub_batch.keys()) + [0]: # 只读取有限选项和价位的target
                    # self.error_msg.append('clean_target表target_id={tids}所属pos_id或sub_batch_id填写错误。'.format(tids=target_id))
                    continue
                self.target_dict[sub_batch_id, pos_id] = self.target_dict.get((sub_batch_id, pos_id), dict())
                self.target_dict[sub_batch_id, pos_id][target_id] = {'name': name, 'mark': mark, 'rank': rank, 'min_price': None, 'max_price': None, 'keywords': []}
                self.tid_map_name[target_id] = name
                tid_map_s_p[target_id] = tid_map_s_p.get(target_id, [])
                tid_map_s_p[target_id].append((sub_batch_id, pos_id))
        target_id_list_s = ', '.join([str(i) for i in self.tid_map_name.keys()])


        if target_id_list_s:
            sql = 'SELECT name, regular, and_name, not_name, ignore_name, target_id, UNIX_TIMESTAMP(updateTime) FROM clean_keyword WHERE target_id IN ({tid_lists}) AND deleteFlag = 0;'.format(tid_lists=target_id_list_s)
            data = self.db_cleaner.query_all(sql)
            self.config_total_count += len(data)
            for row in data:
                name, regular, and_name, not_name, ignore_name, target_id, u_tmp = row
                if regular:
                    regular = re.compile(regular, re.IGNORECASE)
                self.config_max_update_time = max(self.config_max_update_time, u_tmp)
                for v in tid_map_s_p[target_id]:
                    if self.pos[v[1]]['target_no_rank'] > 0 and (and_name or not_name or ignore_name):
                        self.error_msg.append('clean_keyword表target_id={tid}因所属pos的target_no_rank不得填写and_name、not_name和ignore_name。'.format(tid=target_id))
                    self.target_dict[v][target_id]['keywords'].append({
                        'regular': regular, # 正则
                        'and_name': [name.upper()] + and_name.upper().split(',') if and_name else [name.upper()],       # 大小写不敏感，标准化为全大写
                        'not_name': not_name.upper().split(',') if not_name else [],                                    # 大小写不敏感，标准化为全大写
                        'ignore_name': sorted(ignore_name.upper().split(','), key = lambda x:(len(x),x), reverse = True) if ignore_name else []
                    })    # 大小写不敏感，标准化为全大写

            sql = 'SELECT min_amount, max_amount, target_id, UNIX_TIMESTAMP(updateTime) FROM clean_price WHERE target_id IN ({tid_lists}) AND deleteFlag = 0;'.format(tid_lists=target_id_list_s)
            data = self.db_cleaner.query_all(sql)
            self.config_total_count += len(data)
            for row in data:
                min_amount, max_amount, target_id, u_tmp = row
                self.config_max_update_time = max(self.config_max_update_time, u_tmp)
                for v in tid_map_s_p[target_id]:
                    self.target_dict[v][target_id]['min_price'] = min_amount if min_amount else None
                    self.target_dict[v][target_id]['max_price'] = max_amount if max_amount else None

            for key in self.target_dict:
                rank_s_p = dict()
                mark_s_p = dict()
                keywords_no_rank = []
                for target_id in self.target_dict[key]:
                    target = self.target_dict[key][target_id]
                    rank_s_p[target['rank']] = rank_s_p.get(target['rank'], []) + [target_id]
                    if target['mark'] < 0:
                        mark_s_p[target['mark']] = mark_s_p.get(target['mark'], []) + [target_id]
                    elif not target['keywords'] and target['min_price'] == None and target['max_price'] == None:
                        self.error_msg.append('clean_keyword表target_id={tid}缺少关键词或价位。'.format(tid=target_id))
                    if target['min_price'] != None and target['max_price'] != None and target['min_price'] > target['max_price']:
                        self.error_msg.append('clean_price表target_id={tid}价格范围错误。'.format(tid=target_id))
                    if self.pos[key[1]]['target_no_rank'] > 0:
                        for d in target['keywords']:
                            keywords_no_rank.append(d['and_name'][0])
                include_keywords = set()
                keywords_no_rank = sorted(keywords_no_rank, key = lambda x:len(x))
                for j in range(len(keywords_no_rank)):
                    for i in range(j):
                        if self.pos[key[1]]['target_no_rank'] == 1 and keywords_no_rank[j].startswith(keywords_no_rank[i]) or self.pos[key[1]]['target_no_rank'] == 2 and keywords_no_rank[j].endswith(keywords_no_rank[i]):
                            include_keywords.add(keywords_no_rank[i])
                            include_keywords.add(keywords_no_rank[j])
                if include_keywords:
                    self.error_msg.append('clean_target表target_id={tids}因所属pos的target_no_rank，其下属的keyword（{kws}）之间不得出现包含关系。'.format(kws='、'.join(include_keywords), tids=','.join([str(k) for k in self.target_dict[key].keys()])))
                for r in rank_s_p:
                    if len(rank_s_p[r]) > 1:
                        self.error_msg.append('clean_target表target_id={tids}优先级rank冲突。'.format(tids=','.join(map(str, rank_s_p[r]))))
                for m in mark_s_p:
                    if len(mark_s_p[m]) > 1:
                        self.error_msg.append('clean_target表target_id={tids}类别标记mark冲突。'.format(tids=','.join(map(str, mark_s_p[m]))))
                    if self.pos[key[1]]['target_no_rank'] > 0 and m not in [-1,0]:
                        self.error_msg.append('clean_target表target_id={tids}类别标记mark与所属pos的target_no_rank冲突。'.format(tids=','.join(map(str, mark_s_p[m]))))

        self.set_batch_status()
        assert not self.error_msg, ''.join(self.error_msg)

        self.config_record_compare()
        self.calculate_sp_plugin = PluginManager.getPlugin('calculate_sp{batch_id}'.format(batch_id=self.batch_id), defaultPlugin='calculate_sp')
        self.calculate_sp_plugin.batch_now = self
        self.calculate_sp_plugin.init_read_require()

    def config_record_compare(self):
        # 配置记录功能由于记录配置过长，爆内存，因此删掉
        self.clean_ver = 1
        return None
        sql = 'SELECT version_id, last_update, last_count, comments FROM clean_config_version WHERE batch_id = {batch_id} AND deleteFlag = 0 ORDER BY version_id DESC LIMIT 1;'.format(batch_id=self.batch_id)
        tup = self.db_cleaner.query_one(sql)
        if tup:
            last_version_id, last_update, last_count, last_comments = tup
            if self.config_max_update_time == last_update and self.config_total_count == last_count and self.comments in [None, last_comments]:
                self.clean_ver = last_version_id
            else:
                self.clean_ver = last_version_id + 1
                sql = 'INSERT INTO clean_config_version(batch_id, version_id, last_update, last_count, obj_cfg, comments, createTime) VALUES (%s, %s, %s, %s, %s, %s, UNIX_TIMESTAMP());'
                self.db_cleaner.execute(sql, (self.batch_id, self.clean_ver, self.config_max_update_time, self.config_total_count, str(self.__dict__), self.comments))
                self.db_cleaner.commit()
        else:
            self.clean_ver = 1
            sql = 'INSERT INTO clean_config_version(batch_id, version_id, last_update, last_count, obj_cfg, comments, createTime) VALUES (%s, %s, %s, %s, %s, %s, UNIX_TIMESTAMP());'
            self.db_cleaner.execute(sql, (self.batch_id, self.clean_ver, self.config_max_update_time, self.config_total_count, str(self.__dict__), self.comments))
            self.db_cleaner.commit()

    def set_batch_status(self, last_id = None, last_pid = None, status = None):
        sql = 'UPDATE clean_batch SET '
        if last_id != None:
            self.last_id = last_id
            sql += 'last_id = {last_id}, '.format(last_id=last_id)
        if last_pid != None:
            self.last_pid = last_pid
            sql += 'last_pid = {last_pid}, '.format(last_pid=last_pid)
        if status != None:
            sql += "status = '{status}', ".format(status=status)
        if self.error_msg:
            sql += "error_msg = %s WHERE batch_id = {batch_id};".format(batch_id=self.batch_id)
            print(sql)
            self.db_cleaner.execute(sql, (''.join(self.error_msg),))
        else:
            sql += "error_msg = NULL WHERE batch_id = {batch_id};".format(batch_id=self.batch_id)
            print(sql)
            self.db_cleaner.execute(sql)
        self.db_cleaner.commit()

    def table_alter_status(self, table_name):
        try:
            sql = 'SELECT status FROM clean_alter_status WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
            status = self.db_cleaner.query_scalar(sql, (self.batch_id, table_name))
        except:
            sql = 'INSERT INTO clean_alter_status(batch_id, table_name, status, createTime) VALUES (%s, %s, 0, UNIX_TIMESTAMP());'
            self.db_cleaner.execute(sql, (self.batch_id, table_name))
            self.db_cleaner.commit()
            status = 0

        if status != 0:
            pl = self.db_artificial_new.query_all('SHOW PROCESSLIST;') # Id, User, Host, db, Command, Time, State, Info, Rows_sent, Rows_examined
            for r in pl:
                if r[7] and table_name.upper() in r[7].upper():
                    break
            else:
                csql = 'UPDATE clean_alter_status SET status = 0 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                self.db_cleaner.execute(csql, (self.batch_id, table_name))
                self.db_cleaner.commit()
                status = 0

        table_fields = []
        if status == 0:
            try:
                data = self.db_artificial_new.query_all('DESCRIBE ' + table_name)
            except:
                status = -1
            else:
                table_fields = [i[0] for i in data]

        return status, table_fields

    def check_lack_fields(self):
        pos_id_list = sorted(self.pos_id_list)
        item_fields = []

        if self.is_item_table_mysql:
            if self.pos_id_as_model:
                while self.table_alter_status(self.model_table_name)[0] > 0:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '正在等待其它进程ALTER释放表锁：', self.model_table_name)
                    time.sleep(60)
                else:
                    status, model_fields = self.table_alter_status(self.model_table_name)
                    if status == 0:
                        print('model_fields:', model_fields)
                        sql = ''
                        for i in pos_id_list:
                            if 'sp' + str(i) not in model_fields:
                                sql += "\nADD COLUMN sp{i} VARCHAR(200) NOT NULL DEFAULT '' AFTER sp{ii}, ".format(i=i, ii=i-1)
                        if sql:
                            csql = 'UPDATE clean_alter_status SET status = 1 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                            self.db_cleaner.execute(csql, (self.batch_id, self.model_table_name))
                            self.db_cleaner.commit()
                            add_sql = 'ALTER TABLE {model_table} '.format(model_table=self.model_table_name) + sql[:-2] + ';'
                            print(add_sql)
                            self.db_artificial_new.execute(add_sql)
                            csql = 'UPDATE clean_alter_status SET status = 0 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                            self.db_cleaner.execute(csql, (self.batch_id, self.model_table_name))
                            self.db_cleaner.commit()
                    else:
                        print(self.model_table_name, '表不存在。')

            while self.table_alter_status(self.item_table_name)[0] > 0:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '正在等待其它进程ALTER释放表锁：', self.item_table_name)
                time.sleep(60)
            else:
                status, item_fields = self.table_alter_status(self.item_table_name)
                assert status >= 0, '该batch缺少item表，请先导数。'
                print('item_fields:', item_fields)
            sql = ''
            if 'clean_ver' not in item_fields:
                sql += '\nADD COLUMN clean_ver MEDIUMINT UNSIGNED NOT NULL DEFAULT 0 AFTER pid, '
            if 'clean_type' not in item_fields:
                sql += '\nADD COLUMN clean_type SMALLINT NOT NULL DEFAULT 0 AFTER clean_ver, '
            if 'all_bid_sp' not in item_fields:
                sql += '\nADD COLUMN all_bid_sp INT(11) DEFAULT NULL AFTER clean_type, '
            if 'alias_all_bid_sp' not in item_fields:
                sql += '\nADD COLUMN alias_all_bid_sp INT(11) DEFAULT NULL AFTER all_bid_sp, '
            for i in pos_id_list:
                if 'mp' + str(i) not in item_fields:
                    sql += '\nADD COLUMN mp{i} TEXT NOT NULL AFTER {f}, '.format(i=i, f='alias_all_bid_sp' if i == 1 else 'sp' + str(i-1))
                if 'sp' + str(i) not in item_fields:
                    sql += '\nADD COLUMN sp{i} VARCHAR(200) NOT NULL AFTER mp{i}, '.format(i=i)
            if sql:
                csql = 'UPDATE clean_alter_status SET status = 1 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                self.db_cleaner.execute(csql, (self.batch_id, self.item_table_name))
                self.db_cleaner.commit()
                add_sql = 'ALTER TABLE {item_table} '.format(item_table=self.item_table_name) + sql[:-2] + ';'
                print(add_sql)
                self.db_artificial_new.execute(add_sql)
                sql = 'DROP TABLE IF EXISTS {item_table}_modify_tmp;'.format(item_table=self.item_table_name)       # he.duixue: item表新增字段时，需删除修改的临时表
                print(sql)
                self.db_artificial_new.execute(sql)
                csql = 'UPDATE clean_alter_status SET status = 0 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                self.db_cleaner.execute(csql, (self.batch_id, self.item_table_name))
                self.db_cleaner.commit()
                self.set_batch_status(status = '已增加待清洗属性字段', last_id = 0, last_pid = 0)

            try:
                csql = 'UPDATE clean_alter_status SET status = 1 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                self.db_cleaner.execute(csql, (self.batch_id, self.item_table_name))
                self.db_cleaner.commit()
                sql = 'ALTER TABLE {item_table} ADD INDEX `cver_id` (`clean_ver`, `id`);'.format(item_table=self.item_table_name)
                print(sql)
                self.db_artificial_new.execute(sql)
            except:
                print('索引已存在。')
            finally:
                csql = 'UPDATE clean_alter_status SET status = 0 WHERE batch_id = %s AND table_name = %s AND deleteFlag = 0;'
                self.db_cleaner.execute(csql, (self.batch_id, self.item_table_name))
                self.db_cleaner.commit()

        #fixed_fields = ['id', 'tb_item_id', 'source', 'month', 'name', 'sid', 'shop_name', 'shop_type', 'cid', 'real_cid', 'region_str', 'all_bid', 'alias_all_bid', 'sub_brand_name', 'product', 'prop_all', 'trade_prop_all', 'p1', 'avg_price', 'trade', 'num', 'sales', 'visible', 'visible_check', 'clean_flag', 'prop_check', 'clean_type', 'all_bid_sp', 'alias_all_bid_sp', 'pid', 'img', 'created', 'modified']
        p_no_list = [x for x in item_fields if x[0] == 'p' and x[1:].isdigit()] if item_fields else ['p1', 'p2']
        all_used_fields = ['id', 'snum', 'tb_item_id', 'source', 'month', 'name', 'sid', 'shop_name', 'shopkeeper', 'shop_type', 'shop_type_ch', 'cid', 'real_cid', 'region_str', 'brand', 'all_bid', 'alias_all_bid', 'sub_brand_name', 'product', 'prop_all', 'trade_prop_all'] + p_no_list + ['avg_price', 'price', 'trade', 'num', 'sales', 'visible', 'visible_check', 'clean_flag', 'prop_check', 'clean_type', 'clean_ver', 'all_bid_sp', 'alias_all_bid_sp']
        fields_to_read = ['id', 'snum', 'month', 'source', 'cid', 'name', 'product', 'brand', 'all_bid', 'alias_all_bid', 'sub_brand_name', 'avg_price', 'price', 'num', 'region_str', 'prop_all', 'trade_prop_all', 'tb_item_id', 'sid', 'shop_name', 'shopkeeper', 'shop_type', 'shop_type_ch']
        fields_to_write = []
        for i in pos_id_list:
            if self.pos[i]['type'] >= 0:
                fields_to_write.extend(['mp'+str(i), 'sp'+str(i)])
            all_used_fields.extend(['mp'+str(i), 'sp'+str(i)])
        all_used_fields += ['pid', 'img', 'created', 'modified']
        for fi in all_used_fields:
            if item_fields and fi not in item_fields:
                all_used_fields.remove(fi)
                if fi in fields_to_read:
                    fields_to_read.remove(fi)
        self.item_fields_read = tuple(fields_to_read + p_no_list)
        self.item_fields_write = tuple(fields_to_write)
        self.item_fields_use = tuple(all_used_fields)

    def set_last_id(self):
        sql = 'SELECT MIN(id) FROM {item_table} WHERE clean_ver = 0;'.format(item_table=self.item_table_name)
        s_id = self.db_artificial_new.query_scalar(sql)
        if s_id == None:
            s_id = self.db_artificial_new.query_scalar('SELECT MAX(id) FROM {item_table};'.format(item_table=self.item_table_name)) + 1
        self.last_id = s_id - 1
        sql = 'UPDATE clean_batch SET last_id = {last_id} WHERE batch_id = {batch_id};'.format(batch_id=self.batch_id, last_id=self.last_id)
        print(sql)
        self.db_cleaner.execute(sql)
        self.db_cleaner.commit()

    def set_monthly_start_id(self, month = ''):
        # print(month)
        assert self.last_id > 0, '该项目现有的last_id为0，请机洗全量！'
        if not month:
            month = self.db_artificial_new.query_scalar('SELECT MIN(month) FROM {item_table} WHERE id > {last_id};'.format(item_table=self.item_table_name, last_id=self.last_id))
            # print(month)
        if not month:
            curr_time = time.localtime()
            # print(curr_time.tm_year, curr_time.tm_mon, curr_time.tm_mday)
            trans_year = curr_time.tm_year
            trans_month = curr_time.tm_mon - 1 if curr_time.tm_mday < 11 else curr_time.tm_mon
            if trans_month < 1:
                trans_month = 12
                trans_year -= 1
            month = '{y}-0{m}-01'.format(y=trans_year, m=trans_month) if trans_month < 10 else '{y}-{m}-01'.format(y=trans_year, m=trans_month)
            # print(month)
        month = str(month)
        # print(month, len(month), month.endswith('-01'))
        assert len(month) == 10 and month.endswith('-01'), '日期格式为2020-09-01，请重新输入！'
        sql = "SELECT MIN(id) FROM {item_table} WHERE month >= '{month}';".format(item_table=self.item_table_name, month=month)
        print(sql)
        s_id = self.db_artificial_new.query_scalar(sql)
        assert s_id, 'item表里不存在本期月报增量数据或month大于等于输入日期的数据，请修改last_id或重新输入month！'
        if s_id <= self.last_id:
            self.last_id = s_id - 1
            sql = 'UPDATE clean_batch SET last_id = {last_id} WHERE batch_id = {batch_id};'.format(batch_id=self.batch_id, last_id=self.last_id)
            print(sql)
            self.db_cleaner.execute(sql)
            self.db_cleaner.commit()

    def match_filter(self, setting_filter, item_filter):
        for key, value in item_filter.items():
            if value not in setting_filter.get(key, [value]):
                return False
        return True

    def source_cid_map_sub_batch(self, source, data_filter, judge_list):
        for raw_name in judge_list:
            if not raw_name:
                continue
            name_to_judge = common.to_halfwidth(raw_name, case_mode = 'upper')    # 大小写不敏感，标准化为全大写
            self.print_log('***一个判断循环的开始***')
            self.print_log('判断词中小写字母全都转为大写字母：\n【{r}】--->【{n}】'.format(r=raw_name, n=name_to_judge))
            if name_to_judge:
                tup_list = self.sc_category[source].get(data_filter['cid'], [])
                # 如果没找到对应的品牌，有可能是因为没有配cid，也有可能是就是没有
                if len(tup_list) == 0:
                    tup_list = self.sc_category[source].get(-1, [])
                for tup in tup_list:     # 列表已按优先级从高到低排序
                    filters, sub_batch_id, and_words, or_words, not_words, ignore_words, rank = tup
                    if not self.match_filter(filters, data_filter):
                        continue # 判断filter是否符合，不符合就continue，（如果都不符合则进其它————兜底刷默认功能做在页面）
                    obj = self.sub_batch[sub_batch_id]['name'] if self.sub_batch.get(sub_batch_id) else self.default_other
                    self.print_log('- 找到的结果object为: ', obj)
                    name = name_to_judge
                    for word in ignore_words:
                        name = name.replace(word, self.separator)
                    for word in and_words:
                        if word not in name:
                            self.print_log('and_words 不在这个词: ', word, ' 中,所以被淘汰')
                            break
                    else:
                        for word in not_words:
                            if word in name:
                                self.print_log('not_words 在这个词: ', word, ' 中,所以被淘汰')
                                break
                        else:
                            if or_words:
                                for word in or_words:
                                    if word in name:
                                        break
                                else:
                                    self.print_log('or_words 全都不在这个词: ', or_words, ' 中,所以被淘汰')
                                    continue
                            self.print_log('最终找到的词为:', name)
                            self.print_log('ignore_words为:', ignore_words)
                            self.print_log('and_words为:', and_words)
                            self.print_log('or_words为:', or_words)
                            self.print_log('not_words为:', not_words)
                            return sub_batch_id, obj
        # assert matched, 'clean_category表里缺少条件为{filters}数据的对应配置，请检查底层数据错误或机洗配置缺漏。'.format(filters=(source, data_filter))
        return 0, self.default_other

    def get_result_value(self, target_dict_s_p, value_to_judge, no_rank_read_from):
        other_id = None
        mix_id = None
        possible_results = set()
        no_rank_seq = []
        name_to_judge = common.to_halfwidth(value_to_judge, case_mode = 'upper')    # 大小写不敏感，标准化为全大写
        self.print_log('判断词中小写字母全都转为大写字母：【{r}】--->【{n}】'.format(r=value_to_judge, n=name_to_judge))
        for target_id in target_dict_s_p:
            final_target_id = target_id
            if target_dict_s_p[target_id]['mark'] == -1:
                other_id = target_id
            elif target_dict_s_p[target_id]['mark'] == -2:
                mix_id = target_id
            elif target_dict_s_p[target_id]['mark'] > 0:
                final_target_id = target_dict_s_p[target_id]['mark']
            if name_to_judge:       # 即使输入为空，也要算出other和mix
                best_loc = None
                for name_d in target_dict_s_p[target_id]['keywords']:
                    if name_d['regular'] and not name_d['regular'].search(name_to_judge):
                        break
                    if no_rank_read_from == 0:
                        value_name = name_to_judge
                        for word in name_d['ignore_name']:
                            value_name = value_name.replace(word, self.separator)
                        for word in name_d['and_name']:
                            if word not in value_name:
                                break
                        else:
                            for word in name_d['not_name']:
                                if word in value_name:
                                    break
                            else:
                                self.print_log('由于满足一下规则:', str(name_d), '因此将', str(final_target_id), '加入匹配词id列表中')
                                possible_results.add(final_target_id)
                                break
                    elif no_rank_read_from == 1:
                        loc = name_to_judge.find(name_d['and_name'][0])
                        if loc >= 0 and (best_loc == None or best_loc > loc):
                            best_loc = loc
                    else:
                        loc = name_to_judge.rfind(name_d['and_name'][0])
                        if loc >= 0 and (best_loc == None or best_loc < loc):
                            best_loc = loc
                if best_loc != None:
                    no_rank_seq.append((final_target_id, best_loc))

        if no_rank_seq:
            no_rank_seq = sorted(no_rank_seq, key = lambda x:x[1], reverse = True if no_rank_read_from == 2 else False)
            possible_results.add(no_rank_seq[0][0])
        self.print_log('匹配出的对应词id为列表为:', possible_results)
        self.print_log('no_rank_seq:', no_rank_seq)

        mp_list = [self.tid_map_name[tid] for tid in possible_results]
        self.print_log('将id转换过后的匹配出来的词列表为:', mp_list)
        other = self.tid_map_name.get(other_id) if other_id else None
        mix = self.tid_map_name.get(mix_id) if mix_id else None
        self.print_log('mp, other, mix:', mp_list, other, mix)
        length = len(possible_results)
        if (length == 0):       # 匹配不出结果
            result = ''
        elif (length == 1):     # 匹配出一个结果
            result = mp_list[0]      # 判定为这个结果
        else:       # 匹配出两个及以上结果
            lis = [(i, target_dict_s_p[i]['rank'], target_dict_s_p[i]['name']) for i in possible_results]
            sorted_list = sorted(lis, key = lambda x:x[1], reverse = True)      # 优先级降序
            if mix_id:      # 该属性组存在混合类
                if (length == 2) and (mix_id in possible_results):      # 匹配到了混合和另一个类，直接判定为另一个类
                    result = sorted_list[0][2] if (sorted_list[1][0] == mix_id) else sorted_list[1][2]
                else:
                    result_id = sorted_list[0][0]       # 选优先级最高的结果
                    if target_dict_s_p[result_id]['rank'] < target_dict_s_p[mix_id]['rank']:
                        result_id = mix_id          # 所有结果的优先级低于混合则判定为混合
                    result = self.tid_map_name[result_id]
            else:       # 该属性组无混合类
                result = sorted_list[0][2]      # 选优先级最高的结果
        return mp_list, result, length, other

    def get_prop_value(self, target_dict_s_p, judge_list, no_rank_read_from):
        self.print_log('判断列表Judge list:\n', judge_list)
        if (not target_dict_s_p) or (not judge_list):
            return '', '', 0

        all_names_null = True
        for name in judge_list:
            if name:
                all_names_null = False
                mp_list, result, length, other = self.get_result_value(target_dict_s_p, name, no_rank_read_from)
                if result:
                    break
        if all_names_null:
            mp_list, result, length, other = self.get_result_value(target_dict_s_p, '', no_rank_read_from)

        result = other if (not result) and other else result
        mp = ' & '.join(mp_list) if mp_list else ''
        return mp, result, length

    def price_limit(self, price, t_dict):
        for target_id in t_dict:
            if t_dict[target_id]['min_price'] and price < t_dict[target_id]['min_price']:
                continue
            if t_dict[target_id]['max_price'] and price > t_dict[target_id]['max_price']:
                continue
            return price, self.tid_map_name[target_id]
        return price, self.default_other

    # def get_brand_new_cache(self, lv1cid, *, brandId=None, bid=0, confirmed=1): # zhou.wenjun 2019-01-30
    #     key = str(lv1cid)+'-'+str(bid)
    #     if brandId != None:
    #         key = 'brandId:'+str(bid)

    #     if key not in self.brand_cache:
    #         find = "a.bid="+str(bid)
    #         if brandId != None:
    #             find = "a.brandId="+str(brandId)

    #         sql = "select a.brandId, a.bid, a.alias_brandId, a.cid, a.name, a.confirmed, b.brandId sub_brand, d.name sub_brand_keyword \
    #                 from cleaner.brandNew a \
    #                 left join cleaner.brand_parent b on (a.brandId=b.parent) \
    #                 left join cleaner.brandKeywordNew c on (b.brandId=c.brandId) \
    #                 left join cleaner.keyword d on (c.keywordId=d.keywordId) \
    #                 where {find} and (c.deleteFlag=0 or c.deleteFlag is NULL)".format(find=find)
    #         data = self.db_cleaner.query_all(sql)

    #         cids = []
    #         ret = []
    #         for r in data:
    #             cids.append(str(r[3]))
    #         if len(cids) > 0:
    #             cids = ','.join(cids)
    #             sql = "select lv1cid,lv2cid,lv1name,cid from apollo.item_category_backend_all where cid in ({cids})".format(cids=cids)
    #             clist = self.db_1_apollo.query_all(sql)
    #             cmap = {}
    #             for r in clist:
    #                 cmap[str(r[3])] = list(r)

    #             for r in data:
    #                 if confirmed != None and r[5] != confirmed:
    #                     continue

    #                 # if True: # test
    #                 if cmap[str(r[3])][0] == lv1cid:
    #                     tmp = list(r)
    #                     tmp.extend(cmap[str(r[3])])
    #                     ret.append(tmp)

    #         self.brand_cache[key] = ret

    #     return self.brand_cache[key]

    def search_all_brand(self, all_bid):
        return self.brand_obj.search(all_bid)

    def alias_brand(self, judgelist):
        name = judgelist[0]
        all_bid = judgelist[1]
        alias_all_bid = judgelist[2]
        cid = judgelist[3]

        brand_name = ''
        alias_brand_name = ''
        brand_cnen_name_tuple = ()
        alias_brand_cnen_name_tuple = ()
        if all_bid > 0:
            brand_cnen_name_tuple = self.search_all_brand(all_bid)[:-1]                     # for filter model
            brand_name = brand_cnen_name_tuple[0]
        if alias_all_bid > 0:
            alias_brand_cnen_name_tuple = self.search_all_brand(alias_all_bid)[:-1]         # for filter model
            alias_brand_name = alias_brand_cnen_name_tuple[0]

        return brand_name, alias_brand_name, brand_cnen_name_tuple, alias_brand_cnen_name_tuple

        # data = self.get_brand_new_cache(cid, bid=all_bid)       # zhou.wenjun 2019-01-30

        # # 没有能用的
        # if len(data) == 0:
        #     return 0, alias_all_bid

        # # alias_all_bid还不是topbid
        # alias_brandId = data[0][2]
        # if alias_brandId > 0:
        #     data = self.get_brand_new_cache(cid, brandId=alias_brandId)

        # # 没有能用的
        # if len(data) == 0:
        #     return 0, brand_name

        # brandId = data[0][0]
        # bid = data[0][1]
        # brand_name = data[0][4]
        # alias_all_bid = data[0][1]

        # # list sub brand
        # sub_brand = bid
        # sub_brand_name = brand_name
        # for r in data:
        #     if not r[6] or r[0] == r[6]:
        #         continue

        #     if name.find(r[7]) != -1:
        #         sub_brand = r[6]
        #         sub_brand_name = r[7]
        #         d = self.get_brand_new_cache(cid, brandId=sub_brand)
        #         if len(d) > 0:
        #             sub_brand = data[0][1]
        #             sub_brand_name = data[0][4]
        #             break

        # return (str(sub_brand)+'-'+sub_brand_name).replace('\'','\\\''), brand_name.replace('\'','\\\'')

    def standardize_for_model(self, s):     # （可改为类方法）
        if s:
            s = unescape(s)
            s = common.to_halfwidth(s, case_mode = 'upper', extra_map = self.model_need_trans)
            s = common.remove_spilth(s, erase_duplication = self.model_no_duplicate)
            return s
        else:
            return ''

    def can_split(self, ori_s):     # （可改为类方法）
        s = ori_s[:-1]          # 对型号字段末尾出现的符号特殊处理
        for sep in self.model_separators:
            s = s.replace(sep, self.separator)
        s += ori_s[-1]
        l = [x.strip(self.model_strip) for x in s.split(self.separator)]
        lset = set([x for x in l if x and x not in self.model_strip])
        return lset, len(lset)

    def get_model(self, judge_list, trade_p = dict(), models_in_this_brand = set()):
        use_list = []
        pending_list = []
        for s in judge_list:
            if (not s) or (s.strip(self.separator+' ') == ''):
                continue
            elif self.separator in s:     # 用于识别被拼接过的原始数据
                pending_list.append(s)
            else:
                use_list.append(s)
        judgelist = use_list + pending_list

        trade_prop_set = set([self.standardize_for_model(r) for r in trade_p.values()])
        self.print_log('- Trade Props original:', trade_p)
        self.print_log('- Trade Props after processing:', trade_prop_set, '\n')
        mp = ''
        result = ''
        sure_models = []
        pending_multi = []
        pending_chn = []
        for raw_model in judgelist:
            x = self.standardize_for_model(raw_model).strip(self.model_strip)
            self.print_log('- MODEL original:', raw_model)
            self.print_log('- MODEL after processing:', x)
            if not x:
                continue
            split_set, length = self.can_split(x)
            if length > 1:                                                   # 该文本出现分隔符 获取型号列表
                candidate_list = []
                for i in split_set:
                    pattern = r'([-/0-9A-Z]*)({r_s})([-/0-9A-Z]*)'.format(r_s=re.escape(i))
                    got = False
                    for trade_prop in trade_prop_set:
                        search_obj = re.findall(pattern, trade_prop, re.I)
                        for j in search_obj:
                            if not j[0] and not j[2]:
                                got = True
                                candidate_list.append(i)                     # 出现在交易属性 则加入临时候选列表
                                break
                        if got:
                            break
                len_candidate_list = len(candidate_list)
                if len_candidate_list == 0:                                # 多型号内没有一个出现在交易属性里过
                    if self.cmpl_alnum.search(x):
                        pending_multi.append((raw_model, x))                          # 则多型号放入候选列表
                    else:
                        pending_chn.append((raw_model, x))
                elif len_candidate_list == 1:                              # 多型号内只有一个出现在交易属性里过
                    if self.cmpl_alnum.search(candidate_list[0]):                # 出现英文或数字 则采纳为真正型号
                        mp = raw_model
                        result = candidate_list[0]
                        self.print_log('Only one MODEL in trade props:', result)
                        break
                    else:
                        pending_chn.append((raw_model, candidate_list[0]))            # 纯中文则放入候选列表
                        self.print_log('Only one MODEL in trade props:', candidate_list[0], '【NO letter or number! Pending】')
                else:                                                       # 多型号内有两个及以上出现在交易属性里过
                    sure_models = [i for i in candidate_list if self.cmpl_alnum.search(i)]      # 出现英文或数字 则采纳为真正型号
                    if sure_models:                                         # 去掉纯中文结果 采纳其它型号
                        pending_multi.append((raw_model, x))
                        self.print_log('MODELs in trade props:', sure_models)
                    else:
                        pending_chn.append((raw_model, x))
                        self.print_log('MODELs in trade props:', candidate_list, '【NO letter or number! Pending】')
            elif length == 1:                                                    # 该文本未出现分隔符 可视作单个型号
                strip_x = split_set.pop()
                if self.cmpl_alnum.search(strip_x):                                    # 出现英文或数字 则采纳为真正型号
                    mp = raw_model
                    result = strip_x
                    self.print_log('Single MODEL:', result)
                    break
                else:
                    pending_chn.append((raw_model, strip_x))                                # 纯中文则放入候选列表

        if not mp:
            brand_model_candidates = []
            for i in models_in_this_brand:                                  # 没找到确定的型号，先去参考型号库里该品牌下搜索
                pattern = r'([-/0-9A-Z]*)({r_s})([-/0-9A-Z]*)'.format(r_s=re.escape(i))
                got = False
                for trade_prop in trade_prop_set:
                    search_obj = re.findall(pattern, trade_prop, re.I)
                    for j in search_obj:
                        if not j[0] and not j[2]:
                            got = True
                            brand_model_candidates.append((trade_prop, i))                     # 出现在交易属性 则加入临时候选列表
                            break
                    if got:
                        break
            self.print_log('Brand MODELs in trade props:', brand_model_candidates)

            if len(brand_model_candidates) == 1:                                    # 品牌参考型号里匹配到单个则采纳
                mp, result = brand_model_candidates[0]
                self.print_log('MODEL from same brand:', result)
            elif pending_multi:
                mp, result = pending_multi[0]
                self.print_log('MODEL multi:', result)
            elif pending_chn:
                mp, result = pending_chn[0]
                self.print_log('MODEL chn:', result)
        return mp, result

    def get_sku(self, judge_list, trade_p = dict(), skus_in_this_brand = set()):
        mp, sp = ('', '')
        if judge_list:
            for tmp_text in judge_list:
                if tmp_text:
                    mp = tmp_text
                    sp = self.standardize_for_model(tmp_text)   # 因SKU库props_value分组统计unique_key限定，标准化为全大写
                    break
        return mp, sp

    def number_transfer(self, name):     # （可改为类方法）
        get_num = self.pure_num.search(name)
        if get_num:
            return float(get_num.group(1))
        else:
            r_name = reversed(name)
            name = ''
            total = 0
            for x in r_name:
                if (x in self.chn_number_map.keys()) or (x in self.chn_unit_map.keys()):
                    name = x + name
                else:
                    break
            for u in self.chn_unit_map.keys():
                if not name:
                    break
                idx = -1
                idx = name.find(u)
                if idx > 0:
                    total += self.chn_number_map.get(name[idx-1],1) * self.chn_unit_map[u]
                    name = name[idx+1:]
                elif idx == 0:
                    total += self.chn_unit_map[u]
                    name = name[idx+1:]
            if name:
                total += self.chn_number_map.get(name[-1],0)
            return total

    def to_digit(self, raw_name, calculate):     # （可改为类方法）
        name = raw_name.strip('+.*').replace(' ','')
        if not name:
            return ''
        elif '+' in name:
            lis = name.split('+')
            ans = [self.number_transfer(i) for i in lis]
            if '+' in calculate:
                return str(sum(ans))
            else:
                return str(ans[-1])
        elif '*' in name:
            lis = name.split('*')
            ans = [self.number_transfer(i) for i in lis]
            if '*' in calculate:
                r = 1
                for i in ans:
                    r = r * i if i else r
                return str(r)
            else:
                return str(ans[-1])
        else:
            return str(self.number_transfer(name))
        return raw_name

    def unit_conversion(self, num, q, default_q = None, unit_conv_dict = None):     # （可改为类方法）
        if not num:
            return num
        if (not q) and default_q != None:
            q = default_q
        if (default_q == None) or (not unit_conv_dict) or (unit_conv_dict.get(q) == None):
            try:
                num = round(float(num), self.decimal_place)
            except:
                return num + q
            else:
                new_num = int(num) if int(num) == num else num
                return str(new_num) + q
        else:
            coefficient, aim_q = unit_conv_dict[q]
            if type(coefficient) == list:
                multiplier = coefficient[0]
                addend = coefficient[1]
            else:
                multiplier = coefficient
                addend = 0
            try:
                num = float(num)
            except:
                return num + q
            else:
                new_num = round(num * multiplier + addend, self.decimal_place)
                if int(new_num) == new_num:
                    new_num = int(new_num)
                return str(new_num) + aim_q

    def num_in_range(self, num_str, num_range):
        r_num = self.pure_num.search(num_str)
        try:
            pn = eval(r_num.group(1)) if r_num else None
        except:
            pn = None
        if num_range and pn != None:
            if pn < num_range[0] or pn > num_range[1]:
                pn = None
        return pn

    def match_volume_with_count(self, s, pos_now):
        default_q, unit_conv = pos_now['default_quantifier'], pos_now['unit_conversion']
        only_int = self.quantifier[pos_now['type']]['only_int']
        for i in self.quantifier[2]['middle']:
            s = s.replace(i, '+')
        for i in self.quantifier[2]['pre']:
            s = s.replace(i, '*')
        self.print_log('String:', s)
        obj_list = [w[0] for w in self.cmpl_extract.findall(s)]
        self.print_log('Obj list:', obj_list)
        final_s = ''
        mark_v_unit = ''
        for sub_s in reversed(obj_list):        # 提取分隔的各不可加数值的最后一部分
            v_unit_in = pos_now['cmpl_v'].search(sub_s)
            c_unit_in = pos_now['cmpl_c'].search(sub_s)
            o_unit_in = pos_now['cmpl_o'].search(sub_s)
            num_in = pos_now['cmpl_n'].search(sub_s)
            if not num_in:
                continue
            if not mark_v_unit:
                if v_unit_in:
                    final_s = sub_s + final_s
                    mark_v_unit = v_unit_in.group(1)
                elif not o_unit_in and c_unit_in:
                    final_s = sub_s + final_s
            else:
                if final_s[0] == '+' or sub_s[-1] == '+':
                    if v_unit_in:
                        final_s = sub_s + final_s
                        mark_v_unit = v_unit_in.group(1)
                    elif not o_unit_in:
                        if sub_s[-1].isdigit():
                            final_s = sub_s + mark_v_unit + final_s
                        else:
                            final_s = sub_s + final_s
                else:
                    break
        last_list = final_s.split('+')
        self.print_log('Last list:', last_list)
        mark_v_unit = ''
        add_list = []
        for sub_s in reversed(last_list):                # 最后一部分加号前纯数字加单位
            v_unit_in = pos_now['cmpl_v'].search(sub_s)
            c_unit_in = pos_now['cmpl_c'].search(sub_s)
            o_unit_in = pos_now['cmpl_o'].search(sub_s)
            num_in = pos_now['cmpl_n'].search(sub_s)
            if not num_in:
                continue
            if not mark_v_unit:
                if v_unit_in:
                    mark_v_unit = v_unit_in.group(1)
                    add_list.insert(0, sub_s)
                elif not o_unit_in and c_unit_in:
                    add_list.insert(0, sub_s)
            else:
                if v_unit_in:
                    add_list.insert(0, sub_s)
                    mark_v_unit = v_unit_in.group(1)
                elif not o_unit_in:
                    if sub_s[-1].isdigit():
                        add_list.insert(0, sub_s + mark_v_unit)
                    else:
                        add_list.insert(0, sub_s)
        self.print_log('Add list:', add_list)

        middle_list = []
        ans = []
        for name in add_list:
            if not name:
                continue
            #self.print_log('Pattern:', pattern)
            #self.print_log('String:', name)
            all_obj = pos_now['cmpl'].findall(name)
            self.print_log('All_obj:', all_obj)
            prior_list = []
            rangeq_list = []
            noq_list = []
            for o in all_obj:
                if set(o) != {''}:
                    obj = list(o)
                    if not obj[5]:
                        noq_list.append(obj)
                    elif obj[6]:
                        rangeq_list.append(obj)
                    else:
                        prior_list.append(obj)
            sort_list = prior_list + rangeq_list + noq_list
            if not sort_list:
                continue
            self.print_log('Sorted_all_obj:', sort_list)
            obj = sort_list[0]
            num = None
            if obj[5]:
                if only_int and obj[4]!= '' and set(obj[4]) != {'0', '.'}:
                    continue
                if obj[3] == obj[4]:
                    if obj[3] != '':
                        if obj[1] + obj[2] == '' and obj[0].replace('.','').isdigit():
                            num = obj[0] + obj[3]
                            obj[0] = ''
                        else:
                            num = obj[3]
                    elif obj[1] + obj[2] == '' and obj[0]:
                        num = obj[0]
                        obj[0] = ''
                elif obj[3].replace('.','').isdigit():
                    num = obj[3]
                if num:
                    mp_list = ['', '', '', num, obj[5], '', '', '', '']
                    existed_units = {obj[5]}
                    if obj[6]:
                        mp_list[0] = '~'
                    if obj[0] and obj[2] != '共' and obj[1]+obj[2] and obj[1] not in existed_units:
                        num = obj[0] + '*' + num
                        mp_list[1], mp_list[2] = (obj[0], '*')
                        if obj[1]:
                            existed_units.add(obj[1])
                    if obj[8] and obj[7] != '共' and obj[7]+obj[9] and obj[9] not in existed_units:
                        num += '*' + obj[8]
                        mp_list[5], mp_list[6] = ('*', obj[8])
                        if obj[9]:
                            existed_units.add(obj[9])
                    if obj[13] and obj[12] != '共' and obj[12]+obj[14] and obj[14] not in existed_units:
                        num += '*' + obj[13]
                        mp_list[7], mp_list[8] = ('*', obj[13])
                        if obj[14]:
                            existed_units.add(obj[14])
                    ans.append(self.unit_conversion(self.to_digit(num,'*'), obj[5], default_q, unit_conv))
                    middle_list.append(mp_list)

        u_diff = False
        q_dict = dict()
        if not middle_list:
            return '', '', u_diff, q_dict, middle_list
        middle = ' + '.join([''.join(i) for i in middle_list])
        result = 0
        for x in ans:
            n = ''
            for i in x:
                if i == '.' or i.isdigit():
                    n += i
            q = x.replace(n, '')
            q_dict[q] = q_dict.get(q, 0) + float(n)

        if len(q_dict) > 1:
            self.print_log('【Notice!】different units:', q_dict)
            u_diff = True
        if default_q in q_dict.keys():
            result = q_dict[default_q]
            final_q = default_q
        else:
            result = sum(q_dict.values())
            final_q = sorted(q_dict.keys())[0]
        result = self.unit_conversion(str(result), final_q, default_q, unit_conv)

        self.print_log('Middle result:', middle)
        self.print_log('Final result:', result, '\n')
        return middle, result, u_diff, q_dict, middle_list

    def quantify_num(self, pos_now, raw_judgelist, can_pure_number = []):
        q_post = pos_now['q_post']
        q_pre = pos_now['q_pre']
        q_middle = self.quantifier[pos_now['type']]['middle']
        calculate = self.quantifier[pos_now['type']]['calculate']
        only_int = self.quantifier[pos_now['type']]['only_int']
        self.print_log('用于单位匹配的q_post:', q_post)
        self.print_log('前置单位q_pre:', q_pre)

        judgelist = []
        for x in raw_judgelist:    # 全角转半角，数字前括号换空格，其余括号删除，方便识别单位
            name = self.parenthesis_before_num.sub(r'\1 \2', self.standardize_for_model(x)).replace('(','').replace(')','')
            for word in pos_now['ignore_word']:
                name = name.replace(word, self.separator)
            for word in pos_now['remove_word']:
                name = name.replace(word, '')
            judgelist.append(name.strip(self.model_strip))
        len_judgelist = len(judgelist)
        if not pos_now['pure_num_in']:
            can_pure_number = [False] * len_judgelist   # 所有纯数字都不被采纳
        elif not can_pure_number:
            can_pure_number = [True] * len_judgelist   # 默认所有纯数字都可被采纳

        mp = ''
        if pos_now['calculate_sum']:        # 复杂容量重量乘以量词匹配、多元相加计算
            ans_list = []
            diff_list = []
            range_list = []
            for name in judgelist:
                if not name:
                    continue
                middle, result, is_diff_unit, q_dict, middle_list = self.match_volume_with_count(name, pos_now)
                if middle:
                    mp += ' & ' + middle
                    pn = self.num_in_range(result, pos_now['num_range'])
                    final_tuple = ((is_diff_unit, q_dict, middle_list), pn, result) if pn != None else None
                    if final_tuple:
                        if is_diff_unit:
                            diff_list.append(final_tuple)
                        elif '~' in middle:
                            range_list.append(final_tuple)
                        else:
                            ans_list.append(final_tuple)
                            break
            all_ans_list = ans_list + range_list + diff_list
        else:
            pre_q = []
            with_q = []
            without_q = []
            within_q = []
            range_with_q = []
            for now in range(len_judgelist):
                name = judgelist[now]
                if not name:
                    continue
                self.print_log('\n新的一轮词匹配从此处开始：')
                self.print_log('- Original String:', raw_judgelist[now])
                self.print_log('- String{c}: {n}'.format(c='[Accept Pure Number]' if can_pure_number[now] else '[Reject Pure Number]', n=name))
                if q_pre and pos_now['pre_unit_in']:
                    all_obj = pos_now['cmpl_m'].findall(name)
                    if all_obj:
                        self.print_log('Search Obj for pre*:', all_obj)
                        final_tuple_list = []
                        for obj in all_obj:
                            q, result = obj
                            tup = ('', q, result, '')
                            final_r = self.unit_conversion(self.to_digit(tup[2], calculate), tup[3], pos_now['default_quantifier'], pos_now['unit_conversion'])
                            pn = self.num_in_range(final_r, pos_now['num_range'])
                            final_pre_tuple = (tup, pn, final_r) if pn != None else None
                            if final_pre_tuple:
                                if only_int and pn != int(pn):
                                    continue
                                final_tuple_list.append(final_pre_tuple)
                        if final_tuple_list:
                            self.print_log('Final Tuple List for pre*:', final_tuple_list)
                            if pos_now['read_from_right'] in (0, 1):
                                final_ans_tuple = final_tuple_list[0] if pos_now['read_from_right'] == 0 else final_tuple_list[-1]
                            else:
                                sorted_final_tuple_list = sorted(final_tuple_list, key = lambda x:x[1])
                                final_ans_tuple = sorted_final_tuple_list[0] if pos_now['read_from_right'] == 2 else sorted_final_tuple_list[-1]
                            self.print_log('pre - Accept!', final_ans_tuple)
                            mp += ' & ' + ''.join(final_ans_tuple[0])
                            pre_q.append(final_ans_tuple)
                if q_post:
                    all_obj = pos_now['cmpl'].findall(name)
                    if all_obj:
                        self.print_log('Search Obj:', all_obj)
                        accept_list = []
                        accept_with_q_list = []
                        for obj in all_obj:
                            prefix_with_q = '' if not pos_now['prefix'] else obj[0]
                            num1 = obj[0] if not pos_now['prefix'] else obj[1]
                            decimal1 = obj[1] if not pos_now['prefix'] else obj[2]
                            unit1 = obj[2] if not pos_now['prefix'] else obj[3]
                            operator = obj[3] if not pos_now['prefix'] else obj[4]
                            num2 = obj[4] if not pos_now['prefix'] else obj[5]
                            decimal2 = obj[5] if not pos_now['prefix'] else obj[6]
                            unit2 = obj[6] if not pos_now['prefix'] else obj[7]
                            discard = obj[7] if not pos_now['prefix'] else obj[8]
                            decimal1 = '' if decimal1 and set(decimal1) == {'0', '.'} else decimal1
                            decimal2 = '' if decimal2 and set(decimal2) == {'0', '.'} else decimal2
                            # 范围判断，如果连接符后面没有字符的话，就不能判断为范围，而是一个独立的数字，因为判断符号过多，容易误判
                            operator = '' if not (num2 and unit2) else operator
                            if (unit1 and unit1 not in q_post) or (only_int and decimal1):
                                unit1 = ''
                                num1 = ''
                            if (unit2 and unit2 not in q_post) or (only_int and decimal2):
                                unit2 = ''
                                num2 = ''
                            if not operator and not unit2 and not num2:
                                    unit2 = unit1
                                    num2 = num1
                            if unit2 and discard:
                                if unit2[-1] < 'A' or unit2[-1] > 'Z' and (discard not in self.range_indicator_post):
                                    discard = ''
                                elif (discard in self.range_indicator_post):
                                    num1, unit1, discard, operator = ('', '', '', self.range_indicator_middle[0])
                            self.print_log('丢弃匹配组!' if discard else '接收匹配组!', obj)
                            if not discard:
                                accept_list.append((prefix_with_q, num1, unit1, operator, num2, unit2, discard) if pos_now['prefix'] else ('', num1, unit1, operator, num2, unit2, discard))
                                if unit1 or unit2:
                                    accept_with_q_list.append((prefix_with_q, num1, unit1, operator, num2, unit2, discard) if pos_now['prefix'] else ('', num1, unit1, operator, num2, unit2, discard))

                        final_tuple_list = []
                        if accept_with_q_list or not pos_now['pure_num_in']:
                            accept_list = accept_with_q_list
                        for search_obj in accept_list:
                            self.print_log('被接受的匹配词在此进行计算:')
                            self.print_log('Final Obj:', search_obj)
                            prefix_with_q, num1, unit1, operator, num2, unit2, discard = search_obj
                            if operator:
                                if operator != '*':
                                    unit1 = unit2 if not unit1 else unit1
                                if operator in calculate:
                                    if '*' in calculate and operator == '*':
                                        unit2 = unit1 if not unit2 else unit2
                                    if unit1 == unit2:
                                        tmp = num1 + operator + num2
                                        num1 = tmp
                                        num2 = tmp
                                elif (operator == '±') or (not num2 and not unit2):
                                    num2 = num1
                                    unit2 = unit1
                                elif operator in self.range_indicator_middle:
                                    unit2 = unit1 if not unit2 else unit2
                            elif not unit2 and not num2:
                                    unit2 = unit1
                                    num2 = num1
                            if unit2 and discard:
                                if (unit2[-1] < 'A' or unit2[-1] > 'Z') and (discard not in self.range_indicator_post):
                                    discard = ''

                            if not discard:
                                final_tuple, left_tuple, right_tuple = (None, None, None)
                                round_sign = '~' if operator in self.range_indicator_middle else ''
                                if num1:
                                    if unit1:
                                        tup = (round_sign, prefix_with_q, num1, unit1)
                                    elif can_pure_number[now]:
                                        tup = (round_sign, prefix_with_q, num1, '')
                                    else:
                                        tup = None
                                    if tup:
                                        final_r = tup[1] + self.unit_conversion(self.to_digit(tup[2], calculate), tup[3], pos_now['default_quantifier'], pos_now['unit_conversion']) # 转换后数字,识别到的单位,默认单位
                                        pn = self.num_in_range(final_r, pos_now['num_range'])
                                        left_tuple = (tup, pn, final_r) if pn != None else None
                                if num2:
                                    if unit2:
                                        tup = (round_sign, prefix_with_q, num2, unit2)
                                    elif can_pure_number[now]:
                                        tup = (round_sign, prefix_with_q, num2, '')
                                    else:
                                        tup = None
                                    if tup:
                                        final_r = tup[1] + self.unit_conversion(self.to_digit(tup[2], calculate), tup[3], pos_now['default_quantifier'], pos_now['unit_conversion']) # 转换后数字,识别到的单位,默认单位
                                        pn = self.num_in_range(final_r, pos_now['num_range'])
                                        right_tuple = (tup, pn, final_r) if pn != None else None
                                if pos_now['read_from_right'] in (0, 1):
                                    if pos_now['read_from_right'] == 0:
                                        final_tuple = left_tuple if left_tuple else right_tuple
                                    else:
                                        final_tuple = right_tuple if right_tuple else left_tuple
                                else:
                                    if left_tuple and right_tuple:
                                        if left_tuple[1] < right_tuple[1]:
                                            final_tuple = left_tuple if pos_now['read_from_right'] == 2 else right_tuple
                                        else:
                                            final_tuple = left_tuple if pos_now['read_from_right'] == 3 else right_tuple
                                    else:
                                        final_tuple = left_tuple if left_tuple else right_tuple
                                if final_tuple:
                                    final_tuple_list.append(final_tuple)
                            else:
                                self.print_log('Discard!')

                        if final_tuple_list:
                            self.print_log('被接受的元组在此进行计算:')
                            self.print_log('Final Tuple List:', final_tuple_list)
                            if pos_now['read_from_right'] in (0, 1):
                                final_ans_tuple = final_tuple_list[0] if pos_now['read_from_right'] == 0 else final_tuple_list[-1]
                            else:
                                sorted_final_tuple_list = sorted(final_tuple_list, key = lambda x:x[1])
                                final_ans_tuple = sorted_final_tuple_list[0] if pos_now['read_from_right'] == 2 else sorted_final_tuple_list[-1]
                            self.print_log('post - Accept!', final_ans_tuple)
                            mp += ' & ' + ''.join(final_ans_tuple[0])
                            if final_ans_tuple[0][0]:
                                range_with_q.append(final_ans_tuple)
                            elif final_ans_tuple[0][-1]:
                                with_q.append(final_ans_tuple)
                            else:
                                without_q.append(final_ans_tuple)
                if q_middle:
                    idx = -1
                    for q in q_middle:
                        if pos_now['read_from_right'] == 1:
                            idx = name.rfind(q)
                        else:
                            idx = name.find(q)
                        if idx > 0:
                            result = '*'
                            for i in range(idx-1, -1, -1):
                                if name[i].isdigit() or name[i] == ' ' or name[i] == '.' or name[i] in self.chn_numbers:
                                    result = name[i] + result
                                else:
                                    break
                            for i in range(idx+1, len(name)):
                                if name[i].isdigit() or name[i] == ' ' or name[i] == '.' or name[i] in self.chn_numbers:
                                    result += name[i]
                                else:
                                    break
                            result = result.replace(' ','')
                            if result != '*':
                                tup = ('', '', result, '')
                                mp += ' & ' + result.replace('*', q)
                                final_r = tup[1] + tup[2] + pos_now['default_quantifier'] # 直接加默认单位
                                within_q.append((tup, result.replace('*', ''), final_r))
                                break
                if (q_middle and within_q) or (q_post and with_q):
                    break
            all_ans_list = within_q + with_q + pre_q + without_q + range_with_q

        mp = mp[3:] if mp.startswith(' & ') else mp
        self.print_log('\n最终匹配结果:All Answer List:', all_ans_list)
        if all_ans_list:
            result = str(all_ans_list[0][-2]) if pos_now['pure_num_out'] else all_ans_list[0][-1]
        else:
            result = ''
        for base_unit in pos_now['multi_units']:
            start = result.rfind(base_unit)
            if start > 0:
                try:
                    trans_num = float(result[:start])
                except:
                    continue
                else:
                    for tup in pos_now['multi_units'][base_unit]:
                        base_min_value, base_max_value, change_unit, unit_conv = tup
                        if base_min_value != None and trans_num < base_min_value:
                            continue
                        if base_max_value != None and trans_num > base_max_value:
                            continue
                        result = self.unit_conversion(trans_num, base_unit, change_unit, unit_conv)
                        break
        return mp, result, all_ans_list

    def map_values(self, sp, all_bid, alias_all_bid, f_map):
        sp['all_bid_sp'] = None
        sp['alias_all_bid_sp'] = None
        sp['all_bid'] = str(all_bid)
        sp['alias_all_bid'] = str(alias_all_bid)
        sp['source'] = str(f_map['source'])
        sp['cid'] = str(f_map['cid'])
        sp['sid'] = str(f_map['sid'])
        for rank in sorted(self.unify_classify_dict.keys()):
            if self.unify_classify_dict[rank][0] == 'unify':
                idx, change_pos_id, change_sp_value, base_dict, base_not_dict = self.unify_classify_dict[rank]
                for b_p_id in base_dict:
                    if sp[b_p_id] not in base_dict[b_p_id]:
                        break
                else:
                    for b_p_id in base_not_dict:
                        if sp[b_p_id] in base_not_dict[b_p_id]:
                            break
                    else:
                        self.print_log('【sp】', sp)
                        sp[change_pos_id] = change_sp_value
                        log_msg = 'Unify(rank {r}): {b_d} → {c_n} = {c_v}'.format(r=rank, b_d=', '.join(set(['sp'+str(i) if type(i) == int else i for i in base_dict] + ['sp'+str(i) if type(i) == int else i for i in base_not_dict])), c_n='sp' + str(change_pos_id) if type(change_pos_id) == int else change_pos_id, c_v=change_sp_value)
                        if change_pos_id == 'all_bid_sp':
                            brand_name = self.search_all_brand(change_sp_value)[0]
                            alias_bid = self.search_all_brand(change_sp_value)[-1]
                            if alias_bid > 0:
                                sp['alias_all_bid_sp'] = alias_bid
                                brand_name = self.search_all_brand(alias_bid)[0]
                            else:
                                sp['alias_all_bid_sp'] = change_sp_value
                            sp[self.pos_id_brand] = brand_name
                            log_msg += ', alias_all_bid_sp = {ab}, sp{pos_id_brand} = {alias_brand_name}'.format(ab=sp['alias_all_bid_sp'], pos_id_brand=self.pos_id_brand, alias_brand_name=brand_name)
                        self.print_log(log_msg)
            elif self.unify_classify_dict[rank][0] == 'classify':
                idx, change_pos_id, change_value, base_pos_id, base_unit, base_min_value, base_max_value = self.unify_classify_dict[rank]
                start = sp[base_pos_id].upper().rfind(base_unit)
                if start > 0:
                    try:
                        trans_num = float(sp[base_pos_id][:start])
                    except:
                        continue
                    else:
                        if base_min_value != None and trans_num < base_min_value:
                            continue
                        if base_max_value != None and trans_num > base_max_value:
                            continue
                        self.print_log('【sp】', sp)
                        self.print_log('Classify(rank {r}): sp{b_n} = {b_v} → sp{c_n} = {c_v}'.format(r=rank, b_n=base_pos_id, b_v=sp[base_pos_id], c_n=change_pos_id, c_v=change_value))
                        sp[change_pos_id] = change_value

        if self.clean_sku_pos in sp and sp[self.clean_sku_pos] in self.clean_sku:
            vv = self.clean_sku[sp[self.clean_sku_pos]]
            sp.update({int(kk.replace('spid','')) if kk.find('spid')==0 else kk.replace('alias_all_bid','all_bid_sp'):vv[kk] for kk in vv if vv[kk].strip()!=''})

            brand_name = self.search_all_brand(sp['all_bid_sp'])[0]
            alias_bid = self.search_all_brand(sp['all_bid_sp'])[-1]
            if alias_bid > 0:
                sp['alias_all_bid_sp'] = alias_bid
                brand_name = self.search_all_brand(alias_bid)[0]
            else:
                sp['alias_all_bid_sp'] = sp['all_bid_sp']
            sp[self.pos_id_brand] = brand_name

        del sp['all_bid']
        del sp['alias_all_bid']
        del sp['source']
        del sp['cid']
        del sp['sid']
        self.print_log('【sp】', sp, '\n')
        return sp

    def read_items(self, start_item_id=None, end_item_id=None, condition='', chunksize=None):
        if chunksize != None and chunksize <= 0:
            chunksize = self.default_limit
        main_sql = 'SELECT {fr} FROM {item_table} '.format(fr=', '.join(self.item_fields_read), item_table=self.item_table_name)

        conds = [None, None, None]
        if start_item_id != None:
            conds[0] = 'id > ' + str(start_item_id)
        if end_item_id != None:
            conds[1] = 'id <= ' + str(end_item_id)
        if condition:
            conds[2] = condition

        if not chunksize:
            final_conds = [r for r in conds if r]
            if final_conds:
                main_sql += 'WHERE ' + ' AND '.join(final_conds)
            # print(main_sql)
            data = self.db_artificial_new.query_all(main_sql)
            self.db_artificial_new.commit()
            return data

        def gen():
            start_id = 0 if start_item_id == None or start_item_id < 0 else start_item_id
            condw = ' AND '.join([r for r in conds[1:] if r])
            while True:
                sql = main_sql + 'WHERE id > {start} {cond} ORDER BY id ASC LIMIT {limit};'.format(start=start_id, cond='AND ' + condw if condw else '', limit=chunksize)
                # print(sql)
                data = self.db_artificial_new.query_all(sql)
                self.db_artificial_new.commit()
                if data:
                    yield data
                else:
                    return
                start_id = data[-1][0]
        return gen()

    def process_given_items(self, data, other_info=None):
        result_dict = self.calculate_sp_plugin.start(self, data, other_info)
        return result_dict

    def update_given_items(self, result_dict):
        sql = 'UPDATE {item_table} SET '.format(item_table=self.item_table_name)
        for field in self.item_fields_write:
            sql += field + ' = TRIM(%s), '
        sql += 'clean_type = %s, clean_ver = %s, all_bid_sp = %s, alias_all_bid_sp = %s, alias_all_bid = %s WHERE id = %s;'

        val_list = []
        for item_id in result_dict:
            val_now = [result_dict[item_id][key] for key in self.item_fields_write] + [result_dict[item_id]['clean_type'], result_dict[item_id]['clean_ver'], result_dict[item_id]['all_bid_sp'], result_dict[item_id]['alias_all_bid_sp'], result_dict[item_id]['alias_all_bid'], item_id]
            val_list.append(tuple(val_now))
        values = tuple(val_list)

        self.print_log(values, '\n', sql)
        self.db_artificial_new.execute_many(sql, values)
        self.db_artificial_new.commit()

    def trim_result(self, sql_id_limit = 'id > 0'):
        sql_sub = ', '.join(['sp{i} = TRIM(sp{i})'.format(i=i) for i in self.pos_id_list])
        sql = 'UPDATE {item_table} SET {sql_sub} WHERE {sql_id_limit};'.format(item_table=self.item_table_name, sql_sub=sql_sub, sql_id_limit=sql_id_limit)   # 解决因100个字符截断的问题
        self.print_log(sql)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

    def item_distribution(self):
        start_time = time.time()
        self.db_artificial_new.connect()
        sql = 'SELECT MAX(clean_ver) FROM {item_table};'.format(item_table=self.item_table_name)
        max_clean_ver = self.db_artificial_new.query_scalar(sql)
        if not self.entity:
            self.entity = Entity(self.batch_id)
        row_count = self.entity.import_toclean(max_clean_ver)
        self.time_monitor.add_record(action='copy_to_clickhouse', start=start_time, end=time.time(), num=row_count)

        start_time = time.time()
        first_row = ['source', 'cid', '子品类', '计数0', '空列0']
        sql = 'SELECT source, cid, sp{i}, COUNT(1) FROM artificial_local.{clean_table} WHERE clean_ver = {max_clean_ver} GROUP BY cid, source, sp{i} ORDER BY source, cid, sp{i};'.format(i=self.pos_id_category, clean_table=self.clean_table_name, max_clean_ver=max_clean_ver)
        k_data = list(self.db_chsop.query_all(sql))
        c_data = []
        data = []
        max_len = len(k_data)
        for i in self.pos_id_list:
            if i == self.pos_id_brand:
                first_row.extend(['sp{i}_{name}'.format(i=i, name=self.pos[i]['name']), 'alias_all_bid', '计数'+str(i), '空列'+str(i)])
                sql = 'SELECT sp{i}, IFNULL(alias_all_bid_sp, alias_all_bid) AS combined_alias_all_bid, COUNT(1) FROM artificial_local.{clean_table} WHERE clean_ver = {max_clean_ver} AND clean_type >= 0 GROUP BY combined_alias_all_bid, sp{i} ORDER BY sp{i}, combined_alias_all_bid;'.format(i=i, clean_table=self.clean_table_name, max_clean_ver=max_clean_ver)
            else:
                first_row.extend(['sp{i}_{name}'.format(i=i, name=self.pos[i]['name']), '计数'+str(i), '空列'+str(i)])
                sql_w = '' if i == self.pos_id_category else 'AND clean_type >= 0'
                sql = 'SELECT sp{i}, COUNT(1) FROM artificial_local.{clean_table} WHERE clean_ver = {max_clean_ver} {sql_w} GROUP BY sp{i} ORDER BY sp{i};'.format(i=i, clean_table=self.clean_table_name, max_clean_ver=max_clean_ver, sql_w=sql_w)
            data_sp = [ [str(x) for x in row] for row in self.db_chsop.query_all(sql) ]
            max_len = max(len(data_sp), max_len)
            c_data.append(data_sp)
        k_data.extend([('','','','')] * (max_len-len(k_data)))
        for cc in c_data:
            cc.extend([ [''] * len(cc[0]) ] * (max_len-len(cc)))
        for i in range(max_len):
            row = list(k_data[i]) + ['']
            for cc in c_data:
                row.extend(cc[i] + [''])
            data.append(row)
        print('分布总行数', len(data))
        self.time_monitor.add_record(action='analyze_item_distribution', start=start_time, end=time.time(), num=row_count*len(self.pos_id_list))

        start_time = time.time()
        common.table_backup(self.db_artificial_new, self.distribution_table_name)

        sql = 'CREATE TABLE {distribution_table_name} ( '.format(distribution_table_name=self.distribution_table_name)
        for word in first_row:
            sql += '`{f}` VARCHAR({n}), '.format(f=word, n=200 if word == '子品类' or word.startswith('sp') else 20)
        sql = sql[:-2] + ') ENGINE=MyISAM DEFAULT CHARSET=utf8;'
        self.print_log(sql)
        self.db_artificial_new.execute(sql)

        sql = 'INSERT INTO {distribution_table_name} VALUES '.format(distribution_table_name=self.distribution_table_name)
        sql_v = '(' + ('%s, ' * len(first_row))[:-2] + ')'
        print(sql, sql_v)
        print('正在生成各sp取值分布...')
        self.db_artificial_new.batch_insert(sql, sql_v, data)

        sql = 'ALTER TABLE {distribution_table_name} ADD COLUMN `clean_ver{max_clean_ver}分布生成时间` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;'.format(distribution_table_name=self.distribution_table_name, max_clean_ver=max_clean_ver)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()
        self.time_monitor.add_record(action='write_item_distribution', start=start_time, end=time.time(), num=row_count*len(self.pos_id_list))

        return first_row, data, row_count

    def old_item_distribution(self):
        start_time = time.time()
        sql = 'SELECT MAX(clean_ver) FROM {item_table};'.format(item_table=self.item_table_name)
        max_clean_ver = self.db_artificial_new.query_scalar(sql)
        first_row = ['source', 'cid', '子品类', '计数0', '空列0']
        sql = 'SELECT source, cid, sp{i}, COUNT(id) FROM {item_table} GROUP BY cid, source, sp{i};'.format(i=self.pos_id_category, item_table=self.item_table_name)
        k_data = list(self.db_artificial_new.query_all(sql))
        id_count = sum([i[-1] for i in k_data])
        c_data = []
        data = []
        max_len = len(k_data)
        for i in self.pos_id_list:
            if i == self.pos_id_brand:
                first_row.extend(['sp{i}_{name}'.format(i=i, name=self.pos[i]['name']), 'alias_all_bid', '计数'+str(i), '空列'+str(i)])
                sql = 'SELECT sp{i}, IFNULL(alias_all_bid_sp, alias_all_bid) AS combined_alias_all_bid, COUNT(1) FROM {item_table} WHERE clean_ver = {max_clean_ver} AND clean_type >= 0 GROUP BY combined_alias_all_bid, sp{i} ORDER BY sp{i}, combined_alias_all_bid;'.format(i=i, item_table=self.item_table_name, max_clean_ver=max_clean_ver)
            else:
                first_row.extend(['sp{i}_{name}'.format(i=i, name=self.pos[i]['name']), '计数'+str(i), '空列'+str(i)])
                sql_w = '' if i == self.pos_id_category else 'AND clean_type >= 0'
                sql = 'SELECT sp{i}, COUNT(1) FROM {item_table} WHERE clean_ver = {max_clean_ver} {sql_w} GROUP BY sp{i} ORDER BY sp{i};'.format(i=i, item_table=self.item_table_name, max_clean_ver=max_clean_ver, sql_w=sql_w)
            data_sp = [ [str(x) for x in row] for row in self.db_artificial_new.query_all(sql) ]
            max_len = max(len(data_sp), max_len)
            c_data.append(data_sp)
        k_data.extend([('','','','')] * (max_len-len(k_data)))
        for cc in c_data:
            cc.extend([ [''] * len(cc[0]) ] * (max_len-len(cc)))
        for i in range(max_len):
            row = list(k_data[i]) + ['']
            for cc in c_data:
                row.extend(cc[i] + [''])
            data.append(row)
        print('分布总行数', len(data))
        self.time_monitor.add_record(action='analyze_item_distribution', start=start_time, end=time.time(), num=id_count*len(self.pos_id_list))

        start_time = time.time()
        common.table_backup(self.db_artificial_new, self.distribution_table_name)

        sql = 'CREATE TABLE {distribution_table_name} ( '.format(distribution_table_name=self.distribution_table_name)
        for word in first_row:
            sql += '`{f}` VARCHAR({n}), '.format(f=word, n=200 if word == '子品类' or word.startswith('sp') else 20)
        sql = sql[:-2] + ') ENGINE=MyISAM DEFAULT CHARSET=utf8;'
        self.print_log(sql)
        self.db_artificial_new.execute(sql)

        sql = 'INSERT INTO {distribution_table_name} VALUES'.format(distribution_table_name=self.distribution_table_name)
        for r in data:
            sql += str(tuple(r)) + ', '
        sql = sql[:-2] + ';'
        print('正在生成各sp取值分布...')
        self.db_artificial_new.execute(sql)

        sql = 'ALTER TABLE {distribution_table_name} ADD COLUMN `clean_ver{max_clean_ver}分布生成时间` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;'.format(distribution_table_name=self.distribution_table_name, max_clean_ver=max_clean_ver)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()
        self.time_monitor.add_record(action='write_item_distribution', start=start_time, end=time.time(), num=id_count*len(self.pos_id_list))

        return first_row, data, id_count

    def create_model_table(self, suffix = ''):
        assert self.last_pid == 0 or suffix, 'Model library already exsits, total {last_pid} records.'.format(last_pid=self.last_pid)    # 已有提交过的型号库，只能跑增量，防止误操作重建

        model_table_name = self.model_table_name + suffix
        props_table_name = self.props_table_name + suffix
        common.table_backup(self.db_artificial_new, props_table_name)
        common.table_backup(self.db_artificial_new, model_table_name)

        sql = """
            CREATE TABLE {props_table_name} (
            `alias_all_bid` INT UNSIGNED NOT NULL COMMENT 'all_brand表里品牌bid',
            `brand` VARCHAR(200) NOT NULL DEFAULT '' COMMENT '品牌名',
            `name` VARCHAR(200) NOT NULL COMMENT '型号名',
            `sp_no` TINYINT(3) NOT NULL COMMENT 'clean_pos表里的pos_id',
            `sp_value` VARCHAR(200) NOT NULL COMMENT '清洗出的属性值',
            `model_prop_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '该型号对应该属性的计数',
            `model_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '该型号的计数',
            `prop_ratio` FLOAT NOT NULL COMMENT '该属性计数在该型号非空属性中的比例'
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8;


            CREATE TABLE {model_table_name} (
            `pid` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '（品牌bid-品牌名-型号名去重后的）型号编号pid',
            `alias_all_bid` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'all_brand表里品牌bid',
            `brand` VARCHAR(200) DEFAULT '' COMMENT '品牌名',
            `name` VARCHAR(200) NOT NULL DEFAULT '' COMMENT '型号名',
            `real_cid` INT UNSIGNED DEFAULT 0,
            `price` BIGINT UNSIGNED DEFAULT 0,
            `tb_item_id` VARCHAR(40) DEFAULT '0',
            `img` VARCHAR(255) DEFAULT '',
            `all_bid_sp` INT(11) DEFAULT NULL COMMENT '人工修改的品牌bid',
        """.format(model_table_name=model_table_name, props_table_name=props_table_name)

        for i in sorted(self.pos_id_list):
            sql += "`sp{i}` VARCHAR(200) NOT NULL DEFAULT '', \n".format(i=i)

        sql += """
            `model_count` BIGINT UNSIGNED DEFAULT 0 COMMENT '该型号计数',
            `tb_item_count` BIGINT UNSIGNED DEFAULT 0 COMMENT '该型号宝贝数',
            `item_and_trade_count` BIGINT UNSIGNED DEFAULT 0 COMMENT '该型号宝贝及交易属性数',
            `brand_sales` BIGINT UNSIGNED DEFAULT 0 COMMENT '该品牌总销售额',
            `sum_sales` BIGINT UNSIGNED DEFAULT 0 COMMENT '该型号总销售额',
            `recent_sales` BIGINT UNSIGNED DEFAULT 0 COMMENT '该型号近半年销售额',
            `confirmed` TINYINT(3) NOT NULL DEFAULT 0 COMMENT '人工确认批次标记',
            `alias_pid` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '关联的主型号编号pid',
            `del_flag` TINYINT(3) NOT NULL DEFAULT 0,
            `created` TIMESTAMP NOT NULL DEFAULT '{t}',
            `modified` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`pid`),
            UNIQUE KEY `x_product` (`alias_all_bid`,`name`)
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """.format(t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

        print(sql)
        self.db_artificial_new.execute(sql)

    def set_pid_in_item(self):
        self.set_batch_status(status='正在关联宝贝型号')

        sql = 'UPDATE {item_table_name} SET pid = 0 WHERE id > {last_id};'.format(item_table_name=self.item_table_name, last_id=self.last_id)
        print(sql)       # 只有增量部分set_pid
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

        sql = 'UPDATE {item_table_name} i, {model_table_name} m SET i.pid = m.pid WHERE i.id > {last_id} AND i.clean_ver > 0 AND i.clean_type >= 0 AND m.alias_all_bid = IFNULL(i.alias_all_bid_sp, i.alias_all_bid) AND i.sp{m} = m.name;'.format(model_table_name=self.model_table_name, item_table_name=self.item_table_name, last_id=self.last_id, m=self.pos_id_as_model)
        print(sql)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

        sql = "UPDATE {model_table_name} m INNER JOIN (SELECT pid, img FROM {item_table_name} WHERE id > {last_id} AND img IS NOT NULL AND img != '' GROUP BY pid) i ON m.pid = i.pid SET m.img = i.img WHERE m.img IS NULL OR m.img = '';".format(item_table_name=self.item_table_name, model_table_name=self.model_table_name, last_id=self.last_id)
        print(sql)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

    def analyze_model_group(self):
        assert self.pos_id_brand and self.pos_id_as_model, '尚未存在品牌或设为as_model的清洗属性，无法进行型号库或SKU库相关操作，请检查clean_pos表配置。'
        self.set_batch_status(status='正在生成型号库')
        if self.last_pid == 0:     # 如已有提交过的型号库，只能跑增量，防止误操作清空
            self.create_model_table()

        field_names = ['sp' + str(i) for i in self.pos_id_list if self.pos[i]['type'] >= 0 and self.pos[i]['type'] < 1000] # + ['mp' + str(i) for i in pos_id_list]
        start = 0
        limit = self.default_limit
        last_time = time.time()
        df_item = pd.DataFrame(dict())
        while True:
            sql = 'SELECT id, tb_item_id, source, month, IFNULL(alias_all_bid_sp, alias_all_bid) AS alias_all_bid, sales, p1, {fields} FROM {item_table_name} WHERE id > {start} AND clean_ver > 0 AND clean_type >= 0 ORDER BY id ASC LIMIT {limit};'.format(fields=', '.join(field_names), item_table_name=self.item_table_name, start=start, limit=limit)
            print(sql)
            tmp = pd.read_sql(sql, self.db_an_conn, parse_dates=['month'])
            if tmp.size > 0:
                df_item = df_item.append(tmp)
                self.time_monitor.add_record(action='read_item_for_model_library', start=last_time, end=time.time(), num=len(tmp.index))
                last_time = time.time()
                start = tmp.id.max()
            else:
                break

        start_time = time.time()
        df_item['s_t'] = df_item['tb_item_id'] + df_item['source']
        df_item['s_t_p'] = df_item['s_t'] + df_item['p1'].astype('str')
        recent_month = df_item.month.max() - pd.Timedelta('{n} days'.format(n=self.model_recent_days))
        df_item.rename(columns = {'sp' + str(self.pos_id_as_model): 'name'}, inplace = True)
        b_m_group = df_item.groupby(['alias_all_bid', 'name'])
        b_m_group_recent = df_item.loc[ df_item['month'] > recent_month ].groupby(['alias_all_bid', 'name'])

        df_model = pd.DataFrame({'model_count': b_m_group['id'].nunique(),
                                'tb_item_count': b_m_group['s_t'].nunique(),
                                'item_and_trade_count': b_m_group['s_t_p'].nunique(),
                                'sum_sales': b_m_group['sales'].sum(),
                                'recent_sales': b_m_group_recent['sales'].sum(),
                                }).fillna(0)

        df_model = df_model.join(df_item.groupby(['alias_all_bid']).sales.sum().rename('brand_sales'))
        df_model = df_model.reset_index().set_index(['alias_all_bid', 'name'])

        if self.last_pid > 0:      # 如已有提交过的型号库，将pid对应到全部型号表里
            sql = 'SELECT pid, alias_all_bid, name FROM {model_table_name};'.format(model_table_name=self.model_table_name)
            df_model_existed = pd.read_sql(sql, self.db_an_conn)
            df_model_existed = df_model_existed.set_index(['alias_all_bid', 'name'])
            df_model = df_model.join(df_model_existed, how='left')
        self.time_monitor.add_record(action='analyze_model_library', start=start_time, end=time.time(), num=len(df_model.index))

        start_time = time.time()
        df_model_all_props = pd.DataFrame(dict())
        for i in sorted(self.pos_id_list):
            if self.pos[i]['type'] >= 0 and self.pos[i]['type'] < 1000:
                self.set_batch_status(status='正在分析型号属性sp' + str(i))
                print('Start analyze sp' + str(i))
                df_model['sp' + str(i)] = ''
                if i == self.pos_id_as_model:
                    continue
                df_sp = b_m_group['sp' + str(i)].value_counts(normalize = True).rename('prop_ratio')
                df_sp = df_sp.reset_index(level = 'sp' + str(i)).rename(columns = {'sp' + str(i): 'sp_value'})
                df_sp['sp_no'] = i
                df_sp_tmp = df_sp[df_sp['sp_value'] != '']
                keys = df_sp_tmp.index
                df_sp_tmp = df_sp_tmp.set_index(['sp_no', 'sp_value'], append = True)
                for k in keys:
                    idx_now = list(k) + [i]
                    get_max_value = df_sp_tmp.xs(tuple(idx_now)).idxmax()
                    df_model.loc[[k], ['sp' + str(i)]] = get_max_value.item()
                if df_model_all_props.size == 0:
                    df_model_all_props = df_sp.reset_index()
                else:
                    df_model_all_props = df_model_all_props.append(df_sp.reset_index(), ignore_index = True)

        df_model.reset_index(inplace = True)
        self.time_monitor.add_record(action='analyze_props_for_model_library', start=start_time, end=time.time(), num=len(df_model_all_props.index))

        start_time = time.time()
        if self.last_pid > 0:      # 如已有提交过的型号库，有pid的记录update六个数值，其余的insert
            df_to_update = df_model.loc[df_model.pid.notna(), ['model_count', 'tb_item_count', 'item_and_trade_count', 'brand_sales', 'sum_sales', 'recent_sales', 'pid']]
            values = tuple([tuple(r) for r in df_to_update.get_values()])
            sql = 'UPDATE {model_table_name} SET model_count = %s, tb_item_count = %s, item_and_trade_count = %s, brand_sales = %s, sum_sales = %s, recent_sales = %s WHERE pid = %s;'.format(model_table_name=self.model_table_name)
            print(values, '\n', sql)
            self.db_artificial_new.execute_many(sql, values)
            self.db_artificial_new.commit()
            df_model = df_model[df_model.pid.isna()]

        print('Writing to Mysqldb...')
        df_model.to_sql(self.model_table_name, self.db_an_engine, schema='artificial', chunksize=limit, if_exists='append', index=False)

        sql = 'UPDATE {model_table_name} a INNER JOIN brush.all_brand b ON a.alias_all_bid = b.bid SET a.brand = b.name;'.format(model_table_name=self.model_table_name)
        print(sql)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()
        self.time_monitor.add_record(action='write_model_library', start=start_time, end=time.time(), num=len(df_model.index))

        start_time = time.time()
        sql = 'TRUNCATE TABLE {props_table_name};'.format(props_table_name=self.props_table_name)      # props所有百分比和属性重新写入
        print(sql)
        self.db_artificial_new.execute(sql)
        df_model_all_props.to_sql(self.props_table_name, self.db_an_engine, schema='artificial', chunksize=limit, if_exists='append', index=False)
        self.time_monitor.add_record(action='write_props_for_model_library', start=start_time, end=time.time(), num=len(df_model_all_props.index))

        del df_item
        del df_model
        del df_model_all_props
        start_time = time.time()
        self.set_pid_in_item()
        sql = 'SELECT MAX(pid) FROM {model_table_name};'.format(model_table_name=self.model_table_name)
        max_pid = self.db_artificial_new.query_scalar(sql)
        self.set_batch_status(status='已生成型号库', last_pid=max_pid)
        self.time_monitor.add_record(action='set_pid_in_item', start=start_time, end=time.time(), num=max_pid)

    def update_case(self, table_name, fields_to_write, result_dict): # 数据多的时候运行慢
        field_on = 'pid'    # 型号库用
        sql = 'UPDATE {name} SET '.format(name=table_name)
        for field in fields_to_write:
            sql_sub = '\n {fw} = CASE {fo}'.format(fw=field, fo=field_on)
            for pid in result_dict:
                val = result_dict[pid].get(field, field)    # 如果没有需要修改的值，就等于自身
                val_w = 'TRIM(' + common.to_sql_string(val) + ')' if type(val) == str and val != field else val
                sql_sub += '\n WHEN {on} THEN {val}'.format(on=pid, val=val_w)
            sql_sub += '\n END,'
            sql += sql_sub
        sql = sql[:-1] + '\n WHERE {fo} IN ({s});'.format(fo=field_on, s=', '.join([str(pid) for pid in result_dict.keys()]))
        print(sql)
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

    def modify_model_library(self, csv_data = []):
        assert self.pos_id_brand and self.pos_id_as_model, '尚未存在品牌或设为as_model的清洗属性，无法进行型号库SKU库相关操作，请检查clean_pos表配置。'
        from_multi_sp_list = []
        for pos_id in self.pos:
            if self.pos[pos_id]['from_multi_sp']:
                from_multi_sp_list.append(pos_id)

        start_time = time.time()
        if csv_data:
            first_row = [x.split('_')[0] if x.startswith('sp') else x for x in csv_data[0]]
            m_data = csv_data[1:]
        else:
            first_row = ['sp'+str(i) for i in self.pos_id_list] + ['pid', 'alias_all_bid', 'name', 'all_bid_sp', 'confirmed']
            sql = 'SELECT {fs} FROM {model_table_name} WHERE confirmed > 0;'.format(fs =', '.join(first_row), model_table_name=self.model_table_name)
            m_data = self.db_artificial_new.query_all(sql)

        no_map = dict.fromkeys(first_row)
        for i in range(len(first_row)):
            no_map[first_row[i]] = i
        print('First Row Mapping:', no_map)
        self.time_monitor.add_record(action='read_confirmed_model_library', start=start_time, end=time.time(), num=len(m_data))

        start_time = time.time()
        pid_dict = dict()
        data = self.db_artificial_new.query_all('SELECT pid, alias_all_bid, name FROM {model_table_name};'.format(model_table_name=self.model_table_name))
        for row in data:
            pid_dict[(str(row[1]), row[2])] = row[0]

        check_list = []
        repeated_pid = set()
        confirmed_pid = set()
        lack_all_bid_sp = set()
        for row in m_data:
            pid = int(row[no_map['pid']])
            alias_all_bid = str(row[no_map['alias_all_bid']])
            name = row[no_map['name']]
            all_bid_sp = str(row[no_map['all_bid_sp']]).strip()
            changed_brand = row[no_map['sp'+str(self.pos_id_brand)]]
            confirmed = str(row[no_map['confirmed']])
            if confirmed != '0':
                if pid in confirmed_pid:
                    repeated_pid.add(pid)
                if not all_bid_sp or not all_bid_sp.isdigit() or (all_bid_sp != alias_all_bid and not changed_brand) or (all_bid_sp == alias_all_bid and changed_brand):
                    lack_all_bid_sp.add(pid)
            if pid_dict.get((alias_all_bid, name), -1) != pid:
                check_list.append((pid, alias_all_bid, name))
        msg = ''
        if check_list:
            msg += '型号库上传数据pid错位：{l}。'.format(l=check_list)
        if repeated_pid:
            msg += '型号库上传数据pid重复：{l}。'.format(l=repeated_pid)
        if lack_all_bid_sp:
            msg += '型号库上传数据pid待确认品牌相关字段：{l}。'.format(l=lack_all_bid_sp)
        if msg:
            self.error_msg.append(msg)
            self.set_batch_status()
            raise Exception(''.join(self.error_msg))
        self.time_monitor.add_record(action='check_model_library', start=start_time, end=time.time(), num=len(m_data))

        start_time = time.time()
        model_dict = dict()
        alias_pid_set = set()
        confirmed_pid_set = set()
        for row in m_data:
            pid = int(row[no_map['pid']])
            alias_all_bid = str(row[no_map['alias_all_bid']])
            name = row[no_map['name']]
            all_bid_sp = str(row[no_map['all_bid_sp']]) if row[no_map['all_bid_sp']] else None
            confirmed = str(row[no_map['confirmed']])
            if confirmed == '0':
                continue
            results_d = dict()
            confirmed_pid_set.add(pid)
            for i in self.pos_id_list:
                target = row[no_map['sp'+str(i)]].strip()
                if (i == self.pos_id_as_model) and (target or all_bid_sp != alias_all_bid):
                    target = self.standardize_for_model(target).strip(self.model_strip)
                    target = '' if target == name else target
                    search_bid = all_bid_sp if all_bid_sp else alias_all_bid
                    search_model = target if target else name
                    alias_pid = pid_dict.get((search_bid, search_model))
                    if alias_pid and alias_pid != pid:
                        results_d['alias_pid'] = alias_pid
                        alias_pid_set.add(alias_pid)
                results_d['sp'+str(i)] = target
            if results_d:
                for pos_id in from_multi_sp_list:
                    if self.pos[pos_id]['type'] < 0:
                        continue
                    for lis in self.pos[pos_id]['from_multi_sp']:
                        concat = ''.join([results_d['sp'+si] for si in lis])
                        if concat:
                            results_d['sp'+str(pos_id)] = concat
                            break
                results_d['all_bid_sp'] = int(all_bid_sp)
                results_d['confirmed'] = int(confirmed)
                model_dict[pid] = results_d
        self.time_monitor.add_record(action='process_model_library', start=start_time, end=time.time(), num=len(m_data))

        if model_dict:
            start_time = time.time()
            sql = 'UPDATE {model_table_name} SET alias_pid = 0;'.format(model_table_name=self.model_table_name)
            print(sql)
            self.db_artificial_new.execute(sql)
            self.db_artificial_new.commit()
            fields_to_write = ['sp'+str(i) for i in self.pos_id_list] + ['all_bid_sp', 'alias_pid', 'confirmed']
            self.update_case(self.model_table_name, fields_to_write, model_dict)
            self.time_monitor.add_record(action='update_model_library', start=start_time, end=time.time(), num=len(model_dict))

        pid_lack = alias_pid_set.difference(confirmed_pid_set)
        if pid_lack:
            msg = '请补充确认如下pid：{pid_lack}。'.format(pid_lack=pid_lack)
            self.error_msg.append(msg)
            self.set_batch_status(status='需人工确认型号库')
            raise Exception(''.join(self.error_msg))

    def writeback_model_to_item(self):
        assert self.pos_id_brand and self.pos_id_as_model, '尚未存在品牌或设为as_model的清洗属性，无法进行型号库SKU库相关操作，请检查clean_pos表配置。'
        start_time = time.time()
        model_field_list = ['pid', 'alias_pid', 'all_bid_sp'] + ['sp'+str(i) for i in self.pos_id_list]
        no_map = dict.fromkeys(model_field_list)
        for i in range(len(model_field_list)):
            no_map[model_field_list[i]] = i
        print('First Row Mapping:', no_map)
        model_dict = dict()
        data = self.db_artificial_new.query_all('SELECT {fs} FROM {model_table_name} WHERE confirmed > 0;'.format(fs=', '.join(model_field_list), model_table_name=self.model_table_name))
        for row in data:
            model_dict[row[no_map['pid']]] = row
        self.time_monitor.add_record(action='read_model_library_for_writeback', start=start_time, end=time.time(), num=len(model_dict))

        start_time = time.time()
        result_dict = dict()
        for r in data:
            results_d = dict()
            pid = r[no_map['pid']]
            alias_pid = r[no_map['alias_pid']]
            row = r
            if alias_pid > 0:
                if r[no_map['sp'+str(self.pos_id_as_model)]].strip():
                    results_d['sp'+str(self.pos_id_as_model)] = r[no_map['sp'+str(self.pos_id_as_model)]].strip()
                if r[no_map['sp'+str(self.pos_id_brand)]].strip():
                    results_d['sp'+str(self.pos_id_brand)] = r[no_map['sp'+str(self.pos_id_brand)]].strip()
                row = model_dict[alias_pid]
            if row[no_map['all_bid_sp']]:
                results_d['all_bid_sp'] = row[no_map['all_bid_sp']]
                brand_name = self.search_all_brand(results_d['all_bid_sp'])[0]
                alias_bid = self.search_all_brand(results_d['all_bid_sp'])[-1]
                if alias_bid > 0:
                    results_d['alias_all_bid_sp'] = alias_bid
                    brand_name = self.search_all_brand(alias_bid)[0]
                else:
                    results_d['alias_all_bid_sp'] = results_d['all_bid_sp']
                results_d['sp'+str(self.pos_id_brand)] = brand_name
            for i in self.pos_id_list:
                if self.pos[i]['type'] >= 0 and row[no_map['sp'+str(i)]].strip():
                    results_d['sp'+str(i)] = row[no_map['sp'+str(i)]].strip()
            if results_d:
                result_dict[pid] = results_d
        self.time_monitor.add_record(action='process_model_library_for_writeback', start=start_time, end=time.time(), num=len(data))

        if result_dict:
            start_time = time.time()
            fields_to_write = ['all_bid_sp', 'alias_all_bid_sp'] + ['sp'+str(i) for i in self.pos_id_list]
            self.update_case(self.item_table_name, fields_to_write, result_dict)
            self.time_monitor.add_record(action='update_item_from_model_library', start=start_time, end=time.time(), num=len(result_dict))

        self.set_last_id()
        self.item_distribution()
        self.set_batch_status(status='完成型号库回填')

    def get_trade_str(self, trades):
        ans = trades
        trade_prop_all = dict()
        if type(trades) is str:
            try:
                trade_prop_all = ujson.loads(common.prev_format_for_json(trades)) if trades else dict()
            except:
                try:
                    trade_prop_all = eval(common.prev_format_for_json(trades)) if trades else dict()
                except:
                    trade_prop_all = dict()
        elif type(trades) is dict:
            trade_prop_all = trades
        if trade_prop_all:
            for k in trade_prop_all:
                if type(trade_prop_all[k]) is not str:
                    trade_prop_all[k] = ' '.join(trade_prop_all[k])
                trade_prop_all[k] = unescape(trade_prop_all[k])
            ans = ' '.join(trade_prop_all.values())
        return ans

    def match_item_sku_pid(self, start_item_id, end_item_id):
        assert self.pos_id_brand and self.pos_id_as_model, '尚未存在品牌或设为as_model的清洗属性，无法进行型号库SKU库相关操作，请检查clean_pos表配置。'
        self.set_batch_status(status='正在导入SKU')
        skus = self.db_artificial_new.query_all('SELECT pid, alias_all_bid, brand, name FROM {model_table_name};'.format(model_table_name=self.model_table_name))
        sku_lib = dict()
        brand_sku_lib = dict()
        for tup in skus:
            pid, alias_all_bid, brand, name = tup
            sku = Sku(pid, self.standardize_for_model(brand + ' ' + name))
            sku_lib[pid] = (sku, alias_all_bid, brand, self.standardize_for_model(name))
            brand_sku_lib[alias_all_bid] = brand_sku_lib.get(alias_all_bid, dict())
            brand_sku_lib[alias_all_bid][pid] = sku

        self.set_batch_status(status='正在匹配宝贝SKU')
        sku_match = SkuMatch()

        sql = 'UPDATE {item_table_name} SET pid = 0 WHERE id > {last_id};'.format(item_table_name=self.item_table_name, last_id=self.last_id)
        print(sql)       # 只有增量部分set_pid
        self.db_artificial_new.execute(sql)
        self.db_artificial_new.commit()

        fields_read = ['id', 'name', 'trade_prop_all', 'alias_all_bid', 'mp'+str(self.pos_id_brand), 'sp'+str(self.pos_id_brand), 'sp'+str(self.pos_id_as_model)]
        no_map = dict()
        for i in range(len(fields_read)):
            if fields_read[i] not in no_map:
                no_map[fields_read[i]] = i
        fields_read[fields_read.index('alias_all_bid')] = 'IFNULL(alias_all_bid_sp, alias_all_bid)'
        print(fields_read, no_map)

        max_id = self.db_artificial_new.query_scalar('SELECT MAX(id) FROM {item_table};'.format(item_table=self.item_table_name))
        start_item_id = self.last_id if start_item_id == None or start_item_id < 0 else start_item_id
        end_item_id = max_id if end_item_id == None or end_item_id > max_id else end_item_id
        assert start_item_id < end_item_id, '参数输入错误，start_item_id需小于end_item_id。'
        start_id = start_item_id
        while True:
            sql = 'SELECT {fr} FROM {item_table_name} WHERE id > {start} AND id <= {end} AND clean_ver > 0 AND clean_type >= 0 ORDER BY id ASC LIMIT {limit};'.format(fr=','.join(fields_read), item_table_name=self.item_table_name, start=start_id, end=end_item_id, limit=self.default_limit)
            data = self.db_artificial_new.query_all(sql)
            cand_dict = dict()
            cand_limit = 1
            for row in data:
                item = Item(self.standardize_for_model(row[no_map['sp'+str(self.pos_id_brand)]] + ' ' + row[no_map['mp'+str(self.pos_id_brand)]] + ' ' + row[no_map['name']]), self.standardize_for_model(row[no_map['sp'+str(self.pos_id_as_model)]]), self.standardize_for_model(self.get_trade_str(row[no_map['trade_prop_all']])))
                print(row[no_map['id']], item.full_text, item.char_level_tokens)

                brand_results = sku_match.item_match_sku(brand_sku_lib.get(int(row[no_map['alias_all_bid']]), dict()).values(), item, mincov=0.6, mincom=0.7, limit=cand_limit)
                if brand_results:
                    r = brand_results[0]
                    print(r[0].id, r[0].name, r[0].char_level_tokens, r[1])
                    cand_dict[row[no_map['id']]] = r[0].id
                else:
                    print('item_alias_all_bid:', row[no_map['alias_all_bid']])
                    results = sku_match.item_match_sku([x[0] for x in sku_lib.values() if x[1] != int(row[no_map['alias_all_bid']])], item, mincov=0.6, mincom=0.7, limit=cand_limit)
                    if results:
                        r = results[0]
                        print(r[0].id, sku_lib[r[0].id][1:], r[0].name, r[0].char_level_tokens, r[1])
                        cand_dict[row[no_map['id']]] = r[0].id
            if cand_dict:
                values = tuple([(t[1], t[0]) for t in cand_dict.items()])
                sql = 'UPDATE {item_table_name} SET pid = %s WHERE id = %s;'.format(item_table_name=self.item_table_name)
                print(sql)
                self.db_artificial_new.execute_many(sql, values)
                self.db_artificial_new.commit()
            if data:
                start_id = data[-1][0]
            else:
                break
        self.set_batch_status(status='完成匹配宝贝SKU')

    def get_jdzy_models(self):
        assert self.pos_id_model, '尚未存在型号type属性，无法生成京东自营型号库，请检查clean_pos表配置。'
        self.set_batch_status(status='正在生成京东自营型号库')
        jdzy_condition = 'snum = 2 AND shop_type IN (11,21)'

        start_time = time.time()
        sql = 'SELECT id FROM {item_table_name} WHERE {condition};'.format(item_table_name=self.item_table_name, condition=jdzy_condition)
        data = self.db_artificial_new.query_all(sql)
        if not data:
            return
        else:
            len_jdzy_ids = len(data)
            count_id = self.db_artificial_new.query_scalar('SELECT COUNT(id) FROM {item_table};'.format(item_table=self.item_table_name))
            if len_jdzy_ids / count_id > 0.8:
                return
        jdzy_ids = ','.join([str(r[0]) for r in data])
        raw_data = self.read_items(condition='id IN ({id_list})'.format(id_list=jdzy_ids))
        result_dict = self.process_given_items(raw_data)
        self.time_monitor.add_record(action='process_jdzy_item', start=start_time, end=time.time(), num=len_jdzy_ids)

        start_time = time.time()
        df_item = pd.DataFrame.from_dict(result_dict, orient='index')
        df_item.rename(columns = {'sp' + str(self.pos_id_model): 'name'}, inplace = True)
        b_m_group = df_item.groupby(['alias_all_bid', 'name'])
        df_model = b_m_group.size().reset_index(name='model_count')
        self.time_monitor.add_record(action='analyze_jdzy_model_library', start=start_time, end=time.time(), num=len(df_model.index))

        start_time = time.time()
        df_model = df_model[(df_model['name'] != '') & (df_model['alias_all_bid'] > 0)]
        self.create_model_table(self.jdzy_suffix)
        print('Writing to Mysqldb...')
        df_model.to_sql(self.model_table_name_jdzy, self.db_an_engine, schema='artificial', chunksize=self.default_limit, if_exists='append', index=False)
        self.time_monitor.add_record(action='write_jdzy_model_library', start=start_time, end=time.time(), num=len(df_model.index))

    # @classmethod
    # def batch_overlap_within_same_cid(cls, not_batch_id_list = [], use_batch_id_list = []):
    #     cls.db_cleaner.connect()
    #     cls.db_an_conn = cls.db_artificial_new.connect()

    #     cid_batch_dict = dict()
    #     sql = 'SELECT c.source, c.cid, c.batch_id, b.eid FROM clean_category c, clean_batch b WHERE b.batch_id = c.batch_id AND c.deleteFlag = 0 AND b.deleteFlag = 0 GROUP BY c.source, c.cid, c.batch_id;'
    #     data = cls.db_cleaner.query_all(sql)
    #     for row in data:
    #         source, cid, batch_id, eid = row
    #         if not_batch_id_list and batch_id in not_batch_id_list:
    #             continue
    #         if use_batch_id_list and batch_id not in use_batch_id_list:
    #             continue
    #         cid_batch_dict[source, cid] = cid_batch_dict.get((source, cid), []) + [(batch_id, eid)]

    #     sql = 'SELECT batch_id, tb_item_id, source, month, p1, sp_category AS new_spid_category FROM entity_sp_modify;'
    #     print(sql)
    #     df_modify_category = pd.read_sql(sql, cls.db_an_conn)

    #     df_overlap_dict = dict()
    #     for key in cid_batch_dict:
    #         if len(cid_batch_dict[key]) > 1:
    #             print(key, cid_batch_dict[key])
    #             source, cid = key
    #             df_item_list = []
    #             for r in cid_batch_dict[key]:
    #                 batch_id, eid = r
    #                 sp_no_category = cls.db_cleaner.query_scalar('SELECT pos_id FROM clean_pos WHERE batch_id = {batch_id} AND type = 900 AND deleteFlag = 0;'.format(batch_id=batch_id))
    #                 try:
    #                     sql = "SELECT id, tb_item_id, source, cid, month, p1, sp{i}, spid{i} FROM entity_{eid}_item WHERE source = '{source}' AND cid = {cid};".format(eid=eid, source=source, cid=cid, i=sp_no_category)
    #                     print(sql)
    #                     df_item = pd.read_sql(sql, cls.db_an_conn)
    #                 except:
    #                     continue
    #                 else:
    #                     df_item.rename(columns = {'sp' + str(sp_no_category): 'sp_category', 'spid' + str(sp_no_category): 'spid_category'}, inplace = True)
    #                     df_item['batch_id'] = batch_id
    #                     try:
    #                         sql = "SELECT tb_item_id, source, month, p1, spidV FROM entity_{eid}_item_modify WHERE spidN = 'spid{i}' AND source = '{source}';".format(eid=eid, source=source, i=sp_no_category)
    #                         print(sql)
    #                         df_modify_spid = pd.read_sql(sql, cls.db_an_conn)
    #                     except:
    #                         pass
    #                     else:
    #                         if not df_modify_spid.empty:
    #                             df_item = df_item.merge(df_modify_spid, how = 'left', on = ['source', 'tb_item_id', 'p1', 'month'])
    #                             df_item['spid_category'] = df_item['spidV'].fillna(df_item['spid_category'])
    #                             del df_item['spidV']
    #                     df_item_list.append(df_item)
    #             if not df_item_list:
    #                 continue
    #             df_total_item = df_item_list[0]
    #             for df in df_item_list[1:]:
    #                 df_total_item = df_total_item.append(df)
    #             df_sp = df_total_item.loc[ (df_total_item['sp_category'] != '') & (df_total_item['sp_category'] != cls.default_other), ['source', 'cid', 'tb_item_id', 'p1', 'month', 'sp_category'] ].drop_duplicates()
    #             df_ca = df_sp.groupby(['source', 'cid', 'tb_item_id', 'p1', 'month'])['sp_category'].count().rename('category_count').reset_index()
    #             df_ov = df_ca[df_ca['category_count'] > 1]
    #             if not df_ov.empty:
    #                 df_item_ov = df_total_item.merge(df_ov, how = 'inner', on = ['source', 'cid', 'tb_item_id', 'p1', 'month'])
    #                 df_item_ov = df_item_ov.merge(df_modify_category, how = 'left', on = ['batch_id', 'source', 'tb_item_id', 'p1', 'month'])
    #                 df_overlap_dict[key] = df_item_ov

    #     cls.db_cleaner.close()
    #     cls.db_artificial_new.close()
    #     return cid_batch_dict, df_overlap_dict
