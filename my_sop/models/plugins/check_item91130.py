# encoding: utf-8
import sys
from os.path import abspath, join, dirname, exists

from sqlalchemy import null
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
from os import listdir, mkdir, chmod
import application as app
import re
import collections

from models.plugin_manager import Plugin
from models.plugins.check_item import CheckItem

class main(CheckItem):
    check_table = None
    ans_table = None
    product_table = None
    db26 = None
    clean_brush = None

    def get_err_na_list(self):
        # 6.7规则为23.0717新添加
        return ['1_1', '1_2', '2_1', '2_2', '2_3', '2_4', '3', '4', '5', '6', '7']

    # 检查sku答题
    # 检查的时候要把强制通过的排除掉
    def check_sku(self, pid_list = [], err_na = ''):
        pid_list = [str(pid) for pid in pid_list]
        # sp1  子品类
        # sp2  百洋子品类
        # sp11 标准_人群（单选_
        # sp16 剂型
        # sp18 单品数
        # sp19 单品规格（液体单位为ml）
        # sp20 总规格（液体单位为ml）
        # sp40 每日服用次数
        # sp41 每次服用颗/粒/片数
        # sp42 每次服用量(g/ml，无颗数才答)
        # sp56 BLK_人群
        sql = '''
            select pid, spid56, spid11, name, spid16, spid18, spid19, spid20, spid40, spid41, spid42
            from {product_table}
            {where_sql}
        '''.format(ans_table=self.ans_table, product_table=self.product_table, where_sql = '' if pid_list == [] else "where pid in (\'{}\')".format("\', \'".join(pid_list)))
        data = self.db26.query_all(sql)
        print('需要检查的题目数量' + str(len(data)))

        error_info = set()
        id_list = []
        for info in data:
            err_len = len(error_info)
            ans_pid, sp56, sp11, sku_name, sp16, sp18, sp19, sp20, sp40, sp41, sp42 = list(info)
            id_list.append(ans_pid)
            if err_na == '':
                error_info.add(tuple([ans_pid] + self.check_1_1(sp56, sp11)))
                error_info.add(tuple([ans_pid] + self.check_1_2(sp56, sp11, sku_name)))
                error_info.add(tuple([ans_pid] + self.check_2_1(sp16, sp18, sp19, sp20)))
            elif err_na == '1_1':
                error_info.add(tuple([ans_pid] + self.check_1_1(sp56, sp11)))
            elif err_na == '1_2':
                error_info.add(tuple([ans_pid] + self.check_1_2(sp56, sp11, sku_name)))
            elif err_na == '2_1':
                error_info.add(tuple([ans_pid] + self.check_2_1(sp16, sp18, sp19, sp20)))
            elif err_na == '6':
                error_info.add(tuple([ans_pid] + self.check_6(sku_name)))
            elif err_na == '7':
                error_info.add(tuple([ans_pid] + self.check_7(sku_name, sp18, sp19, sp20)))

            # 去除空白错误
            try:
                error_info.remove(tuple([ans_pid] + []))
            except:
                pass

        # 有个问题，如果被强制通过的属性以后修改了，那检测错误会依旧强制通过
        count_sql = '''
            select a.ans_pid, a.max_check_count, a.err_na, b.ans_pid
            from
            (
                select ans_pid, max(check_count) as max_check_count, err_na
                from {check_table}
                group by ans_pid, err_na
            ) a
            left join
            (
                select ans_pid, check_count, err_na, err_status from {check_table} where err_status = 2
            ) b
            on a.ans_pid = b.ans_pid and a.max_check_count = b.check_count and a.err_na = b.err_na
        '''.format(check_table=self.check_table)
        count_data = self.db26.query_all(count_sql)
        count_dict = {}
        force_pass_dict = {}
        for info in count_data:
            count_dict[info[0]] = info[1]
            if info[3] != None:
                force_pass_dict[(info[0], info[2])] = info[1]
        write_info = []
        for info in error_info:
            if force_pass_dict.get((info[0], info[1])) != None:
                write_info.append(
                    list(info) + [
                        2,
                        force_pass_dict.get((info[0], info[1])) + 1
                    ]
                )
            elif info[3] == '无错误':
                write_info.append(
                    list(info) + [
                        0,
                        count_dict.get(info[0], 0) + 1
                    ]
                )
            else:
                write_info.append(
                    list(info) + [
                        1,
                        count_dict.get(info[0], 0) + 1
                    ]
                )

        insert_sql = '''
            insert into {check_table}
            (ans_pid, err_na, err_sp, err_info, err_status, check_count)
            values
            (%s, %s, %s, %s, %s, %s)
        '''.format(check_table=self.check_table)
        self.db26.execute_many(insert_sql, tuple(write_info))
        self.db26.commit()

    # 检查属性答题答题
    def check_item(self, item_id_list = [], err_na = ''):
        pid_list = [str(pid) for pid in pid_list]
        # sp56 BLK_人群
        # sp11 标准_人群（单选_
        # sp16 剂型
        # sp18 单品数
        # sp19 单品规格（液体单位为ml）
        # sp20 总规格（液体单位为ml）
        sql = '''
            select a.id, a.spid56, a.spid11, b.name, a.spid16, a.spid18, a.spid19, a.spid20
            from {ans_table} a
            left join {product_table} b
            on a.pid = b.pid
            where a.flag != 0 {where_sql}
        '''.format(ans_table=self.ans_table, product_table=self.product_table, where_sql = '' if item_id_list == [] else "and ans_pid in (\'{}\')".format("\', \'".join(item_id_list)))
        data = self.db26.query_all(sql)
        print('需要检查的题目数量' + str(len(data)))

    def check_1_1(self, sp56, sp11):
        err_type = '1_1'
        err_sp = 'sp56,sp11'
        error_info = "BLK_人群为{}时，标准_人群（单选_不是{}"
        error_info_2 = "标准_人群（单选_为{}时，BLK_人群不是{}"
        check_dict = {
            '婴幼儿/儿童': ('婴幼儿/儿童', '通用'),
            '孕产妇': ('孕产妇', '女士', '通用'),
            '男士(备孕)': ('男士(备孕)', '男士')
        }
        check_dict_2 = {
            '男士': ('男士(备孕)', '男士'),
            '女士': ('孕产妇', '女士'),
            '通用': ('通用', '婴幼儿/儿童', '孕产妇'),
        }

        if check_dict.get(sp56) != None and sp11 not in check_dict.get(sp56):
            return [err_type, err_sp, error_info.format(sp56, ','.join(check_dict.get(sp56)))]
        elif sp56 in ('中老年男性', '中老年女性', '中老年', '男士', '女士', '通用', '不适用') and sp11 != sp56:
            return [err_type, err_sp, '当BLK_人群为{}时，标准_人群（单选_和BLK_人群为不一致'.format(sp56)]
        
        if check_dict_2.get(sp11) != None and sp56 not in check_dict_2.get(sp11):
            return [err_type, err_sp, error_info_2.format(sp11, ','.join(check_dict_2.get(sp11)))]
        elif sp11 in ('婴幼儿/儿童', '孕产妇', '男士(备孕)', '中老年男性', '中老年女性', '中老年', '不适用') and sp11 != sp56:
            return [err_type, err_sp, '当标准_人群（单选_为{}时，标准_人群（单选_和BLK_人群为不一致'.format(sp11)]
        return [err_type, err_sp, '无错误']

    def check_1_2(self, sp56, sp11, sku_name):
        err_type = '1_2'
        err_sp = 'sp56,sp11,sku_name'
        check_dict = {re.compile('婴|儿|少|幼'): '婴幼儿/儿童',
                        re.compile('孕妇|孕期|怀孕|孕产|孕前|乳母|产前|产后|女.*孕|孕.*女'): '孕产妇',
                        re.compile('男.*孕|孕.*男'): '男士(备孕)',
                        re.compile('中老年男'): '中老年男性',
                        re.compile('中老年女|更年期'): '中老年女性',
                        re.compile('老年'): '中老年',
                        re.compile('男'): '男士',
                        re.compile('女|卵巢'): '女士',
                        re.compile('成人'): '通用'}
        error_info = 'sku_name中含有{}，但是BLK_人群或者标准_人群（单选_不是{}'

        for matcher,value in check_dict.items():
            if matcher.findall(sku_name) != [] and (sp56 != value or sp11 != value):
                return [err_type, err_sp, error_info.format(','.join(matcher.findall(sku_name)), value)]

        return [err_type, err_sp, '无错误']

    def check_2_1(self, sp16, sp18, sp19, sp20):
        err_type = '2_1'
        err_sp = 'sp16,sp18,sp19,sp20'
        error_info = '剂型为{}时 单品数或单品规格或总规格逻辑, 单位不匹配'

        check_dict = {
            '胶囊': [re.compile(r'粒'), ''],
            '粉剂': [re.compile(r'袋|瓶|桶|罐'), re.compile(r'g')],
            '液体': ['', re.compile(r'ml')],
            '泡腾片': [re.compile(r'片'), ''],
            '软糖': [re.compile(r'粒'), ''],
            '片剂': [re.compile(r'片'), ''],
            '丸剂': [re.compile(r'粒'), '']
        }

        if sp16 == '其它':
            return [err_type, err_sp, '剂型为其它的时候,单品数 单品规格 总规格逻辑全都需要进行检查']
        elif check_dict.get(sp16) != None:
            unit_check, capacity_check = check_dict.get(sp16)
            if unit_check != '' and unit_check.findall(sp18) == []:
                return [err_type, err_sp, error_info.format(sp16)]
            if capacity_check != '' and capacity_check.findall(sp19) == [] and capacity_check.findall(sp20) == []:
                return [err_type, err_sp, error_info.format(sp16)]

        return [err_type, err_sp, '无错误']

    def check_2_2(self, sku_name, sp18, sp19, sp20):
        err_type = '2_2'
        err_sp = 'sku_name,sp18,sp19,sp20'
        error_info = 'sku名称，单品数，单品规格，总规格不匹配'

        brackets_match = re.compile(r'\[.*\]')
        have_brack = brackets_match.findall(sku_name) != []
        unit_match = re.compile(r'g|ml')
        num_match = re.compile(r'\d+\.?\d*')
        operator_match = re.compile(r'\*\/\-')

        if sp19 == '' and have_brack:
            return [err_type, err_sp, error_info]
        elif sp19 != '' and not have_brack:
            return [err_type, err_sp, error_info]
        elif unit_match.findall(sp18) == [] or unit_match.findall(sp19) == []:
            return [err_type, err_sp, error_info]
        elif unit_match.findall(sp19) == []:
            return [err_type, err_sp, error_info]
        elif operator_match.findall(sp18) != []:
            return [err_type, err_sp, '单品数中有除+之外的符号']
        else:
            num18_list = num_match.findall(sp18)
            num19_list = num_match.findall(sp19)
            num20_list = num_match.findall(sp20)
            num18 = sum([float(num) for num in num18_list])
            num19 = sum([float(num) for num in num19_list])
            if len(num20_list) == 1 and num18 * num19 != float(num20_list[0]):
                return [err_type, err_sp, '单品数*单品规格不等于总规格']

        return [err_type, err_sp, '无错误']
    
    def check_2_3(self, sp16, sp40, sp41, sp42):
        err_type = '2_3'
        err_sp = 'sp16,sp40,sp41,sp42'
        error_info = '剂型,每日服用次数,每次服用颗/粒/片数,每次服用量 规则不匹配'

        if sp40.isnumeric() and sp41 == '未注明' and sp42 != '' and sp16 not in ('粉剂', '液体'):
            return [err_type, err_sp, error_info]
        elif sp40.isnumeric() and sp41.isnumeric() and sp42 == '' and sp16 not in ('胶囊', '软糖', '泡腾片', '片剂', '丸剂', '其它'):
            return [err_type, err_sp, error_info]
        elif sp40 == '未注明' and sp41 != '未注明':
            return [err_type, err_sp, error_info]

        return [err_type, err_sp, '无错误']
    
    def check_2_4(self, sku_name, sp18, ):
        pass

    def check_3(self, sp1, sp2):
        err_type = '2_3'
        err_sp = 'sp1,sp2'
        error_info = '子品类和仙乐子品类对应关系错误'
        category_dict = {
            '钙': ['钙'],
            '维生素': ['复合维生素'],
            '氨糖软骨素': ['氨糖软骨素'],
            '蛋白粉': ['营养蛋白粉','胶原蛋白','增肌粉'],
            '矿物质': [''],
            '植物精华': ['膳食纤维','酵素','胶原蛋白'],
            '益生菌': ['益生菌'],
            '鱼油': ['鱼油'],
            '辅酶Q10': [''],
            '其它保健品': [''],
            '美容保健': ['胶原蛋白'],
            '体重管理': [''],
            '氨基酸': [''],
            '动物精华': [''],
            '酵素': ['酵素'],
            '膳食纤维': ['膳食纤维'],
            '其它': ['']
        }

        xianle = category_dict.get(sp1)
        if xianle != None and sp2 not in xianle:
            return [err_type, err_sp, error_info]

        return []

    def check_4(self):
        pass

    def check_5(self):
        pass

    def check_6(self, sku_name):
        err_type = '6'
        err_sp = ''
        error_info = ''
        index_spid_dict = {
            2: 1,
            3: 4,
            4: 5,
            5: 7,
            6: 11,
            7: 13,
            8: 14,
            9: 16,
            10: 40,
            11: 41,
            12: 42,
            13: 45,
            14: 46,
            15: 51,
            16: 56,
            17: 58,
            18: 59,
            19: 62,
            20: 63,
            21: 70,
            22: 73,
            23: 74,
            24: 75,
            25: 78,
            26: 80
        }



        real_name = sku_name.split('(')[0].split('[')[0]

        sql = '''
            select pid, name, spid1, spid4, spid5, spid7, spid11, spid13, spid14, spid16, spid40, spid41, spid42, spid45, spid46, spid51, spid56, spid58, spid59, spid62, spid63, spid70, spid73, spid74, spid75, spid78, spid80
            from {product_table}
            {where_sql}
        '''.format(product_table=self.product_table, where_sql = "where name like '%{}%'".format(real_name))
        print(sql)
        data = self.db26.query_all(sql)

        sql = '''
            select spid, name from dataway.clean_props where eid = 91130
        '''
        spid_data = self.db26.query_all(sql)
        spid_name_dict = {info[0]: info[1] for info in spid_data}
        # 需要检测所有属性
        base_info = []
        pid_list = []
        error_spid_list = set()

        if len(data) > 0:
            base_info = data[0]
        else:
            return [err_type, 'sku_name', '型号库中没有对应{}的sku'.format(real_name)]
        # import pdb;pdb.set_trace()

        for i in range(1, len(data)):
            info = data[i]
            pid_list.append([info[0], info[1]])
            for m in range(2, len(base_info)):
                if info[m] != base_info[m]:
                    error_spid_list.add(spid_name_dict[index_spid_dict[m]])

        if len(error_spid_list) > 0:
            return [err_type, 'sku_name', str(pid_list) + ' 这几个sku被认为是相同sku，进行属性检测。 检测结果中，' + str(error_spid_list) + '这几个属性在这几个sku中不严格相同，请检查']
        else:
            return []
    
    def check_7(self, sku_name, sp18, sp19, sp20):
        err_type = '7'
        # sp18：单品数, sp19：单品规格（液体单位为ml）, sp20：总规格（液体单位为ml）
        err_sp = 'sp18, sp19, sp20'
        error_info = ''
        err_no = 1


        if ('[' in sku_name or ']' in sku_name) and sp19 == '':
            error_info += str(err_no) + ':sku名称中有中括号, 但单品规格为空。'
            err_no += 1
        if ('[' not in sku_name and ']' not in sku_name) and sp19 != '':
            error_info += str(err_no) + ':sku名称中没有有中括号, 但单品规格不为空。'
            err_no += 1
        if sp18.isdigit():
            error_info += str(err_no) + ':单品数没有单位。'
            err_no += 1
        if 'g' in sp18.lower() and 'ml' in sp18.lower():
            error_info += str(err_no) + ':单品数中有g或者ml。'
            err_no += 1
        if 'g' not in sp19.lower() and 'ml' not in sp19.lower():
            error_info += str(err_no) + ':单品规格中有g或者ml。'
            err_no += 1
        
        pattern = r'\d+\.?\d*'
        sp18_list = re.findall(pattern, sp18)
        sp19_list = re.findall(pattern, sp19)
        sp20_list = re.findall(pattern, sp20)
        if len(sp18_list) != 1 or len(sp19_list) != 1 or len(sp20_list) != 1:
            error_info += str(err_no) + ':单品数,单品规格,总规格中有属性没有数字或者数字超过1个。'
            err_no += 1
        else:
            num_18 = float(sp18_list[0])
            num_19 = float(sp19_list[0])
            num_20 = float(sp20_list[0])
            if num_18 * num_19 != num_20:
                error_info += str(err_no) + ':单品数和单品规格的乘积不等于总规格。'
                err_no += 1
        
        if len(re.findall(r'\||\,|\.|\?|\!|\~|\-|\_|\ˉ|\=|\;|\:|\/|\*|\^|\`|\·', sp18)) > 0:
            error_info += str(err_no) + ':单品数有除”+”以外的符号。'
            err_no += 1

        if err_no == 1:
            return []
        else:
            return [err_type, err_sp, error_info]