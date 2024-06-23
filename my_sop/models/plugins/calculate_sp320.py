import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
import re
from models.plugins import calculate_sp

class main(calculate_sp.main):
    def init_read_require(self):
        self.split_word = 'Ծ‸ Ծ'

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict
    
    def process_row(self, item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map):
        mp = dict()
        sp = dict()
        detail = dict()

        for pos_id in self.batch_now.pos_id_list:
            mp[pos_id], sp[pos_id] = ('', '')
            judgelist = []

            if self.batch_now.pos[pos_id]['type'] == 900: # 子品类
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, 'product,name+product', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                data_filter = {'all_bid': all_bid, 'alias_all_bid': alias_all_bid, 'cid': cid, 'item_id': item_id}
                for fkey in self.batch_now.category_filter_keys:
                    if fkey not in data_filter:
                        data_filter[fkey] = f_map[fkey]
                mp[pos_id], sp[pos_id] = self.batch_now.source_cid_map_sub_batch(source, data_filter, judgelist)
                sub_batch_id = mp[pos_id]
                clean_type = -1 if sub_batch_id == 0 else self.batch_now.sub_batch[sub_batch_id]['clean_type']    # others
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] >= 700 and self.batch_now.pos[pos_id]['type'] <= 701: # 原始文本
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, '', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                if judgelist:
                    for tmp_text in judgelist:
                        if tmp_text:
                            mp[pos_id] = tmp_text
                            if self.batch_now.pos[pos_id]['type'] != 701:
                                sp[pos_id] = tmp_text.upper().strip()   # 因型号库props_value分组统计unique_key限定，标准化为全大写
                            else:
                                sp[pos_id] = tmp_text.strip() # 701不要大写
                            break
                self.batch_now.print_log('原始文本列表: \n', judgelist)
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 1: # 有限种类：关键词匹配
                if self.batch_now.pos[pos_id]['p_key']:
                    seq = self.get_pkey(pos_id, source, cid, 'product,name', prop_all.keys())
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [prop_all[k] for k in key_list if prop_all.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                elif self.batch_now.pos[pos_id]['p_no']:
                    seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                    for combined_keys in seq:
                        key_list = combined_keys.split('+')
                        v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                        judgelist.append(self.batch_now.separator.join(v))
                mp[pos_id], sp[pos_id], length = self.batch_now.get_prop_value(self.batch_now.target_dict.get((sub_batch_id, pos_id)), judgelist, self.batch_now.pos[pos_id]['target_no_rank'])
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 800: # 价格段
                if self.batch_now.target_dict.get((sub_batch_id, pos_id)):
                    mp[pos_id], sp[pos_id] = self.batch_now.price_limit(f_map.get('price', f_map.get('avg_price', 0)), self.batch_now.target_dict[sub_batch_id, pos_id])
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] == 1000: # 品牌
                mp[pos_id], sp[pos_id], brand_cnen_name_tuple, alias_brand_cnen_name_tuple = self.batch_now.alias_brand([prop_all['name'], all_bid, alias_all_bid, cid])
                detail[pos_id] = (brand_cnen_name_tuple, alias_brand_cnen_name_tuple)
                en_cn_brand = set(brand_cnen_name_tuple + alias_brand_cnen_name_tuple)
                current_brand = sp[pos_id]
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif f_map['snum'] == 11 and self.batch_now.pos[pos_id]['type'] >= 1100 and self.batch_now.pos[pos_id]['type'] < 1200: #抖音模型数据更新
                if self.batch_now.dy_status == 0:
                    assert '项目的dy_status状态为不使用抖音模型数据时，清洗抖音相关字段，请修正后重试！'
                else:
                    dy_bid = f_map['prop_all'].get('抖音辅助_bid', 0)
                    dy_bname = f_map['prop_all'].get('抖音辅助_brand', '')
                    dy_category = f_map['prop_all'].get('抖音辅助_category', '')

                    if self.batch_now.pos[pos_id]['type'] == 1100:
                        sp[pos_id] = dy_bid
                    elif self.batch_now.pos[pos_id]['type'] == 1101:
                        sp[pos_id] = dy_bname
                    elif self.batch_now.pos[pos_id]['type'] == 1102:
                        sp[pos_id] = dy_category

            elif self.batch_now.pos[pos_id]['type'] in (100, 101): # 型号/sku
                if sub_batch_id > 0:
                    if self.batch_now.pos[pos_id]['p_key']:
                        seq = self.get_pkey(pos_id, source, cid, 'sub_brand_name,product', prop_all.keys())
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [prop_all[k] for k in key_list if prop_all.get(k)]
                            judgelist.append('|'.join(v))
                    elif self.batch_now.pos[pos_id]['p_no']:
                        seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [f_map.get(k, '') for k in key_list if f_map.get(k)]
                            judgelist.append(self.batch_now.separator.join(v))
                    mp[pos_id], sp[pos_id] = self.batch_now.get_model(judgelist, trade_prop_all, self.batch_now.ref_models.get(alias_all_bid, set())) if self.batch_now.pos[pos_id]['type'] == 100 else self.batch_now.get_sku(judgelist, trade_prop_all, set())
                    self.batch_now.print_log('Model:', sp[pos_id])
                    remove_cate = ['型号', self.batch_now.batch_name, self.batch_now.sub_batch[sub_batch_id]['name']]
                    sp[pos_id] = common.remove_spilth(sp[pos_id], erase_all = [self.batch_now.standardize_for_model(b) for b in en_cn_brand] + remove_cate)
                    self.batch_now.print_log('Model:', sp[pos_id], '      Removed brand and category:', en_cn_brand, remove_cate)
                    sp[pos_id] = common.remove_spilth(self.batch_now.invisible.sub(' ', sp[pos_id]), erase_all = self.batch_now.model_should_remove, erase_duplication = self.batch_now.model_no_duplicate).strip(self.batch_now.model_strip)
                    self.batch_now.print_log('Model:', sp[pos_id], '      Removed bracket:', self.batch_now.model_should_remove)
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

            elif self.batch_now.pos[pos_id]['type'] >= 2 and self.batch_now.pos[pos_id]['type'] < 100: # 提取量词
                if sub_batch_id > 0:
                    can_pure_num = []
                    if self.batch_now.pos[pos_id]['p_key']:
                        seq = self.get_pkey(pos_id, source, cid, 'product,name', prop_all.keys())
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = ''
                            for k in key_list:
                                p_name = self.batch_now.standardize_for_model(k)
                                start = p_name.rfind('(')
                                unit_from_prop = p_name[start:].replace(')','').strip() if start > 0 else ''
                                value = self.batch_now.standardize_for_model(prop_all.get(k,''))
                                if value:
                                    if value[-1] == '.' or value[-1].isdigit() or value[-1] in self.batch_now.chn_numbers:
                                        v += value + unit_from_prop + self.batch_now.separator
                                    else:
                                        v += value + self.batch_now.separator
                            judgelist.append(v)
                            if combined_keys in ['product', 'name']:
                                can_pure_num.append(False)
                            else:
                                can_pure_num.append(True)
                    elif self.batch_now.pos[pos_id]['p_no']:
                        seq = self.batch_now.pos[pos_id]['p_no'].split(',')
                        for combined_keys in seq:
                            key_list = combined_keys.split('+')
                            v = [self.batch_now.standardize_for_model(f_map.get(k, '')) for k in key_list if self.batch_now.standardize_for_model(f_map.get(k, ''))]
                            judgelist.append(self.batch_now.separator.join(v))
                            if combined_keys in ['product', 'name']:
                                can_pure_num.append(False)
                            else:
                                can_pure_num.append(True)
                    mp[pos_id], sp[pos_id], detail[pos_id] = self.batch_now.quantify_num(self.batch_now.pos[pos_id], judgelist, can_pure_num)
                    self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')
            elif pos_id in (6,7,8,10):
                seq = self.get_pkey(pos_id, source, cid, '', prop_all.keys())
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
                
                re_dict = {
                    6: re.compile(r'一人份|宝宝|婴儿|婴幼儿|辅食|无糖|0糖|零糖|零脂|0脂|0脂油|零脂肪|脱脂|伴手礼|便携|不加碘|未加碘|不加糖|餐饮装|陈酿|大包装|代餐|赤藓糖醇|代糖|单晶|低卡|低钠|低糖|薄盐|低盐|减盐|低脂|减脂|定制|非转基因|复合|广式|湖盐|淮盐|加碘|家庭用|家用|金标|进口|经典|矿盐|量贩装|0添加|零添加|无添加|无添加剂|露营|买1送1|买一送一|拍一赠一|轻食|日式|散装|开店|商用|上等|泰式|套装|特产|特级|天然|添加防腐剂|不含碘|无典|无碘|不含抗结剂|不加抗结剂|无抗结剂|无硫|无盐|无油|无渣|0蔗糖|无蔗糖|细盐|细盐巴|线下商超|新包装|有机|中华老字号|自制', re.I),
                    7: re.compile(r'牛油|豆豉|外婆菜|萝卜|花生酱|扇贝|黄油|芥末|苹果|蛋黄|青瓜|黄瓜|木糖醇|百香果|桂花|桃子|蜜桃|芒果|蓝莓|榴莲|虾酱|虾子酱|虾米酱|虾仔酱|虾仁|鲅鱼|蚕豆|蟹黄|蟹粉|蒜蓉|蒜末|花蛤|瑶柱|鸡枞菌|牛肝菌|松茸菌|竹笋|雷笋|春笋|烤笋|烧椒|剁椒|番茄|黄豆|玉米|丸大豆|黑松露|松茸|黑豆|八角|白胡椒|芝麻|辣椒|草莓|大豆|干贝|黑胡椒|红枣|纳豆|蘑菇|奶酪|牛肉|糯米|泡菜|虾米|香菇|鱼子|鱼籽|鱼', re.I),
                    8: re.compile(r'玻璃瓶|超值装|袋|倒置|独立包装|罐|盒|挤挤|挤压瓶|颗装|块装|礼盒装|量贩装|散装|塑料瓶|套装|桶|箱|新包装|牙膏装|原装|整箱|组合|瓶', re.I),
                    10: re.compile(r'特辣|微辣|辣而不燥|蒜香|咸鲜|酸甜|甘甜|清淡|浓烈|质感|芳醇|平衡|暴辣|川香|混合口味|麻辣|蜜汁味|鲜辣|鲜香|香辣|香甜|甜辣|咸辣', re.I),
                }



                tran_dict = {
                    6: {
                        '一人份': '一人份',
                        '宝宝': '宝宝',
                        '婴儿': '婴儿',
                        '婴幼儿': '婴幼儿',
                        '辅食': '辅食',
                        '无糖': '0糖',
                        '0糖': '0糖',
                        '零糖': '0糖',
                        '零脂': '0脂',
                        '0脂': '0脂',
                        '0脂油': '0脂',
                        '零脂肪': '0脂',
                        '脱脂': '0脂',
                        '伴手礼': '伴手礼',
                        '便携': '便携',
                        '不加碘': '不加碘',
                        '未加碘': '不加碘',
                        '不加糖': '不加糖',
                        '餐饮装': '餐饮装',
                        '陈酿': '陈酿',
                        '大包装': '大包装',
                        '代餐': '代餐',
                        '赤藓糖醇': '代糖',
                        '代糖': '代糖',
                        '单晶': '单晶',
                        '低卡': '低卡',
                        '低钠': '低钠',
                        '低糖': '低糖',
                        '薄盐': '低盐',
                        '低盐': '低盐',
                        '减盐': '低盐',
                        '低脂': '低脂',
                        '减脂': '低脂',
                        '定制': '定制',
                        '非转基因': '非转基因',
                        '复合': '复合',
                        '广式': '广式',
                        '湖盐': '湖盐',
                        '淮盐': '淮盐',
                        '加碘': '加碘',
                        '家庭用': '家用',
                        '家用': '家用',
                        '金标': '金标',
                        '进口': '进口',
                        '经典': '经典',
                        '矿盐': '矿盐',
                        '量贩装': '量贩装',
                        '0添加': '零添加',
                        '零添加': '零添加',
                        '无添加': '零添加',
                        '无添加剂': '零添加',
                        '露营': '露营',
                        '买1送1': '买1送1',
                        '买一送一': '买1送1',
                        '拍一赠一': '买1送1',
                        '轻食': '轻食',
                        '日式': '日式',
                        '散装': '散装',
                        '开店': '商用',
                        '商用': '商用',
                        '上等': '上等',
                        '泰式': '泰式',
                        '套装': '套装',
                        '特产': '特产',
                        '特级': '特级',
                        '天然': '天然',
                        '添加防腐剂': '添加防腐剂',
                        '不含碘': '无碘',
                        '无典': '无碘',
                        '无碘': '无碘',
                        '不含抗结剂': '无抗结剂',
                        '不加抗结剂': '无抗结剂',
                        '无抗结剂': '无抗结剂',
                        '无硫': '无硫',
                        '无盐': '无盐',
                        '无油': '无油',
                        '无渣': '无渣',
                        '0蔗糖': '无蔗糖',
                        '无蔗糖': '无蔗糖',
                        '细盐': '细盐',
                        '细盐巴': '细盐',
                        '线下商超': '线下商超',
                        '新包装': '新包装',
                        '有机': '有机',
                        '中华老字号': '中华老字号',
                        '自制': '自制'
                    },
                    7:{
                        '牛油': '牛油',
                        '豆豉': '豆豉',
                        '外婆菜': '外婆菜',
                        '萝卜': '萝卜',
                        '花生酱': '花生酱',
                        '扇贝': '扇贝',
                        '黄油': '黄油',
                        '芥末': '芥末',
                        '苹果': '苹果',
                        '蛋黄': '蛋黄',
                        '青瓜': '青瓜',
                        '黄瓜': '黄瓜',
                        '木糖醇': '代糖',
                        '百香果': '百香果',
                        '桂花': '桂花',
                        '桃子': '桃子',
                        '蜜桃': '桃子',
                        '芒果': '芒果',
                        '蓝莓': '蓝莓',
                        '榴莲': '榴莲',
                        '虾酱': '虾',
                        '虾子酱': '虾',
                        '虾米酱': '虾',
                        '虾仔酱': '虾',
                        '虾仁': '虾',
                        '鲅鱼': '鲅鱼',
                        '蚕豆': '蚕豆',
                        '蟹黄': '螃蟹',
                        '蟹粉': '螃蟹',
                        '蒜蓉': '大蒜',
                        '蒜末': '大蒜',
                        '花蛤': '花蛤',
                        '瑶柱': '瑶柱',
                        '鸡枞菌': '鸡枞菌',
                        '牛肝菌': '牛肝菌',
                        '松茸菌': '松茸菌',
                        '竹笋': '竹笋',
                        '雷笋': '竹笋',
                        '春笋': '竹笋',
                        '烤笋': '竹笋',
                        '烧椒': '辣椒',
                        '剁椒': '辣椒',
                        '番茄': '番茄',
                        '黄豆': '黄豆',
                        '玉米': '玉米',
                        '丸大豆': '丸大豆',
                        '黑松露': '黑松露',
                        '松茸': '松茸',
                        '黑豆': '黑豆',
                        '八角': '八角',
                        '白胡椒': '白胡椒',
                        '芝麻': '芝麻',
                        '辣椒': '辣椒',
                        '草莓': '草莓',
                        '大豆': '大豆',
                        '干贝': '干贝',
                        '黑胡椒': '黑胡椒',
                        '红枣': '红枣',
                        '纳豆': '纳豆',
                        '蘑菇': '蘑菇',
                        '奶酪': '奶酪',
                        '牛肉': '牛肉',
                        '糯米': '糯米',
                        '泡菜': '泡菜',
                        '虾米': '虾米',
                        '香菇': '香菇',
                        '鱼子': '鱼籽',
                        '鱼籽': '鱼籽',
                        '鱼': '鱼'
                    },
                    8:{
                        '玻璃瓶': '玻璃瓶',
                        '超值装': '超值装',
                        '袋': '袋装',
                        '倒置': '倒置装',
                        '独立包装': '独立包装',
                        '罐': '罐装',
                        '盒': '盒装',
                        '挤挤': '挤挤装',
                        '挤压瓶': '挤压瓶',
                        '颗装': '颗装',
                        '块装': '块装',
                        '礼盒装': '礼盒装',
                        '量贩装': '量贩装',
                        '散装': '散装',
                        '塑料瓶': '塑料瓶',
                        '套装': '套装',
                        '桶': '桶装',
                        '箱': '箱装',
                        '新包装': '新包装',
                        '牙膏装': '牙膏装',
                        '原装': '原装',
                        '整箱': '整箱装',
                        '组合': '组合装',
                        '瓶': '瓶装',
                    },
                    10:{
                        '特辣': '特辣',
                        '微辣': '微辣',
                        '辣而不燥': '辣而不燥',
                        '蒜香': '蒜香',
                        '咸鲜': '咸鲜',
                        '酸甜': '酸甜',
                        '甘甜': '甘甜',
                        '清淡': '清淡',
                        '浓烈': '浓烈',
                        '质感': '质感',
                        '芳醇': '芳醇',
                        '平衡': '平衡',
                        '暴辣': '暴辣',
                        '川香': '川香',
                        '混合口味': '混合口味',
                        '麻辣': '麻辣',
                        '蜜汁味': '蜜汁味',
                        '鲜辣': '鲜辣',
                        '鲜香': '鲜香',
                        '香辣': '香辣',
                        '香甜': '香甜',
                        '甜辣': '甜辣',
                        '咸辣': '咸辣',
                    },
                }

                final = []
                for key in judgelist:
                    ans_list = re.findall(re_dict[pos_id], key)
                    if tran_dict.get(pos_id) != None:
                        ans_list = [tran_dict[pos_id].get(word, word) for word in ans_list]
                    final += ans_list
                
                final = list(set(final))
                if pos_id == 8 and '瓶装' in final and ('玻璃瓶' in final or '挤压瓶' in final or '塑料瓶' in final):
                    final.remove('瓶装')
                mp[pos_id] = ''
                # if final != []:
                mp[pos_id] = judgelist
                sp[pos_id] = self.split_word.join(sorted(final))
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')
            elif pos_id == 11:
                seq = self.get_pkey(pos_id, source, cid, '', prop_all.keys())
                for combined_keys in seq:
                    key_list = combined_keys.split('+')
                    v = [prop_all[k] for k in key_list if prop_all.get(k)]
                    judgelist.append(self.batch_now.separator.join(v))
                
                reg = re.compile(r'日光发酵|国家专利减盐|恒温发酵|陈酿|古法|古方|秘制配方|高盐稀态|传统滩晒|低温酿造')
                craft_reg = re.compile(r'工艺')
                method_reg = re.compile(r'传统|日式|非日晒|日晒|无硫|碳化|手工|川酒|绍兴')

                tran_dict = {
                    11:{
                        '日光发酵': '日光发酵工艺',
                        '国家专利减盐': '国家专利减盐技术',
                        '恒温发酵': '恒温发酵工艺',
                        '陈酿': '陈酿工艺',
                        '古法': '古法工艺',
                        '古方': '古方工艺',
                        '秘制配方': '秘制配方',
                        '高盐稀态': '高盐稀态工艺',
                        '低温酿造': '低温酿造工艺',
                        '传统滩晒': '传统滩晒工艺',
                    },
                    'special': {
                        '传统': '传统工艺',
                        '日式': '日式工艺',
                        '非日晒': '非日晒工艺',
                        '日晒': '日晒工艺',
                        '无硫': '无硫工艺',
                        '碳化': '碳化工艺',
                        '手工': '手工工艺',
                        '川酒': '川酒工艺',
                        '绍兴': '绍兴工艺',
                    }
                }
                
                final = []
                for key in judgelist:
                    ans_list = re.findall(reg, key)
                    if tran_dict.get(pos_id) != None:
                        ans_list = [tran_dict[pos_id].get(word, word) for word in ans_list]
                    final += ans_list

                    if len(re.findall(craft_reg, key)) > 0:
                        method_list = re.findall(method_reg, key)
                        final += [tran_dict['special'].get(word) for word in method_list]
                    final = list(set(final))

                mp[pos_id] = judgelist
                sp[pos_id] = self.split_word.join(sorted(final))
                self.batch_now.print_log(self.batch_now.line, 'sp'+str(pos_id), self.batch_now.pos[pos_id]['name'], '中间结果：', mp[pos_id], '最终结果：', sp[pos_id], '\n')

        return clean_type, mp, sp, detail
