import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import re

class main(calculate_sp.main):
    # 规则：
    # jd规则品类受cid和子品类类目字段影响
    # B列sp品类的值对应D&E列的底层数据交叉的部分，sp品类可以输出多个值，用Ծ‸ Ծ隔开，后期e表可以分割显示
    # 例如：当cid=2677&prop_all 中的字段“子类目名”火锅底料时，sp品类=汤底Ծ‸ Ծ火锅底料

    # 然后子品类要在品类匹配到的情况下才匹配
    # C列SP子品类要在SP品类的前提下用F,G列的关键词排除词清洗，SP子品类允许输出多个值，用Ծ‸ Ծ隔开，后期e表可以分割显示
    # 例如：当sp品类=汤底Ծ‸ Ծ火锅底料，pkey值为”咖喱汤底 花椒鸡汤底 ”。SP子品类=咖喱汤底Ծ‸ Ծ花椒鸡汤底Ծ‸ Ծ火锅底料


    # 天猫抖音 用cid匹配
    # B列sp品类的值对应E/G列的底层数据，sp品类可以输出多个值，用Ծ‸ Ծ隔开，后期e表可以分割显示
    # 例如：当cid=50009822时，sp品类=汤底Ծ‸ Ծ火锅底料

    # C列SP子品类要在SP品类的前提下用H,I列的关键词排除词清洗，SP子品类允许输出多个值，用Ծ‸ Ծ隔开，后期e表可以分割显示
    # 例如：当sp品类=汤底Ծ‸ Ծ火锅底料，宝贝为”咖喱汤底 花椒鸡汤底 ”。SP子品类=咖喱汤底Ծ‸ Ծ花椒鸡汤底Ծ‸ Ծ火锅底料


    split_word = 'Ծ‸ Ծ'

    config_dict = {
        ((2, 2677), None): [
            {
                "category": "汤底",
                "prop_list": ["火锅底料", "复合调味料", "浓汤宝", "其他调味料"],
                "sub_cate_dict": {
                    '汤底': [re.compile('(汤底|汤料)')], 
                    '番茄汤底': [re.compile('番茄.*(汤底|汤料)'), re.compile('(汤底|汤料).*番茄')], 
                    '金汤汤底': [re.compile('金汤|酸汤.*(汤底|汤料)'), re.compile('(汤底|汤料).*金汤|酸汤')], 
                    '冬阴功汤底': [re.compile('冬阴功.*(汤底|汤料)'), re.compile('(汤底|汤料).*冬阴功')], 
                    '寿喜锅汤底': [re.compile('寿喜锅.*(汤底|汤料)'), re.compile('(汤底|汤料).*寿喜锅')], 
                    '关东煮汤底': [re.compile('关东煮.*(汤底|汤料)'), re.compile('(汤底|汤料).*关东煮')], 
                    '糟粕醋汤底': [re.compile('糟粕醋.*(汤底|汤料)'), re.compile('(汤底|汤料).*糟粕醋')], 
                    '贵州红酸汤': [re.compile('红酸汤.*(汤底|汤料)'), re.compile('(汤底|汤料).*红酸汤')], 
                    '牛肝菌汤底': [re.compile('牛肝菌.*(汤底|汤料)'), re.compile('(汤底|汤料).*牛肝菌')], 
                    '日式豆乳汤底': [re.compile('豆乳.*(汤底|汤料)'), re.compile('(汤底|汤料).* 豆乳')], 
                    '螺蛳粉汤底': [re.compile('螺蛳粉.*(汤底|汤料)'), re.compile('(汤底|汤料).*螺蛳粉')], 
                    '猪肚鸡汤底': [re.compile('猪肚鸡.*(汤底|汤料)'), re.compile('(汤底|汤料).*猪肚鸡')], 
                    '花椒鸡汤底': [re.compile('花椒鸡.*(汤底|汤料)'), re.compile('(汤底|汤料).*花椒鸡')], 
                    '咖喱汤底': [re.compile('咖喱.*(汤底|汤料)'), re.compile('(汤底|汤料).*咖喱')]
                },
                "sub_cate_ignore_dict": {
                    "金汤汤底": [re.compile('红酸汤')]
                }
            },
            {
                "category": "复合调味汁",
                "prop_list": ["复合调味料", "其他调味料", "沙拉酱", "其他酱料"],
                "sub_cate_dict": {
                    '复合调味汁': [re.compile('调味汁|凉拌|海鲜捞汁|捞汁海鲜|捞汁小海鲜|生腌|凉拌汁|熟醉|卤醉|醉卤|油醋汁|辣鲜露|照烧汁|卤水汁|红烧汁|红烧酱汁|糖醋|小炒汁|钵钵鸡')], 
                    '白灼汁': [re.compile('白灼汁')], 
                    '凉拌汁': [re.compile('凉拌汁')], 
                    '海鲜捞汁': [re.compile('海鲜捞汁|捞汁海鲜|捞汁小海鲜')], 
                    '捞拌汁': [re.compile('捞拌')], 
                    '生腌汁': [re.compile('生腌')], 
                    '熟醉汁': [re.compile('熟醉|卤醉|醉卤')], 
                    '凉拌汁+海鲜捞汁+生腌汁': [re.compile('凉拌|海鲜捞汁|捞汁海鲜|捞汁小海鲜|生腌')], 
                    '油醋汁': [re.compile('油醋汁')], 
                    '四川钵钵鸡': [re.compile('钵钵鸡')], 
                    '辣鲜露': [re.compile('辣鲜露')], 
                    '照烧汁': [re.compile('照烧汁')], 
                    '卤水汁': [re.compile('卤水汁')], 
                    '红烧汁': [re.compile('红烧汁|红烧酱汁')], 
                    '糖醋汁': [re.compile('糖醋汁')], 
                    '小炒汁': [re.compile('小炒汁')]
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "调味料包",
                "prop_list": ["辣椒酱", "其他酱料", "复合调味料"],
                "sub_cate_dict": {
                    '调味料包': [re.compile('酱.*调料'), re.compile('调料.*酱')], 
                    '麻婆豆腐调味料': [re.compile('麻婆豆腐')], 
                    '鱼香肉丝调味料': [re.compile('鱼香肉丝')], 
                    '避风塘炒虾调味料': [re.compile('避风塘.*(虾|蟹)'), re.compile('(虾|蟹).*避风塘')], 
                    '萝卜焖牛腩调味料': [re.compile('牛腩.*萝卜'), re.compile('萝卜.*牛腩')], 
                    '夫妻肺片调味料': [re.compile('夫妻肺片')], 
                    '煲仔饭调味料': [re.compile('煲仔饭')], 
                    '啫啫煲调味料': [re.compile('啫啫煲')], 
                    '黄焖鸡调味料': [re.compile('黄焖鸡')], 
                    '石锅拌饭酱': [re.compile('石锅拌饭')]
                },
                "sub_cate_ignore_dict": {
                    "调味料包": [re.compile('下饭酱|拌饭酱|拌面酱')]
                }
            },
            {
                "category": "下饭酱",
                "prop_list": ["辣椒酱", "其他酱料", "复合调味料"],
                "sub_cate_dict": {
                    '下饭酱/拌饭酱': [re.compile('下饭酱|拌饭酱|拌面酱')], 
                    '紫苏酱': [re.compile('紫苏酱|紫苏牛蛙酱|紫苏辣椒酱|紫苏辣酱')], 
                    '红葱香酱/红葱酱': [re.compile('红葱香酱|红葱油酱|红葱头酱|红葱酱')], 
                    '普宁豆酱': [re.compile('普宁豆酱')], 
                    '烧椒酱/青椒酱': [re.compile('烧椒酱|青椒酱')], 
                    '二八酱': [re.compile('二八酱')], 
                    '肉燥酱': [re.compile('肉燥酱')], 
                    '贵州糟辣酱': [re.compile('糟辣酱')], 
                    '八宝酱': [re.compile('八宝酱')]
                },
                "sub_cate_ignore_dict": {
                    "下饭酱/拌饭酱": [re.compile('下饭菜')]
                }
            },
            {
                "category": "蚝油",
                "prop_list": ["蚝油"],
                "sub_cate_dict": {
                    '蚝油': None, 
                    '零添加蚝油': [re.compile('零添加|0添加|无添加')], 
                    '薄盐蚝油': [re.compile('薄盐|减盐')], 
                    '蚝油挤挤装': [re.compile('挤挤装|挤挤瓶')], 
                    '有机蚝油': [re.compile('有机')], 
                    '素蚝油': [re.compile('素蚝油')]
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "腐乳",
                "prop_list": ["腐乳"],
                "sub_cate_dict": {
                    '腐乳': None, 
                    '辣腐乳': [re.compile('辣腐乳|辣豆腐乳')], 
                    '红方': [re.compile('红方|红曲|玫瑰')], 
                    '青方': [re.compile('青方|霉腐乳|臭豆腐乳')], 
                    '米酱腐乳': [re.compile('米酱')], 
                    '糟方腐乳': [re.compile('糟方|米糟|酒酿')], 
                    '玫瑰腐乳': [re.compile('玫瑰|进京')], 
                    '白方': [re.compile('白方|白腐乳')], 
                    '白辣腐乳': [re.compile('(白方|白腐乳).*辣'), re.compile('辣.*(白方|白腐乳)')], 
                    '麻油白腐乳': [re.compile('麻油.*(白腐乳|白方)'), re.compile('(白腐乳|白方).*麻油')], 
                    '白腐乳-不加麻油': [re.compile('白腐乳|白方')], 
                    '一块腐乳': [re.compile('一块腐乳|独立')], 
                    '南乳': [re.compile('南乳|南乳汁')], 
                    '有机腐乳': [re.compile('有机')], 
                    '零添加腐乳': [re.compile('零添加|0添加|无添加')], 
                    '油腐乳': [re.compile('油腐乳')]
                },
                "sub_cate_ignore_dict": {
                    "白腐乳-不加麻油": [re.compile('麻油')]
                }
            },
            {
                "category": "醋",
                "prop_list": ["醋"],
                "sub_cate_dict": {
                    '醋': None, 
                    '陈醋': [re.compile('陈醋')], 
                    '米醋': [re.compile('米醋')], 
                    '香醋': [re.compile('香醋')], 
                    '果醋': [re.compile('果醋')], 
                    '白醋': [re.compile('白醋')], 
                    '零添加醋': [re.compile('零添加|0添加|无添加')], 
                    '有机醋': [re.compile('有机醋')]
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "料酒",
                "prop_list": ["料酒"],
                "sub_cate_dict": {
                    '料酒': None, 
                    '有机料酒': [re.compile('有机')], 
                    '零添加料酒': [re.compile('零添加|0添加|无添加')]
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "辣酱",
                "prop_list": ["辣椒酱"],
                "sub_cate_dict": {
                    '辣酱': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "火锅底料",
                "prop_list": ["火锅底料"],
                "sub_cate_dict": {
                    '火锅底料': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "鸡精",
                "prop_list": ["味精", '鸡精/鸡汁', '鸡精/味精'],
                "sub_cate_dict": {
                    '鸡精': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
            {
                "category": "香料",
                "prop_list": ["香辛料"],
                "sub_cate_dict": {
                    '香料': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((2, None), None): [
            {
                "category": "芝麻油",
                "prop_list": ["芝麻香油", "食用油"],
                "sub_cate_dict": {
                    '芝麻油': [re.compile('芝麻油|芝麻香油')]
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1, 123194003), (1, 50009822), (11, 31213), (11, 31210)): [
            {
                "category": "汤底",
                "sub_cate_dict": {
                    '汤底': None, 
                    '番茄汤底': [re.compile('番茄')], 
                    '金汤汤底': [re.compile('金汤|酸汤')], 
                    '冬阴功汤底': [re.compile('冬阴功')], 
                    '寿喜锅汤底': [re.compile('寿喜锅')], 
                    '关东煮汤底': [re.compile('关东煮')], 
                    '糟粕醋汤底': [re.compile('糟粕醋')], 
                    '贵州红酸汤': [re.compile('红酸汤')], 
                    '牛肝菌汤底': [re.compile('牛肝菌')], 
                    '日式豆乳汤底': [re.compile('豆乳')], 
                    '螺蛳粉汤底': [re.compile('螺蛳粉')], 
                    '猪肚鸡汤底': [re.compile('猪肚鸡')], 
                    '花椒鸡汤底': [re.compile('花椒鸡')], 
                    '咖喱汤底': [re.compile('咖喱')]
                },
                "sub_cate_ignore_dict": {
                    '金汤汤底': [re.compile('红酸汤')], 
                }
            },
        ],
        ((1, 201169511), (1, 50009856), (1, 50009833), (11, 31203), (11, 31216), (11, 31199)): [
            {
                "category": "复合调味汁",
                "sub_cate_dict": {
                    '复合调味汁': None, 
                    '白灼汁': [re.compile('白灼汁')], 
                    '凉拌汁': [re.compile('凉拌汁')], 
                    '海鲜捞汁': [re.compile('海鲜捞汁|捞汁海鲜|捞汁小海鲜')], 
                    '捞拌汁': [re.compile('捞拌')], 
                    '生腌汁': [re.compile('生腌')], 
                    '熟醉汁': [re.compile('熟醉|卤醉|醉卤')], 
                    '凉拌汁+海鲜捞汁+生腌汁': [re.compile('凉拌|海鲜捞汁|捞汁海鲜|捞汁小海鲜|生腌')], 
                    '油醋汁': [re.compile('油醋汁')], 
                    '四川钵钵鸡': [re.compile('钵钵鸡')], 
                    '辣鲜露': [re.compile('辣鲜露')], 
                    '照烧汁': [re.compile('照烧汁')], 
                    '卤水汁': [re.compile('卤水汁')], 
                    '红烧汁': [re.compile('红烧汁|红烧酱汁')], 
                    '糖醋汁': [re.compile('糖醋汁')], 
                    '小炒汁': [re.compile('小炒汁')]
                },
                "sub_cate_ignore_dict": {
                    '复合调味汁': [re.compile('沙拉|千岛|蛋黄酱|生抽|老抽|鲜酱油|蒸鱼豉油|红烧酱油|凉拌酱油|功能酱油')], 
                }
            },
        ],
        ((1, 50009823), (1, 201375218), (11, 28216), (11, 31197), (11, 31199), (11, 31204), (11, 31205), (11, 31206), (11, 31207), (11, 31208), (11, 31211), (11, 32138), (11, 32140)): [
            {
                "category": "调味料包",
                "sub_cate_dict": {
                    '调味料包': [re.compile('酱.*调料'), re.compile('调料.*酱')], 
                    '麻婆豆腐调味料': [re.compile('麻婆豆腐')], 
                    '鱼香肉丝调味料': [re.compile('鱼香肉丝')], 
                    '避风塘炒虾调味料': [re.compile('避风塘.*(虾|蟹)'), re.compile('(虾|蟹).*避风塘')], 
                    '萝卜焖牛腩调味料': [re.compile('牛腩.*萝卜'), re.compile('萝卜.*牛腩')], 
                    '夫妻肺片调味料': [re.compile('夫妻肺片')], 
                    '煲仔饭调味料': [re.compile('煲仔饭')], 
                    '啫啫煲调味料': [re.compile('啫啫煲')], 
                    '黄焖鸡调味料': [re.compile('黄焖鸡')], 
                    '石锅拌饭酱': [re.compile('石锅拌饭')]
                },
                "sub_cate_ignore_dict": {
                    '调味料包': [re.compile('下饭酱|拌饭酱|拌面酱')], 
                }
            },
        ],
        ((1, 50009823), (1, 50050640), (1, 201375218), (1, 50050642), (11, 28216), (11, 31197), (11, 31199), (11, 31204), (11, 31205), (11, 31206), (11, 31207), (11, 31208), (11, 31211), (11, 32138), (11, 32140)): [
            {
                "category": "下饭酱",
                "sub_cate_dict": {
                    '下饭酱/拌饭酱': [re.compile('下饭酱|拌饭酱|拌面酱')], 
                    '紫苏酱': [re.compile('紫苏酱|紫苏牛蛙酱|紫苏辣椒酱|紫苏辣酱')], 
                    '红葱香酱/红葱酱': [re.compile('红葱香酱|红葱油酱|红葱头酱|红葱酱')], 
                    '普宁豆酱': [re.compile('普宁豆酱')], 
                    '烧椒酱/青椒酱': [re.compile('烧椒酱|青椒酱')], 
                    '二八酱': [re.compile('二八酱')], 
                    '肉燥酱': [re.compile('肉燥酱')], 
                    '贵州糟辣酱': [re.compile('糟辣酱')], 
                    '八宝酱': [re.compile('八宝酱')]
                },
                "sub_cate_ignore_dict": {
                    '下饭酱/拌饭酱': [re.compile('下饭菜')], 
                }
            },
        ],
        ((1, 50013185), (11, 28223)): [
            {
                "category": "蚝油",
                "sub_cate_dict": {
                    '蚝油': None, 
                    '零添加蚝油': [re.compile('零添加|0添加|无添加')], 
                    '薄盐蚝油': [re.compile('薄盐|减盐')], 
                    '蚝油挤挤装': [re.compile('挤挤装|挤挤瓶')], 
                    '有机蚝油': [re.compile('有机')], 
                    '素蚝油': [re.compile('素蚝油')]
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50010891), (11, 28217)): [
            {
                "category": "腐乳",
                "sub_cate_dict": {
                    '腐乳': None, 
                    '辣腐乳': [re.compile('辣腐乳|辣豆腐乳')], 
                    '红方': [re.compile('红方|红曲|玫瑰')], 
                    '青方': [re.compile('青方|霉腐乳|臭豆腐乳')], 
                    '米酱腐乳': [re.compile('米酱')], 
                    '糟方腐乳': [re.compile('糟方|米糟|酒酿')], 
                    '玫瑰腐乳': [re.compile('玫瑰|进京')], 
                    '白方': [re.compile('白方|白腐乳')], 
                    '白辣腐乳': [re.compile('(白方|白腐乳).*辣'), re.compile('辣.*(白方|白腐乳)')], 
                    '麻油白腐乳': [re.compile('麻油.*(白腐乳|白方)'), re.compile('(白腐乳|白方).*麻油')], 
                    '白腐乳-不加麻油': [re.compile('白腐乳|白方')], 
                    '一块腐乳': [re.compile('一块腐乳|独立')], 
                    '南乳': [re.compile('南乳|南乳汁')], 
                    '有机腐乳': [re.compile('有机')], 
                    '零添加腐乳': [re.compile('零添加|0添加|无添加')], 
                    '油腐乳': [re.compile('油腐乳')]
                },
                "sub_cate_ignore_dict": {
                    '白腐乳-不加麻油': [re.compile('麻油')], 
                }
            },
        ],
        ((1, 50009836), (1, 50025676), (1, 50050378), (1, 50050379), (1, 50050380), (1, 50050381), (1, 50050382), (1, 50050383), (1, 50050385), (1, 50050388), (1, 50050390), (1, 50050391), (1, 50050393), (1, 50050394), (1, 50050395), (1, 50050396), (1, 50050397), (1, 50776032), (1, 201778002), (11, 21666), (11, 28222), (11, 28223), (11, 28224), (11, 28225), (11, 28226), (11, 28227), (11, 28228), (11, 28229), (11, 28230), (11, 28231), (11, 28232), (11, 28233), (11, 28234), (11, 28235), (11, 28236), (11, 28237), (11, 28238), (11, 28239), (11, 28240), (11, 34052), (11, 34189), (11, 34190), (11, 35510)): [
            {
                "category": "芝麻油",
                "sub_cate_dict": {
                    '芝麻油': [re.compile('芝麻油|芝麻香油')], 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50009828), (11, 31219)): [
            {
                "category": "醋",
                "sub_cate_dict": {
                    '醋': None, 
                    '陈醋': [re.compile('陈醋')], 
                    '米醋': [re.compile('米醋')], 
                    '香醋': [re.compile('香醋')], 
                    '果醋': [re.compile('果醋')], 
                    '白醋': [re.compile('白醋')], 
                    '零添加醋': [re.compile('零添加|0添加|无添加')], 
                    '有机醋': [re.compile('有机醋')]
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50009878), (11, 31215)): [
            {
                "category": "料酒",
                "sub_cate_dict": {
                    '料酒': None, 
                    '有机料酒': [re.compile('有机')], 
                    '零添加料酒': [re.compile('零添加|0添加|无添加')]
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50050642), (11, 31206)): [
            {
                "category": "辣酱",
                "sub_cate_dict": {
                    '辣酱': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50009822), (11, 31210)): [
            {
                "category": "火锅底料",
                "sub_cate_dict": {
                    '火锅底料': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50009830), (11, 31218)): [
            {
                "category": "鸡精",
                "sub_cate_dict": {
                    '鸡精': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
        ((1,50009880), (11, 31198)): [
            {
                "category": "香料",
                "sub_cate_dict": {
                    '香料': None, 
                },
                "sub_cate_ignore_dict": {
                }
            },
        ],
    }


    # 用醋测试
    # config_dict = {
    #     ((2, 2677)): [
            
    #         {
    #             "category": "醋",
    #             "prop_list": ["醋"],
    #             "sub_cate_dict": {
    #                 '醋': None, 
    #                 '陈醋': [re.compile('陈醋')], 
    #                 '米醋': [re.compile('米醋')], 
    #                 '香醋': [re.compile('香醋')], 
    #                 '果醋': [re.compile('果醋')], 
    #                 '白醋': [re.compile('白醋')], 
    #                 '零添加醋': [re.compile('零添加|0添加|无添加')], 
    #                 '有机醋': [re.compile('有机醋')]
    #             },
    #             "sub_cate_ignore_dict": {
    #             }
    #         },
    #     ]
    #     }

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            # (source,None)对应不需要子品类限制的属性清洗
            cate_set = set()
            sub_cate_set = set()
            for key in [(f_map['snum'], cid), (f_map['snum'], None)]:
                for option_list in self.config_dict.keys():
                    for option in option_list:
                        if key == option:
                            for match_dict in self.config_dict[option_list]:
                                # jd的规则，要加一个prop_all属性对应
                                found_cid = False
                                if match_dict.get('prop_list') != None and prop_all.get('子类目名') in match_dict['prop_list']:
                                    cate_set.add(match_dict["category"])
                                    found_cid = True
                                elif match_dict.get('prop_list') == None:
                                    cate_set.add(match_dict["category"])
                                    found_cid = True

                                # import pdb;pdb.set_trace()
                                # 如果匹配到品类才能进行子品类匹配
                                if found_cid:
                                    match_prop_str = self.batch_now.pos[4]['p_key'].get(str(source)+str(cid))
                                    if match_prop_str != None:
                                        prop_list = match_prop_str.split(',')
                                        for prop in prop_list:
                                            # 判断词如果用+号连接，是用两个属性的拼接
                                            if len(prop_list) == 1:
                                                judge_word = ''
                                                and_props = prop.split('+')
                                                for and_prop in and_props:
                                                    judge_word = judge_word + ' ' + str(f_map[and_prop])
                                            else:
                                                judge_word = str(f_map[prop])
                                            # 判断规则
                                            self.batch_now.print_log('子品类判断词为：', judge_word)
                                            for sub_cate, rule_list in match_dict["sub_cate_dict"].items():
                                                not_rule_found = False
                                                if match_dict["sub_cate_ignore_dict"].get(sub_cate) != None:
                                                    for not_rule in match_dict["sub_cate_ignore_dict"][sub_cate]: 
                                                        if len(re.findall(not_rule, judge_word)) != 0:
                                                            not_rule_found = True
                                                            break

                                                if not not_rule_found:
                                                    if rule_list == None:
                                                        sub_cate_set.add(sub_cate)
                                                    else:
                                                        for rule in rule_list:
                                                            # 判断是否有
                                                            if len(re.findall(rule, judge_word)) != 0:
                                                                sub_cate_set.add(sub_cate)
                                    else:
                                        sp[4] = 'source:' + str(source) + ', cid: ' + str(cid) + ', 需要p_key配置，但是现在没有，请补充'

            
            sp[3] = self.split_word.join(list(cate_set))
            sp[4] = self.split_word.join(list(sub_cate_set))


            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            # 抖音数据特殊处理
            # self.result_dict[item_id]['all_bid_sp'] = self.process_dy(f_map['snum'], alias_all_bid, f_map, self.result_dict[item_id]['all_bid_sp'])
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict