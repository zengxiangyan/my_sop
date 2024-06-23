import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import application as app
from models import common
from models.plugins import calculate_sp
import re

class main(calculate_sp.main):
    split_word = 'Ծ‸ Ծ'

    rule_list = [
        {
            'first_cate': '番茄酱',
            'second_cate': '番茄酱',
            'third_cate': '番茄酱',
            'tmall_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄'),
            'tmall_not_word': re.compile('肉酱|番茄膏|番茄丁|番茄块|西红柿块|去皮番茄|碎番茄|整番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿'),
            'dy_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄'),
            'dy_not_word': re.compile('肉酱|番茄膏|番茄丁|番茄块|西红柿块|去皮番茄|碎番茄|整番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿')
        },
        {
            'first_cate': '番茄酱',
            'second_cate': '番茄丁',
            'third_cate': '番茄丁',
            'tmall_word': re.compile('番茄丁|番茄块|西红柿块|去皮番茄|碎番茄|整番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿'),
            'tmall_not_word': re.compile('肉酱|番茄膏'),
            'dy_word': re.compile('番茄丁|番茄块|西红柿块|去皮番茄|碎番茄|整番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿'),
            'dy_not_word': re.compile('肉酱|番茄膏')
        },
        {
            'first_cate': '番茄酱',
            'second_cate': '番茄膏',
            'third_cate': '番茄膏',
            'tmall_word': re.compile('番茄膏'),
            'tmall_not_word': re.compile('肉酱'),
            'dy_word': re.compile('番茄膏'),
            'dy_not_word': re.compile('肉酱')
        },
        {
            'first_cate': '沙拉酱',
            'second_cate': '沙拉酱,蛋黄酱,千岛酱',
            'third_cate': '沙拉酱,蛋黄酱,千岛酱',
            'tmall_word': re.compile('沙拉酱|沙拉汁|油醋汁|千岛酱|蛋黄酱|色拉酱|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁|蜂蜜芥末酱|芥末蜂蜜汁酱'),
            'tmall_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|油醋汁|沙拉汁|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱'),
            'dy_word': re.compile('沙拉酱|沙拉汁|油醋汁|千岛酱|蛋黄酱|色拉酱|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁|蜂蜜芥末酱|芥末蜂蜜汁酱'),
            'dy_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|油醋汁|沙拉汁|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱')
        },
        {
            'first_cate': '沙拉酱',
            'second_cate': '沙拉汁',
            'third_cate': '沙拉汁',
            'tmall_word': re.compile('沙拉汁|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁'),
            'tmall_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|油醋汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱'),
            'dy_word': re.compile('沙拉汁|色拉汁|千岛汁|沙拉酱汁|蜂蜜芥末汁|芥末蜂蜜汁'),
            'dy_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|油醋汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱')
        },
        {
            'first_cate': '沙拉酱',
            'second_cate': '油醋汁',
            'third_cate': '油醋汁',
            'tmall_word': re.compile('油醋汁'),
            'tmall_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱'),
            'dy_word': re.compile('油醋汁'),
            'dy_not_word': re.compile('番茄酱|番茄汁|西红柿酱|西红柿汁|番茄沙司|蕃茄膏|番茄丁|番茄块|西红柿块|番茄甜辣椒酱|番茄辣酱|番茄辣椒|辣椒番茄|西红柿辣椒|西红柿辣酱|去皮番茄|切块番茄|西红柿丁|去皮西红柿|切块西红柿|碎番茄|整番茄|花生酱|芝麻酱|白灼汁|小炒汁|冷泡汁|熟醉汁|生腌汁|照烧汁|海鲜捞汁|凉拌汁|烧烤汁|热泡汁|黄焖鸡酱汁|牛油果酱|牛油果泥|酸奶油酱|芝士榴莲酱')
        },
        {
            'first_cate': '芥末酱',
            'second_cate': '黄芥末酱',
            'third_cate': '黄芥末',
            'tmall_word': re.compile('黄芥末'),
            'tmall_not_word': re.compile('芥末籽酱'),
            'dy_word': re.compile('黄芥末'),
            'dy_not_word': re.compile('粉|青瓜酱|芥末籽酱')
        },
        {
            'first_cate': '芥末酱',
            'second_cate': '芥末籽酱',
            'third_cate': '芥末籽',
            'tmall_word': re.compile('芥末籽酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('芥末籽酱'),
            'dy_not_word': re.compile('种子')
        },
        {
            'first_cate': '芥末酱',
            'second_cate': '蜂蜜芥末酱',
            'third_cate': '蜂蜜芥末',
            'tmall_word': re.compile('蜂蜜芥末'),
            'tmall_not_word': re.compile('黄芥末|芥末籽酱'),
            'dy_word': re.compile('蜂蜜芥末'),
            'dy_not_word': re.compile('黄芥末')
        },
        {
            'first_cate': '披萨酱',
            'second_cate': '披萨酱',
            'third_cate': '披萨酱',
            'tmall_word': re.compile('披萨酱'),
            'tmall_not_word': re.compile('意大利面酱|意面酱|番茄意面酱|番茄罗勒|奶油意面酱|芝士意面酱|青酱|罗勒酱'),
            'dy_word': re.compile('披萨酱'),
            'dy_not_word': re.compile('意大利面酱|意面酱|番茄意面酱|番茄罗勒|奶油意面酱|芝士意面酱|青酱|罗勒酱')
        },
        {
            'first_cate': '意面酱',
            'second_cate': '红酱',
            'third_cate': '番茄,番茄罗勒',
            'tmall_word': re.compile('番茄意面酱|番茄罗勒'),
            'tmall_not_word': re.compile('番茄沙司|速食面|方便面|黑松露酱'),
            'dy_word': re.compile('番茄意面酱|番茄罗勒'),
            'dy_not_word': re.compile('番茄沙司|速食面|方便面|黑松露酱')
        },
        {
            'first_cate': '意面酱',
            'second_cate': '白酱',
            'third_cate': '奶油蘑菇',
            'tmall_word': re.compile('奶油意面酱|芝士意面酱'),
            'tmall_not_word': re.compile('番茄|速食面|方便面|黑松露酱'),
            'dy_word': re.compile('奶油意面酱|芝士意面酱'),
            'dy_not_word': re.compile('番茄|速食面|方便面|黑松露酱')
        },
        {
            'first_cate': '意面酱',
            'second_cate': '青酱',
            'third_cate': '罗勒青酱',
            'tmall_word': re.compile('青酱|罗勒酱'),
            'tmall_not_word': re.compile('番茄|黑松露酱'),
            'dy_word': re.compile('青酱|罗勒酱'),
            'dy_not_word': re.compile('番茄|黑松露酱')
        },
        {
            'first_cate': '黑椒酱',
            'second_cate': '黑椒酱',
            'third_cate': '黑椒酱,黑椒汁',
            'tmall_word': re.compile('黑椒酱|黑椒汁'),
            'tmall_not_word': None,
            'dy_word': re.compile('黑椒酱|黑椒汁'),
            'dy_not_word': None
        },
        {
            'first_cate': '牛排酱',
            'second_cate': '牛排酱',
            'third_cate': '牛排酱',
            'tmall_word': re.compile('牛排酱'),
            'tmall_not_word': re.compile('烧烤酱|黑椒酱|黑椒汁|黑松露酱'),
            'dy_word': re.compile('牛排酱'),
            'dy_not_word': re.compile('烧烤酱|黑椒酱|黑椒汁|腌料|黑松露酱')
        },
        {
            'first_cate': '西式烧烤酱',
            'second_cate': '欧美式烧烤酱',
            'third_cate': '美式/德式/墨西哥式烧烤酱',
            'tmall_word': re.compile('美式烧烤酱|美式BBQ酱'),
            'tmall_not_word': re.compile('辣酱|牛排酱'),
            'dy_word': re.compile('美式烧烤酱|美式BBQ酱'),
            'dy_not_word': re.compile('辣酱|牛排酱')
        },
        {
            'first_cate': '西式烧烤酱',
            'second_cate': '欧美式烧烤酱',
            'third_cate': '美式/德式/墨西哥式烧烤酱',
            'tmall_word': re.compile('德式烧烤酱'),
            'tmall_not_word': re.compile('辣酱|牛排酱'),
            'dy_word': re.compile('德式烧烤酱'),
            'dy_not_word': re.compile('辣酱|牛排酱')
        },
        {
            'first_cate': '西式烧烤酱',
            'second_cate': '欧美式烧烤酱',
            'third_cate': '美式/德式/墨西哥式烧烤酱',
            'tmall_word': re.compile('墨西哥式烧烤酱'),
            'tmall_not_word': re.compile('辣酱|牛排酱'),
            'dy_word': re.compile('墨西哥式烧烤酱'),
            'dy_not_word': re.compile('辣酱|牛排酱')
        },
        {
            'first_cate': '西式烧烤酱',
            'second_cate': '日韩式烧烤酱',
            'third_cate': '韩式烧烤酱',
            'tmall_word': re.compile('韩式烧烤酱|韩式烤肉酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('韩式烧烤酱|韩式烤肉酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式烧烤酱',
            'second_cate': '日韩式烧烤酱',
            'third_cate': '日式烧烤酱',
            'tmall_word': re.compile('日式烧烤酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('日式烧烤酱|照烧汁'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '番茄辣酱',
            'third_cate': '番茄辣酱',
            'tmall_word': re.compile('番茄辣椒酱'),
            'tmall_not_word': re.compile('老干妈|火烧大树|云南'),
            'dy_word': re.compile('番茄辣椒酱'),
            'dy_not_word': re.compile('老干妈|火烧大树|云南')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '芒果辣酱',
            'third_cate': '芒果辣酱',
            'tmall_word': re.compile('芒果辣椒酱'),
            'tmall_not_word': re.compile('闽南|桂林|湖南|甜辣酱'),
            'dy_word': re.compile('芒果辣椒酱'),
            'dy_not_word': re.compile('闽南|桂林|湖南|甜辣酱|蒜蓉酱|沙茶酱')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '东南亚 （泰式,越南,印度）',
            'third_cate': '是拉差辣酱/Sriracha',
            'tmall_word': re.compile('是拉差酱|红飞鹰'),
            'tmall_not_word': None,
            'dy_word': re.compile('是拉差酱|红飞鹰'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '东南亚 （泰式,越南,印度）',
            'third_cate': '叁巴酱/Sambal',
            'tmall_word': re.compile('叁巴酱|参巴酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('叁巴酱|参巴酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '东南亚 （泰式,越南,印度）',
            'third_cate': '甜辣酱/Sweet Chili ',
            'tmall_word': re.compile('甜辣酱'),
            'tmall_not_word': re.compile('石锅拌饭酱'),
            'dy_word': re.compile('甜辣酱'),
            'dy_not_word': re.compile('石锅拌饭酱|番茄沙司')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '东南亚 （泰式,越南,印度）',
            'third_cate': '泰国酸辣酱',
            'tmall_word': re.compile('泰国酸辣酱'),
            'tmall_not_word': re.compile('汁|泰国辣椒酱'),
            'dy_word': re.compile('泰式酸辣酱'),
            'dy_not_word': re.compile('汁|泰式辣椒酱')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '东南亚 （泰式,越南,印度）',
            'third_cate': '泰国辣椒酱/NAM PRIK PAO',
            'tmall_word': re.compile('泰国辣椒酱'),
            'tmall_not_word': re.compile('是拉差酱'),
            'dy_word': re.compile('泰式辣椒酱'),
            'dy_not_word': re.compile('是拉差酱')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '日韩式 （韩式辣酱,火鸡酱）',
            'third_cate': '火鸡面酱',
            'tmall_word': re.compile('火鸡面酱'),
            'tmall_not_word': re.compile('韩式辣酱'),
            'dy_word': re.compile('火鸡面酱'),
            'dy_not_word': re.compile('韩式辣酱')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '日韩式 （韩式辣酱,火鸡酱）',
            'third_cate': '韩式辣酱',
            'tmall_word': re.compile('韩式辣酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('韩式辣酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '美式 （莎莎辣酱,chipotle,buffalo）',
            'third_cate': '哈瓦那辣椒酱/Habanero',
            'tmall_word': re.compile('habanero|哈瓦那酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('哈瓦那酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '美式 （莎莎辣酱,chipotle,buffalo）',
            'third_cate': '布法罗辣酱/Buffalo',
            'tmall_word': re.compile('布法罗酱|水牛城辣椒酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('布法罗酱|水牛城辣椒酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '美式 （莎莎辣酱,chipotle,buffalo）',
            'third_cate': '辣椒仔酱/Tabasco',
            'tmall_word': re.compile('tabasco|辣椒仔酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('tabasco|辣椒仔酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '美式 （莎莎辣酱,chipotle,buffalo）',
            'third_cate': '墨西哥莎莎酱/Salsa',
            'tmall_word': re.compile('salsa|莎莎酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('salsa酱|莎莎酱|萨尔萨酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '美式 （莎莎辣酱,chipotle,buffalo）',
            'third_cate': '墨西哥烟熏辣酱/Chipotle',
            'tmall_word': re.compile('启波特雷|墨西哥烟熏酱'),
            'tmall_not_word': re.compile('辣椒粒|辣椒干'),
            'dy_word': re.compile('墨西哥烟熏酱'),
            'dy_not_word': re.compile('辣椒粒|辣椒干')
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '欧式 & 非式（periperi）& 中东',
            'third_cate': '哈里萨辣酱/Harissa',
            'tmall_word': re.compile('harissa|哈里萨酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('harissa|哈里萨酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '西式辣椒酱',
            'second_cate': '欧式 & 非式（periperi）& 中东',
            'third_cate': '霹雳霹雳辣酱/Peri Peri',
            'tmall_word': re.compile('periperi|南逗'),
            'tmall_not_word': None,
            'dy_word': re.compile('南逗'),
            'dy_not_word': None
        },
        {
            'first_cate': '咖喱酱',
            'second_cate': '咖喱酱',
            'third_cate': '咖喱酱',
            'tmall_word': re.compile('咖喱酱'),
            'tmall_not_word': re.compile('咖喱块|咖喱粉|咖喱椰浆'),
            'dy_word': re.compile('咖喱酱'),
            'dy_not_word': re.compile('咖喱块|咖喱粉|咖喱椰浆')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '胡椒',
            'tmall_word': re.compile('胡椒粉|绿胡椒'),
            'tmall_not_word': re.compile('黑胡椒|白胡椒|胡椒酱|混合'),
            'dy_word': re.compile('胡椒粉|绿胡椒'),
            'dy_not_word': re.compile('黑胡椒|白胡椒|胡椒酱|混合')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '黑胡椒',
            'tmall_word': re.compile('黑胡椒粉|黑胡椒粒'),
            'tmall_not_word': re.compile('黑胡椒盐|白胡椒粉|白胡椒粒'),
            'dy_word': re.compile('黑胡椒粉|黑胡椒粒'),
            'dy_not_word': re.compile('黑胡椒盐|白胡椒粉|白胡椒粒')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '白胡椒',
            'tmall_word': re.compile('白胡椒粉|白胡椒粒'),
            'tmall_not_word': None,
            'dy_word': re.compile('白胡椒粉|白胡椒粒'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '辣椒',
            'tmall_word': re.compile('甜椒粉|红甜椒粉'),
            'tmall_not_word': re.compile('辣椒面|火锅'),
            'dy_word': re.compile('甜椒粉|红甜椒粉'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '茴香',
            'tmall_word': re.compile('茴香粉|洋茴香粉'),
            'tmall_not_word': re.compile('五香|川菜|甘肃|云南'),
            'dy_word': re.compile('茴香粉|洋茴香粉'),
            'dy_not_word': re.compile('五香|川菜|甘肃|云南')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '豆蔻',
            'tmall_word': re.compile('豆蔻粉'),
            'tmall_not_word': None,
            'dy_word': re.compile('豆蔻粉'),
            'dy_not_word': re.compile('干货|玉果|颗粒')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '罗勒',
            'tmall_word': re.compile('罗勒粉'),
            'tmall_not_word': re.compile('叶|新鲜'),
            'dy_word': re.compile('罗勒粉'),
            'dy_not_word': re.compile('新鲜')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '百里香',
            'tmall_word': re.compile('百里香'),
            'tmall_not_word': re.compile('干枝|新鲜'),
            'dy_word': re.compile('百里香'),
            'dy_not_word': re.compile('干枝|新鲜')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '迷迭香',
            'tmall_word': re.compile('迷迭香|迷迭香碎'),
            'tmall_not_word': re.compile('干枝|新鲜'),
            'dy_word': re.compile('迷迭香|迷迭香碎'),
            'dy_not_word': re.compile('干枝|新鲜')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '葱',
            'tmall_word': re.compile('葱粉'),
            'tmall_not_word': re.compile('葱姜蒜|洋葱'),
            'dy_word': re.compile('葱粉'),
            'dy_not_word': re.compile('葱姜蒜|洋葱')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '欧芹',
            'tmall_word': re.compile('欧芹粉|欧芹碎'),
            'tmall_not_word': None,
            'dy_word': re.compile('欧芹粉|欧芹碎'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '芥末',
            'tmall_word': re.compile('芥末粉'),
            'tmall_not_word': re.compile('芥末.+粉'),
            'dy_word': re.compile('芥末粉'),
            'dy_not_word': re.compile('芥末.+粉')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '大蒜',
            'tmall_word': re.compile('大蒜粒|大蒜粉'),
            'tmall_not_word': re.compile('新鲜|葱姜蒜|欧芹大蒜|复合'),
            'dy_word': re.compile('大蒜粒|大蒜粉'),
            'dy_not_word': re.compile('新鲜|葱姜蒜|欧芹大蒜|复合')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '洋葱',
            'tmall_word': re.compile('洋葱粒|洋葱粉|洋葱碎'),
            'tmall_not_word': None,
            'dy_word': re.compile('洋葱粒|洋葱粉|洋葱碎'),
            'dy_not_word': re.compile('块')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '单一香辛料',
            'third_cate': '莳萝',
            'tmall_word': re.compile('莳萝碎|莳萝草'),
            'tmall_not_word': re.compile('新鲜'),
            'dy_word': re.compile('莳萝碎|莳萝草'),
            'dy_not_word': re.compile('新鲜')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '孜然',
            'tmall_word': re.compile('孜然'),
            'tmall_not_word': re.compile('新疆|淄博|东北'),
            'dy_word': re.compile('孜然'),
            'dy_not_word': re.compile('新疆|淄博|东北')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '胡椒海盐',
            'tmall_word': re.compile('胡椒海盐粉|胡椒海盐粒|胡椒海盐碎'),
            'tmall_not_word': None,
            'dy_word': re.compile('胡椒海盐粉|胡椒海盐粒|胡椒海盐碎'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '欧芹大蒜',
            'tmall_word': re.compile('欧芹大蒜盐'),
            'tmall_not_word': None,
            'dy_word': re.compile('欧芹大蒜盐'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '奥尔良',
            'tmall_word': re.compile('奥尔良腌料'),
            'tmall_not_word': re.compile('半成品'),
            'dy_word': re.compile('奥尔良腌料|奥尔良调料'),
            'dy_not_word': re.compile('半成品')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '咖喱',
            'tmall_word': re.compile('玛莎拉|咖喱粉'),
            'tmall_not_word': re.compile('咖喱块'),
            'dy_word': re.compile('玛莎拉粉|咖喱粉'),
            'dy_not_word': re.compile('咖喱块')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '芥辣',
            'tmall_word': re.compile('芥末粉|芥辣粉'),
            'tmall_not_word': re.compile('酱'),
            'dy_word': re.compile('芥末粉|芥辣粉'),
            'dy_not_word': re.compile('酱')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '葱姜蒜',
            'tmall_word': re.compile('葱姜蒜粉'),
            'tmall_not_word': re.compile('脱水葱姜蒜'),
            'dy_word': re.compile('葱姜蒜粉'),
            'dy_not_word': re.compile('脱水葱姜蒜|大蒜粉')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '混合胡椒',
            'tmall_word': re.compile('黑胡椒酱|黑胡椒盐'),
            'tmall_not_word': None,
            'dy_word': re.compile('黑胡椒酱|黑胡椒盐'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '烧烤',
            'tmall_word': re.compile('烧烤蘸料|烧烤腌料'),
            'tmall_not_word': re.compile('奥尔良|烧烤酱'),
            'dy_word': re.compile('烧烤蘸料|烧烤腌料'),
            'dy_not_word': re.compile('奥尔良|烧烤酱')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '蔬菜混合调味粉',
            'tmall_word': re.compile('蔬菜混合粉'),
            'tmall_not_word': re.compile('番茄粉|绿女神粉'),
            'dy_word': re.compile('蔬菜混合粉'),
            'dy_not_word': re.compile('番茄粉|绿女神粉')
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '普罗旺斯风味混合香料',
            'tmall_word': re.compile('普罗旺斯调料'),
            'tmall_not_word': None,
            'dy_word': re.compile('普罗旺斯调料'),
            'dy_not_word': None
        },
        {
            'first_cate': 'H&S',
            'second_cate': '混合香辛料',
            'third_cate': '柠檬辣椒粉',
            'tmall_word': re.compile('柠檬辣椒粉'),
            'tmall_not_word': None,
            'dy_word': re.compile('柠檬辣椒粉'),
            'dy_not_word': None
        },
        {
            'first_cate': '蒜香酱',
            'second_cate': '蒜香黄油',
            'third_cate': '蒜香黄油',
            'tmall_word': re.compile('蒜香黄油'),
            'tmall_not_word': None,
            'dy_word': re.compile('蒜香黄油'),
            'dy_not_word': None
        },
        {
            'first_cate': '青瓜酱',
            'second_cate': '芥末青瓜酱',
            'third_cate': '芥末青瓜酱,黄芥末酸瓜酱',
            'tmall_word': re.compile('芥末青瓜酱|芥末青瓜蓉|黄芥末酸黄瓜酱'),
            'tmall_not_word': re.compile('酸甜青瓜酱|酸黄瓜'),
            'dy_word': re.compile('芥末青瓜酱|芥末青瓜蓉|黄芥末酸黄瓜酱'),
            'dy_not_word': re.compile('酸甜青瓜酱|酸黄瓜')
        },
        {
            'first_cate': '青瓜酱',
            'second_cate': '酸黄瓜',
            'third_cate': '腌制青瓜,酸黄瓜罐头',
            'tmall_word': re.compile('酸甜青瓜酱|酸黄瓜'),
            'tmall_not_word': None,
            'dy_word': re.compile('酸甜青瓜酱|酸黄瓜'),
            'dy_not_word': None
        },
        {
            'first_cate': '咸蛋黄酱',
            'second_cate': '咸蛋黄南瓜酱',
            'third_cate': '咸蛋黄南瓜酱',
            'tmall_word': re.compile('咸蛋黄南瓜酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('咸蛋黄南瓜酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '松露酱',
            'second_cate': '黑松露酱',
            'third_cate': '黑松露酱',
            'tmall_word': re.compile('黑松露酱'),
            'tmall_not_word': re.compile('白松露酱'),
            'dy_word': re.compile('黑松露酱'),
            'dy_not_word': re.compile('白松露酱')
        },
        {
            'first_cate': '松露酱',
            'second_cate': '白松露酱',
            'third_cate': '白松露酱',
            'tmall_word': re.compile('白松露酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('白松露酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '甜酱',
            'second_cate': '咖椰酱',
            'third_cate': '咖椰酱',
            'tmall_word': re.compile('咖椰酱|斑斓酱|kaya'),
            'tmall_not_word': None,
            'dy_word': re.compile('咖椰酱|kaya酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '甜酱',
            'second_cate': '果酱',
            'third_cate': '果酱',
            'tmall_word': re.compile('果酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('果酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '甜酱',
            'second_cate': '坚果酱',
            'third_cate': '扁桃仁酱,奇亚籽酱,开心果酱',
            'tmall_word': re.compile('坚果酱'),
            'tmall_not_word': re.compile('花生酱'),
            'dy_word': re.compile('坚果酱'),
            'dy_not_word': re.compile('花生酱')
        },
        {
            'first_cate': '甜酱',
            'second_cate': '坚果酱',
            'third_cate': '栗子酱',
            'tmall_word': re.compile('栗子酱|栗子泥'),
            'tmall_not_word': re.compile('板栗馅'),
            'dy_word': re.compile('栗子酱|栗子泥'),
            'dy_not_word': re.compile('板栗馅')
        },
        {
            'first_cate': '甜酱',
            'second_cate': '巧克力酱',
            'third_cate': '巧克力酱',
            'tmall_word': re.compile('巧克力酱|可可酱'),
            'tmall_not_word': re.compile('粉|花生酱'),
            'dy_word': re.compile('巧克力酱|可可酱'),
            'dy_not_word': re.compile('粉|花生酱')
        },
        {
            'first_cate': '甜酱',
            'second_cate': '抹茶酱',
            'third_cate': '抹茶酱',
            'tmall_word': re.compile('抹茶酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('抹茶酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '甜酱',
            'second_cate': '牛油果酱',
            'third_cate': '牛油果酱',
            'tmall_word': re.compile('牛油果酱|牛油果泥'),
            'tmall_not_word': None,
            'dy_word': re.compile('牛油果酱|牛油果泥'),
            'dy_not_word': None
        },
        {
            'first_cate': '花生酱',
            'second_cate': '花生酱',
            'third_cate': '花生酱',
            'tmall_word': re.compile('幼滑花生酱|柔滑花生酱'),
            'tmall_not_word': re.compile('火锅蘸料|麻酱'),
            'dy_word': re.compile('幼滑花生酱|柔滑花生酱'),
            'dy_not_word': re.compile('火锅蘸料|麻酱')
        },
        {
            'first_cate': '花生酱',
            'second_cate': '花生酱',
            'third_cate': '花生酱',
            'tmall_word': re.compile('颗粒花生酱'),
            'tmall_not_word': re.compile('火锅蘸料|麻酱'),
            'dy_word': re.compile('颗粒花生酱'),
            'dy_not_word': re.compile('火锅蘸料|麻酱')
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '芝士/奶油酱',
            'third_cate': '酸奶油酱',
            'tmall_word': re.compile('酸奶油酱'),
            'tmall_not_word': re.compile('希腊酸奶酱'),
            'dy_word': re.compile('酸奶油'),
            'dy_not_word': None
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '芝士/奶油酱',
            'third_cate': '奶香芝士酱&海盐芝士酱',
            'tmall_word': re.compile('芝士酱|奶酪酱'),
            'tmall_not_word': re.compile('意面酱|沙拉酱|芝士块|芝士碎|芝士条|芝士片|希腊酸奶酱'),
            'dy_word': re.compile('芝士酱|奶酪酱'),
            'dy_not_word': re.compile('意面酱|沙拉酱|芝士块|芝士碎|芝士条|芝士片')
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '芝士/奶油酱',
            'third_cate': '榴莲芝士酱',
            'tmall_word': re.compile('芝士榴莲酱'),
            'tmall_not_word': re.compile('希腊酸奶酱'),
            'dy_word': re.compile('芝士榴莲酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '炼乳',
            'third_cate': '炼乳',
            'tmall_word': re.compile('炼乳|炼奶'),
            'tmall_not_word': None,
            'dy_word': re.compile('炼乳|炼奶'),
            'dy_not_word': None
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '酸奶酱',
            'third_cate': '酸奶酱',
            'tmall_word': re.compile('奇亚籽酸奶酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('奇亚籽酸奶酱'),
            'dy_not_word': None
        },
        {
            'first_cate': '奶制品调味料',
            'second_cate': '酸奶酱',
            'third_cate': '酸奶酱',
            'tmall_word': re.compile('希腊酸奶酱'),
            'tmall_not_word': None,
            'dy_word': re.compile('希腊酸奶酱'),
            'dy_not_word': None
        }
    ]

    heinz_rule_dict = {
        '美式/德式/墨西哥式烧烤酱' : {
            'first_cate': '西式烧烤酱',
            'second_cate': '欧美式烧烤酱',
            'third_cate': '美式/德式/墨西哥式烧烤酱',
            'tmall_word': re.compile('烧烤酱'),
            'tmall_not_word': re.compile('辣酱|牛排酱|黑椒酱|黑椒汁|黑胡椒酱|黑胡椒汁'),
            'dy_word': re.compile('烧烤酱'),
            'dy_not_word': re.compile('辣酱|牛排酱|黑椒酱|黑椒汁|黑胡椒酱|黑胡椒汁')
        },
        '黑椒酱,黑椒汁': {
            'first_cate': '黑椒酱',
            'second_cate': '黑椒酱',
            'third_cate': '黑椒酱,黑椒汁',
            'tmall_word': re.compile('黑椒酱|黑椒汁|黑胡椒酱|黑胡椒汁'),
            'tmall_not_word': None,
            'dy_word': re.compile('黑椒酱|黑椒汁|黑胡椒酱|黑胡椒汁'),
            'dy_not_word': None
        },
    }

    def start(self, batch_now, read_data, other_info=None):
        self.batch_now = batch_now
        self.read_data = self.one_to_multi(read_data, other_info)
        self.result_dict = dict()

        for row in self.read_data:
            self.batch_now.print_log('{line}Printing Clean Process{line}'.format(line=self.batch_now.line))
            item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map = self.get_row_props(row)
            clean_type, mp, sp, detail = self.process_row(item_id, source, cid, all_bid, alias_all_bid, prop_all, trade_prop_all, f_map)

            thrid_cate_set, second_cate_set, first_cate_set = set(), set(), set()

            tmall_judge = False
            dy_judge = False
            if cid in [50009821,50009822,50009823,50009824,50009825,50009826,50009827,50009828,50009829,50009830,50009831,50009833,50009834,50009835,50009856,50009878,50009879,50009880,50010891,50013185,50017138,50017140,50017141,50025222,50025674,50025675,50025697,50050640,50050641,50050642,121392005,121456006,121458029,121470031,123150002,123194003,124326002,126462003,201169511,201196801,201234402,201234702,201236902,201241601,201358213,201375218,2677]:
                tmall_judge = True
            elif cid in [20104,21664,21665,21666,21672,21673,21674,21675,28208,28209,28210,28211,28212,28213,28214,28215,28216,28217,28218,28219,28220,28221,28222,28223,28224,28225,28226,28227,28228,28229,28230,28231,28232,28233,28234,28235,28236,28237,28238,28239,28240,28241,28242,28243,28244,28245,28246,28247,28248,28249,28250,28255,28257,28259,28260,28261,28262,28263,28264,28265,28266,28267,28268,31183,31184,31185,31186,31187,31188,31189,31190,31191,31192,31193,31194,31195,31196,31197,31198,31199,31200,31201,31202,31203,31204,31205,31206,31207,31208,31209,31210,31211,31212,31213,31214,31215,31216,31217,31218,31219,31220,31221,31222,31223,31224,31225,31226,31227,31228,31229,31230,31231,31232,31233,31234,31235,31236,31237,31238,31239,31240,31241,31242,31243,31244,31245,31246,31247,31248,31249,31250,31251,31252,31253,31254,31255,31256,31257,31258,31259,31260,31261,31262,31263,31264,31265,32138,32139,32140,32141,32142,32143,32144,32145,32146,32147,32987,33377,33378,34052,34189,34190,34191,34192,34193,34194,34290,34291,34807,34808,34809,34810,34811,35492,35493,35494,35495,35496,35497,35498,35499,35500,35501,35502,35503,35504,35510,35511,35512,36418,36628,36629,36799,36802,36803,36804,36805,36806,36807,36808,36903,36991,37001,37002,37003,37110,38887]:
                dy_judge = True

            # 额外的新的限制
            # 品牌限制
            second_give_up_dict = {
                188947: ['花生酱'],
                48266: ['牛排酱','披萨酱'],
                6359637: ['牛排酱'],
                17435: ['披萨酱'],
                271388: ['披萨酱'],
                1875344: ['青酱'],
                331898: ['青酱'],
                6432156: ['日韩式 （韩式辣酱,火鸡酱）'],
                426198: ['酸黄瓜'],
                116483: ['酸黄瓜'],
                5919244: ['酸奶酱'],
                135268: ['红酱'],
                187744: ['东南亚 （泰式,越南,印度）'],
                87365: ['东南亚 （泰式,越南,印度）'],
            }

            all_give_up_list = [5534466,5877108,7168638,6577954,187613]


            match_prop_str = self.batch_now.pos[3]['p_key'].get(str(source)+str(cid))
            if match_prop_str != None and (tmall_judge or dy_judge) and alias_all_bid not in all_give_up_list:
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
                    self.batch_now.print_log('三级子品类判断词为：', judge_word)

                    if tmall_judge:
                        keyword, not_keyword = 'tmall_word', 'tmall_not_word'
                    if dy_judge:
                        keyword, not_keyword = 'dy_word', 'dy_not_word'
                    
                    for rule in self.rule_list:
                        not_rule_found = False
                        found = False

                        # 如果是亨氏，并且是某几个品类，使用单独的清洗规则
                        if alias_all_bid == 48266 and self.heinz_rule_dict.get(rule['third_cate']) != None:
                            rule = self.heinz_rule_dict.get(rule['third_cate'])
                        
                        if rule[not_keyword] != None and len(re.findall(rule[not_keyword], judge_word)) != 0:
                            not_rule_found = True
                        
                        if not not_rule_found:
                            if rule[keyword] == None:
                                found = True
                            elif len(re.findall(rule[keyword], judge_word)) != 0:
                                found = True
                            
                        not_update_flag = False
                        if second_give_up_dict.get(alias_all_bid) != None and rule['second_cate'] in second_give_up_dict.get(alias_all_bid):
                            not_update_flag = True
                        
                        if found and not not_update_flag:
                            thrid_cate_set.add(rule['third_cate'])
                            second_cate_set.add(rule['second_cate'])
                            first_cate_set.add(rule['first_cate'])

            sp[3] = self.split_word.join(list(thrid_cate_set))
            sp[4] = self.split_word.join(list(second_cate_set))
            sp[5] = self.split_word.join(list(first_cate_set))

            self.result_dict[item_id] = self.post_process_row(item_id, all_bid, alias_all_bid, clean_type, mp, sp, detail, f_map)
            self.batch_now.print_log('id =', item_id, self.result_dict[item_id])

        return self.result_dict