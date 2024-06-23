# import MySQLdb
import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app

class EntityItem():
    def __init__(self, cols, data, cln):
        self.cols = list(cols)
        self.dict = {}
        for i, k in enumerate(cols):
            self.dict[k] = data[i]
        self.cleaner = cln

    # def __str__(self):
    #     return 'test'

    def __getitem__(self, key):
        if self.cleaner.entity['update_alias_bid'] and key == 'alias_all_bid':
            return self.cleaner.get_allbrand(self.dict['all_bid'], 'alias_bid')

        if key in self.dict:
            return self.dict[key]

        ext = {'tb_item_id': 'item_id'}
        if key in ext:
            return self.dict[ext[key]]

        if key == 'prop_all':
            return self.prop_all()

        if key == 'trade_prop_all':
            return self.trade_prop_all()

        if key == 'shop_type_ch':
            return self.shop_type_ch()

        if key == 'shop_name':
            return self.shop_name()

        if key == 'shopkeeper':
            return self.shopkeeper()

        return None

    def __setitem__(self, key, value):
        if key not in self.cols:
            self.cols.append(key)
        self.dict[key] = value

    # @property
    def prop_all(self):
        prop_all = {}
        for i, k in enumerate(self.dict['props.name']):
            if self.dict['props.value'][i] == '':
                continue
            prop_all[k] = self.dict['props.value'][i]
        return prop_all

    # @property
    def trade_prop_all(self):
        trade_prop_all = {}
        for i, k in enumerate(self.dict['trade_props.name']):
            if self.dict['trade_props.value'][i] == '':
                continue
            trade_prop_all[k] = self.dict['trade_props.value'][i]
        return trade_prop_all

    def get_data(self, cols=None):
        data = {}
        cols = cols or self.cols
        for k in cols:
            data[k] = self[k]
        return data

    # @property
    def shop_type_ch(self):
        mpp = {
            'tb'    : {11: '淘宝', 12: '淘宝'},
            'tmall' : {21: '天猫', 22: '天猫国际', 23: '天猫超市', 24: '天猫国际', 25: '天猫', 26: '猫享自营', 27: '猫享自营国际', 28: '阿里健康国内', 9: '阿里健康国际'},
            'jd'    : {11: '京东国内自营', 12: '京东国内POP', 21: '京东海外自营',22: '京东海外POP'},
            'gome'  : {11: '国美国内自营', 12: '国美国内POP', 21: '国美海外自营',22: '国美海外POP'},
            'jumei' : {11: '聚美国内自营', 12: '聚美海外自营'},
            'kaola' : {11: '考拉国内自营', 12: '考拉国内POP', 21: '考拉海外自营',22: '考拉海外POP'},
            'pdd'   : {11: '拼多多国内自营', 12: '拼多多国内POP', 21: '拼多多海外自营',22: '拼多多海外POP'},
            'suning': {11: '苏宁国内自营', 12: '苏宁国内POP', 21: '苏宁海外自营',22: '苏宁海外POP'},
            'vip'   : {11: '唯品会国内自营', 12: '唯品会海外自营'},
            'jx'    : {11: '酒仙自营', 12: '酒仙非自营'},
            'tuhu'  : {11: '途虎'},
            'dy'  : {11: '抖音'},
            'cdf'  : {11: 'cdf'},
            'lvgou'  : {11: '旅购日上优选', 12:'旅购日上上海'},
            'dewu'  : {11: '得物'},
            'hema'  : {11: '盒马'},
            'sunrise'  : {11: '国内购', 21: '跨境购'},
            'test17'  : {11: '得物'},
            'test18'  : {11: '得物'},
            'test19'  : {11: '得物'},
            'ks'  : {11: '快手'},
            '999' : {}
        }
        source, shop_type = self.dict['source'], self.dict['shop_type']
        if source not in mpp:
            return ''
        if shop_type not in mpp[source]:
            return ''
        return mpp[source][shop_type]

    shop_cache = {}
    def get_shop(self):
        dbch = app.get_clickhouse('default')

        source, sid = self.dict['source'], self.dict['sid']
        k = '{}-{}'.format(source, sid)
        if k not in EntityItem.shop_cache:
            sql = 'SELECT nick, title FROM artificial.shop_{}'.format(self.dict['eid'])
            ret = dbch.query_all(sql)
            EntityItem.shop_cache[k] = ['', ''] if len(ret)==0 or ret[0][0] is None else list(ret[0])
        return EntityItem.shop_cache[k]

    # @property
    def shopkeeper(self):
        return self.get_shop()[1]

    def shop_name(self):
        return self.get_shop()[0]