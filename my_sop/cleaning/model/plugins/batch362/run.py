import pandas as pd
import json
import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), './程序/1程序/1程序/'))
from classifier import *
import application as app

def get_info_by_newno(newno):
    # try:
    db = app.connect_db('wq')
    sql = """ select platform, newno, `no`, `time`, c4, name, brand, url, shopname, unitold, priceold, salesold, unit, price, sales, unitlive, pricelive, c6, Category, SubCategory, SubCategorySegment, BrandName, BrandCN, BrandEN, `User`, ShopType1, ShopType2, Manufacturer, Division, Selectivity, BrandLRL, 
    trade_props_name, trade_props_value,
    cid,brand_id,item_id,sid,shopnamefull,shopurl,shop_create_time,real_num,real_sales,subplatform,img
    from makeupall where newno={}
    """.format(newno)
    data = db.query_all(sql, as_dict=True)
    print(data[0])
    return data[0]
    # except Exception as e:
    #     print(e)
    # return []

def process_log(newno):
    s = '--------------------------------------------------'
    row = get_info_by_newno(newno)
    classifier = Classifier()
    classifier.load_rules(sys.path[0]+ "/rules.xlsx", sheet_name="category.rule2")
    category = classifier.test(row)

    classifier = Classifier()
    classifier.load_rules(sys.path[0]+ "/rules.xlsx", sheet_name="brand.rule")
    brand1 = classifier.test(row)

    classifier = Classifier()
    row2 = row
    row2['name'] = row2['brand']
    classifier.load_rules(sys.path[0] + "/rules.xlsx", sheet_name="brand.rule2")
    brand2 = classifier.test(row2)
    print("result:",category, brand1,brand2)
    result = {'清洗原始数据':'清洗原始数据:'+str(row),
              "category":s+'category:'+category,
              "brand1":s+'brand1:'+brand1,
              "brand2":s+'brand2:'+brand2,
              "最终结果":"【sp】最总结果："+str({"category":category,"brand1":brand1,"brand2":brand2})}
    return result
# if __name__ == "__main__":
#     process_log(53845728)
# import subprocess
#
# script_path = '../程序/1程序/1程序/run.dy1.all.202400509.py'  # 替换为你的脚本路径
# script = 'classifier.py'
# action = 'process'
# process_where = '1'
# cpu_max = 8
# test = 2
# prefix = ''
#
# cmd = [
#     'cd',script_path,
#     'python', script,
#     '--action', action,
#     '--process_where', process_where,
#     '--cpu_max', str(cpu_max),
#     '--test', str(test),
#     '--prefix', prefix
# ]
#
# result = subprocess.run(cmd, capture_output=True, text=True)
#
# print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
# print("Return Code:", result.returncode)


