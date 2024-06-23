# coding=utf-8
import sys, getopt, os, io
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import collections
import math

'''
2021.03.31
数据调整系数--价格和销量
'''

def adjust_process_by_rate(data, target_name, target_value, adjust_sales_by = 'price'):
    # 给所有数据乘以相同的系数
    res = []
    if 'sales' in target_name:
        for row in data:
            date_, item_id, price, org_price, num, sales = row

            if adjust_sales_by == 'price':
                price_new = price * target_value
                sales_new = num * price_new
                res.append([date_, item_id, round(target_value, 3), price, num, sales, round(price_new, 2), num,
                            round(sales_new, 2), '%.2f%%' % ((price_new / price - 1) * 100), '0%', '0%'])
            else:
                num_new = num * target_value
                sales_new = num_new * price
                res.append([date_, item_id, round(target_value, 3), price, num, sales, price, num_new,
                            round(sales_new,2), '0%', '%.2f%%' % ((num_new / num - 1) * 100), '0%'])

    elif 'num' in target_name:
        for row in data:
            date_, item_id, price, org_price, num, sales = row
            num_new = num * target_value
            res.append([date_, item_id, round(target_value, 3), price, num, sales, price, num_new,
                        round(num_new * price,2), '0%', '%.2f%%' % ((num_new / num - 1) * 100),'0%'])

    else:
        for row in data:
            date_, item_id, price, org_price, num, sales = row
            price_new = max(org_price, price * target_value)
            res.append([date_, item_id, round(target_value, 3), price, num, sales, round(price_new, 2), num,
                        round(price_new * num,2), '%.2f%%' % ((price_new / price - 1) * 100), '0%', '0%'])

    print('系数为:{}'.format(round(target_value, 3)))
    raw = sum(i[5] for i in res)
    new = sum(j[8] for j in res)
    print('原始销售额：{}，乘以系数后的销售额：{}'.format(raw, new))
    print('与原始销售额相差：{:.2%}'.format(new / raw - 1))

    return res

def adjust_process_by_exp(data, target_name, target_value, adjust_sales_by = 'price'):
    gr = collections.defaultdict(lambda:[])
    out = collections.defaultdict(lambda:[])

    # 根据预期值来选择哪些乘系数，哪些不乘系数，以及选择最佳系数
    if target_name == 'Exp_sales':
        raw_total = sum(i[5] for i in data)
        avg_value = target_value/raw_total

        for i in [avg_value + j * 0.001 for j in range(100)]:
            res = []
            value_total = 0
            for row in data:
                date_, item_id, price, org_price, num, sales = row
                if adjust_sales_by == 'price':
                    if org_price > 0 and value_total <= target_value:
                        if price * i <= org_price:
                            price_new = price * i
                            index = i
                        else:
                            price_new = max(price, org_price)
                            index = price_new/price
                    else:
                        price_new = price
                        index = 0
                    value_total += price_new * num
                    res.append([date_, item_id, round(index, 3), price, num, sales, round(price_new, 2), num,
                                round(price_new*num, 2), '%.2f%%'%((price_new/price-1)*100),'0%',
                                '%.2f%%'%((price_new/price-1)*100)])
                else:
                    if value_total <= target_value:
                        num_new = num * i
                        index = i
                    else:
                        num_new = num
                        index = 0
                    value_total += num_new * price
                    res.append([date_, item_id, round(index, 3), price, num, sales, price, num_new,
                                round(num_new * price, 2),'0%', '%.2f%%'%((num_new/num-1)*100),
                                '%.2f%%'%((num_new/num-1)*100)])

            sales_total_new = sum(k[8] for k in res)
            gr[i] = target_value / sales_total_new - 1
            print('调整后的销售额为：%d' % sales_total_new)
            print('与目标销售额相差：{:.2%}'.format(target_value / sales_total_new - 1))
            out[i] = res

        coef = sorted(gr.items(), key=lambda x: abs(x[1]))[0][0]
        print('最佳系数及相差倍数分别为：%s %.4f' % (coef, sorted(gr.items(), key=lambda x: abs(x[1]))[0][1]))
        output = out[coef]

    elif target_name == 'Exp_num':
        raw_total = sum(i[5] for i in data)
        avg_value = target_value/raw_total

        res = []
        value_total = 0
        for row in data:
            date_, item_id, org_price, price, num, sales = row
            if value_total <= target_value:
                num_new = math.floor(num * avg_value)
                index = avg_value
            else:
                num_new = num
                index = 0
            value_total += num_new
            res.append([date_, item_id, round(index, 3), price, num, sales, price, num_new,
                        round(num_new*price, 2), '0%', '%.2f%%'%((num_new/num-1)*100), '%.2f%%'%((num_new/num-1)*100)])
        output = res

    else:
        sales_t = sum(i[5] for i in data)
        num_t = sum(i[4] for i in data)
        avg_price_t = sales_t/num_t
        avg_value = target_value/avg_price_t

        res = []
        value_total = 0
        num_total = 0
        for row in data:
            date_, item_id, org_price, price, num, sales = row
            if num_total != 0 and value_total/num_total <= target_value:
                price_new = price * avg_value
                index = avg_value
            else:
                price_new = price
                index = 0
            sales_new = num * price_new
            num_total += num
            value_total += sales_new
            res.append([date_, item_id, round(index, 3), price, num, sales, round(price_new, 2), num,
                        round(price_new*num, 2), '%.2f%%'%((price_new/price-1)*100),'0%',
                        '%.2f%%'%((price_new/price-1)*100)])
        output = res

    return output


def get_target_param(sales, num, price, sales_rate, num_rate, price_rate):
    key_value_dict = {'Exp_sales':sales, 'Exp_num':num, 'Exp_price':price,
                      'Exp_sales_rate':sales_rate,'Exp_num_rate':num_rate,'Exp_price_rate':price_rate}

    for i in key_value_dict:
        if key_value_dict[i] > 0:
            return (i, key_value_dict[i])
    else:
        raise Exception('预期值全为0，请重新输入！！')

def adjust_coef(param):
    sales, num, price, sales_rate, num_rate, price_rate, type, data = param
    target_name, target_value = get_target_param(sales, num, price, sales_rate, num_rate, price_rate)

    if 'rate' in target_name:
        # 用给定的系数去乘
        res_new = adjust_process_by_rate(data, target_name, target_value, type)
    else:
        # 根据给定的预期值，获取最佳系数，并返回最佳系数及乘以系数后的最新数据
        res_new = adjust_process_by_exp(data, target_name, target_value, type)

    return res_new  # 'date','item_id','coef','price','num','sales','price_new','num_new','sales_new','price_rate','num_rate','sales_rate'

if __name__ == '__main__':
    # price	org_price	num	sales
    # 调大	90	60	900	70000  # 1.04  1.14  1.06
    # 调小	50	30	600	60000  # 0.89  0.76  0.59
    param = [70000,0,0,0,0,0,'price',[['2021-02-04','628425535137',44.95,49,2,89.9],
                                ['2021-02-12','598223139923',46.9,54,1,46.90],
                                ['2021-02-11','620582189395',21.9,29.9,1,21.90],
                                ['2021-02-28','601291775693',19.87,41.9,9,178.83],
                                ['2021-02-25','13579574370',12.66,24.9,95,1202.70],
                                ['2021-02-14','559982736870',46.9,62.9,9,422.10],
                                ['2021-02-06','534699214932',4.2,7.2,10,42.00],
                                ['2021-02-16','595712136448',4.9,9.9,26,127.40],
                                ['2021-02-16','597361907362',21.9,24.9,7,153.30],
                                ['2021-02-01','525274201436',102.89,130,632,65026.48]]]
    out = adjust_coef(param)
    print('调数后的最新结果为：{}'.format(out))

    # import json, requests
    # url = 'http://127.0.0.1:8000/api/adjust_coef/'
    # d = json.dumps(param)
    # r = requests.post(url, data=d)
    # print(r.json())

