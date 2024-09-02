import os, sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import base64
import json
import pandas as pd
import requests
import time
import datetime
import argparse
import urllib.parse
import asyncio
from pathlib import Path
import imghdr
from PIL import Image

from chatgpt import gpt_utils
from extensions import utils
from sklearn.metrics import accuracy_score
import application as app

dbname = 'product_lib'
dbname2 = 'douyin2_cleaner'
table_response = f'{dbname}.chatgpt_response'
table_item = f'{dbname}.qsi_item_list_0904'
table_model2 = f'{dbname}.nlp_model_qsi_0904_v2'
table_class3 = f'{dbname}.class_model_qsi_0904_v3'
table_cv = f'{dbname}.qsi_item_list_0904_shibie'
table_spu_sample = f'{dbname}.qsi_item_list_0904_spu'
table_spu_confirmed = f'{dbname}.qsi_item_list_0904_spu_confirmed'
table_image_encode = f'{dbname}.image_encode'
rate_limit_by_nlp_for_new = 0.1 #划定为新品的阈值

where2 = 'is_machine=0 and common_test=1 and is_real_test=1'

headers_model_scope = {"Authorization": f"Bearer aa90895e-7aa5-4a76-bdf6-2dc4387645c0"}

db = app.get_db('default')

def get_in_ref(filename, key='id'):
    '''
    从文件中加载ref
    :param filename:
    :return:
    '''
    df = pd.read_csv(filename)
    h = {idx: row[key] for idx, row in df.iterrows()}
    return h

async def easy_call_openai(messages, model=''):
    # # gpt3.5 用的是azure
    # _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages)
    # # gpt3.5 日本账号
    # _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages, model='gpt3', mode='nintjp')
    # # gpt3.5 16k
    # _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages, model='gpt3new', mode='nintjp')
    # gpt4
    # _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages, model='gpt4', mode='nintjp')
    # gpt4 32k
    # _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages, model='gpt4new', mode='nintjp')

    if model == 'gpt4':
        _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages, model='gpt4', mode='nintjp')
    else:
        _, ask_result_dict = await gpt_utils.get_chat_answers_nint(messages)
    return ask_result_dict

def save_all(db, ask_result_dict, h_message, batch=0, table=table_response, cols=['category', 'aweme_id', 'input', 'type', 'response', 'batch']):
    category = 1
    res = []
    i = -1
    for mm, response in ask_result_dict.items():
        i += 1
        resp_cont = json.dumps(response, ensure_ascii=False)
        # print(response)
        ttltokens = 0  # response['usage']['total_tokens']
        res.append((category, h_message[mm], mm, 1, resp_cont, batch))
        # print(response)
    utils.easy_batch(db, table, cols, res, ignore=True)

def parse_result(res, key_list = ['choices', 'message', 'content']):
    for k in key_list:
        if k not in res:
            print('need key:', k, res)
            break
        res = res[k]
        if type(res) == list:
            res = res[0]

        if type(res) is str:
            return res
    return res

def get_file_id(res):
    res = res.text
    res = json.loads(res)
    return res['id']

def aiapi_files(filename, openai_key):
    url = "https://aiapi.sh.nint.com/v1/files"
    headers = {
        "Authorization": f"Bearer {openai_key}"
    }
    payload = {
        "purpose": "fine-tune"
    }
    files = {
        "file": ("valid.json", open(f"{filename}", "rb"))
    }

    res = requests.post(url, headers=headers, data=payload, files=files)
    train_file_id = get_file_id(res)
    return train_file_id

def aiapi_finetune(train_file_id, openai_key):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_key}"
    }

    url = "https://aiapi.sh.nint.com/v1/fine_tuning/jobs"

    # 设置请求的 JSON 数据
    payload = {
        "training_file": train_file_id,
        #     "validation_file":validation_file_id,
        "model": "gpt-3.5-turbo-0613",
        "suffix": "qsi_sample"
    }

    # 发送 POST 请求
    res = requests.post(url, headers=headers, data=json.dumps(payload))
    if res.status_code == 200:
        print("Success:", res.json())
    else:
        print("Failed:", res.status_code, res.text)
    model_id = res.json()['id']
    return model_id

def aiapi_jobs(model_id, openai_key):
    headers = {
        "Authorization": f"Bearer {openai_key}"
    }

    res = requests.get(
        f"https://aiapi.sh.nint.com/v1/fine_tuning/jobs/{model_id}",
        headers=headers
    )

    # Check if the request was successful
    if res.status_code == 200:
        print("Success:", res.json())
    else:
        print("Failed:", res.status_code, res.text)

    model_name = res.json()['fine_tuned_model']
    return model_name

def aiapi_cancel(job_id, openai_key):
    headers = {
        "Authorization": f"Bearer {openai_key}"
    }

    res = requests.post(
        f"https://aiapi.sh.nint.com/v1/fine_tuning/jobs/{job_id}/cancel",
        headers=headers
    )

    # Check if the request was successful
    if res.status_code == 200:
        print("Success:", res.json())
    else:
        print("Failed:", res.status_code, res.text)

    model_name = res.json()
    return model_name


def aiapi_chat_completions(openai_key, model_name, messages, max_tokens=500):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_key}"
    }

    url = "https://aiapi.sh.nint.com/v1/chat/completions"

    # 设置请求的 JSON 数据
    payload = {
        "model": f"{model_name}",
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens
    }

    # 发送 POST 请求
    response = requests.post(url, headers=headers, json=payload)

    return response.json()

def aiapi_chat_completions_vision(openai_key, messages, max_tokens=500, try_count=1):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_key}"
    }

    url = 'https://aiapi.sh.nint.com/v1/chat/completions'
    payload = {
        'model': "gpt-4-vision-preview",
        'messages': messages,
        'max_tokens': max_tokens,
    }

    # 发送 POST 请求
    res = requests.post(url, headers=headers, json=payload)

    if res.status_code == 200:
        print("Success:", res.json())
        return res.json()
    else:
        if res.status_code == 429 and try_count < 3:
            time.sleep(900)
            return aiapi_chat_completions_vision(openai_key, messages, max_tokens, try_count=try_count+1)

        print("Failed:", res.status_code, res.text)
        return res.text


def use_self_llm(prompt='', llm_name='EcomGPT'):
    url = f"http://10.21.90.183:46666/call_llm/?llm_name={llm_name}&prompt=" + urllib.parse.quote(prompt)
    res = requests.get(url, headers={'Content-Type':'application/json'})
    return res.json()

def use_qwen_vl_large_call(messages, model='qwen-vl-max'):
    from nlp.llm.qwen import simple_multimodal_conversation_call
    return simple_multimodal_conversation_call(messages, model)

def get_new_list(
        db, where2=where2, rate_limit=rate_limit_by_nlp_for_new, debug=False, rate_limit_similary=0.9, rate_limit_cv=70, rate_limit_class=0.9,
        table_item=table_item,
        table_model=table_model2,
        table_class=table_class3,
        table_cv=table_cv
    ):
    '''
    根据伟恒的模型判断为新品
    :return:
    '''
    global g_data, g_data2, g_data3, g_data4, g_data5
    if not utils.isset('g_data') or not utils.isset('g_data2') or not utils.isset('g_data3') or not utils.isset('g_data4') or not utils.isset('g_data5'):
        g_data, g_data2, g_data3, g_data4, g_data5 = None, None, None, None, None
    if g_data is None:
        sql = f"select id, new_flag from {table_item} a where {where2}"
        g_data = data = db.query_all(sql)
    else:
        data = g_data
    l_id = [x[0] for x in data]
    id_str = ','.join([str(x) for x in l_id])

    h = {x[0]: x[1] for x in data}
    if g_data2 is None:
        print('g_data2 is None')
        sql = f"select id, full_res_text from {table_model} where id in ({id_str})"
        g_data2 = data2 = db.query_all(sql)
    else:
        data2 = g_data2

    if g_data3 is None:
        print('g_data3 is None')
        sql = f"select id, full_res_text from {table_class} where id in ({id_str})"
        g_data3 = data5 = db.query_all(sql)
    else:
        data5 = g_data3
    h_class = {}
    for id, txt in data5:
        txt = json.loads(txt)[:5]
        folder_id, rate = list(txt[0][:2])
        if rate < rate_limit_class:
            h_class[id] = 1

    if g_data4 is None:
        print('g_data4 is None')
        sql = f"select task_id, sim from {table_cv} where  task_id not in (select distinct task_id from {table_cv} where sim>70)"
        #sim<{rate_limit_cv}
        data3 = db.query_all(sql)
        g_data4 = data3
    else:
        data3 = g_data4
    h_id_cv = {}
    for id, sim in data3:
        if sim < rate_limit_cv:
            h_id_cv[id] = 1

    if g_data5 is None:
        print('g_data5 is None')
        sql = f"select distinct id from {table_item} where id not in (select distinct task_id from {table_cv})"
        data4 = db.query_all(sql)
        h_id_no_cv = {x[0]:1 for x in data4}
        g_data5 = h_id_no_cv
    else:
        h_id_no_cv = g_data5

    l_real = []
    l_pre = []
    h_new = {}  #存储新品的判断状态
    c1, c2, c3, c4 = 0, 0, 0, 0   #新品数 非新品数 新品判断为新品数 非新品判断为非新品数
    for id, txt in data2:
        txt = json.loads(txt)[:5]
        name, folder_id, rate1, rate2 = list(txt[0])
        new_flag_real = h[id]
        new_flag_pre = 1 if ((id in h_id_cv or id in h_id_no_cv) and (rate2 < rate_limit and rate1 < rate_limit_similary) and id in h_class) else 0
        l_real.append(new_flag_real)
        l_pre.append(new_flag_pre)
        #判断为新品的数据，全部不用
        if new_flag_pre == 1:
            h_new[id] = new_flag_pre
        if new_flag_real == 1:
            c1 += 1
            if new_flag_pre == 1:
                c3 += 1
        else:
            c2 += 1
            if new_flag_pre == 0:
                c4 += 1
    acc = '{:.2f}'.format(accuracy_score(l_real, l_pre))
    print('acc:', acc)
    print(f'新品数：{c1} 非新品数：{c2} 新品判断为新品：{c3} 非新品判断为非新品：{c4}')
    if debug:
        return acc, h_new, h, c1, c2, c3, c4
        # return acc, h_new, '{:.2f}'.format(c3/c1), '{:.2f}'.format(c4/c2), c1, c2, c3, c4
    else:
        return acc, h_new

def test_get_new_list():
    global rate_limit_by_nlp_for_new, g_data, g_data2, g_data3, g_data4, g_data5
    g_data, g_data2, g_data3, g_data4, g_data5 = None, None, None, None, None
    table_item = args.table_item if args.table_item != '' else f'{dbname}.qsi_item_list_1009'
    table_model = args.table_model if args.table_model != '' else f'{dbname}.nlp_model_qsi_1009_v2'
    table_class = args.table_class if args.table_class != '' else f'{dbname}.class_model_qsi_1009'
    table_cv = args.table_cv if args.table_cv != '' else f'{dbname}.qsi_item_list_1009_shibie'
    prefix = args.prefix
    utils.easy_call([db])
    h = {}
    result = []
    flag = False
    filename = app.output_path(f'llm/rate{prefix}.csv')
    #c3: TP c2-c4: FP c4: TN c1-c3: FN  通过率TP/(TP+FN) 误识别率FP/(FP+TN)
    csv_writer = utils.easy_csv_writer(filename, ['key', '总准确率', '新品召回率(通过率)', '老品召回率', '新品准确率', '误识别率', '新品数', '老品数', '新品判断为新品数', '老品判断为老品数'])

    for i in range(0, 100, 5):
        for j in range(0, 100, 5):
            for k in range(0, 70, 5):
                for l in range(0, 100, 5):
                    row = get_new_list(db,where2='common_test=1 and folder_id>0', rate_limit=i / 100, rate_limit_similary=j / 100, rate_limit_cv=k, rate_limit_class=l/100, debug=True,
                                                         table_model=table_model,table_class=table_class, table_item=table_item,
                                                         table_cv=table_cv)
                    acc, h_new, h2, c1, c2, c3, c4 = list(row)
                    rate4 = (c2-c4)/c2
                    print(f'rate4:{rate4}')
                    if rate4 > 0.1:
                        continue
                    rate1, rate2 = '{:.2f}'.format(c3/c1), '{:.2f}'.format(c4/c2)
                    rate3 = '{:.2f}'.format(c3/(c3 + c2 - c4)) if (c3 + c2 - c4) > 0 else '-'
                    rate4 = '{:.2f}'.format((c2-c4)/c2)
                    csv_writer.writerow([i, acc, rate1, rate2, rate3, rate4, c1, c2, c3, c4])
        #             flag = True
        #             break
        #         if flag:
        #             break
        #     if flag:
        #         break
        # if flag:
        #     break

def test_get_new_list2():
    '''
    通过top1置信度，正确数，top1与top2置信度差值3个值来划定新品范围
    :return:
    '''
    global rate_limit_by_nlp_for_new, g_data, g_data2, g_data3, g_data4, g_data5
    g_data, g_data2, g_data3, g_data4, g_data5 = None, None, None, None, None
    table_item = args.table_item if args.table_item != '' else f'{dbname}.qsi_item_list_1009'
    table_class = args.table_class if args.table_class != '' else f'{dbname}.class_model_qsi_1009'
    prefix = args.prefix
    utils.easy_call([db])

    #取数据
    where = args.where
    where = "common_test=1 and folder_id>0" if where == '' else where
    sql = f"select a.id, a.full_res_text, b.new_flag, a.rate1, a.rate1-a.rate2 from {table_class} a left join {table_item} b on a.id=b.id where {where}"
    data = db.query_all(sql)
    h_item = {}
    for row in data:
        id, txt, new_flag, rate1, rate_sub = list(row)
        txt = json.loads(txt)
        folder_id, rate, c = list(txt[0])
        h_item[id] = [new_flag, rate1, rate_sub, c]

    filename = app.output_path(f'llm/rate{prefix}.csv')
    #c3: TP c2-c4: FP c4: TN c1-c3: FN  通过率TP/(TP+FN) 误识别率FP/(FP+TN) 非新品错误识别率
    csv_writer = utils.easy_csv_writer(filename, ['key', '总准确率', '新品召回率(通过率)', '老品召回率', '新品准确率', '误识别率', '新品数', '老品数', '新品判断为新品数', '老品判断为老品数'])
    # for i in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 8500, 9000, 9100,9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900, 9900,9901,9902,9903,9904,9905,9906,9907,9908,9909,9910,9911,9912,9913,9914,9915,9916,9917,9918,9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,9931,9932,9933,9934,9935,9936,9937,9938,9939,9940,9941,9942,9943,9944,9945,9946,9947,9948,9949,9950,9951,9952,9953,9954,9955,9956,9957,9958,9959,9960,9961,9962,9963,9964,9965,9966,9967,9968,9969,9970,9971,9972,9973,9974,9975,9976,9977,9978,9979,9980,9981,9982,9983,9984,9985,9986,9987,9988,9989,9990,9991,9992,9993,9994,9995,9996,9997,9998,9999]:
    #     for j in range(2000, 10000, 10):
    #         for k in range(0, 5, 1):
    for i in [7000, 8000, 8500, 9000, 9100,9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900, 9900,9901,9902,9903,9904,9905,9906,9907,9908,9909,9910,9911,9912,9913,9914,9915,9916,9917,9918,9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,9931,9932,9933,9934,9935,9936,9937,9938,9939,9940,9941,9942,9943,9944,9945,9946,9947,9948,9949,9950,9951,9952,9953,9954,9955,9956,9957,9958,9959,9960,9961,9962,9963,9964,9965,9966,9967,9968,9969,9970,9971,9972,9973,9974,9975,9976,9977,9978,9979,9980,9981,9982,9983,9984,9985,9986,9987,9988,9989,9990,9991,9992,9993,9994,9995,9996,9997,9998,9999]:
        for j in range(7000, 10000, 10):
            for k in range(3, 5, 1):
                c1, c2, c3, c4 = 0, 0, 0, 0  # 新品数 非新品数 新品判断为新品数 非新品判断为非新品数
                l_real, l_pre = [], []
                for id in h_item:
                    new_flag_real, rate1, rate_sub, c = list(h_item[id])
                    new_flag_pre = 0 if (rate1 >= i and rate_sub < j and c >=k) else 1
                    l_real.append(new_flag_real)
                    l_pre.append(new_flag_pre)
                    if new_flag_real == 1:
                        c1 += 1
                        if new_flag_pre == 1:
                            c3 += 1
                    else:
                        c2 += 1
                        if new_flag_pre == 0:
                            c4 += 1
                acc = '{:.2f}'.format(accuracy_score(l_real, l_pre))
                rate4 = (c2-c4)/c2
                print(f'rate4:{rate4}')
                if rate4 > 0.1:
                    continue
                rate1, rate2 = '{:.2f}'.format(c3/c1), '{:.2f}'.format(c4/c2)
                rate3 = '{:.2f}'.format(c3/(c3 + c2 - c4)) if (c3 + c2 - c4) > 0 else '-'
                rate4 = '{:.2f}'.format((c2-c4)/c2)
                csv_writer.writerow([f'{i}#{j}#{k}', acc, rate1, rate2, rate3, rate4, c1, c2, c3, c4])

def output_rate(c_total, c_correct):
    print(f'{c_correct}/{c_total}', '{:.2f}'.format(c_correct*100/c_total))

def get_spu_ref(db):
    sql = f"select id,spu_id,name from {table_spu_sample}"
    data = db.query_all(sql)
    h_spu_id_sample = {x[0]: x[1:] for x in data}

    sql = f"select spu_id2, name2, spu_id, name from {table_spu_confirmed} where confirm_type=1"
    data = db.query_all(sql)
    h_name_spu_id = {x[1]: x[2] for x in data}
    for spu_id2, name2, spu_id, name in data:
        h_spu_id_sample[spu_id2] = [spu_id, name2, name]
    return h_spu_id_sample, h_name_spu_id

def easy_nlp_corom(source, target, key_list=['scores']):
    API_URL = "https://api-inference.modelscope.cn/api-inference/v1/models/damo/nlp_corom_sentence-embedding_chinese-base-ecom"
    # 请用自己的SDK令牌替换{YOUR_MODELSCOPE_SDK_TOKEN}（包括大括号）
    headers = headers_model_scope

    def query(payload):
        data = json.dumps(payload)
        response = requests.request("POST", API_URL, headers=headers, data=data)
        return json.loads(response.content.decode("utf-8"))

    payload = {"input": {"source_sentence": source,
                         "sentences_to_compare": target}}
    res = query(payload)
    if 'Data' in res:
        res = res['Data']
    for k in key_list:
        if k in res:
            res = res[k]
    print(res)
    return res

def query_relevant_items_in_db(db_name, query, k=5, debug=False):
    url = f"http://10.21.90.183:46666/query_relevant_items_in_db/?db_name={db_name}&k={k}&query=" + urllib.parse.quote(query)
    res = requests.get(url, headers={'Content-Type':'application/json'})
    res = res.json()
    if debug:
        print(query, res)

    #转成之前一样的结构
    l = []
    for row in res:
        info, score = list(row)
        info['metadata']['score'] = score
        l.append(info)
    return l

def query_relevant_items_in_db_with_rerank(db_name, query, recall_k=100, rank_k=5, debug=False):
    url = f"http://10.21.90.183:46666/query_relevant_items_in_db_with_rerank/?db_name=" + urllib.parse.quote(db_name) + f"&recall_k={recall_k}&rank_k={rank_k}&query=" + urllib.parse.quote(query)
    res = requests.get(url, headers={'Content-Type':'application/json'})
    res = res.json()
    if debug:
        print(url, query, res)
    return res

def test():

    prompt = '''
    商品名：透真爆水面霜夏天学生补水滋润保湿乳液控油懒人晚霜贵妇膏女男士
    商品规格：50g,所有肤质
    商品介绍： # 
    请从上面的商品信息中总结出商品的品牌及产品，返回一个json串，里面包含品牌和产品两个键
    '''
    # res = use_self_llm(prompt)
    # print(res)

    db_name = 'spu_sample20230928.csv'
    query_relevant_items_in_db_with_rerank(db_name, prompt)



async def call_gpt2():
    messages = []
    mmm = [{'role': 'system', 'content': 'hello'},
           {'role': 'user', 'content': 'how are you today'}]
    messages.append(mmm)

    _, ask_result_dict11 = await gpt_utils.get_chat_answers_nint(messages, model='gpt4', mode='nintjp')
    print(_)
    print(ask_result_dict11)
    return ask_result_dict11


def add_and_fix_spu_id(db, table, primary_key='item_id'):
    l = []
    l2 = []
    for i in range(5):
        l.append(f"add column `spu_id_top{i + 1}` int(11) DEFAULT '-1'")
        l2.append(f"spu_id_top{i+1}=folder_id_top{i+1}")
    l_str = ','.join(l)
    try:
        sql = f"alter table {table} {l_str}"
        db.execute(sql)
    except:
        pass

    l2_str = ','.join(l2)
    sql = f"update {table} set {l2_str}"
    db.execute(sql)
    db.commit()

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def easy_get_image_encode(db, image_path, prefix=''):
    sql = f"select id, txt, file_type, path from {table_image_encode} where img=%s"
    data = db.query_all(sql, (image_path,))
    if len(data) > 0:
        return data[0]

    max_id = db.query_one(f"select max(id) from {table_image_encode}")[0]
    if max_id is None:
        max_id = 0
    id = max_id + 1
    base_dir = Path(r'f:\\images')
    path = f'{prefix}{id}.jpg'
    filename = str(base_dir / path)
    flag = utils.download_image(image_path, filename)
    if not flag:
        return [-1, '', '', '']

    # file_type = imghdr.what(filename)
    img = Image.open(filename)
    file_type = img.format.lower()

    if file_type is None or file_type not in ['png', 'jpeg', 'webp', 'gif']:
        return [-1, '', file_type, path]

    txt = encode_image(filename)
    item_vals = [[id, image_path, txt, path, file_type]]
    utils.easy_batch(db, table_image_encode, ['id', 'img', 'txt', 'path', 'file_type'], item_vals)
    return [id, txt, file_type, path]

def easy_output_messages(l, l2, filename_output, filename_sample, cols=['id', 'item_id', 'name', 'attr_name', 'bid', 'brand_name', 'prop_all', 'folder_id', 'folder_name', 'spu_id', 'spu_name', 'spu_info', 'input']):
    fileObject = open(filename_output, 'w+', encoding='utf-8')
    i = 0
    for s in l2:
        i += 1
        #     if i > 5:
        #         break
        fileObject.write(json.dumps(s, ensure_ascii=False) + "\n")
    fileObject.close()

    utils.easy_csv_write(filename_sample, l, cols)
    return l, l2

def fix_base64():
    utils.easy_call([db])
    table = f"{dbname}.image_encode"
    sql = f"select id,path from {table}"
    data = db.query_all(sql)
    item_vals = []
    for id,path in data:
        base_dir = Path(r'f:\\images')
        path = f'{path}'
        filename = str(base_dir / path)
        file_type = imghdr.what(filename)

        if file_type is not None:
            print(f'The file {filename} is of type {file_type}.')
            item_vals.append([file_type, id])
        else:
            print(f'Could not determine the file type of {filename}.')
    #     txt = encode_image(filename)
    #     item_vals.append([txt, id])
    if len(item_vals) > 0:
        sql = f"update {table} set file_type=%s where id=%s"
        db.execute_many(sql, item_vals)
        db.commit()

def test_easy_get_image_encode():
    utils.easy_call([db])
    img = 'https://gd2.alicdn.com/imgextra/i1/305507801/O1CN01T2slVS27UuQsXwnQf_!!305507801.jpg'
    prefix = '2321142'
    r = easy_get_image_encode(db, img, prefix)
    print(r)

def vl_add_image(filepath, filename, debug=False):
    url = f"http://172.16.50.11:19302/add_image"
    files = {'image': (filename, open(filepath,'rb'))}
    if debug:
        print(f'add image:{filepath}')
    res = requests.post(url, files=files)
    r = res.json()
    if debug:
        print(r)
    return r

def vl_add_text(text, debug=False):
    url = f"http://172.16.50.11:19302/add_text"
    data = {'text': text}
    if debug:
        print(f'add text:{text}')
    res = requests.post(url, data=data)
    r = res.json()
    if debug:
        print(r)
    return r

def vl_generate(debug):
    url = f"http://172.16.50.11:19302/generate"
    res = requests.get(url)
    if debug:
        print('generate done')
    return res.json()

def main(args):
    action = args.action
    eval(action)()

def test_call_gpt2():
    task_list = [call_gpt2()]
    loops=asyncio.get_event_loop()
    loops.run_until_complete(asyncio.wait(task_list))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params for ')
    parser.add_argument('--action', type=str, default='test_easy_get_image_encode', help='action')
    parser.add_argument('--prefix', type=str, default='', help='prefix')
    parser.add_argument('--table_item', type=str, default='', help='table_item')
    parser.add_argument('--table_class', type=str, default='', help='table_class')
    parser.add_argument('--table_model', type=str, default='', help='table_model')
    parser.add_argument('--table_cv', type=str, default='', help='table_cv')
    parser.add_argument('--where', type=str, default='', help='where')

    args = parser.parse_args()
    start_time = time.time()
    main(args)

    end_time = time.time()
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))

