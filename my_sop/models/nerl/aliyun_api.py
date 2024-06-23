import json
# from aliyunsdkcore.client import AcsClient
# from aliyunsdkcore.request import CommonRequest

def get_client():
    # 读取账号密码
    conf = json.load(open('./conf/aliyun.json', 'r', encoding='utf-8'))

    # 创建AcsClient实例
    client = AcsClient(
        conf['user'],
        conf['password'],
        "cn-hangzhou"
    )
    return client


def get_ner_api_result(client, text):
    request = CommonRequest()

    # domain和version是固定值
    request.set_domain('alinlp.cn-hangzhou.aliyuncs.com')
    request.set_version('2020-06-29')

    # action name可以在API文档里查到
    request.set_action_name('GetNerChEcom')

    # 需要add哪些param可以在API文档里查到
    request.add_query_param('ServiceCode', 'alinlp')
    request.add_query_param('Text', text)
    request.add_query_param('LexerId', 'ECOM')
    response = client.do_action_with_exception(request)
    resp_obj = json.loads(response)
    result = json.loads(resp_obj['Data'])
    # print(result)
    if result['success']:
        output = []
        offset = 0
        for r in result['result']:
            if r['word'] == 'replace' and text[offset] == '|':
                r['word'] = '|'
            output.append([r['word'], offset, offset + len(r['word']), r['tag']])
            offset += len(r['word'])
        return output
    else:
        raise ConnectionError('Failed to connect to API Server.')


if __name__ == '__main__':
    # test
    res = get_ner_api_result("街档酱香金钱肚广式早茶点心速冻半成品肚新鲜冷冻熟食速食卤牛肚")
    print(res)
