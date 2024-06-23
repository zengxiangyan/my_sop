# -*- coding: utf-8 -*-
import json
import time
from datetime import timedelta
import pandas as pd
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
config_data = pd.read_excel(r'E:\母婴机洗\category_words_demo.xlsx')
config_data = config_data.sort_values('优先级', ascending=True).copy()
config_data = config_data.reset_index(drop=True)
config_data = config_data[['三级类目','四级类目','and_word(必须出现关键词）','or_word（任意一个关键词）','not_word(排除词）','ignore_word(忽略词）']].fillna('')
for i,pannel in enumerate(config_data['三级类目']):
    if  config_data['三级类目'].isna()[i]:
        config_data.iloc[i,0] = config_data.iloc[i-1,0]

replace1 = ['\\','.','^','$','*','+','?','{','}','[',']','(',')']
replace2  = ['\\\\','\\.','\\^', '\\$','\\*','\\+','\\?','\\{','\\}','\\[','\\]','\\(','\\)']
not_ = "洗手机|棉棒|退热贴|泡脚散|泡奶机|暖奶机|纸尿裤|夏凉被|调奶|指甲油|眼影|睡袋|凉席|恒温|消毒柜|女性内衣|女士专用|内裤专用|耳温枪|体温计|黄疸仪|口罩|磨甲器|指甲剪|指甲钳|肚脐贴|女神礼包|挖耳勺|会阴|苦甲水|喂药器|罩衣|冰宝贴|中药贴|清洗机|矫正器|成人围裙|咳喘贴|发炎贴|吹风机|通鼻贴|照蓝光|护理贴|痔疮|坐浴|鼻通贴|耳勺|碘伏棉签|呼吸贴|牛皮藓|安抚玩具|护脐贴|指甲刀|口红套装|收纳箱|脚痒|幼儿园表演|股廯|运费|退烧贴|淤青|邮费|温度计|水温计|足跟皴裂|冲奶器|消毒器|消毒锅|温奶器|彩妆盒|中老年|泡脚散|泡奶机|暖奶机|夏凉被|调奶|指甲油|眼影|睡袋|凉席|恒温|消毒柜|女性内衣|女士专用|耳温枪|体温计|黄疸仪|口罩|磨甲器|指甲剪|指甲钳|肚脐贴|女神礼包|挖耳勺|会阴|苦甲水|喂药器|罩衣|棉签|冰宝贴|中药贴|清洗机|矫正器|成人围裙|咳喘贴|发炎贴|吹风机|通鼻贴|照蓝光|护理贴|痔疮|坐浴|鼻通贴|耳勺|碘伏棉签|呼吸贴|牛皮藓|安抚玩具|护脐贴|指甲刀|口红套装|收纳箱"
not_list = not_.split('|')
r='AND NOT ('
for i,w in enumerate(not_list):
    if i!=len(not_list)-1:
        r += f"name LIKE '%{w}$' OR "
    else:
        r += f"name LIKE '%{w}$')\n "
sql = f"""SELECT source,shop_type,cid,alias_all_bid,sid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as `品牌`
,dictGet('all_shop', 'title', tuple(toUInt8(source), toUInt32(sid))) AS `店铺名`
,case
    when source = 1 and (shop_type < 20 and shop_type > 10 ) then 'tb'
    when source*100+shop_type in (109,121,122,123,124,125,126,127,128) then 'tmall'
    when source = 2 then 'jd'
    when source = 3 then 'gome'
    when source = 4 then 'jumei'
    when source = 5 then 'kaola'
    when source = 6 then 'suning'
    when source = 7 then 'vip'
    when source = 8 then 'pdd'
    when source = 9 then 'jiuxian'
    when source = 11 then 'dy'
    when source = 12 then 'cdf'
    when source = 14 then 'dw'
    when source = 15 then 'hema'
    when source = 16 then 'xinlvgou'
    else '其他' end as `平台`
,case
    when source = 1 and shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',tb_item_id)
    when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',tb_item_id)
    when source = 2 then CONCAT('https://item.jd.com/',tb_item_id,'.html')
    when source = 3 then CONCAT('https://item.gome.com.cn/',tb_item_id,'.html')
    when source = 4 then CONCAT('//item.jumeiglobal.com/',tb_item_id,'.html')
    when source = 5 then CONCAT('https://goods.kaola.com/product/',tb_item_id,'.html')
    when source = 6 then CONCAT('https://product.suning.com/',tb_item_id,'.html')
    when source = 7 then CONCAT('https://detail.vip.com/detail-',tb_item_id,'.html')
    when source = 8 then CONCAT('https://mobile.yangkeduo.com/goods.html?goods_id=',tb_item_id)
    when source = 9 then CONCAT('www.jiuxian.com/goods.',tb_item_id,'.html')
    when source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',tb_item_id)
    else '其他' end as url
,IF(`平台`='dy',concat('''',toString(item_id)),item_id) as `tb_item_id`,name, `trade_props.value` as `交易属性`,concat('''',toString(argMax(uuid2,num))) as uuid2,argMax(img,num) as `图片`,sum(num) as `销量`,SUM(sales)/100 as `销售额`
FROM sop.entity_prod_92232_A
WHERE `date`>='2023-01-01'
AND `date`<'2023-08-01'
AND NOT (name LIKE '%叮叮%' AND name LIKE '%皲裂%')
AND NOT (name LIKE '%叮叮%' AND name LIKE '%皴裂%')
{r}
group by cid,source,shop_type,alias_all_bid,sid,`平台`,item_id,name,`交易属性`
"""
def edit_keyword(text,replace1,replace2):
    if len(replace1) == len(replace2):
        if isinstance(text, str):
            for i,r1 in enumerate(replace1):
                text = text.replace(r1,replace2[i])
        return text
    else:
        print('{}不是字符串'.format(text))
        return None
def get_pattern(and_word,or_word,not_word):
    if and_word!=['']:
        if len(and_word) == 1:
            pattern1 = '(?=.*(' + ')(?=.*'.join(and_word) + '))'
        else:
            pattern1 = '(?=.*' + ')(?=.*'.join(and_word) + ')'
    else:
        pattern1 = ''
    if or_word!=['']:
        pattern2 = '(?=.*?('+'|'.join(or_word)+'))'
    else:
        pattern2 = ''
    if not_word!=['']:
        pattern3 = '((?!'+'|'.join(not_word)+').)*'
    else:
        pattern3 = ''
    if pattern3 != '':
        pattern = '^'+ pattern3 + pattern1 + pattern2 + pattern3 + '$'
    else:
        pattern = '^'+ pattern3 + pattern1 + pattern2 + pattern3 + '.*$'
    # print(pattern)
    return pattern
def clean_config():
    and_word_list = []
    or_word_list = []
    not_word_list = []
    ignore_word_list = []
    pattern_list = []
    for row in range(config_data.shape[0]):
        if (config_data['and_word(必须出现关键词）'][row]!='') or (config_data['or_word（任意一个关键词）'][row]!='') or (config_data['not_word(排除词）'][row]!=''):
            and_word = edit_keyword(config_data['and_word(必须出现关键词）'][row],replace1,replace2).split('&')
            or_word = edit_keyword(config_data['or_word（任意一个关键词）'][row],replace1,replace2).split('|')
            not_word = edit_keyword(config_data['not_word(排除词）'][row],replace1,replace2).split('|')
            ignore_word = config_data['ignore_word(忽略词）'][row].split('|')
            and_word_list.append(and_word)
            or_word_list.append(or_word)
            not_word_list.append(not_word)
            ignore_word_list.append({str(i):'' for i in ignore_word})
            pattern = get_pattern(and_word,or_word,not_word)
            pattern_list.append(pattern)
        else:
            and_word_list.append('')
            or_word_list.append('')
            not_word_list.append('')
            ignore_word_list.append('')
            pattern_list.append('')
    config_data['and_word'] = and_word_list
    config_data['or_word'] = or_word_list
    config_data['not_word'] = not_word_list
    config_data['ignore_word'] = ignore_word_list
    config_data['pattern'] = pattern_list
    return config_data
def get_brush_table():
    conf = {
        "user": "yinglina",
        "password": "xfUW5GMr",
        "server_host": "10.21.90.15",
        "port": "10081",
        "db": "sop"
    }
    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        brush_table = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
    finally:
        cursor.close()
        session.close()
#     brush_table.to_csv(r'E:\母婴机洗\entity_prod_92232_A.csv',encoding='utf-8-sig')
    return brush_table

def get_time_dif(start_time):
    """
    获取已经使用的时间
    :param start_time:
    :return:
    """
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))
def get_global_ignore():
    gn = ['1','2','3','4','5','6','7','8','9','10','一','二','三','四','五','六','七','八','九','十']
    kn1 = ['买','拍']
    kn2 = ['赠','送']
    global_ignore = []
    for k1 in kn1:
        for k2 in kn2:
            for i1,g1 in enumerate(gn):
                for i2,g2 in enumerate(gn):
                    if i1>=i2:
                        global_ignore.append(k1+g1+k2+g2)
    global_ignore += ['买多赠多','拍多赠多','多买多赠','多拍多赠','买多送多','拍多送多','多买多送','多拍多送']
    global_ignore = {str(i):'' for i in global_ignore}
    return global_ignore
def clean_brush_category1(brush_table,pkey,clean_config):
    clean_config1 =  clean_config.loc[clean_config['pattern']!='']
    global_ignore = get_global_ignore()
    for p in pkey:
        brush_table_c = brush_table
        brush_table_c[p].replace(global_ignore, regex=True)
        brush_table_c[p].str.replace(r'(送|赠).*', '', regex=True)
        for i,category in enumerate(clean_config1['四级类目']):
            if clean_config1['ignore_word'][i]!={'':''}:
                brush_table_c[p] = brush_table_c[p].replace(clean_config1['ignore_word'][i])
            brush_table.loc[brush_table_c[p].str.contains(clean_config1['pattern'].to_list()[i]), '清洗子品类'] = brush_table.loc[brush_table_c[p].str.contains(clean_config1['pattern'].to_list()[i]), '清洗子品类'].apply(lambda x: x + [category])
    return brush_table

def run_clean():
    brush_table = get_brush_table()
    clean_config = clean_config()
    brush_table['清洗子品类'] = [[] for i in range(brush_table.shape[0])]
    pkey = ['交易属性','name']
    start_time = time.time()
    clean_brush_table = clean_brush_category1(brush_table,pkey,clean_config)
    clean_brush_table['四级类目'] = [(cp1 + ['其它'])[0] for cp1 in clean_brush_table['清洗子品类']]
    clean_brush_table.to_csv(r'E:\母婴机洗\entity_prod_00000_E.csv', encoding='utf-8-sig', index=False)
    time_dif = get_time_dif(start_time)
    print("机洗时间时间：", time_dif)

def clean_brush_search(uuid2,clean_brush_table,clean_config):
    pkey = ['交易属性', 'name']
    search_report_list = {}
    clean_brush_table = clean_brush_table.loc[clean_brush_table['uuid2']==uuid2]
    clean_config1 =  clean_config.loc[clean_config['pattern']!='']
    global_ignore = get_global_ignore()
    for p in pkey:
        brush_table_c = clean_brush_table.loc[clean_brush_table['uuid2']==uuid2].copy()
        brush_table_c[p] = brush_table_c[p].replace(global_ignore, regex=True)
        brush_table_c[p] = brush_table_c[p].str.replace(r'(送|赠).*', '', regex=True)
        for i,category in enumerate(clean_config1['四级类目']):
            if clean_config1['ignore_word'][i]!={'':''}:
                brush_table_c[p] = brush_table_c[p].replace(clean_config1['ignore_word'][i])
            if brush_table_c.loc[brush_table_c[p].str.contains(clean_config1['pattern'].to_list()[i])].shape[0]!=0:
                search_report = {}
                search_report[f'{p}_value'] = clean_brush_table[p].to_list()[0] +'->'+ brush_table_c[p].to_list()[0]
                search_report['and_word'] = clean_config1['and_word'][i]
                search_report['or_word'] = clean_config1['or_word'][i]
                search_report['not_word'] = clean_config1['not_word'][i]
                search_report['ignore_word'] = clean_config1['ignore_word'][i]
                search_report['category_result'] = category
                search_report_list[i] = search_report
    return search_report_list
def get_clean_brush_search(uuid2):
    clean_config1 = clean_config()
    clean_brush_table = pd.read_csv(r'E:\母婴机洗\entity_prod_92232_A.csv', encoding='utf-8-sig')
    search_report_list = clean_brush_search(f'\'{uuid2}',clean_brush_table,clean_config1)
    return json.dumps(search_report_list, ensure_ascii=False, indent=4)