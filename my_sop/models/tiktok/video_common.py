project_table = "douyin2_cleaner.project"
run_table = 'douyin2_cleaner.douyin_video_run_3'
user_table = 'xintu_category.author'
ocr_table = 'douyin2.douyin_video_ocr'
xunfei_table = 'douyin2.douyin_video_xunfei'
xintu_table = 'xintu_category.author'
creator_table = 'douyin2.video_creator'
ocr_task_table = "douyin2.douyin_video_process_cleaner_py"
xunfei_task_table = "douyin2.stream_manual_live"
waiting_queue_table = "douyin2_cleaner.waiting_queue"
filter_log_table = "douyin2_cleaner.douyin_video_filter"
origin_table = "douyin2.douyin_video"
clean_model_table = "douyin2_cleaner.douyin_video_clean_model"
clean_log_table = "douyin2_cleaner.douyin_video_clean_log"
saas_table = 'video_power.douyin_video_sass_stream'
xhs_saas_table = 'video_power.xiaohongshu_sass'
saas_log_table = "douyin2_cleaner.douyin_video_result_saas_log"
saas_awms_table = "douyin2_cleaner.douyin_video_result_saas_daily_awms"
xhs_saas_awms_table = "douyin2_cleaner.xiaohongshu_result_saas_daily_awms"
saas_kid_awm_tbl = "douyin2_cleaner.hotword_kid_awm_quanleimu_{category}_{clean_id}"
clean_status_table = "douyin2_cleaner.douyin_video_clean_status"
match_log_table = "douyin2_cleaner.douyin_match_task_log"
hotword_cfg_tbl = 'douyin2_cleaner.hotword_cfg'
hotword_raw_text_tbl = 'douyin2_cleaner.hotword_raw_text'
hotword_kid_awm_tbl = 'douyin2_cleaner.hotword_kid_awm'
hotword_info_tbl = 'douyin2_cleaner.douyin_video_hotword_info_{cat}'
wx_notification_key = ''
prop_tables = {'prop_cat': 'douyin2_cleaner.prop_category',
               'prop': 'douyin2_cleaner.prop',
               'prop_nm': 'douyin2_cleaner.prop_name',
               'prop_val': 'douyin2_cleaner.prop_value',
               'prop_key': 'douyin2_cleaner.prop_keywords_new',
               'prop_key_new': 'douyin2_cleaner.prop_keyword',
               'key': 'douyin2_cleaner.keyword'}
cleantmp_tbls = {'bid': "douyin2_cleaner.douyin_video_brand_zl_tmp",
                 'sub_cid': "douyin2_cleaner.douyin_video_sub_cid_zl_tmp",
                 'pid': "douyin2_cleaner.douyin_video_pid_zl_tmp"}

prop_metrics_en2cn = {'precision': '准确率',
                      'recall': '召回率',
                      'f1-score': 'F1值',
                      'video_num': '视频数'}

# !! 2023.01.29: remove all_categories and get project info from douyin2_cleaner.project
cat2name_ = {
    3: '运动',
    6: '美妆',
    7: '家清个护',
    8: '食品饮料',
    11: '母婴',
    15: '3C家电',
    20: '奢侈品',
    61: '欧莱雅',
    20018: '零食',
    26396: '牙膏/口腔护理',
    20109: '保健品',
    21114: '氛围感',
    26415: '电动牙刷',
    21616: '剃须刀',
    28191: '冲牙器',
    27051: '漱口水',
    20111: '笔记本电脑',
    95305: '杜蕾斯',
    2020109: '江中肠胃'
}
#  557055  1605631
type2name = {
    1: '视频标题',
    2: 'xunfei文本',
    3: 'ocr字幕',
    4: 'ocr封面',
    5: '达人标签',
    6: '官号对应',
    7: '挂车',
    8: '推荐词文本',
    9: '用户名',
    10: '视频时长',
    11: '作者评论文本',
    12: 'ocr_mass',
    13: 'chatgpt文本',
    14: 'whisper文本',
    15: '抓取的ocr文本',
    16: '图文ocr（小红书）',  # 和小红书ocr 3 合并
    17: '官方自带asr（小红书）',
    20: 'tmp',  # 默认放最后第二个
    21: 'finish',  # 默认放最后一个
    100: '人工',
    1001: '挂车商品机洗',
    1100: '挂车商品人工'
}
name2type = {n: t for t, n in type2name.items()}
tmp_result_status = name2type['tmp']
finish_status = name2type['finish']
clean_finish_status_before_tmp = sum(map(lambda i: 2 ** (i - 1), sorted(t for t in type2name if t < 100)[:-2]))
clean_finish_status = sum(map(lambda i: 2 ** (i - 1), sorted(t for t in type2name if t < 100)[:-1]))
result_finish_status = sum(map(lambda i: 2 ** (i - 1), sorted(t for t in type2name if t < 100)))

data_sql = {
    1: "select aweme_id, concat_ws(' ', `desc`, text_extra) from douyin2_cleaner.douyin_video_zl{version}_{category} where aweme_id in ({ids});",
    # 2: "select aweme_id, txt, id, start_time from douyin2.douyin_video_xunfei where aweme_id in ({ids}) order by aweme_id, id;",
    2: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    # 3: "select aweme_id, captions, id, second from douyin2.douyin_video_ocr_sub where aweme_id in ({ids}) order by aweme_id, id;",
    3: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    # 4: "select aweme_id, cover_text, id, 0 from douyin2.douyin_video_ocr_txt where aweme_id in ({ids}) order by aweme_id, id;",
    4: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    5: """
        select distinct aweme_id, tags_relation from xintu_category.author c join douyin2_cleaner.douyin_video_zl{version}_{category} zl on zl.uid=c.core_user_id 
        where aweme_id in ({ids}) and (uid, date) in (
            select core_user_id, max(date) from xintu_category.author 
            where core_user_id in (select uid from douyin2_cleaner.douyin_video_zl{version}_{category} where aweme_id in ({ids})) 
            group by core_user_id); 
    """,
    # 6: "",
    # 7: "",
    # 'cover': "select aweme_id, ocr_cover from douyin2.douyin_video_ocr where aweme_id in ({ids});",
    # 8: "select aweme_id, word from douyin2.douyin_video_suggested_word where aweme_id in ({ids});",
    8: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    9: """
        select aweme_id, nickname from douyin2.video_creator c join douyin2_cleaner.douyin_video_zl{version}_{category} zl
        on zl.uid=c.uid where aweme_id in ({ids}); 
    """,
    10: "select aweme_id, duration from douyin2_cleaner.douyin_video_zl{version}_{category} where aweme_id in ({ids});",
    # 11: "select aweme_id, text, id, create_time from douyin2_cleaner.douyin_video_zl_comment where aweme_id in ({ids});",
    11: "select aweme_id, text from douyin2_cleaner.douyin_video_comment_author where aweme_id in ({ids});",
    # 12: "select aweme_id, txt, id, second from douyin2.douyin_video_ocr_mass where aweme_id in ({ids}) order by aweme_id, id;",
    12: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    13: "select aweme_id, response, id, `type` from douyin2_cleaner.chatgpt_response where aweme_id in ({ids}) and col='{col}' order by aweme_id, id;",
    # 14: "select aweme_id, txt, id, `second_s` from  douyin2.whisper_result where aweme_id in ({ids}) order by aweme_id, id;",
    14: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    # 15: "select aweme_id, ocr from  douyin2.douyin_video_ocr_content where aweme_id in ({ids}) order by aweme_id, id;",
    15: "select aweme_id, txt, id, start_time from douyin2_cleaner.douyin_video_ocr_asr where aweme_id in ({ids}) and `type`={tpp} order by aweme_id, id;",
    16: "",
    17: ""
}
# SELECT aweme_id,note_id,asr,ocr FROM xintu_category.xiaohongshu_asr_result WHERE aweme_id=
xiaohongshu_data_sql = {
    2: "SELECT aweme_id,asr FROM xintu_category.xiaohongshu_asr_result WHERE aweme_id in ({ids}) order by aweme_id;",
    3: "SELECT aweme_id,ocr FROM xintu_category.xiaohongshu_asr_result WHERE aweme_id in ({ids}) order by aweme_id;",
    # 4: 40.199  douyin2_cleaner.xhs_note_video_info
    # 16: "select a.aweme_id, b.text, b.id from douyin2_cleaner.douyin_video_zl{version}_{category} a inner join xintu_category.img_ocr_task b on a.note_id=b.item_id where a.aweme_id in ({ids}) order by a.aweme_id, b.id;"
    16: "SELECT aweme_id,ocr FROM xintu_category.xiaohongshu_asr_result WHERE aweme_id in ({ids}) order by aweme_id;",
    17: "SELECT aweme_id,generatedText FROM xintu_category.xhs_note_video_info WHERE aweme_id in ({ids});",
}
juliang_data_sql = {
    2: "select aweme_id, txt, id, start_time from xintu_category.douyin_video_xunfei where aweme_id in ({ids}) order by aweme_id, id;",
    3: "select ame_id, captions, id, second from xintu_category.juliang_video_ocr_sub where ame_id in ({ids}) order by ame_id, id;",
    12: "select ame_id, txt, id, second from xintu_category.juliang_video_ocr_mass where ame_id in ({ids}) order by ame_id, id;",
}


clean2part = {'bid': 'brand', 'sub_cid': 'sub_cid', 'video_type': 'video_type', 'pid': 'pid', 'prop': 'prop'}
clean2chatcol = {'bid': 'brand', 'sub_cid': 'sub_cid', 'video_type': 'video_type', 'pid': 'pid', 'prop': 'pidprop'}
clean2column = {'bid': ['bid'], 'sub_cid': ['sub_cid'], 'video_type': ['video_type'], 'pid': ['pid'],
                'prop': ['pid', 'pnvid']}
clean2columntail = {'bid': ['`type`', 'location'], 'sub_cid': ['`type`', 'location'], 'video_type': ['`type`', 'location'], 'pid': ['`type`', 'kws', 'location'],
                'prop': ['`type`', 'location']}
tbl_config = ['douyin2_cleaner.douyin_video_{part}_{ver}{category}',
              'douyin2_cleaner.douyin_video_{part}_zl{ver}_{category}',
              'douyin2_cleaner.douyin_video_{part}{ver}_{category}_comment',
              'douyin2_cleaner.douyin_video_manual_{part}']
is_juliang = lambda i: i in [2000006, 3020109, 4020109]
redundant_field = ['digg_count', 'create_time', 'content_type']
get_each_result_columns = lambda _clean: ['aweme_id', *clean2column[_clean], '`type`']

x_words = {'适用肤质','网络热词','适用场景','适用人群','使用体验','活动事件','成分配料','用户体验','人物角色','用户痛点','人物设定','销售宣传','产品功效'}

data_sources = {0:"抖音", 1:"标准库", 2:"小红书", 3:"小红书", 4:"巨量", 5:"抖音全类目", 6:"小红书全类目", 7:"feigua抖音特殊数据"}
data_using_type = {
    0: {'brand':[1, 2, 3, 4, 6, 7, 8, 11, 12, 13, 14, 15],'sub_cid':[1, 2, 3, 4, 7, 8, 11, 12, 13, 14, 15],'video_type':[1,],'pid':[1, 2, 3, 4, 8, 11, 12, 13, 14, 15],'prop':[1, 2, 3, 4, 8, 11, 12, 13, 14, 15]},
    1: {'brand':[1,],'sub_cid':[1,],'video_type':[1,],'pid':[1,],'prop':[1,]},
    2: {'brand':[1, 2, 3, 17],'sub_cid':[1, 2, 3, 17],'video_type':[1, 2, 3],'pid':[1, 2, 3, 17],'prop':[1, 2, 3, 17]},
    3: {'brand':[1, 2, 3, 17],'sub_cid':[1, 2, 3, 17],'video_type':[1, 2, 3],'pid':[1, 2, 3, 17],'prop':[1, 2, 3, 17]},
    4: {'brand':[1, 2, 3, 4, 8, 13, 14],'sub_cid':[1, 2, 3, 4, 8, 13, 14],'video_type':[1, 2, 3, 4, 8, 13, 14],'pid':[1, 2, 3, 4, 8, 13, 14],'prop':[1, 2, 3, 4, 8, 13, 14]},
    5: {'brand':[1, 15, 7, 8, 11],'sub_cid':[1, 15, 7, 8, 11],'video_type':[1, 15, 8, 11],'pid':[1, 15, 8, 11],'prop':[1, 15, 8, 11]},
    6: {'brand':[1, 2, 3, 17],'sub_cid':[1, 2, 3, 17],'video_type':[1, 2, 3],'pid':[1, 2, 3, 17],'prop':[1, 2, 3, 17]},
    7: {'brand':[1, 15, 7, 8, 11],'sub_cid':[1, 15, 7, 8, 11],'video_type':[1, 15, 8, 11],'pid':[1, 15, 8, 11],'prop':[1, 15, 8, 11]},
}

cid_to_subcid = {
    26396: {'dy': {26931: 1}},
}

def get_each_clean_table(clean, prefix, category):
    return tbl_config[0].format(part=clean2part[clean], ver=prefix, category=category)


def get_each_result_table(clean, prefix, category):
    return tbl_config[1].format(part=clean2part[clean], ver=prefix, category=category)


def get_video_table(category, prefix):
    return f"douyin2_cleaner.douyin_video_zl{prefix if prefix else ''}_{category}"


def check_using_type(dy2, clid, cat, pre, clean=''):
    import ast
    """ 读project 统一控制清洗 type """
    sql = f'''
    select clean_types from {clean_log_table} where id={clid}
    '''
    rr = dy2.query_all(sql)
    if rr and rr[0][0]:
        return ast.literal_eval(rr[0][0])
    tblnm = f"douyin_video_zl{pre}_{cat}"
    sql = f"select clean_data_types from {project_table} where table_name='{tblnm}';"
    rr = dy2.query_all(sql)
    try:
        gg = ast.literal_eval(rr[0][0])
        return gg[clean]
    except:
        raise ValueError("no data types, check table")

def check_insert(dy2, tblnm):
    sql = f'''
    SELECT count(1)
    FROM information_schema.`PROCESSLIST` t 
    WHERE t.`COMMAND` NOT IN ('Sleep') AND STATE<>'null' and info regexp 'INSERT IGNORE INTO {tblnm}'
    ORDER BY TIME DESC
    '''
    rr = dy2.query_one(sql)
    if rr[0] > 1:
        return False
    return True

def check_delete(dy2, tblnm):
    sql = f'''
    SELECT count(1)
    FROM information_schema.`PROCESSLIST` t 
    WHERE t.`COMMAND` NOT IN ('Sleep') AND STATE<>'null' and info regexp 'delete from {tblnm}'
    ORDER BY TIME DESC
    '''
    rr = dy2.query_one(sql)
    if rr[0] > 1:
        return False
    return True

def load_data(data, batch_size):
    """分batch加载data"""
    # 按照batch_size来分割data
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        yield batch

def check_mutations_end(dba, tbl, process={}):
    import time
    a, b = tbl.split('.')
    sql = '''
        SELECT parts_to_do, latest_fail_reason FROM system.mutations
        WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
    '''.format(a, b)
    r = dba.query_all(sql)

    if len(r) == 0:
        return True

    if 'total' not in process:
        process['total'] = r[0][0]
    process['parts_to_do'] = (r[0][0] or 1)
    process['latest_fail_reason'] = r[0][1]
    process['process'] = 100 - process['parts_to_do']/process['total']*100

    if process['latest_fail_reason'] != '':
        # 报错了 过60s再看 进度变了说明没报错
        time.sleep(60)
        sql = '''
            SELECT parts_to_do, latest_fail_reason FROM system.mutations
            WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
        '''.format(a, b)
        r = dba.query_all(sql)
        if len(r) > 0 and r[0][0] == process['parts_to_do']:
            sql = 'KILL MUTATION WHERE database = \'{}\' AND table = \'{}\''.format(a, b)
            dba.execute(sql)
            raise Exception(process['latest_fail_reason'])
        else:
            return True
    return False
