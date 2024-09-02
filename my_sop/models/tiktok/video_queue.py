import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from models.tiktok import *
from models.tiktok.video_utils import get_data_source


def insert_into_xunfei_queue(db, category, ids, priority, force=False):
    """插入讯飞队列，force:True重跑"""
    _ids = ','.join(list(map(lambda i: str(i), ids)))
    id2create = {}
    # id2create = dict(
    #     db.query_all(f"select aweme_id, create_time from douyin2.douyin_video where aweme_id in ({_ids});"))
    if force:
        utils.easy_batch(db, xunfei_task_table,
                         ['aweme_id', 'video_create_time', 'priority'],
                         [[aweme_id, id2create.get(aweme_id, 0), priority] for aweme_id in ids],
                         "on duplicate key update status=0, priority=values(priority)")
    else:
        utils.easy_batch(db, xunfei_task_table,
                         ['aweme_id', 'video_create_time', 'priority'],
                         [[aweme_id, id2create.get(aweme_id, 0), priority] for aweme_id in ids],
                         ignore=True)
    # 插入等待队列，等待抓取完成后重洗
    insert_into_waiting_queue(db, category, ids)


def insert_into_ocr_queue(db, category, ids, priority, data_source=None, force=False, db_sop=None):
    """插入ocr队列，force:True重跑，whisper也加入队列"""
    if db_sop is None:
        db_sop = app.connect_clickhouse('chsop')
    _ids = ','.join(list(map(lambda i: str(i), ids)))
    if data_source is None:
        data_source = get_data_source(db, category)
    if force:
        dup_update = "on duplicate key update cover_flag=1, ocr_flag=0,download_flag=0,download_try_count=0,try_count=0,speech_flag=1,speech_try_count=0, priority=values(priority)"
        ign = False
    else:
        dup_update = ''
        ign = True
    if data_source in (2, 3, 6):
        id2ntd = dict(
            db_sop.query_all(f"select id, note_id from douyin2_cleaner.xhs_notes where id in ({_ids});", print_sql=False))
        id2pltf = dict(
            db_sop.query_all(f"select id, if(`note_type`='video', 3, 4) from douyin2_cleaner.xhs_notes where id in ({_ids});", print_sql=False))
        utils.easy_batch(db, ocr_task_table,
                         ['aweme_id','platform', 'cover_flag', 'speech_flag', 'split_flag', 'ocr_flag', 'priority'],
                         [[id2ntd[aweme_id], id2pltf[aweme_id], 1, 1, 1, 1 if id2pltf[aweme_id] == 4 else 0, priority] for aweme_id in ids if aweme_id in id2ntd],
                         sql_dup_update=dup_update, ignore=ign)
    else:
        utils.easy_batch(db, ocr_task_table,
                         ['aweme_id','platform', 'cover_flag', 'speech_flag', 'split_flag', 'ocr_flag', 'priority'],
                         [[aweme_id, 1, 1, 1, 1, 0, priority] for aweme_id in ids],
                         sql_dup_update=dup_update, ignore=ign)
    # 插入等待队列，等待抓取完成后重洗
    insert_into_waiting_queue(db, category, ids)


def insert_into_waiting_queue(db, category, ids):
    utils.easy_batch(db, waiting_queue_table,
                     ['category', 'aweme_id'],
                     [[category, aweme_id] for aweme_id in ids],
                     ignore=True)


def get_xunfei_ocr_processing(db, category, prefix=''):
    video_table = f"douyin2_cleaner.douyin_video_zl{prefix}_{category}"

    video_sql = f"select count(1) from {video_table} where digg_count>500;"
    video_count = db.query_all(video_sql)[0][0]

    ocr_status_map = {0: '不抓取', 1: '抓取中', 2: '封面中无文本', 3: '提取成功'}
    xunfei_status_map = {0: '待抓取', 2: '抓取中', 3: '提取成功', 9: '发生错误'}
    xunfei_sql = f"""
        select status, count(1) from {xunfei_task_table}
        where aweme_id in (select aweme_id from {video_table} where digg_count>500) group by status;
    """
    xunfei_count = {status: 0 for status in list(xunfei_status_map.values()) + ['未知状态']}
    for status, count in db.query_all(xunfei_sql):
        xunfei_count[xunfei_status_map.get(status, '未知状态')] += count

    ocr_sql = f"""
        select ocr_flag, count(1) from {ocr_task_table}
        where aweme_id in (select aweme_id from {video_table} where digg_count>500) group by ocr_flag;
    """
    ocr_count = {status: 0 for status in list(ocr_status_map.values()) + ['未知状态']}
    for status, count in db.query_all(ocr_sql):
        ocr_count[ocr_status_map.get(status, '未知状态')] += count
    return {'video': video_count, 'ocr': ocr_count, 'xunfei': xunfei_count}


def test():

    dy2 = app.connect_db('dy2')
    # sql = "select distinct aweme_id from douyin_video_zl_2020018 where aweme_id in (select aweme_id from douyin_video_brand_zl_2020018 where bid in (select bid from brand_2020018 where is_show=1)) and aweme_id in (select aweme_id from douyin_video_sub_cid_zl_2020018 where sub_cid not in (99,999)) and digg_count>20;"
    ids = [str(i) for (i) in [7232962732348247330,7233728893675408695,7234110917057776956,7232630016700337445,7232915484667088186,7225524978685447457,7225512379889536308,7231497315117206843,7226615066504809783,7225626635016473856,7231496193510083899,7231858460781006138,7230760053282753847,7231001882578177332,7225521508909731107,7226715878144937231,7231497564393065788,7226192785139977528,7230367824521219388,7227142151950388492,7231049982319594807,7225498949640097056,7229311486273850681,7227018913387777317,7230640473746967869,7227045170708057401,7231027642227608892]]

    insert_into_ocr_queue(dy2, 26396, ids, 99, data_source=0)
    # update_sql = f"update {ocr_task_table} set speech_flag=1, priority=99 where aweme_id in ({','.join(ids)})"
    # dy2.execute(update_sql)
    # dy2.commit()