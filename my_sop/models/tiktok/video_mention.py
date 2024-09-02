import ast
import argparse
import json
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from models.tiktok import *
from models.tiktok.video_queue import get_xunfei_ocr_processing
from models.nlp.common import text_normalize
from scripts.cdf_lvgou.tools import Fc

dy2 = app.get_db('dy2')

mention_table = "douyin2_cleaner.mention"
clean2col_type = {'bid': -1, 'sub_cid': -3, 'pid': -4, 'video_type': -5}
# video_type < prop < cid < sub_cid < bid < pid
# [-5, >0, -3, -1, -4]
mention_rate = {-5: -9, -3: 999997, -1: 999998, -4: 999999}
col_type2clean = {v: p for p, v in clean2col_type.items()}


def move_to_mention_old(db, category, clean):
    columns = clean2column[clean]
    clean_table = tbl_config[0].format(part=clean2part[clean], category=category)

    old_pid_type2normalize_type = {
        'bid': {1: 1, 2: 2, 3: 3, 4: 4, 5: 11, 6: 6, 7: 7, 8: 8, 12: 12},
        'video_type': {1: 1, 2: 2, 3: 3, 4: 4, 8: 8, 12: 12},
        'sub_cid': {1: 1, 2: 2, 3: 3, 4: 4, 8: 8, 12: 12},
        'pid': {1: 1, 2: 2, 3: 3, 4: 4, 8: 8, 12: 12}
    }

    dy2.execute(
        f"delete from douyin2_cleaner.mention where category={category} and {'col_type>=0' if clean == 'prop' else f'col_type={clean2col_type[clean]}'}")
    dy2.commit()

    sql = f"""
        select id, aweme_id, concat_ws('_', {','.join(columns)}), `type`, location from {clean_table} 
        where {1 if clean in ('prop', 'sub_cid') else f'{columns[-1]}!=0'} and location!='[]' and id>%d order by id limit %d;
    """

    wrong_aweme_id = []

    def dy_callback(data):
        insert = []

        if clean == 'video_type':
            ids = ','.join(map(lambda i: str(i[1]), data))
            ocr_txt = {}
            ocr_sql = f"select aweme_id, ocr_cover, ocr_captions from douyin2.douyin_video_ocr where aweme_id in ({ids})"
            for aweme_id, ocr_cover, ocr_captions in db.query_all(ocr_sql):
                ocr_txt[aweme_id] = (text_normalize(ocr_cover), text_normalize(ocr_captions))

        for _, aweme_id, col_id, type, location in data:
            if clean in old_pid_type2normalize_type:
                type = old_pid_type2normalize_type[clean].get(type, type)

            try:
                location = ast.literal_eval(location)
            except:
                location = location[:location.rfind(']') + 1] + ']'
                location = ast.literal_eval(location)
                wrong_aweme_id.append(aweme_id)
                # print(aweme_id)
                # continue

            for start_time, offset, length in location:
                # if clean == 'video_type' and type == 4 and offset >= len(ocr_txt[aweme_id][0]):
                #     _offset = offset - len(ocr_txt[aweme_id][0])
                #     _type = 3
                # else:
                # _offset = offset
                # _type = type
                #
                # insert.append([aweme_id, category, _type, clean2col_type[clean], col_id, start_time, _offset, length])
                # todo 1:prop->pnid
                insert.append(
                    [aweme_id, category, type, int(clean2col_type.get(clean, col_id)), col_id, start_time, offset,
                     length])

        replace_sql = f"replace into {mention_table} (aweme_id,category,`type`,col_type,col_id,start_time,offset,length) values"
        db.batch_insert(replace_sql, '(%s,%s,%s,%s,%s,%s,%s,%s)', insert)

    utils.easy_traverse(db, sql, dy_callback, 0, 1000)
    print(wrong_aweme_id)


def move_to_mention(db, category, clean, prefix='', batch='', start=0, end=0, clean_id=0):
    columns = clean2column[clean]
    clean_table = tbl_config[0].format(part=clean2part[clean], ver=f'{prefix}_' if prefix else '', category=category)
    # category_condition = f"category={category}"
    start_condition = f"id>{start}" if start else "1"
    end_condition = f"id<={end}" if end else "1"
    if ',' in batch or batch.isdigit():
        batch_condition = f"batch in ({batch})"
    elif '>' in batch or '<' in batch:
        batch_condition = f"batch{batch}"
    else:
        batch_condition = "1"
    # prefix_condition = f"prefix={int(prefix) if prefix else 0}"
    # where_condition = " and ".join([category_condition, prefix_condition, batch_condition])
    data_table = get_video_table(category, prefix)
    where_condition = " and ".join([batch_condition, start_condition, end_condition])
    awm_condition = f"aweme_id in (select aweme_id from {run_table} where clean_id={clean_id} and {where_condition})" if clean_id else "1"

    id_sql = f"select aweme_id from {data_table} force index (primary) where {awm_condition} and aweme_id>%d order by aweme_id limit %d;"

    if clean == 'prop':
        pnvid2pnid = dict(
            db.query_all(f"select id, pnid from douyin2_cleaner.prop where id in (select pnvid from {clean_table});"))

    wrong_aweme_id = []

    def dy_callback(id_list):
        if len(id_list) == 0:
            return
        ids = ','.join(map(lambda i: str(i[0]), id_list))
        result_sql = f"""
            select id, aweme_id, concat_ws('_', {','.join(columns)}), `type`, location from {clean_table} 
            where {1 if clean in ('prop', 'sub_cid') else f'{columns[-1]}!=0'} and aweme_id in ({ids});
        """
        data = db.query_all(result_sql)
        # print(result_sql)
        # print(data)

        new = []
        for _, aweme_id, col_id, type, location in data:
            try:
                location = ast.literal_eval(location)
            except:
                location = location[:location.rfind(']') + 1] + ']'
                location = ast.literal_eval(location)
                wrong_aweme_id.append(aweme_id)
            try:
                for text_id, offset, length in location:
                    if clean == 'prop':
                        pid, pnvid = col_id.split('_')
                        col_type = int(pnvid2pnid.get(int(pnvid), 0))
                    else:
                        col_type = int(clean2col_type.get(clean, col_id))
                    new.append([aweme_id, category, type, col_type, col_id, text_id, offset, length])
            except:
                wrong_aweme_id.append(aweme_id)

        old_data_sql = f"""
            select id,aweme_id,category,type,col_type,col_id,start_time,offset,length 
            from {mention_table} 
            where category={category} and {'col_type>=0' if clean == 'prop' else f'col_type={clean2col_type[clean]}'} and 
                  aweme_id in ({ids})"""
        old = {tuple(info): id for id, *info in db.query_all(old_data_sql)}

        insert = []
        for info in new:
            if old.get(tuple(info)) is None:
                insert.append(info)
            else:
                del old[tuple(info)]
        delete = set(old.values())

        if len(delete) > 0:
            _delete = list(delete)
            start, step, _count = 0, 1000, len(_delete)
            while start < _count:
                tmp = _delete[start:start+step]
                start += step
                if len(tmp) > 0:
                    delete_str = ','.join(map(lambda i: str(i), tmp))
                    delete_sql = f"delete from {mention_table} where id in ({delete_str})"
                    db.execute(delete_sql)
                    db.commit()

        if len(insert) > 0:
            utils.easy_batch(db, mention_table,
                             ['aweme_id', 'category', '`type`', 'col_type', 'col_id', 'start_time', 'offset', 'length'],
                             insert)

    utils.easy_traverse(db, id_sql, dy_callback, 0, 1000)
    print(wrong_aweme_id)


def get_mention(db, category, aweme_ids, prefix='', data_source=0):
    using_types = [1, 2, 3, 4, 8, 9, 12, 14]
    ids = ','.join([str(aweme_id) for aweme_id in aweme_ids])
    spus = set()

    # ocr/xunfei status
    ocr_status_sql = f"select aweme_id, priority, cover_flag, cover_msg, ocr_flag, ocr_msg, save_img from {ocr_task_table} where aweme_id in ({ids})"
    xunfei_status_sql = f"select aweme_id, priority, status, msg from {xunfei_task_table} where aweme_id in ({ids})"
    ocr_status_map = {0: '不抓取', 1: '抓取中', 2: '封面中无文本', 3: '提取成功'}
    xunfei_status_map = {0: '待抓取', 2: '抓取中', 3: '提取成功', 9: '发生错误'}

    if data_source == 4:
        data_sql.update(juliang_data_sql)
        ocr_status_sql = f"select ame_id, priority, cover_flag, cover_msg, ocr_flag, ocr_msg, save_img from xintu_category.juliang_video_download where ame_id in ({ids})"
        print(ocr_status_sql)
        xunfei_status_sql = f"select aweme_id, priority, status, msg from xintu_category.stream_manual_live where aweme_id in ({ids})"
        ocr_status_map = {0: '待抓取', 2: '发生错误', 3: '提取成功'}
    elif data_source in [2, 3]:
        data_sql.update(xiaohongshu_data_sql)
        xunfei_status_sql = f"select ame_id, priority, speech_flag, speech_msg from xintu_category.juliang_video_download where ame_id in ({ids})"
        xunfei_status_map = {1: '等待处理', 2: '已提取音频', 4: '出现错误'}
    else:
        pass
    status = {int(aweme_id): {'ocr_priority': 0,
                              'cover_status': 0, 'cover_msg': '不在ocr消息队列',
                              'ocr_status': 0, 'ocr_msg': '不在ocr消息队列',
                              'save_img': '',
                              'xunfei_priority': 0,
                              'xunfei_status': 0, 'xunfei_msg': '不在讯飞消息队列'} for aweme_id in aweme_ids}
    for aweme_id, priority, cover_status, cover_msg, ocr_status, ocr_msg, save_img in db.query_all(ocr_status_sql):
        aweme_id = int(aweme_id)
        status[aweme_id]['ocr_priority'] = priority
        status[aweme_id]['cover_status'] = ocr_status_map.get(cover_status, cover_status)
        status[aweme_id]['cover_msg'] = cover_msg if isinstance(cover_msg, str) else status[aweme_id]['cover_status'] if status[aweme_id]['cover_status'] else ''
        status[aweme_id]['ocr_status'] = ocr_status_map.get(ocr_status, ocr_status)
        status[aweme_id]['ocr_msg'] = ocr_msg if isinstance(ocr_msg, str) else status[aweme_id]['ocr_status'] if status[aweme_id]['ocr_status'] else ''
        status[aweme_id]['save_img'] = '保存图片' if save_img else '不存图片'

    for aweme_id, priority, xunfei_status, xunfei_msg in db.query_all(xunfei_status_sql):
        status[aweme_id]['xunfei_priority'] = priority
        status[aweme_id]['xunfei_status'] = xunfei_status_map.get(xunfei_status, xunfei_status)
        status[aweme_id]['xunfei_msg'] = xunfei_msg if isinstance(xunfei_msg, str) else status[aweme_id]['xunfei_status'] if status[aweme_id]['xunfei_status'] else ''

    def to_pnvid(_id):
        return int(_id.split("_")[-1]) if "_" in _id and _id.split("_")[-1] !='' else 0

    def to_pid(_id):
        return int(_id.split("_")[0]) if "_" in _id and _id.split("_")[0] !='' else 0

    # get_prop_table
    sql = f"""
        select pnvid, name, pk.id, pk.confirm_type, pk.relation_type from douyin2_cleaner.prop_keyword pk join douyin2_cleaner.keyword k on pk.kid=k.kid 
        where category in (select if(prop, prop, category) from douyin2_cleaner.project where category={category});
    """
    prop_confirm = {}
    for pnvid, mention, pkid, confirm_type, relation_type in db.query_all(sql):
        prop_confirm[(pnvid, mention)] = (pkid, confirm_type, relation_type)

    aweme_id_type_id2result = {}
    for type in using_types:
        # print(type)
        # print(data_sql[type].format(version='', category=category, ids=ids))
        for aweme_id, txt, *info in db.query_all(data_sql[type].format(version='', category=category, ids=ids)):
            if aweme_id_type_id2result.get(aweme_id) is None:
                aweme_id_type_id2result[aweme_id] = {}
            if aweme_id_type_id2result[aweme_id].get(type) is None:
                aweme_id_type_id2result[aweme_id][type] = {}

            text_id, start_time = (info[0], info[1]) if len(info) > 1 else (0, 0)
            if aweme_id_type_id2result[aweme_id][type].get(text_id) is None:
                aweme_id_type_id2result[aweme_id][type][text_id] = {}
            aweme_id_type_id2result[aweme_id][type][text_id] = {
                'id': text_id,
                'start_time': start_time,
                'text': text_normalize(str(txt)) + ' ',
                'mention': []}

    mention_sql = f"""
        select distinct aweme_id,`type`,col_type,col_id,start_time,offset,length from {mention_table} 
        where category={category} and aweme_id in ({ids})
    """
    for aweme_id, type, col_type, col_id, text_id, offset, length in db.query_all(mention_sql):
        if aweme_id_type_id2result.get(aweme_id) and aweme_id_type_id2result[aweme_id].get(type) and \
                aweme_id_type_id2result[aweme_id][type].get(text_id):
            text = aweme_id_type_id2result[aweme_id][type][text_id]['text']
            mention = text[offset: offset + length]
            spus.add(str(to_pid(col_id)))
            aweme_id_type_id2result[aweme_id][type][text_id]['mention'].append(
                [col_type, col_id, offset, length, *prop_confirm.get((to_pnvid(col_id), mention), [0, 0, 0])])

    for aweme_id, type_id2result in aweme_id_type_id2result.items():
        for type, id2result in type_id2result.items():
            for text_id, result in id2result.items():
                result['mention'] = sorted(result['mention'], key=lambda m: (mention_rate.get(m[0], m[0]), -m[-2]))

    result = {
        aweme_id: {
            type: [result for text_id, result in sorted(id2result.items(), key=lambda i: i[0])]
            for type, id2result in type_id2result.items()}
        for aweme_id, type_id2result in aweme_id_type_id2result.items()
    }

    sql = "select id, name from douyin2_cleaner.prop_name order by id;"
    col_type2clean_prop = dict(db.query_all(sql))
    col_type2clean.update(col_type2clean_prop)

    if spus:
        spu_sql = f"select pid, name from douyin2_cleaner.product_{category} where pid in ({','.join(spus)});"
        spu_dict = dict(db.query_all(spu_sql))
    else:
        spu_dict = {}
    data = {'data': result, 'cols': ['col_type', 'col_id', 'offset', 'length', 'pkid', 'pk_confirm', 'relation_confirm'],
            'type2name': [[type, type2name[type]] for type in using_types],
            'col_type2name': [[col_type, col_name] for col_type, col_name in col_type2clean.items()],
            'status': status,
            'spu': spu_dict}
    return data


def update_mention(db, params_list, user_id):
    # todo change to same column
    # manual_columns = ['category', 'aweme_id', 'type', 'col_type', 'col_id', 'text_id', 'offset', 'length', 'text',
    #                   'pid', 'pnid']
    # mention_columns = ['category', 'aweme_id', 'type', 'col_type', 'col_id', 'start_time', 'offset', 'length']

    manual_columns = {'category': 'category', 'aweme_id': 'aweme_id', 'typeId': '`type`', 'colTypeId': 'col_type',
                      'textValue': 'col_id', 'textId': 'text_id', 'offset': 'offset', 'length': 'length',
                      'copyText': 'text', 'sku': 'pid', 'propName': 'pnid'}
    mention_columns = {'category': 'category', 'aweme_id': 'aweme_id', 'typeId': '`type`', 'colTypeId': 'col_type',
                       'textValue': 'col_id', 'textId': 'start_time', 'offset': 'offset', 'length': 'length'}
    for params in params_list:
        if params["colTypeId"] >= 0:
            pvid = params['textValue']
            pnid = params['colTypeId']
            pnvid = db.query_all(f"select id from douyin2_cleaner.prop where pnid={pnid} and pvid={pvid}")[0][0]
            params['textValue'] = f"{params['sku']}_{pnvid}"

            name = params['copyText']
            sql = "insert ignore into douyin2_cleaner.keyword(name) value (%s)"
            db.execute(sql, (name,))
            db.commit()
            kid = db.query_one(f"select kid from douyin2_cleaner.keyword where name=(%s)", (name,))[0]
            prop = db.query_one(f"select prop from douyin2_cleaner.project where category=(%s)", (params['category'],))[0]
            category = params['category'] if int(prop) == 0 else prop
            columns = ['category', 'pnvid', 'kid', 'source', 'confirm_type', 'uid', 'createTime']
            row = (category,pnvid,kid,params['source'],params['confirm_type'],user_id,int(time.time()))
            utils.easy_batch(db, "douyin2_cleaner.prop_keyword", columns, [row])

        # params['textValue'] = f"{params['sku']}_{pnvid}" if params["colTypeId"] >= 0 else params['textValue']
        # 插入人工表作log用
        utils.easy_batch(db, "douyin2_cleaner.mention_manual", list(manual_columns.values()),
                         [[params[column] for column in manual_columns]])

        # 插入mention表实时更新展示
        utils.easy_batch(db, "douyin2_cleaner.mention", list(mention_columns.values()),
                         [[params[column] for column in mention_columns]])


def remove_mention(db, params):
    manual_columns = {'category': 'category', 'aweme_id': 'aweme_id', 'typeId': '`type`', 'colTypeId': 'col_type',
                      'textValue': 'col_id', 'textId': 'text_id', 'offset': 'offset', 'length': 'length',
                      'copyText': 'text', 'sku': 'pid', 'propName': 'pnid'}
    mention_columns = {'category': 'category', 'aweme_id': 'aweme_id', 'typeId': '`type`', 'colTypeId': 'col_type',
                       'textValue': 'col_id', 'textId': 'start_time', 'offset': 'offset', 'length': 'length'}

    if params["colTypeId"] >= 0:
        pvid = params['textValue']
        pnid = params['colTypeId']
        pnvid = db.query_all(f"select id from douyin2_cleaner.prop where pnid={pnid} and pvid={pvid}")[0][0]
        params['textValue'] = f"{params['sku']}_{pnvid}"

    # params['textValue'] = f"{params['sku']}_{params['textValue']}" if params["colTypeId"] >= 0 else params['textValue']
    utils.easy_batch(db, "douyin2_cleaner.mention_manual", list(manual_columns.values()),
                     [[f'-{params[column]}' if column == 'textValue' else params[column] for column in manual_columns]])
    sql = f"DELETE FROM douyin2_cleaner.mention where {' and '.join([f'{col}=%s' for col in mention_columns.values()])}"
    db.execute(sql, (params[column] for column in mention_columns))
    db.commit()


# def get_mention_remove(db, category, aweme_ids):
#     using_types = [1, 2, 3, 4, 8, 9, 12]
#     ids = ','.join([str(aweme_id) for aweme_id in aweme_ids])
#
#     aweme_id_type_start2result = {}
#     for type in using_types:
#         for aweme_id, txt, *info in db.query_all(data_sql[type].format(version='', category=category, ids=ids)):
#             if aweme_id_type_start2result.get(aweme_id) is None:
#                 aweme_id_type_start2result[aweme_id] = {}
#             if aweme_id_type_start2result[aweme_id].get(type) is None:
#                 aweme_id_type_start2result[aweme_id][type] = {}
#
#             start_time = info[0] if len(info) else 0
#             if aweme_id_type_start2result[aweme_id][type].get(start_time) is None:
#                 aweme_id_type_start2result[aweme_id][type][start_time] = {}
#             aweme_id_type_start2result[aweme_id][type][start_time] = {'text': text_normalize(str(txt)) + ' ',
#                                                                       'mention': []}
#
#     mention_sql = f"""
#         select aweme_id,`type`,col_type,col_id,start_time,offset,length from {mention_table}
#         where category={category} and aweme_id in ({ids})
#     """
#     # print(aweme_id_type_start2result)
#     # mention_result = {}
#     for aweme_id, type, col_type, col_id, start, offset, length in db.query_all(mention_sql):
#         if aweme_id_type_start2result.get(aweme_id) and aweme_id_type_start2result[aweme_id].get(type) and \
#                 aweme_id_type_start2result[aweme_id][type].get(start):
#             aweme_id_type_start2result[aweme_id][type][start]['mention'].append(
#                 [col_type, col_id, start, offset, length])
#
#     for aweme_id, type_start2result in aweme_id_type_start2result.items():
#         for type, start2result in type_start2result.items():
#             for start, result in start2result.items():
#                 result['mention'] = sorted(result['mention'], key=lambda m: (mention_rate.get(m[0], m[0]), -m[-2]))
#
#     # print(aweme_id_type_start2result)
#     result = {
#         aweme_id: {type: [dict({"start_time": start}, **result) for start, result in
#                           sorted(start2result.items(), key=lambda i: i[0])]
#                    # if len(start2result) > 0 else [{'start_time': start, }]
#                    for type, start2result in type_start2result.items()}
#         for aweme_id, type_start2result in aweme_id_type_start2result.items()
#     }
#
#     sql = "select id, name from douyin2_cleaner.prop_name;"
#     col_type2clean_prop = dict(db.query_all(sql))
#     col_type2clean.update(col_type2clean_prop)
#
#     data = {'data': result, 'cols': ['col_type', 'col_id', 'start', 'offset', 'length'],
#             'type2name': [[type, type2name[type]] for type in using_types],
#             'col_type2name': [[col_type, col_type2clean[col_type]] for col_type in col_type2clean]}
#     return data


if __name__ == '__main__':
    # dy2 = app.connect_db('dy2')
    # example = {'category': 26396, 'aweme_id': 6912451312583986446, 'typeId': 1, 'colTypeId': -5, 'textValue': 34, 'textId': 0, 'offset': 0, 'length': 2, 'copyText': '新年快乐', 'sku': 0, 'propName': 0}
    # remove_mention(dy2, example)
    parser = argparse.ArgumentParser(description='mention')
    parser.add_argument('-category', type=int, required=True, help="category")
    parser.add_argument('-prefix', type=str, default='', help="prefix")
    parser.add_argument('-batch', type=str, default='', help="batch")
    parser.add_argument('-col_type', type=str, help="col_type")
    parser.add_argument('-input', action="store_true", help="input")
    parser.add_argument('-test', action="store_true", help="test")
    parser.add_argument('-auto', action="store_true", help="auto")
    parser.add_argument("-aweme_id", nargs='+', type=int, default=[6912451312583986446], help="验证视频id")

    args = parser.parse_args()

    if args.input:
        dy2.connect()
        if args.col_type == '':
            raise ValueError("miss col_type")
        move_to_mention(dy2, args.category, args.col_type, prefix=args.prefix, batch=args.batch)

    if args.test:
        dy2.connect()
        print(get_mention(dy2, args.category, args.aweme_id))

    if args.auto:
        dy2.connect()
        yesterday = (datetime.today() + relativedelta(days=-1)).strftime('%Y-%m-%d')
        for category, prefix, batch in dy2.query_all(f"select distinct category, prefix, batch from {clean_log_table} where flag=5 and update_time>='{yesterday}';"):
            for col in ['bid', 'sub_cid', 'prop']:
                try:
                    move_to_mention(dy2, category, col, prefix, batch)
                except Exception as e:
                    Fc.vxMessage(to='zheng.jiatao',
                                 title=f'mention报错 category={category}, col={col}, prefix={prefix}, batch={batch}',
                                 text=e)
