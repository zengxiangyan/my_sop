#coding=utf-8
import sys
from os.path import abspath, join, dirname
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
from models.tiktok.video_manual import VideoManual
import models.tiktok.video_common as video_common

# vm = VideoManual(9920109, 'douyin_video_zl_9920109', test=False, mode='search')
# vm = VideoManual(26396, 'douyin_video_zl_26396', test=False, mode='search')
# vm = VideoManual(6, 'douyin_video_zl_6_s', test=False, mode='')
vm1 = VideoManual(21000006, 'douyin_video_zl_21000006', test=True, mode='search', user_id=606)
vm2 = VideoManual(23000006, 'douyin_video_zl_23000006', test=True, mode='search', user_id=606)


for clean_part in vm2.all_config():
    print(clean_part)
    for i in ['all_tbl', 'clean_tbl']:
        sql = '''
        insert into {} select * from {}
        '''.format(vm2.all_config()[clean_part][i], vm1.all_config()[clean_part][i])
        print(sql)
        vm2.dy2.execute(sql)
        vm2.dy2.commit()
    clean2column = {'bid': ['bid'], 'sub_cid': ['sub_cid'], 'video_type': ['video_type'], 'pid': ['pid'],
                    'prop': ['pid', 'pnvid']}

    try:
        cols = ','.join(['aweme_id'] + video_common.clean2column[clean_part])
        sql = '''
        insert into {} (category, {}) select (23000006, {}) from {} where category = 21000006
        '''.format(vm2.all_config()[clean_part]['split_tbl'], cols, cols, vm1.all_config()[clean_part]['split_tbl'])
        print(sql)
    except:
        pass
