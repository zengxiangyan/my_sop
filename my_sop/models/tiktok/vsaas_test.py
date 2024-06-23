import json
import time
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse
import os
import sys
import numpy as np
from os.path import join, abspath, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), "../../../"))
import application as app
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.tiktok import *
from models.tiktok.video_hotword import get_data, get_project_cat, get_data_source
from models.tiktok.video_kid_awm_quanleimu import VideoQuanleimu
from scripts.tiktok.video.clean_video_result_new_saas import (
    CleanSaasResult,
    CleanSaasControl,
)
from scripts.cdf_lvgou.tools import Fc


# tbl = "video_power.douyin_video_sass_stream_24000006_1001_tt"
# for tpp in (1, 2, 3, 11, 14, 15):
#     CleanSaasResult.update_contenttxt(tbl, tpp=tpp, platform='douyin')
#     logging.info(f"{tpp} done")

# vkad = VideoQuanleimu(19023,1000015)
# vkad.clear_aweme_id()
# flag = vkad.insert_aweme_id()
# print(flag)

# vkad = VideoQuanleimu(1001, 24000006)
# hotword_flag = vkad.run_quanleimu_hotword(test=-1)
# msg = "热词已跑完" if hotword_flag else "此项目没有热词"
# logging.info(msg)
############################ 飞瓜导入 ###############################
# 598, 599
# for clean_id in (23807, 24061):
#     saasrlt = CleanSaasResult(clean_id, 37000006)
#     saasrlt.create_awms_ck()
#     logging.info("awm视频范围已确定")
#     saasrlt.create_brand_fg()
#     logging.info("品牌导入已完成")
#     saasrlt.create_sub_cid_streamcid_fg()
#     logging.info("品类导入已完成")
#
#     saasrlt.create_video_info_fg()
#     saasrlt.create_videouid_info_dy()
#     logging.info("视频信息导入已完成")
#     saasrlt.create_awms_guache()
#     logging.info("挂车视频已完成")
#     saasrlt.create_kid_info()
#     logging.info("kid信息已完成")
#
#     saasrlt.create_saas_dy()
#     saasrlt.insert_saas_dy()
#     logging.info("#################################################################导入saas已完成###########################################################")
#     saasrlt.drop_tmp_tbls()
###########################  multi多品牌多品类导入  ################################
for clean_id in (20240100, 20240200,):
    saasrlt = CleanSaasResult(clean_id, 33000006)
    saasrlt.create_awms_ck()
    logging.info("awm视频范围已确定")
    saasrlt.create_video_info_dy()
    saasrlt.create_videouid_info_dy()
    logging.info("视频信息导入已完成")
    saasrlt.create_kid_info()
    logging.info("kid信息已完成")
    saasrlt.create_awms_guache()
    logging.info("挂车视频已完成")

    saasrlt.create_saas_dy()
    saasrlt.insert_saas_multi(insert=False)
    logging.info("导入saas已完成")
    saasrlt.update_entity_spu_kws()
    saasrlt.postprocess_lvmh()
    logging.info("后处理完成")

for clean_id in (20240103, 20240203,):
    saasrlt = CleanSaasResult(clean_id, 34000006)
    # saasrlt.create_awms_ck()
    # logging.info("awm视频范围已确定")
    # saasrlt.create_video_info_xhs()
    # saasrlt.create_videouid_info_xhs()
    # logging.info("视频信息导入已完成")
    # saasrlt.create_kid_info()
    # logging.info("kid信息已完成")

    saasrlt.create_saas_xhs()
    saasrlt.insert_saas_multi_xhs(insert=False)
    logging.info("导入saas已完成")
    saasrlt.update_entity_spu_kws()
    saasrlt.postprocess_lvmh()
    logging.info("后处理完成")
################################ 普通导入 ###############################

# saasrlt.create_awms_ck_dy()
# logging.info("awm视频范围已确定")
# saasrlt.create_brand_new()
# logging.info("品牌导入已完成")
# saasrlt.create_sub_cid_streamcid()
# logging.info("品类导入已完成")
# saasrlt.create_video_info_dy()
# saasrlt.create_videouid_info_dy()
# logging.info("视频信息导入已完成")
# saasrlt.create_awms_guache()
# logging.info("挂车视频已完成")
# saasrlt.create_kid_info()
# logging.info("kid信息已完成")
# saasrlt.create_raw_text()
# logging.info("文本来源已完成")
# saasrlt.create_saas_dy()
# saasrlt.insert_saas_dy()
# logging.info("导入saas已完成")

# saasrlt = CleanSaasResult(0, 34450)
# saasrlt.create_awms_ck_xhs()
# logging.info("awm视频范围已确定")
# saasrlt.create_brand_new()
# logging.info("品牌导入已完成")
# saasrlt.create_sub_cid_streamcid()
# logging.info("品类导入已完成")
# saasrlt.create_video_info_xhs()
# saasrlt.create_videouid_info_xhs()
# logging.info("视频信息导入已完成")
# # # saasrlt.create_awms_guache()
# # # logging.info("挂车视频已完成")
# saasrlt.create_kid_info()
# logging.info("kid信息已完成")
# # # saasrlt.create_raw_text()
# # # logging.info("文本来源已完成")
# saasrlt.create_saas_xhs()
# saasrlt.insert_saas_xhs()
# logging.info("导入saas已完成")
# saasrlt.drop_tmp_tbls()
