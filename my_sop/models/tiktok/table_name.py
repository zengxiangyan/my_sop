dbname = 'douyin2_cleaner'
dbname_origin = 'douyin2'
dbname_xingtu = 'xintu_category'
xhs_notes_base = 10000000000
xhs_can_use = "(content_flag != 0 AND (title <> '' or content <> '')) and note_create_time>='2023-01-01 00:00:00'"

h_col_type = {
    'bid': 'brand',
    'pnvid': 'prop'
}
solr_key_list = 'aweme_id,uid,desc,video_uri,create_time,forward_count,comment_count,digg_count,download_count,share_count,collect_count,duration,created,text_extra'.split(',')

def get_video_creator():
    return '{}.video_creator'.format(dbname_origin)

def get_ocr_queue_table():
    return '{}.douyin_video_process_cleaner'.format(dbname_origin)

def get_xunfei_queue_table():
    return '{}.stream_manual_live'.format(dbname_origin)

def get_origin_table():
    return '{}.douyin_video'.format(dbname_origin)

def get_xingtu_table():
    return '{}.xingtu_author_video'.format(dbname_xingtu)
    return '{}.video_xingtu'.format(dbname_origin)
    # return '{}.xingtu_hot_list'.format(dbname_xingtu)

def get_xingtu_table_new():
    # return '{}.video_xingtu'.format(dbname_origin)
    return '{}.xingtu_hot_list'.format(dbname_xingtu)

def get_xingtu_author():
    return '{}.author'.format(dbname_xingtu)

def get_xingtu_author_new():
    return '{}.xingtu_author'.format(dbname_xingtu)

def get_xingtu_ads():
    return '{}.chuangyi_ads'.format(dbname_xingtu)

def get_douyin_video_table(category, prefix=''):
    return '{}.douyin_video_zl{}_{}'.format(dbname, prefix, category)

def get_spu_table(category):
    return '{}.product_{}'.format(dbname, category)

def get_douyin_video_prop_table(category, prefix=''):
    return '{}.douyin_video_prop_zl{}_{}'.format(dbname, prefix, category)

def get_douyin_video_pid_table(category, prefix='', if_test=''):
    return '{}.douyin_video_pid_zl{}_{}{}'.format(dbname, prefix, category,if_test)

def get_summary_table(category, prefix=''):
    return '{}.brand_summary{}_{}'.format(dbname, prefix, category)

def get_douyin_video_brand_table(category, prefix=''):
    return '{}.douyin_video_brand_zl{}_{}'.format(dbname, prefix, category)

def get_douyin_video_sub_cid_table(category, prefix=''):
    return '{}.douyin_video_sub_cid_zl{}_{}'.format(dbname, prefix, category)

def get_douyin_video_video_type_table(category, prefix=''):
    return '{}.douyin_video_video_type_zl{}_{}'.format(dbname, prefix, category)

def get_douyin_video_table_by_col_type(category, prefix='', col_type='bid'):
    col_type = h_col_type[col_type] if col_type in h_col_type else col_type
    return '{}.douyin_video_{}_zl{}_{}'.format(dbname, col_type, prefix, category)

def get_brand_table(category, prefix=''):
    return '{}.brand{}_{}'.format(dbname, prefix, category)

def get_pid_table(category, prefix=''):
    return '{}.product{}_{}'.format(dbname, prefix, category)

def get_prop_table():
    return '{}.prop'.format(dbname)

def get_multi_prop_table(category, prefix='multi'):
    return '{}.prop_zl{}_{}'.format(dbname, prefix, category)

def get_prop_summary_table(category, prefix=''):
    return '{}.prop_summary{}_{}'.format(dbname, prefix, category)

def get_prop_summary_title_table():
    return '{}.prop_summary_title'.format(dbname)

def get_buyin_table():
    return 'apollo_douyin.buyin_dump_extracted'

def get_ck_douyin_video():
    return '{}.douyin_video'.format(dbname)

def get_ck_douyin_video_history():
    return 'douyin2.douyin_video_history'

def get_douyin_video_xunfei():
    return 'douyin2.douyin_video_xunfei'

def get_douyin_video_whisper():
    return 'douyin2.whisper_result'

def get_douyin_video_ocr_txt():
    return 'douyin2.douyin_video_ocr_txt'

def get_douyin_video_ocr_mass():
    return 'douyin2.douyin_video_ocr_mass'

def get_douyin_video_ocr_mass_xhs():
    return 'douyin2.douyin_video_ocr_mass_xhs'

def get_douyin_video_ocr_asr():
    return '{}.douyin_video_ocr_asr'.format(dbname)

def get_douyin_video_ocr_asr_xhs():
    return '{}.douyin_video_ocr_asr_xhs'.format(dbname)

def get_douyin_spider_ocr():
    return '{}.douyin_video_ocr_content'.format(dbname_origin)


def get_douyin_spider_asr_xhs():
    return 'douyin2_cleaner.xhs_note_video_info'

def get_xhs():
    return '{}.xhs_notes'.format(dbname)

def get_xhs_notes_topics():
    return '{}.xiaohongshu_notes_topics'.format(dbname)

def get_xhs_topics():
    return '{}.xiaohongshu_topics'.format(dbname)

def get_xhs_origin():
    return 'opinion.KOL_list_notes'

def get_xhs_notes_origin():
    return 'opinion.xiaohongshu_notes'

def get_xhs_notes_history():
    return 'douyin2_cleaner.xiaohongshu_notes'

def get_xhs_daily():
    return '{}.xhs_notes_daily_join'.format(dbname)

def get_xhs_part():
    return '{}.xhs_notes_daily_part'.format(dbname)

def get_douyin_daily_join():
    return '{}.douyin_daily_join'.format(dbname)

def get_douyin_daily_part():
    return '{}.douyin_video_daily_part'.format(dbname)

def get_douyin_video_comment_author():
    return '{}.douyin_video_comment_author2'.format(dbname)

def get_douyin_video_comment_ck():
    return '{}.douyin_video_comment'.format(dbname_origin)

def get_douyin_video_comment():
    return 'douyin2.douyin_video_comment_{}'

def get_douyin_video_ocr_sub():
    return 'douyin2.douyin_video_ocr_sub'

def get_douyin_video_suggested_word():
    return 'douyin2.douyin_video_suggested_word'
