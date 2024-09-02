from extensions import utils
dbname = 'product_lib'
dbname_ck = 'sop_e'
default_limit = 2000
default_limit_ck = 100000
default_limit_ck_select = 1000000

table_ocr_queue = f"{dbname}.ocr_queue"
table_ocr_result = f"{dbname}.ocr_result"
table_model = f'{dbname}.nlp_model'
table_model_queue = f'{dbname}.nlp_model_queue'
table_model_integration = f'{dbname}.nlp_model_integration'
table_model_integration_detail = f'{dbname}.nlp_model_integration_detail'

table_model_result = f'{dbname}.nlp_model_qsi_1008'
table_model2 = f'{dbname}.nlp_model_qsi_1008_v2'
table_base_item = f'{dbname}.nlp_item_base'
table_base_item_ck = f'{dbname_ck}.nlp_item_base'
table_base_result = f'{dbname}.nlp_result_base'
table_base_result_ck = f'{dbname_ck}.nlp_result_base'
table_class_result = f'{dbname}.class_model_qsi_real'
table_class_result2 = f'{dbname}.class_model_qsi_real_v2'
table_integration_result = f'{dbname}.integration_model_qsi_real'


table_item_meizhuang = f'{dbname}.qsi_item_list_91783'
# table_class_result_meizhuang = f'{dbname}.class_model_qsi_1010_v4'
# table_class_result2_meizhuang = f'{dbname}.class_model_qsi_1010_v3'
table_class_result_meizhuang = f'{dbname}.class_model_qsi_91783'
table_class_result2_meizhuang = f'{dbname}.class_model_qsi_91783_v2'

table_class_result_dy = f'{dbname}.class_model_qsi_real_for_dy_cid'

table_error_message = f'{dbname}.error_message'

status_model_queue_origin = 0
status_model_queue_init = 10
status_model_queue_running = 20
status_model_queue_run_error = 21
status_model_queue_finished = 30
status_model_queue_end = 100

max_waiting_count = 5

h_prop_keys = {x:1 for x in ['品名','单品','产品名称','主货号','商品名称','包装清单','产品','套装']}
h_prop_keys = {x:1 for x in ['品名','单品','产品名称','主货号','商品名称','包装清单','产品','套装','款式','功能','规格','功效','备案/批准文号','产品名称','类型','品类','产品净含量', '净含量']}

if utils.is_windows():
    paddle_multi_class_dir = r'T:\paddlenlp\applications\text_classification\multi_class\\'
else:
    paddle_multi_class_dir = '/home/zhou.li/paddlenlp/applications/text_classification/multi_class/'
model_type_paddle_multi_class = 1

flag_model_id_ref = {
    -1: 256, #数据同步到结果表
    0: 128, #done时状态
    # 1: 4,
    # 2: 8,
    # 3: 1,    #dy 分品类
    # 100: 4,
    # 101: 8,
}
flag_integration_done = 128
flag_out_table_done = 256

zhou_flag_model_done = 15   #所有模型完成标记

#计算准确率时使用到的条件
#该配置用到的代码已废弃
where_for_acc_by_model_id = {
    -1: 'common_test=1 and a.folder_id>0',
    0: 'a.is_machine=0 and common_test=1 and a.folder_id>0',
    1: 'a.is_machine=0 and common_test=1 and a.folder_id>0',
    2: 'a.is_machine=0 and common_test=1 and a.folder_id>0'
}
