from .common import lv0cid2cid, encode_string, generate_location_weight, match_answer, get_batch_data
from .data_module import AbstractData, TextData
from .KGEntity_module import *

# # from .ER_ner_input import get_input_bert
# from .configure import Configure
# from . import logger as logger
# from .evaluate import ner_eval
# from .keras_layers import CRF
#
# # function
# from .data_utils import get_train_data, get_kb_data, kb_processing, generate_description
# from .ER_ner_input import get_input_bert
# from .ER_ner_bert_crf import train_ner, predict_ner
# from .trie import get_split_entity
# from .entity_vec_extract import extract
# from .ER_match_input import entity_embedding, get_input_match
# from .ER_match_bert import train_match, predict_match
# from .ER_result import get_NER_result
# from .EL_result import get_NEL_result
# from .EL_input_bert import get_input
# from .EL_model_binary import train_nel, predict_f1, predict_loss
