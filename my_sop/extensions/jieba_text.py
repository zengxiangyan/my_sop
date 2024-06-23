from os.path import abspath, join, dirname
import jieba
import re
from extensions import utils
import application as app

g_jieba_imported = True
if not g_jieba_imported:
    jieba.load_userdict(app.output_path('dict_all.utf8.txt'))
else:
    print("imported")

def convert_doc_to_wordlist(str_doc,cut_all=False):
    if str_doc is None:
        return []
    # print('cut_all:', cut_all)
    # 分词的主要方法
    sent_list = str_doc.split('\n')
    sent_list = map(rm_char, sent_list) # 去掉一些字符，例如\u3000
    word_2dlist = [rm_tokens(jieba.cut(part,cut_all=cut_all)) for part in sent_list] # 分词
    word_list = sum(word_2dlist,[])
    return word_list

def list_word_tf(word_list):
    if len(word_list) == 0:
        return []
    word_2dlist = [rm_tokens(jieba.cut_for_search(part)) for part in word_list] # 分词
    return word_2dlist

def adjust_thesaurus(file_name):
    jieba.load_userdict(file_name)

def rm_char(text):
    text = re.sub('\u3000','',text)
    return text

def get_stop_words(path=join(abspath(dirname(__file__)), '../output/stop_words.tsv')):
    # stop_words中，每行放一个停用词，以\n分隔
    file = open(path,'rb').read().decode('utf8').split('\r\n')
    # print(file)
    return set(file)

def rm_tokens(words): # 去掉一些停用次和数字
    words_list = list(words)
    # print('words_list:', words_list)
    stop_words = get_stop_words()
    for i in range(words_list.__len__())[::-1]:
        s = words_list[i]
        if s in stop_words: # 去除停用词
            words_list.pop(i)
        elif utils.isNum2(s):
            words_list.pop(i)
        elif len(s) <= 1:
            words_list.pop(i)
        elif re.search(r'\d+[a-z%]', s, re.M|re.I):
            words_list.pop(i)
    return words_list