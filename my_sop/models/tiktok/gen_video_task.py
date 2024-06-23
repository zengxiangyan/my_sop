import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
from extensions import utils
import application as app
from models.tiktok.video_queue import insert_into_ocr_queue, insert_into_xunfei_queue
from models.new_brush_fetch import get_category_by_tablename
from scripts.cdf_lvgou.tools import Fc


def main():
    dy2 = app.get_db('dy2')
    utils.easy_call([dy2], 'connect')
    dy2.execute('use douyin2_cleaner')
    sql = "select distinct table_name from douyin2_cleaner.project where del_flag=0 and if_formal=1 and if_juliang=0 order by table_name"
    r = dy2.query_all(sql)
    for table_name, in r:
        r = dy2.query_one(f"show tables like '{table_name}'")
        if not r:
            continue
        print(table_name, end='\t')
        sql = f'''select aweme_id from douyin2_cleaner.{table_name} where aweme_id in 
            (select distinct aweme_id from xintu_category.xingtu_author_video where digg_count>=500)'''
        r = dy2.query_all(sql)
        ids = [row[0] for row in r]
        print(len(ids))
        if len(ids) == 0:
            continue
        category = get_category_by_tablename(table_name)
        insert_into_ocr_queue(dy2, category, ids, 20)
        insert_into_xunfei_queue(dy2, category, ids, 20)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        Fc.vxMessage('zheng.jiatao', 'gen_video_task', str(e))
