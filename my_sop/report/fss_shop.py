#coding=utf-8
import sys

from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import models.entity_manager as entity_manager
import pandas as pd


from models.compare import compare_ab, get_props

dba = app.get_clickhouse('chsop')
dba.connect()
db26 = app.get_db('26_apollo')
db26.connect()

ent = entity_manager.get_clean(1)
# eid,table,date,source,care_alias_all_bid = 91848,'sop_e.entity_prod_91848_E',202405,('11',''),(219205,3984881,182627,218562,266860,53268,6758,6404,20645,2968842,31466,3779,223274,180407,156557,218502,218724,2169320,53384,218970,202229,52297,68367,266017,23253,2503,11092,5324,8271,14362,11697,218520,53946,53312,16129,52238,52188,181468,218526,105167,124790,1933134,52501,171662,71334,529155,609944,493003,5316,218566,246499,3242356,529151,52711,51962,106548,47858,245844,97604,113650,10744,194248,4873,52567,218518,7012)
# eid,table,date,care_alias_all_bid= 91128,'sop_e.entity_prod_91128_E',202405,None
# eid,table,date,care_alias_all_bid= 91137,'sop_e.entity_prod_91137_E',202405,None
# eid,table,date,care_alias_all_bid= 91863,'sop_e.entity_prod_91863_E',202401,None
# eid,table,care_alias_all_bid,date = 91164,'sop_e.entity_prod_91164_E',None,202405
eid,table,date,source,care_alias_all_bid = 91171,'sop_e.entity_prod_91171_E_1112JD4',202405,('1','2'),(218970,1169730,245183,218793,2335055,218448,219205,218976,218529,218957,3681,3984881,52458,13914,182627,218562,266860,19690,53268,6758,529153,1639060,6404,130781,5624809,20645,2968842,31466,3779,223274,180407,156557,218502,218724,2169320,57099,53384,4147,4686598,202229,52297,68367,266017,14032,1052052,23253,2503,11092,5324,8271,14362,218715,11697,218910,11367,218592,218520,5028,219164,53946,2926939,53312,444983,16129,52238,52188,181468,218526,113866,1042268,105167,124790,7016,1933134,218450,1549320,52501,103459,71334,529155,609944,493003,5316,105342,218566,219031,246499,3242356,529151,218827,53191,52711,4787,12452,218651,219258,51962,106548,3560,47858,245844,594015,97604,496082,218915,2663754,113650,10744,1740242,194248,4873,6805,52567,106578,218518,7012)
# eid,table,date,care_alias_all_bid = 92194,'sop_e.entity_prod_92194_E',202312,(219205,218976,218529,218957,3681,3984881,13914,182627,218562,266860,6758,53268,529153,1639060,6404,130781,5624809,20645,2968842,31466,3779,223274,180407,156557,218502,218724,2169320,57099,53384,4147,4686598,202229,52297,68367,266017,14032,1052052,3242356,23253,2503,11092,5324,8271,14362,218715,11697,218910,11367,218592,218520,5028,219164,53946,2926939,53312,444983,16129,52238,52188,181468,218526,113866,1042268,105167,124790,7016,1933134,1549320,52501,103459,71334,609944,493003,5316,529155,105342,218566,219031,246499,529151,218827,53191,52711,12452,218651,219258,51962,106548,3560,47858,245844,594015,97604,496082,218915,2663754,10744,1740242,194248,6805,52567,106578,218518,7012,113650,19690,218450,4787,4873,52458)

# ent.get_plugin().add_bids(91171, 'sop_e.entity_prod_91171_E_1112JD4')
# ent.get_plugin().add_bids(91164, 'sop_e.entity_prod_91164_E')
# ent.get_plugin().add_bids(91137, 'sop_e.entity_prod_91137_E')
# ent.get_plugin().add_bids(91128, 'sop_e.entity_prod_91128_E')
ent.get_plugin().add_bids(eid=eid, tbl=table,care_alias_all_bid=care_alias_all_bid)

# ent.get_plugin().check_shop(91171, 'sop_e.entity_prod_91171_E_1112JD4')
# ent.get_plugin().check_shop(91164, 'sop_e.entity_prod_91164_E')
# ent.get_plugin().check_shop(91137, 'sop_e.entity_prod_91137_E')
ent.get_plugin().check_shop(eid=eid, tbl=table)

# ret = ent.get_plugin().get_fss_shop_info(91128, 'sop_e.entity_prod_91128_E', 202308)
# ret = ent.get_plugin().get_fss_shop_info(91171, 'sop_e.entity_prod_91171_E_1112JD4', 202310)
ret = ent.get_plugin().get_fss_shop_info(eid=eid, tbl=table, date=date)

# ret = ent.get_plugin().get_fss_shop_all(eid=eid, tbl=table,source=source) #获取关注品牌下面的所有旗舰店信息

# print(ret)
pd.DataFrame(ret[0],columns=['平台','sid','店铺名','url']).to_csv(sys.path[0]+f"/{eid}_new_fss_shop.csv",index=False,encoding='utf-8-sig',header=True)
pd.DataFrame(ret[1],columns=['平台','sid','旧店铺名','新店铺名','url']).to_csv(sys.path[0]+f"/{eid}_change_shopname.csv",index=False,encoding='utf-8-sig',header=True)
# pd.DataFrame(ret,columns=['平台','sid','alias_all_bid','品牌名','是否关注品牌','店铺名','url']).to_csv(sys.path[0]+f"/{eid}_fss_shop_all.csv",index=False,encoding='utf-8-sig',header=True)


# bids = ent.get_plugin().get_top_bids(91171)
# print(bids)

