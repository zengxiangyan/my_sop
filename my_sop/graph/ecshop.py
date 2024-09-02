#coding=utf-8
from nlp import pnlp
from colorama import Fore,Back,Style
from models.report.common import shop_type_ref
import application as app
import csv
import time
from difflib import SequenceMatcher

FSS = 1
EKA = 2 #EKA exclude EKA_FSS
EDT = 3
EKA_FSS = 4

fss_name_keywords = {'旗舰'} #, '官方店', '官方网店', '官方outlets店'} ??
fss_name_keywords_not = {'授权'}
outlet_name_keywords = {'outlets'}
tmall_eka_keywords = {'天猫超市', '天猫国际', '阿里健康', '淘宝心选'}   #含'阿里' '淘宝' 有很多不是EKA

logf = None
logcsv = None
fssb_log = None   
fssb_logcsv = None  #fss shop brand relation

graphdb = app.get_db('graph')

brand_dict = dict()
maker_dict = dict()

def open_log():
    global logf, logcsv, fssb_log, fssb_logcsv
    logf = open(app.output_path('ec_shop_log_%s.csv' % int(time.time())) , 'w', encoding='utf_8_sig', newline='')
    logcsv = csv.writer(logf)
    fssb_log = open(app.output_path('fss_brand_log_%s.csv' % int(time.time())) , 'w', encoding='utf_8_sig', newline='')
    fssb_logcsv = csv.writer(fssb_log)    
    fssb_logcsv.writerow(('source', 'ecshop_id', 'sname', 'snick', 'all_bid', 'bname', 'alias_all_bid', 'alias_bname', 'maker_id', 'mname', 'sales'))

def close_log():
    logf.close()

def get_makers():
    global maker_dict
    maker_dict = dict()
    sql = "select mid, name from graph.maker where deleteFlag=0"
    graphdb.connect()
    rows = graphdb.query_all(sql)    
    for mid, mname in rows:
        mname = pnlp.unify_character(mname)        
        maker_dict[mid] = (mid, mname, [])    

def get_maker_by_brand(all_bid):
    pass
    
def check_common_sub_str(str1, str2):
    seqMatch = SequenceMatcher(None, str1, str2) 
    m = seqMatch.find_longest_match(0, len(str1), 0, len(str2))    
    sub_str = str1[m[0]:m[0]+m[2]]    
    #l = len(pnlp.hanzi_characters(sub_str))    
    return sub_str, m[2]    

def classify_ecshop(source, sid, sname, snick, data):    
    if logcsv is None:
        open_log()

    label = EDT
    
    print('\n', Fore.BLUE)
    print(source, sid, sname, snick, Fore.RESET)
    
    if source == 'tb':       
        return EDT, None    #for performance   

    sname = pnlp.unify_character(sname)
    snick = pnlp.unify_character(snick)
    fss_in_name = False
    for t in fss_name_keywords:
        if t in snick or t in sname:
            fss_in_name = True
            break
    '''
    for t in fss_name_keywords_not:
        if t in snick or t in sname:
            fss_in_name = False
            break
    '''
    
    is_outlet = False
    for t in outlet_name_keywords:
        if t in snick or t in sname:
            is_outlet = True
            break    
    
    logcsv.writerow((source, sid, sname, snick))
    if fss_in_name:    
        #print(data)        
        for r in data:
            logcsv.writerow(r)    
    
    qbt_shop_type = None
    #channel type by brands and categories
    cid_dict = dict()
    bid_dict = dict()
    alias_dict = dict()
    mid_set = set()
    total_sales = 0
    
    for all_bid, bname, alias_all_bid, alias_bname, cid, shop_type, sales, mid, mname in data:  #mid: maker id from legacy qbt db
        print(all_bid, bname, alias_all_bid, alias_bname, cid, shop_type, sales, mid, mname)
        qbt_shop_type = shop_type
        total_sales += sales
        if all_bid == 0 or '其他' in bname or sales==0:    #ignore bid==0
            continue        
        
        alias = alias_all_bid
        if alias_all_bid == 0:
            alias = all_bid
            alias_bname = bname        
        if cid not in cid_dict:
            cid_dict[cid] = dict()        
        if alias not in cid_dict[cid]:
            cid_dict[cid][alias] = 0
        cid_dict[cid][alias] += sales
        if all_bid not in bid_dict:
            bid_dict[all_bid] = [all_bid, bname, alias_all_bid, alias_bname, mid, mname, sales]
        else:
            bid_dict[all_bid][6] += sales
        alias_dict[alias] = (alias, alias_bname, mid, mname)
        mid_set.add(mid)     #mid==0 also add, to diff with other mids
    
    brands_qualify = True
    if fss_in_name:     #for performance            
        for cid,b in cid_dict.items():
            if len(b) > 1:                
                logcsv.writerow(('cid', cid, 'bids', b.keys()))                
                if len(mid_set) == 1:
                    if 0 in mid_set:
                        brands_qualify = False  #do not know about maker
                        break                    
                else:
                    brands_qualify = False  
                    break
            
        #if len(alias_dict) > 1:        
        fssb_logcsv.writerow((source, sid, sname, snick))
        for alias, v in bid_dict.items():                
            fssb_logcsv.writerow(['','','',''] + v[:6] + [int(v[6]/179.35)])     #hide sales data            

        if not brands_qualify:  #对于品牌厂商关系不全的情况，根据店铺名字和品牌/厂商名，进行一定程度的补救
            #check shop name/nick with brand/maker name substr

            fss_strs = []   #times should count
            sales_list = []
            for _,v in bid_dict.items():
                fss_strs.append((pnlp.unify_character(v[1]), pnlp.unify_character(v[3]), pnlp.unify_character(v[5])))
                sales_list.append(v[6])
                #bname
                #alias_bname
                #mname

            print(fss_strs)
            #name_match = False
            match_count = 0
            nstr = sname + ' ' + snick
            match_sales = 0
            for i, ts in enumerate(fss_strs):
                for t in ts:
                    s, l = check_common_sub_str(nstr, t)
                    #print(sname, snick, t, s, l)
                    if l >= 2:
                        #name_match = True
                        #break
                        match_count += 1
                        match_sales += sales_list[i]
                        break
            
            guess = False   #GUESS !!
            if match_sales*2>=total_sales or match_count*2 >= len(bid_dict):  #more than a half name match
                guess = True
            #else:
            #    if len(bid_dict) <= 30:
            #        if match_count >= 7:    
            #            guess = True
            
            if guess:                
                brands_qualify = True
            msg = 'Name match count %s, match sales rate %s, guess %s' % (match_count, float(match_sales/total_sales), guess)
            print(msg)                   
            fssb_logcsv.writerow([source, sid, msg])     #hide sales data   
            
        fssb_logcsv.writerow(['FSS check', brands_qualify])
        fssb_logcsv.writerow(('------',))

    if source == 'jd':      #京东自营旗舰店 => FSS ??
        if sid == 0:    #京东自营 no shop
            label = EKA
        else:            
            if '自营' in sname or '京喜专区' in sname:
                label = EKA
            if fss_in_name:
                if brands_qualify:
                    if '自营' in sname or '京喜专区' in sname:
                        label = EKA_FSS
                    else:
                        label = FSS
                    
    elif source == 'tmall':
        eka_in_name = False
        for t in tmall_eka_keywords:
            if t in snick:
                eka_in_name = True
                break
        
        if qbt_shop_type == 23: #天猫超市
            label = EKA
        elif eka_in_name:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS        
    
    elif source == 'tb':
        label = EDT
    
    elif source == 'suning':
        if qbt_shop_type == 11 or qbt_shop_type == 21 or '自营' in sname:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS
    
    elif source == 'gome':
        if qbt_shop_type == 11 or qbt_shop_type == 21 or '自营' in sname:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS

    elif source == 'kaola':
        if qbt_shop_type == 11 or qbt_shop_type == 21 or '自营' in sname:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS

    elif source == 'dy':
        if '抖音自营' in sname:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS

    elif source == 'dy2':
        if '抖音自营' in sname:
            label = EKA
        elif fss_in_name:
            if brands_qualify:
                label = FSS

    elif source == 'pdd':
        if fss_in_name:
            if brands_qualify:
                label = FSS


    print(Fore.RED, 'brands_qualify:', brands_qualify, 'label:', label, Fore.RESET)
    logcsv.writerow(('FSS in name:', fss_in_name, 'brands count:', len(bid_dict), 'same maker:', brands_qualify, 'label:', label))
    logcsv.writerow(('------',))
    return label, len(bid_dict), is_outlet


if __name__ == '__main__':
    all_bid = 1
    bname = 'nike'
    alias_all_bid = 1
    alias_bname ='nike'
    cid = 1001
    sales = 10000
    shop_type = 0
    
    print(classify_ecshop('tmall', 111, 'NIKE官方旗舰店', 'NIKE官方旗舰店', [(all_bid, bname, alias_all_bid, alias_bname, cid, shop_type, sales)]))




