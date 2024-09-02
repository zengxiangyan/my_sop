# -*- coding: utf-8 -*-
import pandas as pd
df_top = pd.read_excel(r'E:\小黑瓶质检\截图\24年 7月\LRL July 8th screenshot report(CDF).xlsx',sheet_name = 'Sheet1')
df_obser = pd.read_csv(r'E:\小黑瓶质检\数据备份\7-8观察记录表.csv')
df_top['宝贝ID'] = df_top['宝贝ID'].astype(str)
df_obser_del = pd.DataFrame(df_obser[['标签二','划线价','促销价','折后价','单件最大折扣折后价','单件最大折扣促销信息','所有促销信息','应用促销信息','超级促销','ID']])
df_obser_del.columns = ['目录名称','划线价','促销价','折后价','单件最大折扣折后价','单件最大折扣促销信息','所有促销信息','应用促销信息','超级促销','宝贝ID']
df_obser_del['宝贝ID']=df_obser_del['宝贝ID'].str.replace(r'^(1_|2_|3_|67_|154_)', '', regex=True)
# result_df = pd.merge(df_top, df_obser_del, on=['目录名称', '宝贝ID'], how='left',suffixes=('', '_obver'))
result_df = pd.merge(df_top, df_obser_del, on=['宝贝ID'], how='left',suffixes=('', '_obver'))
result_df['划线价'] = result_df['划线价'].astype(float)
result_df['促销价'] = result_df['促销价'].astype(float)
result_df['折后价'] = result_df['折后价'].astype(float)
result_df['应用促销信息'] = result_df['应用促销信息'].astype(str)
result_df['满减'] = result_df['满减'].astype(str)
result_df['促销活动'] = result_df['促销活动'].astype(str)
result_df['超级促销'] = result_df['超级促销'].astype(str)
result_df['应用促销信息'].isna()
result_df['满减'].isna()
result_df['超级促销'].isna()
result_df.to_excel(r'E:\小黑瓶质检\数据备份\0708TOP LRL(CDF).xlsx',index=False,encoding='utf-8-sig')

check = {'划线价(元)':'划线价','促销价(元)':'促销价','最低价(元)':'折后价','促销活动':'超级促销','满减':'应用促销信息'}
for c in check.keys():
    print("check {}".format(c))
    print(result_df.loc[result_df[c]!=result_df[check[c]]])