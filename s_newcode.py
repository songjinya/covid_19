import requests
import os
import time
import datetime
from datetime import *
import pandas as pd
from pandas import DataFrame
import time

def down_csv(api,local_name,url_csv,filename):
    
    all_info = requests.get(api).json()# 发送请求，获取数据
    cur_update = all_info['updated_at']# 获取更新时间
    github_time = time.strptime(cur_update,'%Y-%m-%dT%H:%M:%SZ')
    github_time=time.mktime(github_time)
    print(github_time)#github项目的更新时间戳
    if local_name:
        filemt = os.stat(local_name).st_mtime
        print(filemt)#本地文件的时间戳
    else:
        filemt=0
    if github_time!=filemt:#假如文件有更新，就下载新的文件
        fail_suc=0
        while fail_suc==0:
            print("1")
            try:
                r = requests.get(url_csv, stream=True)
                print("2")
                with open(filename, 'wb') as f:
                    try:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                        print("down success!")
                        fail_suc=1
                    except:
                        print("down fail")
                        fail_suc=0
            except:
                print("request fail")
                fail_suc=0
    else:
        print("no update")
        return 1
        
        
def day_file(file):
    new= pd.read_csv(file,parse_dates=['updateTime'])#导入数据
    #。。。整理文档，每天只保留最新的数据
    # new['updateTime'] = new['updateTime'].apply(pd.to_datetime)#处理updatetime数据类型
    new['date'] = new['updateTime'].dt.date#生成date列
    #处理空值
    new.loc[:,'continentName'].fillna('',inplace=True)
    new.loc[:,'continentEnglishName'].fillna('',inplace=True)
    new.loc[:,'countryName'].fillna('',inplace=True)
    new.loc[:,'countryEnglishName'].fillna('',inplace=True)
    new.loc[:,'provinceName'].fillna('',inplace=True)
    new.loc[:,'provinceEnglishName'].fillna('',inplace=True)
    new.loc[:,'cityName'].fillna('',inplace=True)
    new.loc[:,'cityEnglishName'].fillna('',inplace=True)
    new.loc[:,'province_zipCode'].fillna(0,inplace=True)
    new.loc[:,'province_confirmedCount'].fillna(0,inplace=True)
    new.loc[:,'province_suspectedCount'].fillna(0,inplace=True) 
    new.loc[:,'province_curedCount'].fillna(0,inplace=True)
    new.loc[:,'province_deadCount'].fillna(0,inplace=True)
    new.loc[:,'city_zipCode'].fillna(0,inplace=True)
    new.loc[:,'city_confirmedCount'].fillna(0,inplace=True)
    new.loc[:,'city_suspectedCount'].fillna(0,inplace=True)
    new.loc[:,'city_curedCount'].fillna(0,inplace=True)
    new.loc[:,'city_deadCount'].fillna(0,inplace=True)
    new.loc[:,'updateTime'].fillna('',inplace=True)
    new.loc[:,'date'].fillna('',inplace=True)
    # new.fillna(0,inplace=True)
    groupby_pcd=new.groupby(['provinceName','cityName','date'])
    latest = groupby_pcd['updateTime'].idxmax()
    new = new.loc[latest] 
    return new 
def out_land(file):    
    new= pd.read_csv(file,parse_dates=['updateTime','date'])
    s_outland=new.loc[new['cityName'].isin(['境外输入','境外输入人员'])]
    s_outland['cityName']=s_outland['cityName'].replace(['境外输入人员'],['境外输入'])
    # s_outland['updateTime']=pd.to_datetime(s_outland['updateTime'])
    groupby_out=s_outland.groupby(['provinceName','date'])
    latest_out = groupby_out['updateTime'].idxmax()
    s_outland = s_outland.loc[latest_out]
    s_outland.drop(['updateTime','province_confirmedCount' ,'province_suspectedCount','province_curedCount','province_deadCount'],axis=1,inplace=True)
    #将所有省份缺失的日期数据补齐，生成新增列   
    s_outland.sort_values(by=["provinceName","date"],ascending=True,inplace=True)#排序
    s_outland.reset_index(drop=True,inplace=True)#重置index
    pro_out=s_outland['provinceName'].unique().tolist()#省份列表
    max_date=s_outland['date'].drop_duplicates().max().strftime('%Y-%m-%d')
    k=0#处理数据的行ID
    for i in pro_out:
    #     min_date=s_outland['date'].drop_duplicates().min().strftime('%Y-%m-%d')
        min_date=s_outland.loc[s_outland['provinceName']==i]['date'].min().strftime('%Y-%m-%d')
        date_out=pd.date_range(start=min_date,end=max_date)
        for j in date_out:
            # l=pd.to_datetime(j).date()#取出日期，用来比较。这个用来参照的日期
            m=s_outland.shape[0]#数据表的行数
            # pd.concat([s_outland, pd.DataFrame(columns=['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount','city_confirmedCount','city_suspectedCount','city_curedCount','city_deadCount'])])
            if k<m:#如果要处理的数据ID小于行数，需要插入数据；否则直接在最后append
    #             print(type(s_outland.loc[k,'date']))
    #             print(type(str(l)))
                if j != s_outland.loc[k,'date']:#将要比较的数据取出来与参照日期比较，不同需要插入一行参照日期数据
                    insertRow=s_outland.iloc[k-1,:]
                    insertRow['in_city_confirmedCount']=0#........................................
                    insertRow['in_city_suspectedCount']=0#........................................
                    insertRow['in_city_curedCount']=0#........................................
                    insertRow['in_city_deadCount']=0#........................................
                    #后面是dataframe插入行的操作
                    above = s_outland.iloc[:k]
                    below = s_outland.iloc[k:]
                    s_outland = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
                    m=s_outland.shape[0]#新的数据行数
                    s_outland.loc[k,'date']=j#将参照日期赋值给插入的行
                else:
                    if j!=date_out[0]:#假如是第一个参照日期，不操作
                        now_line=s_outland.loc[k,['city_confirmedCount','city_suspectedCount','city_curedCount','city_deadCount']]
                        last_line=s_outland.loc[k-1,['city_confirmedCount','city_suspectedCount','city_curedCount','city_deadCount']]
                        in_line=now_line-last_line
                        in_line[in_line<0]=0
                        s_outland.loc[k,'in_city_confirmedCount']=in_line[0]
                        s_outland.loc[k,'in_city_suspectedCount']=in_line[1]
                        s_outland.loc[k,'in_city_curedCount']=in_line[2]
                        s_outland.loc[k,'in_city_deadCount']=in_line[3]
            else:
                insertRow=s_outland.iloc[k-1,:]
                insertRow['in_city_confirmedCount','in_city_suspectedCount','in_city_curedCount','in_city_deadCount']=[0,0,0,0]
                s_outland=s_outland.append(insertRow,ignore_index=True)
                s_outland.loc[k,'date']=j
                m=s_outland.shape[0]
            k+=1 
    return s_outland
def china_province(file): 
    #。。。生成国内省份累计数据
    new= pd.read_csv(file,parse_dates=['updateTime','date'])
    china_province=new.loc[(new['countryName']=='中国')&(new['countryName']!=new['provinceName'])]
    groupby_pd=china_province.groupby(['provinceName','date'])
    latest_c = groupby_pd['updateTime'].idxmax()
    china_province = new.loc[latest_c]
    china_province.drop(['updateTime','cityName','cityEnglishName','city_zipCode' ,'city_confirmedCount' ,'city_suspectedCount','city_curedCount','city_deadCount'],axis=1,inplace=True)
    # china_province.drop_duplicates(inplace=True)
    #将所有省份缺失的日期数据补齐，生成新增列   
    china_province.sort_values(by=["provinceName","date"],ascending=True,inplace=True)#排序
    china_province.reset_index(drop=True,inplace=True)
    count_pro=china_province['provinceName'].drop_duplicates().count()#数据表中涉及到的省份的数量，后面会处理的次数
    min_date=china_province['date'].drop_duplicates().min().strftime('%Y-%m-%d')
    max_date=china_province['date'].drop_duplicates().max().strftime('%Y-%m-%d')
    date_r=pd.date_range(start=min_date,end=max_date)#数据表中涉及到日期的范围
    k=0#处理数据的行ID
    for i in range(count_pro):
        for j in date_r:
            # l=pd.to_datetime(j).date()#取出日期，用来比较。这个用来参照的日期
            m=china_province.shape[0]#数据表的行数
            if k<m:#如果要处理的数据ID小于行数，需要插入数据；否则直接在最后append
                # print(china_province.loc[k,'date'])
                # print(type(china_province.loc[k,'date']))
                if j != china_province.loc[k,'date']:#将要比较的数据取出来与参照日期比较，相同不处理，不同需要插入一个参照日期数据
                    if j==date_r[0]:#假如是第一个参照日期，数据需要参考后面一天的数据，其他参考前一天
                        insertRow=china_province.iloc[k,:]
                    else:
                        insertRow=china_province.iloc[k-1,:]
                    # insertRow.loc[,,,]=[0,0,0,0]
                    insertRow['in_province_confirmedCount']=0#........................................
                    insertRow['in_province_suspectedCount']=0#........................................
                    insertRow['in_province_curedCount']=0#........................................
                    insertRow['in_province_deadCount']=0#........................................
                    #后面是dataframe插入行的操作
                    above = china_province.iloc[:k]
                    below = china_province.iloc[k:]
                    china_province = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
                    m=china_province.shape[0]#新的数据行数
                    china_province.loc[k,'date']=j#将参照日期赋值给插入的行
                else:
                    if j!=date_r[0]:#假如是第一个参照日期，不操作
                        now_line=china_province.loc[k,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]
                        last_line=china_province.loc[k-1,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]
                        # print(type(now_line))
                        # print(type(last_line))
                        # print(now_line)
                        # print(last_line)
                        in_line=now_line-last_line
                        in_line[in_line<0]=0
                        china_province.loc[k,'in_province_confirmedCount']=in_line[0]
                        china_province.loc[k,'in_province_suspectedCount']=in_line[1]
                        china_province.loc[k,'in_province_curedCount']=in_line[2]
                        china_province.loc[k,'in_province_deadCount']=in_line[3]

            else:
                insertRow=china_province.iloc[k-1,:]
                insertRow['in_province_confirmedCount','in_province_suspectedCount','in_province_curedCount','in_province_deadCount']=[0,0,0,0]
                china_province=china_province.append(insertRow,ignore_index=True)
                china_province.loc[k,'date']=j
                m=china_province.shape[0]
            k+=1
    return china_province
def world_data(file1,file2):
    #。。。从省份数据计算全国数据
    china_province= pd.read_csv(file1,parse_dates=['date'])
    groupby_d=china_province.groupby(['date'])
    china_data=groupby_d['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount'].sum()
    china_data['date']=china_data.index
    china_row=china_province.iloc[0,:]
    china_row['provinceName','provinceEnglishName','province_zipCode']=['中国','china','951001']
    print('china data success !!!')
    #。。。生成世界国家累计数据
    new= pd.read_csv(file2,parse_dates=['updateTime','date'])
    world_country=new.loc[(new['countryName']==new['provinceName'])&(new['countryName']!='中国')]
    groupby_cd=world_country.groupby(['countryName','date'])
    latest_w = groupby_cd['updateTime'].idxmax()
    world_country = new.loc[latest_w]
    world_country.drop(['updateTime','cityName','cityEnglishName','city_zipCode' ,'city_confirmedCount' ,'city_suspectedCount','city_curedCount','city_deadCount'],axis=1,inplace=True)
    # world_country.drop_duplicates(inplace=True)
    #。。。将中国数据添加到世界里
    for f in china_data.index:
        china_row.loc['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount','date']=china_data.loc[f,:]
        world_country=world_country.append(china_row,ignore_index=True)
    print('china data is in world !!!')    
    return world_country
    
def clean_world_data(file):
    world_country=pd.read_csv(file,parse_dates=['date'])
    #将所有国家缺失的日期数据补齐，主要跟前一天数据一致，如果是统计的第一天就与后一天数据保持一致    
    world_country.sort_values(by=['countryName',"date"],ascending=True,inplace=True)#排序
    world_country.reset_index(drop=True,inplace=True)
    pro_w=world_country['countryName'].unique().tolist()#国家列表
    # count_pro_w=world_country['countryName'].drop_duplicates().count()#数据表中涉及到的国家的数量，后面会处理的次数
    max_date_w=world_country['date'].drop_duplicates().max().strftime('%Y-%m-%d')
    k_w=0#处理数据的行ID
    for i in pro_w:
        #min_date_w=world_country['date'].drop_duplicates().min().strftime('%Y-%m-%d')
        min_date_w=world_country.loc[world_country['countryName']==i]['date'].min().strftime('%Y-%m-%d')
        date_r_w=pd.date_range(start=min_date_w,end=max_date_w)#数据表中涉及到日期的范围
        for j in date_r_w:
            l=pd.to_datetime(j).date()#取出日期，用来比较。这个用来参照的日期
            m=world_country.shape[0]#数据表的行数
            if k_w<m:#如果要处理的数据ID小于行数，需要插入数据；否则直接在最后append
                # print(world_country.loc[k_w,'date'])
                # print(type(world_country.loc[k_w,'date']))
                if j != world_country.loc[k_w,'date']:#将要比较的数据取出来与参照日期比较，不同需要插入一个参照日期数据
                    insertRow=world_country.iloc[k_w-1,:]
                    # insertRow['in_province_confirmedCount','in_province_suspectedCount','in_province_curedCount','in_province_deadCount']=[0,0,0,0]
                    insertRow['in_province_confirmedCount']=0#........................................
                    insertRow['in_province_suspectedCount']=0#........................................
                    insertRow['in_province_curedCount']=0#........................................
                    insertRow['in_province_deadCount']=0#........................................
                    #后面是dataframe插入行的操作
                    above = world_country.iloc[:k_w]
                    below = world_country.iloc[k_w:]
                    world_country = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
                    m=world_country.shape[0]#新的数据行数
                    world_country.loc[k_w,'date']=j#将参照日期赋值给插入的行
                else:
                    if j!=date_r_w[0]:#假如是第一个参照日期，不操作
                        # in_line=world_country.loc[k,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]-world_country.loc[k-1,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]
                        now_line=world_country.loc[k_w,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]
                        last_line=world_country.loc[k_w-1,['province_confirmedCount','province_suspectedCount','province_curedCount','province_deadCount']]
                        in_line=now_line-last_line
                        in_line[in_line<0]=0
                        world_country.loc[k_w,'in_province_confirmedCount']=in_line[0]
                        world_country.loc[k_w,'in_province_suspectedCount']=in_line[1]
                        world_country.loc[k_w,'in_province_curedCount']=in_line[2]
                        world_country.loc[k_w,'in_province_deadCount']=in_line[3]

            else:
                insertRow=world_country.iloc[k_w-1,:]
                insertRow['in_province_confirmedCount','in_province_suspectedCount','in_province_curedCount','in_province_deadCount']=[0,0,0,0]
                world_country=world_country.append(insertRow,ignore_index=True)
                world_country.loc[k_w,'date']=j
                m=world_country.shape[0]
            k_w+=1 
    return world_country
def grow_hubei(file):
    new= pd.read_csv(file,parse_dates=['updateTime','date'])
    hubei=new.loc[(new['provinceName']=='湖北省')&(new['cityName']!='0')]
    groupby_cd=hubei.groupby(['cityName','date'])
    # hubei['updateTime']=pd.to_datetime(hubei['updateTime'])#........................................
    # print(groupby_cd['updateTime'].idxmax())
    latest = groupby_cd['updateTime'].idxmax()
    hubei = new.loc[latest]
    hubei.drop(['updateTime','province_confirmedCount' ,'province_suspectedCount','province_curedCount','province_deadCount'],axis=1,inplace=True)
    #将所有缺失的日期数据补齐，生成新增列   
    hubei.sort_values(by=['cityName',"date"],ascending=True,inplace=True)#排序
    hubei.reset_index(drop=True,inplace=True)
    cities=hubei['cityName'].unique().tolist()#城市列表#........................................
    max_date=hubei['date'].max()#........................................
    k=0#处理数据的行ID
    for i in cities:
        print(i)
        min_date=hubei.loc[hubei['cityName']==i]['date'].min()#........................................
        
        date_r=pd.date_range(start=min_date,end=max_date)#数据表中涉及到日期的范围#........................................
        for j in date_r:
            # l=pd.to_datetime(j).date().strftime('%Y-%m-%d')#取出日期，用来比较。这个用来参照的日期#.........................................strftime('%Y-%m-%d')
            m=hubei.shape[0]#数据表的行数
            if k<m:#如果要处理的数据ID小于行数，需要插入数据；否则直接在最后append
                # print(type(j))
                # print(type(hubei.loc[k,'date']))
                if j != hubei.loc[k,'date']:#将要比较的数据取出来与参照日期比较，相同不处理，不同需要插入一个参照日期数据
                    insertRow=hubei.iloc[k-1,:]
                    # insertRow['in_city_confirmedCount','in_city_suspectedCount']=(0,0)
                    insertRow['in_city_confirmedCount']=0#........................................
                    insertRow['in_city_suspectedCount']=0#........................................
                    insertRow['in_city_curedCount']=0#........................................
                    insertRow['in_city_deadCount']=0#........................................
                    #后面是dataframe插入行的操作
                    above = hubei.iloc[:k]
                    below = hubei.iloc[k:]
                    hubei = above.append(insertRow,ignore_index=True).append(below,ignore_index=True)
                    m=hubei.shape[0]#新的数据行数
                    hubei.loc[k,'date']=j#将参照日期赋值给插入的行
                else:
                    if j!=date_r[0]:#假如是第一个参照日期，不操作
                        now_line=hubei.loc[k,['city_confirmedCount','city_suspectedCount','city_curedCount','city_deadCount']]
                        last_line=hubei.loc[k-1,['city_confirmedCount','city_suspectedCount','city_curedCount','city_deadCount']]
                        in_line=now_line-last_line
                        in_line[in_line<0]=0
                        hubei.loc[k,'in_city_confirmedCount']=in_line[0]
                        hubei.loc[k,'in_city_suspectedCount']=in_line[1]
                        hubei.loc[k,'in_city_curedCount']=in_line[2]
                        hubei.loc[k,'in_city_deadCount']=in_line[3]
            else:
                insertRow=hubei.iloc[k-1,:]
                insertRow['in_city_confirmedCount','in_city_suspectedCount','in_city_curedCount','in_city_deadCount']=[0,0,0,0]
                hubei=hubei.append(insertRow,ignore_index=True)
                hubei.loc[k,'date']=j
                m=hubei.shape[0]
            k+=1   
    return hubei
      
def grow_file():
    
    s_day_data=day_file("./csv/new_DXYArea.csv")    
    s_day_data.to_csv('./csv/s_day_data.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_day_data.csv success !!!')
    
    s_outland=out_land('./csv/s_day_data.csv')    
    s_outland.to_csv('./csv/s_in_outland.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_in_outland.csv success !!!')   
        
    s_china_province=china_province('./csv/s_day_data.csv')    
    s_china_province.to_csv('./csv/s_in_china_province.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_in_china_province.csv success !!!') 

    s_world_data=world_data('./csv/s_in_china_province.csv','./csv/s_day_data.csv')
    s_world_data.to_csv('./csv/s_world_data.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_world_data.csv success !!!')
    
    s_world_country=clean_world_data('./csv/s_world_data.csv')
    s_world_country.to_csv('./csv/s_in_world_country.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_in_world_country.csv success !!!')
    
    hubei=grow_hubei('./csv/s_day_data.csv')
    hubei.to_csv('./csv/s_in_hubei.csv',index=False,encoding="utf_8_sig")#导出文件
    print('s_in_hubei.csv success !!!')  
    

#参数区  
api = 'https://api.github.com/repos/BlankerL/DXY-COVID-19-Data'# Github项目及API接口数据
local_name = './csv/new_DXYArea.csv'  # 下载到本地的路径
url_csv="https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv"#项目的下载路径
filename = r"./csv/new_DXYArea.csv"

file="new_DXYArea.csv"#要整理的文件名称

#执行区
if __name__ == "__main__":
    down_csv(api,local_name,url_csv,filename)    
    grow_file()   
        
        