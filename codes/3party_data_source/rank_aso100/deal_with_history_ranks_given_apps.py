#coding:utf-8
import os
import json
import time
from openpyxl import Workbook
import pandas as pd
import calendar

# 获取某月的天日期
def getAllDaysOfOneMonth(curmonth):
    year, month = curmonth.split('-')
    days_curmonth = calendar.monthrange(int(year), int(month))[1]
    return [curmonth + '-0' + str(i) for i in range(1, 10)] +\
            [curmonth + '-' + str(i) for i in range(10, days_curmonth+1)]
#print getAllDaysOfOneMonth('2017-07')


# 此工作空间即是当前工作空间
curmonth = '2017-08'
working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/第三方数据/' +\
        curmonth + '/rank_aso100/'
path_crawled_ranks = working_space + 'crawled_history_ranks_of_apps/'
path_lookup_table = '/Users/Alas/Documents/TD/CAT/lookup_table_of_cat.xlsx'
path_to_write = working_space + 'ranks_aso100_'  + curmonth + '.xlsx'

# 得到待处理文件列表, 每一个文件是一个应用的iOS免费榜历史排名值
list_filename = os.listdir(path_crawled_ranks)
if '.DS_Store' in list_filename:
    list_filename.remove('.DS_Store')
#print list_filename

# 获取CAT应用的名称对照表
df_lookup_table = pd.read_excel(path_lookup_table, sheetname='lookup_table')
df_lookup_table = df_lookup_table[['Apple Appid', 'Package Name', 'Chinese Name']]
df_lookup_table.dropna(inplace=True)
#print df_lookup_table.head()


wb = Workbook()
ws = wb.active
ws.title = 'raw_data_from_rank_aso100'
ws.append(['Apple Appid', 'Package Name', 'Chinese Name', 'Rank Type',] +\
        getAllDaysOfOneMonth('2017-06') + getAllDaysOfOneMonth('2017-07'))

# 将每个文件挨个处理好格式后存入Excel文件中
for filename in list_filename:
    # 根据文件名得到该应用的苹果应用id, 同时也能得到排名的起始终止时间
    # '1006637877_2014-04-01_2017-03-29.txt'
    app_apple_appid = int(filename.split('_')[0])
    #print [app_apple_appid]

    #获取苹果ID对应的中文名
    try:
        app_cname = df_lookup_table[df_lookup_table['Apple Appid']==
                app_apple_appid]['Chinese Name'].values[0]
    except Exception, e:
        print e, '\t', app_apple_appid
        app_cname = ''

    #获取苹果ID对应的包名
    try:
        app_pkname = df_lookup_table[df_lookup_table['Apple Appid']==
                app_apple_appid]['Package Name'].values[0]
    except Exception, e:
        print e, '\t', app_apple_appid
        app_pkname = ''
    #print app_apple_appid, app_pkname, app_cname


    with open(path_crawled_ranks + filename, 'r') as f1:
        # 根据源文件存储数据的格式, 将数据块处理成方便调用的格式
        try:
            data_json = json.loads(f1.readlines()[0])
            #print data_json.keys()
            # 该列表中不只有"总榜免费", 可能会有"总榜畅销","所属细分行业排名",
            #print len(data_json['data']['list'])
            #print data_json['data']['list']

            # 如果有多个榜单(且有总榜免费), 那么总榜免费总是在第一个榜单的位置
#            print [app_apple_appid],
#            for l in data_json['data']['list']:
#                print l['name'],
#            print '\n'
            # 所以只需取['list'][0]即可得到总榜免费(若有)
            rank_name = data_json['data']['list'][0]['name']
            list_date_with_rank_oneday = data_json['data']['list'][0]['data']
            #print [app_apple_appid], rank_name, list_date_with_rank_oneday[0]

            # 将所需数据整理成适合写入Excel文件的格式(一个列表算作一条记录)
            list_rank_oneday = [app_apple_appid, app_pkname, app_cname, rank_name,]
            for date_with_rank_oneday in list_date_with_rank_oneday:
                # 输出排名对应的时间
                #print time.strftime('%Y-%m-%d', 
                #        time.localtime(date_with_rank_oneday[0]/1000)), date_with_rank_oneday[1]
                list_rank_oneday.append(date_with_rank_oneday[1])
            #print list_rank_oneday
            #print [app_apple_appid], rank_name, len(list_rank_oneday)

            ws.append(list_rank_oneday)
        except Exception, e:
            print e, '\t', app_apple_appid

# 最终文件不仅包含总榜免费的, 需要去除其他类型, 然后匹配上中文名, 加上数据时间
wb.save(path_to_write)
