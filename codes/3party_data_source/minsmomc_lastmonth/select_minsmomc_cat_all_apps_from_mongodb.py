#coding:utf-8
import pandas as pd
from pymongo import MongoClient
from openpyxl import Workbook

# 从mongodb数据库提取上期所有应用的月安装环比用于调数文件
Client = MongoClient()
db = Client.TalkingData
# 本期所需上期的数据存放位置
lastmonth = '2017-07'
# 本期数据的存放位置
curmonth = '2017-08'

# 还有修改待提取数据库名 cat_data_201706(上月)

working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/第三方数据/'
path_lt_cat_apps = working_space + '../../../header/' + 'lookup_table_of_cat.xlsx'
path_to_write = working_space + curmonth + '/Install_Monthly_MoMChange_' + lastmonth + '.xlsx'

# 导入CAT all apps's lookup table, 从中得到待提取包名(和中英文名)
df_lt_cat_apps = pd.read_excel(path_lt_cat_apps, sheetname=0)
df_lt_cat_apps = df_lt_cat_apps[['Chinese Name', 'Package Name']]
#print df_lt_cat_apps.head()

wb = Workbook()
ws = wb.active
ws.title = lastmonth

# 先写入表头
ws.append(['Package Name', 'Chinese Name', 'Install Monthly MoM-Change',])
# 通过包名在数据库中查找指定应用, 得到所需数据
for pk in df_lt_cat_apps['Package Name'].unique():
    # 补上该应用的中文名
    try:
        cname_onepk = df_lt_cat_apps[df_lt_cat_apps[\
                'Package Name']==pk]['Chinese Name'].values[0]
    except Exception, e:
        print e, '\t', pk, '查找不到中文名...'
        cname_onepk = ''
    # 初始化待写入字符串的列表项
    list_minsmomc_onepk = [cname_onepk, pk,]

    # 从数据库提取上月の月安装环比
    try:
	doc = db.cat_data_201707.find_one({'PkName': pk})
	list_minsmomc_onepk.append(doc[lastmonth]['Install_Monthly_MoM-Change'])
    except Exception, e:
	print e, '\t', pk
    ws.append(list_minsmomc_onepk)

wb.save(path_to_write)

