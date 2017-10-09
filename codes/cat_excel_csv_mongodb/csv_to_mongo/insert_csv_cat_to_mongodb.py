#coding:utf-8
import pandas as pd
from pymongo import MongoClient
# CAT中每个月有哪些周

Client = MongoClient()
db = Client.TalkingData

# 确定工作空间及需导入的月数据和周平均数据
# 再更改三处的数据库名 'cat_data_201707'
working_space = '/Users/Alas/Documents/TD_handover/CAT/excel工具表/2017-07/'
name_monthly_csv_file = 'CAT2017-07工具表Monthly.csv'
name_weekly_csv_file = 'CAT2017-07工具表Weekly.csv'

# 读入包名和英文名的对应关系
path_lookup_table_of_cat = '/Users/Alas/Documents/TD/CAT/lookup_table_of_cat.xlsx'
df_lt_cat_apps = pd.read_excel(path_lookup_table_of_cat, sheetname='lookup_table')
df_weeks_diff_months = pd.read_excel(path_lookup_table_of_cat, sheetname='weeks_diff_months', index_col=0)
df_weeks_diff_months.fillna('N/A', inplace=True)
#print df_lt_cat_apps.head()
#print df_weeks_diff_months.head()

# 读入各个月的每周时间段
weeks_diff_months = {}
for i in df_weeks_diff_months.index:
    list_weeks_onemonth = list(df_weeks_diff_months.ix[i].values)
    try:
        del list_weeks_onemonth[list_weeks_onemonth.index('N/A')]
    except Exception, e:
        pass
    #print list_weeks_onemonth
    weeks_diff_months[i] = list_weeks_onemonth
#print [weeks_diff_months[k] for k in sorted(weeks_diff_months.keys())]
#print weeks_diff_months


# 插入月份数据
# 先分应用插入基础描述信息(期间有根据英文名匹配出包名并添加进去), 再根据英文名匹配插入月份数据
def InsertMonthlyData(path_monthly_csv_file):
    df = pd.read_csv(path_monthly_csv_file)
    #print df[:10]
    #print df.columns
    # 对列名进行改造
    list_new_columns = []
    for col_name in df.columns:
	# 如果列名不属于时间, 则原封不动
	if '00:00:00' not in col_name:
	    list_new_columns.append(col_name)
	# 若是时间格式, 则将'2016-12-01 00:00:00'化为'2016-12-01'和'2016-12-26'的格式
	elif '00:00:00' in col_name:
	    list_new_columns.append(col_name[:7])
    df.columns = list_new_columns
    #print df[:10] 
    #print df.columns
    #print df['Category'].unique()
    #print df['Index'].unique()

    for cate in df['Category'].unique():
	df_category = df[df['Category']==cate]
	for app in df_category['Apps'].unique():
	    df_app = df_category[df_category['Apps']==app]

	    # 将该应用的基础信息(不包含数据的部分)整理成字典格式
	    # 对于一个APP, 取第一行数据的描述信息即可, 但是要去掉第一行固有的指标信息(它并不通用)
	    dict_app_desc_info = dict(zip(list(df_app.columns.values[:8]), list(df_app[:1].values[0][:8])))
	    del dict_app_desc_info['Index']
	    #print dict_app_desc_info
	    try:
		dict_app_desc_info['PkName'] =\
                        df_lt_cat_apps[df_lt_cat_apps['English Name']==app]['Package Name'].values[0]
		#print dict_app_desc_info, '\n'
	    except Exception, e:
		print e, '\t', cate, app
		exit(0)
	

	    # 给该应用新建一文档, 写入上述基本描述信息
	    db.cat_data_201707.insert(dict_app_desc_info)

	    # 再根据英文名匹配写入各个月份的数据
	    dict_app_month_data = {}
	    # 由于采用数据自带的日期截取的月份不能很好的与weeks_diff_months中的月份匹配
	    #for i in df_app.columns[8:]:
	    # 尽管看起来可能一样, 所以都从weeks_diff_months中读入月份格式

	    # 第8列以前是基础描述信息
	    for m in sorted(weeks_diff_months.keys()):
		dict_app_month_data[m] = dict(zip(df_app['Index'].values, df_app[m].values))
	    #print dict_app_month_data

	    db.cat_data_201707.update_many(
		    {'Apps': app},
		    {'$set': dict_app_month_data})


def InsertWeeklyData(path_weekly_csv_file):
    df = pd.read_csv(path_weekly_csv_file)

    # 对列名进行改造
    list_new_columns = []
    for col_name in df.columns:
	# 如果列名不属于时间, 则原封不动
	if '00:00:00' not in col_name:
	    list_new_columns.append(col_name)
	# 若是时间格式, 则将'2016-12-01 00:00:00'化为'201601'和'201610'的格式
	elif '00:00:00' in col_name:
	    list_new_columns.append(col_name[:10])
    df.columns = list_new_columns
    #print df[:10]
    #print df.columns
    
    for cate in df['Category'].unique():
	df_category = df[df['Category']==cate]
	for app in df_category['Apps'].unique():
	    df_app = df_category[df_category['Apps']==app]

	    # 再根据英文名匹配后 写入各个月份对应的周的数据
	    for m in sorted(weeks_diff_months.keys()):
		for w in weeks_diff_months[m]:
		    #print df_app[w]
		    dict_app_week_data = dict(zip(list(df_app['Index'].values), list(df_app[w].values)))
		    #print m, w
		    #print dict_app_week_data
		    #print '\n'

		    db.cat_data_201707.update_many(
			    {'Apps': app},
			    {'$set': {m+'.'+w : dict_app_week_data}})


# 至此, 每次更新新月份数据时
# 只需更改数据库集合名, adding_data.py 中的weeks date
# 和下列需导入的文件名即可

# 需要先插入月数据, 再插入周数据
#InsertMonthlyData(working_space + name_monthly_csv_file)
#InsertWeeklyData(working_space + name_weekly_csv_file)



