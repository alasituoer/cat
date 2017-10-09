#coding:utf-8
import pandas as pd
import calendar
from openpyxl import Workbook

curmonth = '2017-07'
working_space = '/Users/Alas/Documents/TD/CAT/CAT工作表格/Deal_With_CAT/monthlydata/3Party_DataSource/' +\
        curmonth + '/'
path_estimated_newuser_with_rank = working_space +\
        'estimated_newuser/estimated_newuser_with_rank_from_model.csv'
path_rank_cat_apps = working_space + 'rank-aso100_' + curmonth + '/rank-aso100_' + curmonth + '.xlsx'
path_to_write = working_space + 'estimated_newuser/estimated_newuser_' + curmonth + '.xlsx'


# 输入模型得到的日活
df_dau_rank_model = pd.read_csv(path_estimated_newuser_with_rank)
df_dau_rank_model = df_dau_rank_model[['rank_total', 'predict_vector']]
# 以防万一将缺失值重置为'N/A'
df_dau_rank_model.fillna('N/A', inplace=True)
#print df_dau_rank_model
#print df_dau_rank_model[df_dau_rank_model['rank_total']==1]['predict_vector'].values[0]
#print df_dau_rank_model[:1].values[0][1]


# 如果工作簿中只有一张工作表, 那么读入的就是一个DataFrame结构数据
df_rank = pd.read_excel(path_rank_cat_apps, sheetname='top_free_rank_cat_apps')
df_rank.fillna(0, inplace=True)
#print df_rank.columns

# 更改列(索引)名
new_columns = []
for col in df_rank.columns:
    if '-' in str(col):
        new_columns.append(str(col)[:10])
    else:
        new_columns.append(col)
#print new_columns
df_rank.columns = new_columns

#print df_dau_rank_model
#print df_rank.head()


# 得到所有应用的中文名列表
list_app_name = list(df_rank['Chinese Name'].unique())
#print list_app_name

wb = Workbook()
ws = wb.active
# 写入表头
ws.append(['date'] + list_app_name)


# 按时间输出所有应用iOS排名对应的模型输出新增值
for date in new_columns[3:]:
    print date
    df_rank_allapps_onemonth = df_rank[[new_columns[0], new_columns[1], new_columns[2], date,]]
#    print df_rank_allapps_onemonth

    list_new_user_estimated = [date,]
    for rank in df_rank_allapps_onemonth[date]:
        #print rank, type(rank), int(rank), type(int(rank))
        try:
            new_user_onemonth_oneapp = df_dau_rank_model[df_dau_rank_model['rank_total']\
                    ==int(rank)]['predict_vector'].values[0]
            #print new_user_onemonth_oneapp
        except Exception, e:
#            print e
            new_user_onemonth_oneapp = 'N/A'
        list_new_user_estimated.append(new_user_onemonth_oneapp)
    ws.append(list_new_user_estimated)
wb.save(path_to_write)

