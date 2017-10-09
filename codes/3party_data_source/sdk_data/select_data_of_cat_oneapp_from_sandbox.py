#coding:utf-8
import pandas as pd
import calendar
from openpyxl import Workbook

working_space = '/Users/Alas/Documents/TD/CAT/CAT工具表/Deal_With_CAT/monthlydata/3Party_DataSource/2017-05/data_sdk/'
path_data_sandbox = '/Users/Alas/Documents/TD/iOS_Rank_Estimated_Model_V1/jupyter_sandbox_data/get_daily_data_from_interface/'
path_to_write = working_space + 'active_android_apps_in_cat_and_sdk_oneapp.xlsx'


df_lt_apps_in_cat_and_sandbox = pd.read_csv(working_space + 'apps_in_cat_and_sdk_active_android_oneapp.txt')
list_pid = list(df_lt_apps_in_cat_and_sandbox['ProductId'].values)
#print df_lt_apps_in_cat_and_sandbox.head()
#print list_pid

def GetDaysTheMonth(onemonth):
    year, month = onemonth.split('-')
    days_of_month = calendar.monthrange(eval(year), eval(month))[1]
    return [year + '-' +  month + '-0' + str(i+1) for i in range(9)] +\
        [year + '-' + month + '-' + str(i) for i in range(10, days_of_month+1)]

list_month = ['2017-02', '2017-03', '2017-04', '2017-05',]
list_day_onemonth = GetDaysTheMonth(list_month[0])
#print list_day_onemonth

wb = Workbook()
ws = wb.active
ws.append(['ProductId', 'Chinese Name', 'date', 'active',])

for pid in list_pid:
#    pkname = df_lt_apps_in_cat_and_sandbox[df_lt_apps_in_cat_and_sandbox[\
#            'ProductId']==pid]['Package Name'].values[0]
    cname = df_lt_apps_in_cat_and_sandbox[df_lt_apps_in_cat_and_sandbox[\
            'ProductId']==pid]['Chinese Name'].values[0]
#    print pid, pkname, cname

    for month in list_month:
        list_day_onemonth = GetDaysTheMonth(month)
        for day in list_day_onemonth:
            df_active_android_oneday = pd.read_csv(path_data_sandbox + month + '/' +\
                    day + '_active_android.txt')
#            print df_active_android_oneday
            try:
                active_android = df_active_android_oneday[df_active_android_oneday[\
                        'productid']==pid]['active'].values[0]
            except Exception, e:
                print e, '\t', pid, cname, month, day
                active_android = ''

            #print pid, cname, day, active_android
            ws.append([pid, cname, day, active_android])
wb.save(path_to_write)





