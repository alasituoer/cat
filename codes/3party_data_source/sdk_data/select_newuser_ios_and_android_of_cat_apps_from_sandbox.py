#coding:utf-8
import pandas as pd
import calendar
from openpyxl import Workbook, load_workbook

def GetDaysTheMonth(curmonth):
    year, month = curmonth.split('-')
    days_of_month = calendar.monthrange(int(year), int(month))[1]
    return [year + '-' +  month + '-0' + str(i+1) for i in range(9)] +\
        [year + '-' + month + '-' + str(i) for i in range(10, days_of_month+1)]

curmonth = '2017-08'
list_days_curmonth = GetDaysTheMonth(curmonth)[:17]
working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/第三方数据/'
path_data_sandbox = working_space + 'sdk_data/'


# 从"Newuser_TD_SDK_Apps_2017-07.xlsx"中(最终结果也会写入该文件的其他表格中)
path_lt_apps_in_cat_and_sandbox = working_space + curmonth + '/sdk_data/Newuser_TD_SDK_Apps_' + curmonth + '.xlsx'
df_lt_apps_in_cat_and_sandbox =\
        pd.read_excel(path_lt_apps_in_cat_and_sandbox, sheetname=['list_android_apps', 'list_ios_apps',])
# 得到待提取的android and iOS apps ProductID列表
df_lt_android_apps = df_lt_apps_in_cat_and_sandbox['list_android_apps']
df_lt_ios_apps = df_lt_apps_in_cat_and_sandbox['list_ios_apps']
#print 'lookup table of android apps: ', df_lt_android_apps.head()
#print 'lookup table of ios apps', df_lt_ios_apps.head()


wb = load_workbook(path_lt_apps_in_cat_and_sandbox)
ws_android = wb['rawdata_newuser_android']
ws_ios = wb['rawdata_newuser_ios']

# 往表"rawdata_newuser_android"中写入当月"newuser_android"的数据
for pid in df_lt_android_apps['ProductId']:
    cname = df_lt_android_apps[df_lt_android_apps['ProductId']==pid]['Chinese Name'].values[0]
#    print pid, cname

    for day in list_days_curmonth:
        df_newuser_android_oneday = pd.read_csv(path_data_sandbox + curmonth + '/' +\
                    day + '_newuser_android.txt')
        try:
            newuser_android = df_newuser_android_oneday[\
                    df_newuser_android_oneday['productid']==pid]['newuser'].values[0]
        except Exception, e:
            print e, '\t', pid, cname, curmonth, day
            newuser_android = ''
#        print pid, cname, day, newuser_android
        ws_android.append([pid, cname, day, newuser_android])

# 往表"rawdata_newuser_ios"中写入当月"newuser_ios"的数据
for pid in df_lt_ios_apps['ProductId']:
    cname = df_lt_ios_apps[df_lt_ios_apps['ProductId']==pid]['Chinese Name'].values[0]
#    print pid, cname

    for day in list_days_curmonth:
        df_newuser_ios_oneday = pd.read_csv(path_data_sandbox + curmonth + '/' +\
                    day + '_newuser_iOS.txt')
        try:
            newuser_ios = df_newuser_ios_oneday[\
                    df_newuser_ios_oneday['productid']==pid]['newuser'].values[0]
        except Exception, e:
            print e, '\t', pid, cname, curmonth, day
            newuser_ios = ''
#        print pid, cname, day, newuser_ios
        ws_ios.append([pid, cname, day, newuser_ios])

wb.save(path_lt_apps_in_cat_and_sandbox)


