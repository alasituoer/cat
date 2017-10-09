#coding:utf-8
import pandas as pd
import calendar
from openpyxl import load_workbook

# 先保证get_daily_data_from_interface中准备好需要提取的沙箱数据

def GetDaysTheMonth(curmonth):
    year, month = curmonth.split('-')
    days_of_month = calendar.monthrange(int(year), int(month))[1]
    return [year + '-' +  month + '-0' + str(i+1) for i in range(9)] +\
        [year + '-' + month + '-' + str(i) for i in range(10, days_of_month+1)]

curmonth = '2017-08'
list_days_curmonth = GetDaysTheMonth(curmonth)[:17]
#print list_days_curmonth

working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/第三方数据/'
path_data_sandbox = working_space + 'sdk_data/'


# 从"TD_SDK_Apps_2017-06.xlsx"中(最终结果也会写入该文件的其他表格中)
path_lt_apps_in_cat_and_sandbox = working_space + curmonth + '/sdk_data/Active_TD_SDK_Apps_' + curmonth + '.xlsx'
df_lt_apps_in_cat_and_sandbox =\
        pd.read_excel(path_lt_apps_in_cat_and_sandbox, sheetname=['list_android_apps', 'list_ios_apps',])
#print df_lt_apps_in_cat_and_sandbox

# 得到待提取的android and iOS apps ProductID列表
df_lt_android_apps = df_lt_apps_in_cat_and_sandbox['list_android_apps']
df_lt_ios_apps = df_lt_apps_in_cat_and_sandbox['list_ios_apps']
#print df_lt_android_apps.head()
#print df_lt_ios_apps.head()

wb = load_workbook(path_lt_apps_in_cat_and_sandbox)
ws_android = wb['rawdata_android_apps']
ws_ios = wb['rawdata_ios_apps']

# 往表"rawdata_android_apps"中写入当月"active_android"的数据
for pid in df_lt_android_apps['ProductId']:
    cname = df_lt_android_apps[df_lt_android_apps['ProductId']==pid]['Chinese Name'].values[0]
#    print pid, cname

    for day in list_days_curmonth:
        df_active_android_oneday = pd.read_csv(path_data_sandbox + curmonth + '/' +\
                    day + '_active_android.txt')
        try:
            active_android = df_active_android_oneday[\
                    df_active_android_oneday['productid']==pid]['active'].values[0]
        except Exception, e:
            print e, '\t', pid, cname, curmonth, day
            active_android = ''
#        print pid, cname, day, active_android
        ws_android.append([pid, cname, day, active_android])

# 往表"rawdata_ios_apps"中写入当月"active_ios"的数据
for pid in df_lt_ios_apps['ProductId']:
    cname = df_lt_ios_apps[df_lt_ios_apps['ProductId']==pid]['Chinese Name'].values[0]
#    print pid, cname

    for day in list_days_curmonth:
        df_active_ios_oneday = pd.read_csv(path_data_sandbox + curmonth + '/' +\
                    day + '_active_iOS.txt')
        try:
            active_ios = df_active_ios_oneday[\
                    df_active_ios_oneday['productid']==pid]['active'].values[0]
        except Exception, e:
            print e, '\t', pid, cname, curmonth, day
            active_ios = ''
#        print pid, cname, day, active_ios
        ws_ios.append([pid, cname, day, active_ios])

# 存入进Excel的日期格式为'2017-08-01'和'2017-08-15', 需要替换'-0'为'/', '-'为'/'
wb.save(path_lt_apps_in_cat_and_sandbox)


