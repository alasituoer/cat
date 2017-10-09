#coding:utf-8
import sys
import MySQLdb
import pandas as pd
import calendar
from openpyxl import Workbook

# 该脚本仅做连接数据库后得到QQ在线人数

curmonth = '2017-08'
working_space = '/Users/Alas/Documents/TD_handover/' +\
        'CAT/data_source/monthlydata/第三方数据/' + curmonth + '/'

try:
    conn = MySQLdb.connect(
            host = '60.205.163.159',
            port = 3306,
            user = 'alas',
            passwd = '6143',
            db = 'thirdpart',
            charset = 'utf8',)
except Exception, e:
    print e
    sys.exit()


mysql = """
        select time, curpeople, realpeople, heightpeople
        from imqqt
        where time like '{0}%'"""
mysql = mysql.format(curmonth)
#print mysql

try:
    df_selected_data = pd.read_sql(mysql, conn)
except Exception, e:
    print e
#print df_selected_data

conn.close()

wb = Workbook()
ws = wb.active
ws.append(['Date', 'Cur_Time', 'Real_Time', 'Max_History',])

for i in df_selected_data.index:
    list_data = list(df_selected_data.ix[i])
    #print list_data
    ws.append([list_data[0]] + [int(j) for j in list_data[1:]])

path_to_write = working_space + 'QQ_Online_' + curmonth + '.xlsx'
wb.save(path_to_write)










