#coding:utf-8
from openpyxl import load_workbook, Workbook
from weeks_diff_months import weeks_diff_months

working_space = '/Users/Alas/Documents/TD/CAT/'

wb = Workbook()
ws = wb.active

for month in sorted(weeks_diff_months.keys()):
    #print [month[:4]+'-'+month[4:]] + [m[:4]+'-'+m[4:6]+'-'+m[6:]  for m in weeks_diff_months[month]]
    ws.append( [month[:4]+'-'+month[4:]] + [m[:4]+'-'+m[4:6]+'-'+m[6:]  for m in weeks_diff_months[month]])

wb.save(working_space + 'weeks_diff_months.xlsx')



