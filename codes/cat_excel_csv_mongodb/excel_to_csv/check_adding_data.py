#coding:utf-8
from adding_data import list_category_full, china_app_tracker, GetIntListCellRange

#print list_category_full, '\n'
#print GetIntListCellRange('Social_IM', 1)

sum_apps = 0
for cate in china_app_tracker.keys():
    sum_apps += sum(china_app_tracker[cate]['app_nums_diff_grade'])
print 'CAT所有应用数:', sum_apps
