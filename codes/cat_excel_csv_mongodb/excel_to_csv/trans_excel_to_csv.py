#coding:utf-8
import os
import pandas as pd
from adding_data import GetIntListCellRange
from adding_data import dict_monthly_index, dict_weekly_index
from adding_data import list_category_full
# 换做直接从读入的文件中统计表数和表名
# 考虑到还需要指定表的顺序 还是不换了


# 每次仅需要修改下列三处(选择要提取数据的表, 文件位置, 文件名)
list_category = list_category_full #可以在adding_data.py中定义所需要的表
#list_category = ['App_Store']
path_excel_file = '/Users/Alas/Documents/TD_handover/CAT/excel工具表/2017-07/'
#name_excel_file = 'CAT2017-07工具表Monthly.xlsx'
name_excel_file = 'CAT2017-07工具表Weekly.xlsx'

# 参数含义：文件（数量一个，必须包含Monthly or Weekly）、行业列表（缺省代表所有行业）
# 行业内数据块索引列表（缺省代表某行业的所有数据块）
#def trans(excel_file, list_category, list_number_index):


# 根据文件名中是否包含 Monthly 或 Weekly 判断到底调用哪一个指标字典
if 'Monthly' in name_excel_file:
    dict_index = dict_monthly_index
elif 'Weekly' in name_excel_file:
    dict_index = dict_weekly_index
else:
    print 'the file format is error!'
    exit(0)
#print dict_index

# sheetname=None 返回Dict of DataFrame 可以通过df['App_Store']指定表格
# 同时header=1 设置以时间为表头
# 此处的逻辑是读取该文件的所有表数据, 然后根据指定的行业筛选所需的行业
df = pd.read_excel(path_excel_file + name_excel_file, sheetname=None, header=1)


# 可以选择只输出一个文件, 包含所有行业的数据块
df_all_categories = []

for c in list_category:
    # 初始化用于轴向连接的 DataFrame 列表 长度是: 15基本数据块*行业内应用等级数(目前最高是3)
    list_df = []
    for i in range(1, len(dict_index)+1):
	#print c
	#print GetIntListCellRange(c, i)
	# 不同的行业类别中的指标块可能有多个等级分类 [[3, 8], [10, 17], ...]
	for j in GetIntListCellRange(c, i):
	    df_t1 = df[c][(j[0]-3):(j[1]-2)]# 左闭右开区间 所以不是j[1]-3
	    # 新增两列表明该数据块属于哪个行业和哪个指标
	    df_t1['Category'] = c 
	    df_t1['Index'] = dict_index[i]
	    #print df_t1
	    # 并将这两列移至列头
	    cols = list(df_t1) 
	    cols.insert(0, cols.pop(cols.index('Index')))
	    cols.insert(0, cols.pop(cols.index('Category')))
	    df_t2 = df_t1.ix[:, cols]
	    #print df_t2
	    list_df.append(df_t2)
    #print pd.concat(list_df)
    # 将一个行业的DataFrame数据块轴向连接到一起
    df_category = pd.concat(list_df)


#     可以选择只输出一个文件, 包含所有行业的数据块
    df_all_categories.append(df_category)
#     将一个行业的数据输出到CSV文件
#    df_category.to_csv(path_created_data_dir2 + c + '.csv', index=False) 

# 可以选择只输出一个文件, 包含所有行业的数据块
pd.concat(df_all_categories).to_csv(path_excel_file + name_excel_file[:-5] + '.csv', index=False)




