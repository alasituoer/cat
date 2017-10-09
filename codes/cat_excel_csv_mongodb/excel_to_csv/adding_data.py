#coding:utf-8

# 数据块序号及数据块名(依据月和周文件的不同而划分的)
dict_monthly_index = {
	1:'Install_Monthly', 2:'Install_Monthly_MoM-Change', 3:'Install_Monthly_YoY-Change', 
	4:'MAU', 5:'MAU_MoM-Change', 6:'MAU_YoY-Change', 
	7:'DAU_Monthly', 8:'DAU_Monthly_MoM-Change', 9:'DAU_Monthly_YoY-Change', 
	10:'Install_Monthly_Rate', 11:'MAU_Rate', 12:'DAU_Monthly_Rate', 
	13:'Avg_Sessions_Monthly', 14:'Avg_Sessions_Monthly_MoM-Change', 15:'Engagement_Monthly',}
dict_weekly_index = {
	1:'Install_Weekly', 2:'Install_Weekly_WoW-Change', 3:'DAU_Weekly', 4:'DAU_Weekly_WoW-Change', 
	5:'Install_Weekly_Rate', 6:'DAU_Weekly_Rate', 7:'Engagement_Weekly',}

# 可以在此处自定义所需要的表

# 客户之一的TDThirdpoint 订阅的行业
#list_category_TDThirdpoint = [
#	'App_Store', 'E-Commerce', 'Social_IM', 'Travel', 'Search', 'Browser', 'Auto', 
#	'Taxi', 'Video', 'News', 'Education', 'Lifestyle', 'Real_Estate', 
#	'Women_Health_and_Fashion', 'Camera', 'Baidu', 'Alibaba', 'Tencent',]

# 全行业版本的名称列表
list_category_full = [
	'App_Store', 'E-Commerce', 'Cross_Border_E-Commerce', 'Social_IM', 'Travel',
	'Search', 'Browser', 'Auto', 'Used_Car', 'Taxi', 'Video', 'News', 'Education',
	'Lifestyle', 'Real_Estate', 'Women_Health_and_Fashion', 'Food_Delivery', 
	'Business_Apps', 'Camera', 'Flight', 'Map', 'Recruitment', 'Mobile_Security',
	'Efficiency', 'Healthcare', 'Finance', 'Music', 'Baidu', 'Alibaba', 'Tencent', 
	'Cheetah', 'Qihoo',]

# 返回指定行业、指定数据块的开始和终止行序号列表
# 如输入 (App_Store, 1) 返回 [[7, 19], [21, 28]]

# 根据lookup_table_of_cat.xlsx中的Category表同步更新
china_app_tracker = {'App_Store': {'num_type_asd': 2, 'app_nums_diff_grade': [13, 8,],}, 
		     'E-Commerce': {'num_type_asd': 1, 'app_nums_diff_grade': [9, 18, 4,],}, 
		     'Cross_Border_E-Commerce': {'num_type_asd': 1, 'app_nums_diff_grade': [3,],},
		     'Social_IM': {'num_type_asd': 1, 'app_nums_diff_grade': [31,],},
		     'Travel': {'num_type_asd': 1, 'app_nums_diff_grade': [6, 8],},
		     'Search': {'num_type_asd': 2, 'app_nums_diff_grade': [6,],},
		     'Browser': {'num_type_asd': 2, 'app_nums_diff_grade': [7,],},
		     'Auto': {'num_type_asd': 1, 'app_nums_diff_grade': [3, 4, 2],},
		     'Used_Car': {'num_type_asd': 1, 'app_nums_diff_grade': [7,],},
		     'Taxi': {'num_type_asd': 1, 'app_nums_diff_grade': [12, 4,],},
		     'Video': {'num_type_asd': 1, 'app_nums_diff_grade': [5, 14],},
		     'News': {'num_type_asd': 1, 'app_nums_diff_grade': [8,],},
		     'Education': {'num_type_asd': 1, 'app_nums_diff_grade': [11,],},
		     'Lifestyle': {'num_type_asd': 1, 'app_nums_diff_grade': [6,],},
		     'Real_Estate': {'num_type_asd': 1, 'app_nums_diff_grade': [21,],},
		     'Women_Health_and_Fashion': {'num_type_asd': 1, 'app_nums_diff_grade': [7,],},
		     'Food_Delivery': {'num_type_asd': 1, 'app_nums_diff_grade': [5,],},
		     'Business_Apps': {'num_type_asd': 1, 'app_nums_diff_grade': [2,],},
		     'Camera': {'num_type_asd': 1, 'app_nums_diff_grade': [15,],},
		     'Flight': {'num_type_asd': 1, 'app_nums_diff_grade': [6,],},
		     'Map': {'num_type_asd': 1, 'app_nums_diff_grade': [6,],},
		     'Recruitment': {'num_type_asd': 1, 'app_nums_diff_grade': [3,],},
		     'Mobile_Security': {'num_type_asd': 2, 'app_nums_diff_grade': [6,],},
		     'Efficiency': {'num_type_asd': 1, 'app_nums_diff_grade': [2,],},
		     'Healthcare': {'num_type_asd': 1, 'app_nums_diff_grade': [10,],},
		     'Finance': {'num_type_asd': 1, 'app_nums_diff_grade': [4,],},
		     'Music': {'num_type_asd': 1, 'app_nums_diff_grade': [5,],},
		     'Baidu': {'num_type_asd': 2, 'app_nums_diff_grade': [26,],},
		     'Alibaba': {'num_type_asd': 2, 'app_nums_diff_grade': [25,],},
		     'Tencent': {'num_type_asd': 2, 'app_nums_diff_grade': [37,],},
		     'Cheetah': {'num_type_asd': 2, 'app_nums_diff_grade': [6,],},
		     'Qihoo': {'num_type_asd': 2, 'app_nums_diff_grade': [5,],},}

# 返回指定行业、指定数据块的开始和终止行序号列表
# 如输入 (App_Store, 1) 返回 [[7, 19], [21, 28]]
# 如输入 (App_Store, 4) 返回 [[82, 94], [96, 103]]
# 月文件与周文件已经调整得格式一致
# 因此for Number_Index in [1,2,3,4,5,6,7]:
# 月文件与周文件中相同的行业对应的返回序号是一样的
def GetIntListCellRange(Category, Number_Index):
    #print Category,
    #print china_app_tracker[Category]['num_type_asd'],
    #print china_app_tracker[Category]['app_nums_diff_grade']
    
    # 表头起始位置 + ASD个数 + Installations 1栏 + 有没有等级划分(有加2没有加1)
    # 得到需截取的起始单元格坐标
    if len(china_app_tracker[Category]['app_nums_diff_grade']) > 1:
	start_range_cell = 2 + china_app_tracker[Category]['num_type_asd'] + 1 + 2
    else:
	start_range_cell = 2 + china_app_tracker[Category]['num_type_asd'] + 1 + 1
    end_range_cell =  start_range_cell + \
	    sum(china_app_tracker[Category]['app_nums_diff_grade']) + \
	    len(china_app_tracker[Category]['app_nums_diff_grade']) - 1 - 1

    index_list = []
    if Number_Index == 1:
	if len(china_app_tracker[Category]['app_nums_diff_grade']) == 2:
	    sep_value = start_range_cell + china_app_tracker[Category]['app_nums_diff_grade'][0]
	    index_list.append([start_range_cell, sep_value-1])
	    index_list.append([sep_value+1, end_range_cell])
	elif len(china_app_tracker[Category]['app_nums_diff_grade']) == 3:
	    sep1_value = start_range_cell + china_app_tracker[Category]['app_nums_diff_grade'][0]
	    index_list.append([start_range_cell, sep1_value-1])
	    sep2_value = end_range_cell - china_app_tracker[Category]['app_nums_diff_grade'][2]
	    index_list.append([sep1_value+1, sep2_value-1])
	    index_list.append([sep2_value+1, end_range_cell])
	else:
	    index_list.append([start_range_cell, end_range_cell])
	return index_list

    for i in range(2, 16):
	if len(china_app_tracker[Category]['app_nums_diff_grade']) > 1:
	    start_range_cell = end_range_cell + 4
	else:
	    start_range_cell = end_range_cell + 3
	end_range_cell =  start_range_cell + \
		sum(china_app_tracker[Category]['app_nums_diff_grade']) + \
		len(china_app_tracker[Category]['app_nums_diff_grade']) - 1 - 1

	if Number_Index == i:
	    if len(china_app_tracker[Category]['app_nums_diff_grade']) == 2:
		sep_value = start_range_cell + china_app_tracker[Category]['app_nums_diff_grade'][0]
		index_list.append([start_range_cell, sep_value-1])
		index_list.append([sep_value+1, end_range_cell])
	    elif len(china_app_tracker[Category]['app_nums_diff_grade']) == 3:
		sep1_value = start_range_cell + china_app_tracker[Category]['app_nums_diff_grade'][0]
		index_list.append([start_range_cell, sep1_value-1])
		sep2_value = end_range_cell - china_app_tracker[Category]['app_nums_diff_grade'][2]
		index_list.append([sep1_value+1, sep2_value-1])
		index_list.append([sep2_value+1, end_range_cell])
	    else:
		index_list.append([start_range_cell, end_range_cell])
	    return index_list



