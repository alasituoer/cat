#coding:utf-8
import pandas as pd
import numpy as np
import csv
from pymongo import MongoClient
from adding_data import ASD_Monthly, AASD_Monthly, AppsPackageNameForAASD
from adding_data import ASD_Weekly, AASD_Weekly
from adding_data import LastMonth, CurMonth
from adding_data import LastWeek, CurWeek

# 连接本地的mongodb数据库(存放有历史CATExcel内的所有指标数据)
client = MongoClient()
db = client.TalkingData

# 计算CAT所需的原始月和周数据路径
working_space = '/Users/Alas/Documents/TD_handover/CAT/'


# 读入CAT所有应用的对照表
df_cat_apps = pd.read_excel(working_space + 'header/lookup_table_of_cat.xlsx', sheetname=0)
#print 'CAT中所有应用的对照表'
#print df_cat_apps.head()


# 得到 TDC 中上期和本期的月安装比例
path_MInsPR_TDC_file = working_space + 'data_source/monthlydata/MInsPR_to_MInsMoMC_TDC/MInsPR_TDC/' +\
        CurMonth + '/MInsPR_TDC_' + CurMonth + '.csv'
df_MInsPR_TDC = pd.read_csv(path_MInsPR_TDC_file, index_col=0)
df_MInsPR_TDC = df_MInsPR_TDC.drop_duplicates(['Chinese Name'])
#print len(df_MInsPR_TDC), len(df_MInsPR_TDC)
#print 'CAT应用在Ranking Center中本期和上期的安装比例: '
#print df_MInsPR_TDC.head()

ss_MInsPR_LM_TDC = df_MInsPR_TDC[LastMonth]
ss_MInsPR_CM_TDC = df_MInsPR_TDC[CurMonth]
# 计算本期较上期的变化量
ss_MInsPR_Diff_TDC = ss_MInsPR_CM_TDC - ss_MInsPR_LM_TDC
#print ss_MInsPR_Diff_TDC.ix['com.cheyipai.ui']
#print '利用raning center中的比例和大盘计算的TDC环比: ', 
#print ss_MInsPR_Diff_TDC[:10], type(ss_MInsPR_Diff_TDC[:10])


# 读入本期所需的4周或5周周均install rate and active rate
# 此数据在结束调数文件后应准备好
path_avg_install_and_active_rate_from_rc_curmonth =\
        working_space + 'data_source/weeklydata/' + CurMonth + '/' +\
        'avg_install_and_active_rate_from_rc_' + CurMonth + '.txt'
df_allapps_avg_DInsPR_DAUPR = pd.read_csv(path_avg_install_and_active_rate_from_rc_curmonth, header=None)
# 根据读取到的数据列数来反推本期的周数, 得以自定义字段名(也会对后面是否对第五周进行调整起到判断作用)
number_weeks = len(df_allapps_avg_DInsPR_DAUPR.columns)-2
df_allapps_avg_DInsPR_DAUPR.columns = ['pkname', 'rate_type',] + ['week' + str(i+1) for i in range(number_weeks)]
#print '本期所有应用的周均日安装比例和周均日活跃比例: '
#print df_allapps_avg_DInsPR_DAUPR[:10]


 #到底是挨个应用调整, 还是整体利用pandas DataFrame一起调?

# 新建CSV文件 存储调整后的 月安装比例 & 月活跃比例, 周均日安装比例, 周均日活跃比例
writer_MInsPR = csv.writer(file(working_space + 'results/' + CurMonth + '/MInsPR_Revised_' + CurMonth + '.csv', 'wb'))
writer_MAUPR = csv.writer(file(working_space + 'results/' + CurMonth + '/MAUPR_Revised_' + CurMonth + '.csv', 'wb'))
writer_ADAU = csv.writer(file(working_space + 'results/' + CurMonth + '/ADAU_Revised_' + CurMonth + '.csv', 'wb'))
#writer_ADAUPR = csv.writer(file('../results/' + CurMonth + '/ADAUPR_Revised_' + CurMonth + '.csv', 'wb'))
writer_DInsPR = csv.writer(file(working_space + 'results/' + CurMonth + '/DInsPR_Revised_' + CurMonth + '.csv', 'wb'))
writer_DAUPR = csv.writer(file(working_space + 'results/' + CurMonth + '/DAUPR_Revised_' + CurMonth + '.csv', 'wb'))

writer_MInsPR.writerow(['PkName', 'MInsPR',])
writer_MAUPR.writerow(['PkName', 'MAUPR',])
writer_ADAU.writerow(['PkName', 'ADAU',])
#writer_ADAUPR.writerow(['PkName', 'ADAUPR',])
writer_DInsPR.writerow(['PkName', 'DInsPR',])
writer_DAUPR.writerow(['PkName', 'DAUPR',])


# 读入修正后的月安装量环比 MInsMoMC_TDC_Revised
# 即是由调数文件调整后最终得到的数
# 先读入该文件是为了以待计算的应用的包名作为索引好从数据库提数
path_MInsMoMC_TDC_Revised_file =\
        working_space + 'data_source/monthlydata/MInsPR_to_MInsMoMC_TDC/MInsMoMC_TDC_Revised/' +\
        CurMonth + '/MInsMoMC_TDC_' + CurMonth + '_Revised' + '.csv'
# 读入时不指定行索引, 是要根据每行的包名进行去重
# 去重后的DataFrame直接指定某列(此处是"PkName")作为行索引 df.set_index=('PkName')
df_MInsMoMC_TDC_Revised = pd.read_csv(path_MInsMoMC_TDC_Revised_file)#, index_col=0)
df_MInsMoMC_TDC_Revised = df_MInsMoMC_TDC_Revised.drop_duplicates(['Package Name'])
df_MInsMoMC_TDC_Revised = df_MInsMoMC_TDC_Revised.set_index('Package Name')
#print len(df_MInsMoMC_TDC_Revised)
#print df_MInsMoMC_TDC_Revised.head()



# 得到上月的交付版的(此处为了强调非TDC的)月安装比例 MInsPR_LM
# 一个一个APP来监测和调整
#for pk in df_MInsMoMC_TDC_Revised.index:
for pk in ['com.tencent.mm', 'com.tencent.mobileqq', 'com.jingdong.app.mall', 'com.qiyi.video']:
    # 根据包名得到某一应用在上月的月安装比例
    try:
	doc = db.cat_data_201706.find_one({'PkName': pk})
	#print doc.keys()
	app_MInsPR_LM_CAT = float(doc[LastMonth]['Install_Monthly_Rate'])
	#print app_MInsPR_LM_CAT, '\n'
    except Exception, e:
	print e, '\t', pk, "there is an error in selecting data from mongodb where key = 'Install_Monthly_Rate' "
	continue

    # 加上 TDC 中该应用的月安装比例变化量
    # 得到 本期该应用还未经确定的 月安装比例
    # 根据包名选择记录可能不唯一, 判断一下如果出错就换一种提数方式
    #app_MInsPR_CM_CAT = app_MInsPR_LM_CAT + ss_MInsPR_Diff_TDC.ix[pk].values[0]
    app_MInsPR_CM_CAT = app_MInsPR_LM_CAT + ss_MInsPR_Diff_TDC.ix[pk]
    #print app_MInsPR_CM_CAT


    # 接着根据上面得到的月安装比例, 乘以上期跟本期的大盘数得到 上期和本期的 月安装量
    # 同时判断一下, 如果应用是安卓独有的就乘以安卓大盘, 其他的乘以全量大盘
    if pk in AppsPackageNameForAASD:
	app_MIns_LM = app_MInsPR_LM_CAT * AASD_Monthly[-2]
	app_MIns_CM = app_MInsPR_CM_CAT * AASD_Monthly[-1]
    else:
	app_MIns_LM = app_MInsPR_LM_CAT * ASD_Monthly[-2]
	app_MIns_CM = app_MInsPR_CM_CAT * ASD_Monthly[-1]

    # 得到 通过增量计算的本期较上期的月安装量环比app_MInsMoMC
    # 与经过人工核对的环比MInsMoMC_TDC_Revised做比较
    # 如果 与 TDC 中计算得到的月安装环比相差很大, 就采用 MInsMoMC_TDC_Revised
    app_MInsMoMC = app_MIns_CM/app_MIns_LM - 1
    #print 'app_MInsMomC:\t\t', app_MInsMoMC

    # 利用判断方式处理重复值, 如果出错说明该值是不重复的
    #app_MInsMoMC_TDC_Revised = df_MInsMoMC_TDC_Revised['MInsMoMC_TDC'].ix[pk].values[0]
    app_MInsMoMC_TDC_Revised = df_MInsMoMC_TDC_Revised['MInsMoMC_TDC'].ix[pk]
    #print app_MInsMoMC_TDC_Revised
    mutiple_MInsMoMC = app_MInsMoMC_TDC_Revised/app_MInsMoMC

    momc_mins = app_MInsMoMC_TDC_Revised/app_MInsMoMC - 1
    #print 'app_MInsMoMC:', app_MInsMoMC
    #print 'app_MInsMoMC_TDC_Revised:', app_MInsMoMC_TDC_Revised
#    print 'mutiple_MInsMomC:\t\t', mutiple_MInsMoMC
    #print 'momc_mins:\t\t', momc_mins

    # 输出调数前后的 月安装比例, 查看是否有变化和如何变化
    #print 'app_MInsPR_CM_CAT:', app_MInsPR_CM_CAT,





    # 如果这两个月安装环比的误差在20%以上, 则进行调整, 即采用经人工核对过的数据
    if momc_mins > 0.2 or momc_mins < -0.2:
	app_MInsMoMC = app_MInsMoMC_TDC_Revised

	# 确定环比无误后, 反推本期月安装量
	app_MIns_CM = (app_MInsMoMC + 1) * app_MIns_LM
#	print 'app_MInsMoMC:\t\t', app_MInsMoMC
#	print 'app_MIns_CM:\t\t', app_MIns_CM
#	print 'app_MIns_LM:\t\t', app_MIns_LM, '\n'

	# 根据得到的 本期の月安装量, 计算得到 本期の月安装比例
	# 涉及到大盘时都得判断一下该应用是否是安卓独有没有iOS版本
	if pk in AppsPackageNameForAASD:
	    app_MInsPR_CM_CAT = app_MIns_CM/AASD_Monthly[-1]
	else:
	    app_MInsPR_CM_CAT = app_MIns_CM/ASD_Monthly[-1]
    # 至此得到经过调整的月安装比例
    #print 'app_MInsPR_CM_CAT:', app_MInsPR_CM_CAT




    # 接着读入某应用的历史月活跃安装比, 计算本期的月活跃安装比
    his_month = ['2016-12', '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', LastMonth,]
    MEngagement_his_month = []
    for m in his_month:
	# 如果提取不到某个月的数据，避免出错
	try:
	    MEngagement_his_month.append(float(doc[m]['Engagement_Monthly']))
	except Exception, e:
            print e
	    continue
    #print MEngagement_his_month
    # 经过线性或者平均计算得到 本期の月活跃安装比(此处还缺一算法)
    # 平均计算不是长久之计但是随意调整也不符合规范，或许整个调数机制改变后才会比现在更加合理
    app_MEngagement_CM = sum(MEngagement_his_month)/len(MEngagement_his_month)
    #print app_MEngagement_CM

    # 计算得到 本期の月活跃比例 = 本期の月安装比例 * 本期の月活跃安装比
    app_MAUPR_CM = app_MInsPR_CM_CAT * app_MEngagement_CM
    #print app_MInsPR_CM_CAT, app_MAUPR_CM, '\n'
    # 同时得到 本期の月活跃量 = 本期の月活跃比例 * 大盘数据(ASD_Monthly or AASD_Monthly)
    if pk in AppsPackageNameForAASD:
	app_MAU_CM = app_MAUPR_CM * AASD_Monthly[-1]
    else:
	app_MAU_CM = app_MAUPR_CM * ASD_Monthly[-1]
    #print app_MAU_CM, '\n'



    # 开始调整本期的月活跃比例
    # 提取上期の月活跃量
    app_MAU_LM = float(doc[LastMonth]['MAU'])
    # 计算得到 本期の月活跃量环比 = 本期の月活跃量 / 上期の月活跃量 - 1
    app_MAUMoMC = app_MAU_CM/app_MAU_LM - 1
    #print 'app_MAU_LM:', app_MAU_LM
    #print 'app_MAU_CM:', app_MAU_CM
    #print 'app_MAUMoMC', app_MAUMoMC

    #print 'app_MAUPR_CM:', app_MAUPR_CM,
    # 如果 本期の月活跃环比 对比 本期の月安装环比 相差太大, 
    momc_mau = app_MInsMoMC/app_MAUMoMC-1
    if momc_mau > 0.2 or momc_mau < -0.2 :
	# 则调整 本月の月活跃环比到, 比安装环比低, 随机在0.7 ~ 1倍间
	app_MAUMoMC = np.random.uniform(app_MInsMoMC*0.7, app_MInsMoMC)
	# 调整好本期的月活跃环比后计算本期的月活跃量
	app_MAU_CM = (app_MAUMoMC + 1) * app_MAU_LM
	# 进而计算本期的月活跃比例
	if pk in AppsPackageNameForAASD:
	    app_MAUPR_CM = app_MAU_CM/AASD_Monthly[-1]
	else:
	    app_MAUPR_CM = app_MAU_CM/ASD_Monthly[-1]
    #print 'app_MAUPR_CM:', app_MAUPR_CM
    #print '\n'
    #print 'app_MInsMoMC', app_MInsMoMC
    #print 'momc_mau', momc_mau
    #print 'app_MAUMoMC', app_MAUMoMC
    #print 'app_MAU_CM', app_MAU_CM, '\n'

    # 将每一款应用 本期の月安装比例 导入到CSV文件
    lines_MInsPR = [[pk, app_MInsPR_CM_CAT,]]
    for line in lines_MInsPR:
	writer_MInsPR.writerow(line)
    # 将每一款应用 本期の月活跃比例 导入到CSV文件
    lines_MAUPR = [[pk, app_MAUPR_CM,]]
    for line in lines_MAUPR:
	writer_MAUPR.writerow(line)



# 开始处理周文件数据, 所需的上期及本期 日安装比例和日活跃比例 已在脚本开头导入
    try:
        df_oneapp_DInsPR_DAUPR = df_allapps_avg_DInsPR_DAUPR[df_allapps_avg_DInsPR_DAUPR['pkname']==pk]
    except Exception, e:
        print e, '\t', pk
    df_oneapp_DInsPR = df_oneapp_DInsPR_DAUPR[df_oneapp_DInsPR_DAUPR['rate_type']=='install_rate']
    df_oneapp_DAUPR = df_oneapp_DInsPR_DAUPR[df_oneapp_DInsPR_DAUPR['rate_type']=='active_rate']
    #print df_oneapp_DInsPR.values
    #print df_oneapp_DAUPR.values

    try:
        app_DInsPR_list = list(df_oneapp_DInsPR.values[0])[2:]
        app_DAUPR_list = list(df_oneapp_DAUPR.values[0])[2:]
    except Exception, e:
        print e, '\t', pk
    #print 'DInsPR_list:', app_DInsPR_list
    #print 'DAUPR_list:', app_DAUPR_list
    #print '\n'
	

    # 计算得到本期的 每周の日安装量和日活跃量 
    if pk in AppsPackageNameForAASD:
	app_DIns_list = [i*j for i, j in zip(AASD_Weekly, app_DInsPR_list)]
	app_DAU_list = [i*j for i, j in zip(AASD_Weekly, app_DAUPR_list)]
    else:
	app_DIns_list = [i*j for i, j in zip(ASD_Weekly, app_DInsPR_list)]
	app_DAU_list = [i*j for i, j in zip(ASD_Weekly, app_DAUPR_list)]
    #print 'DIns_list:', app_DIns_list
    #print 'DAU_list:', app_DAU_list


    # 由上面的 本期の日均活跃量 计算得到 本期の月均日活跃量
    app_ADAU_CM = np.mean(app_DAU_list)
#    print app_ADAU_CM
    # 根据包名从数据库提取 上期の月均日活跃量
    try:
	app_ADAU_LM = float(doc[LastMonth]['DAU_Monthly'])
    except Exception, e:
	print pk, '\t\t', e
	continue
    # 计算得到 本期の月均日活跃量环比
    app_ADAUMoMC = app_ADAU_CM/app_ADAU_LM - 1


    # 如果 本期の月均日活跃量环比 较 本期の月活跃环比 (如，正负、幅度等)相差太大, 则进行调整
    if (app_ADAUMoMC/app_MAUMoMC>2) or (app_ADAUMoMC/app_MAUMoMC<0):
	app_ADAUMoMC = np.random.uniform(app_MAUMoMC*0.5, app_MAUMoMC*1.5)
    # 根据调整后的 本期の月均日活跃量环比 计算得到 本期の月均日活跃量
    app_ADAU_CM_Revised = (app_ADAUMoMC + 1) * app_ADAU_LM 
#    print app_ADAU_CM_Revised
    momc_adau = app_ADAU_CM_Revised / app_ADAU_CM
    app_ADAU_CM = app_ADAU_CM_Revised
    #print 'ADAU_CurMonth_Revised:', app_ADAU_CM

    # 根据调整后的 本期の月均日活跃量, 得到 本期の日均活跃量列表
    app_DAU_list = [i*momc_adau for i in app_DAU_list]

    # 根据 本期の日均活跃量列表, 结合上期最后一周的の日均活跃量
    try:
	app_DIns_LM_LW = float(doc[LastMonth][LastWeek[-1]]['Install_Weekly'])
	app_DAU_LM_LW = float(doc[LastMonth][LastWeek[-1]]['DAU_Weekly'])
	app_DInsWoWC_LM_LW = float(doc[LastMonth][LastWeek[-1]]['Install_Weekly_WoW-Change'])
	app_DAUWoWC_LM_LW = float(doc[LastMonth][LastWeek[-1]]['DAU_Weekly_WoW-Change'])
    except:
	print 'error', pk, "doc[LastMonth]['WeeklyDataX']", '\n'
	continue
    #print LastMonth, LastWeek[-1], 'DInsWoWC_LM_LW', app_DInsWoWC_LM_LW
    #print LastMonth, LastWeek[-1], 'DAUWoWC_LM_LW', app_DAUWoWC_LM_LW
    #print CurMonth, CurWeek[0], 'DInsWoWC_CM_FW', momc_dins_fw
    #print CurMonth, CurWeek[0], 'DAUWoWC_CM_FW', momc_dau_fw
    try:
        momc_dins_fw = app_DIns_list[0]/app_DIns_LM_LW - 1
        momc_dau_fw = app_DAU_list[0]/app_DAU_LM_LW - 1
    except:
	print 'error', pk, "doc[LastMonth]['WeeklyDataX']", '\n'
	continue

#    print 'app_dau_list:\t\t', np.mean(app_DAU_list), app_DAU_list, momc_dau_fw



    # 如果 本期の第一周の日均活跃量环比 （暂定为）变化量超过20%, 则进行调整 
    # 结果证明, 虽然还不能达到最理想的状态
    if momc_dau_fw > 0.15 or momc_dau_fw < -0.15:
	# 本期第一周の日均活跃量环比 幅度在上周与下周的环比间随机, 正负不变 
	momc_dau_fw = momc_dau_fw/abs(momc_dau_fw) * np.random.uniform(abs(app_DAUWoWC_LM_LW)*0.9, 1.05*abs(app_DAUWoWC_LM_LW))
	#print 'DAU_WoWC_CurMonth_FirstWeek_Revised:\t', momc_dau_fw

    # 对于某些行业特别指定第一周的升降(或者某一周的升降)
#    if df_cat_apps[df_cat_apps['Apps Package Name']==pk]['Apps Category'].values[0] in ['Social_IM', 'Video',]:
	#momc_dau_fw = momc_dau_fw/abs(momc_dau_fw) * np.random.uniform(abs(app_DAUWoWC_LM_LW), 1.02*abs(app_DAUWoWC_LM_LW))
#	momc_dau_fw = abs(momc_dau_fw)
#    if df_cat_apps[df_cat_apps['Apps Package Name']==pk]['Apps Category'].values[0] in ['Education', 'Recruitment', 'Food_Delivery', 'Real_Estate', 'Lifestyle', 'Taxi', 'Used_Car',]:
	#momc_dau_fw = momc_dau_fw/abs(momc_dau_fw) * np.random.uniform(abs(app_DAUWoWC_LM_LW)*0.9, abs(app_DAUWoWC_LM_LW))
#	momc_dau_fw = -1 * abs(momc_dau_fw)

    # 初始化改进后的日均活跃量
    app_DAU_list_revised = app_DAU_list
    # 暂存本期第一周的日均活跃量环比
    app_DAU_list_fw_revised = app_DAU_LM_LW * (1 + momc_dau_fw)
    # 调整好第一周的数据后, 存入新列表
    app_DAU_list_revised[0] = app_DAU_list_fw_revised

    # 初步调整整体的水平, 可能调整好第一周后整体水平就没问题, 及时由于只有第一周的数据有问题
    momc_dau_1 = app_DAU_list[1]/app_DAU_list[0]-1
    if momc_dau_1 > 0.15 or momc_dau_1 < -0.15:
	avg_index = app_DAU_list[0]/(sum(app_DAU_list[1:])/len(app_DAU_list[1:]))
	for i in range(len(app_DAU_list)-1):
	    app_DAU_list_revised[i+1] = app_DAU_list[i+1]*avg_index
#    print 'app_dau_list_revised_1\t', np.mean(app_DAU_list_revised), app_DAU_list_revised, app_DAU_list_revised[0]/app_DAU_LM_LW-1
    # 为了保持改变第一周后的平均值不变
    app_DAU_list_revised = [i/(np.mean(app_DAU_list)/app_ADAU_CM) for i in app_DAU_list_revised]
    #print np.mean(app_DAU_list_revised), app_DAU_list_revised
#    print 'app_dau_list_revised_2\t', np.mean(app_DAU_list_revised), app_DAU_list_revised, app_DAU_list_revised[0]/app_DAU_LM_LW-1

    try:
	# 再次检查一下第二周的数据
	momc_dau_2 = app_DAU_list[1]/app_DAU_list[0]-1
	if momc_dau_2 > 0.15 or momc_dau_2 < -0.15:
	    momc_dau_2 = momc_dau_2/abs(momc_dau_2) * np.random.uniform(abs(momc_dau_fw)*0.9, 1.1*abs(momc_dau_fw))
	    app_DAU_list[1] = app_DAU_list[0]*(1+momc_dau_2)	

	momc_dau_3 = app_DAU_list[2]/app_DAU_list[1]-1
	if momc_dau_3 > 0.15 or momc_dau_3 < -0.15:
	    momc_dau_3 = momc_dau_3/abs(momc_dau_3) * np.random.uniform(abs(momc_dau_2)*0.9, 1.1*abs(momc_dau_2))
	    app_DAU_list[2] = app_DAU_list[1]*(1+momc_dau_3)	

	momc_dau_4 = app_DAU_list[3]/app_DAU_list[2]-1
	if momc_dau_4 > 0.15 or momc_dau_4 < -0.15:
	    momc_dau_4 = momc_dau_4/abs(momc_dau_4) * np.random.uniform(abs(momc_dau_3)*0.9, 1.1*abs(momc_dau_3))
	    app_DAU_list[3] = app_DAU_list[2]*(1+momc_dau_4)	

        if number_weeks == 5:
	    momc_dau_5 = app_DAU_list[4]/app_DAU_list[3]-1
	    if momc_dau_5 > 0.15 or momc_dau_5 < -0.15:
	        momc_dau_5 = momc_dau_5/abs(momc_dau_5) * np.random.uniform(abs(momc_dau_4)*0.9, 1.1*abs(momc_dau_4))
	        app_DAU_list[4] = app_DAU_list[3]*(1+momc_dau_5)	

    except Exception, e:
	print e, pk
	continue
#    print 'app_dau_list_revised_3\t', np.mean(app_DAU_list_revised), app_DAU_list_revised, app_DAU_list_revised[0]/app_DAU_LM_LW-1
#    print '\n'


    # 由上面的 本期の日均活跃量 计算得到 本期の月均日活跃量
    app_ADAU_CM = np.mean(app_DAU_list)
    # 根据包名从数据库提取 上期の月均日活跃量
    try:
        app_ADAU_LM = float(doc[LastMonth]['DAU_Monthly'])
    except Exception, e:
        print pk, '\t\t', e
        continue
    # 计算得到 本期の月均日活跃量环比
    app_ADAUMoMC = app_ADAU_CM/app_ADAU_LM - 1
    
    # 如果 本期の月均日活跃量环比 较 本期の月活跃环比 (如，正负、幅度等)相差太大, 则进行调整
    if (app_ADAUMoMC/app_MAUMoMC>2) or (app_ADAUMoMC/app_MAUMoMC<0):
    #if app_ADAUMoMC/app_MAUMoMC<0:
        app_ADAUMoMC = np.random.uniform(app_MAUMoMC*0.9, app_MAUMoMC*1.1)
    # 根据调整后的 本期の月均日活跃量环比 计算得到 本期の月均日活跃量
    app_ADAU_CM_Revised = (app_ADAUMoMC + 1) * app_ADAU_LM 
    momc_adau = app_ADAU_CM_Revised / app_ADAU_CM
    app_ADAU_CM = app_ADAU_CM_Revised 
#    print 'ADAU_MoMC_Revised:', app_ADAUMoMC_Revised
#    print 'ADAU_CurMonth_Revised:', app_ADAU_CM

    app_DAU_list = [x*momc_adau for x in app_DAU_list]
#    print app_DAU_list
        



#	if (app_DAU_list[2]/app_DAU_list[1]-1>0.15) or (app_DAU_list[2]/app_DAU_list[1]-1<0.15):
#	    avg_index = app_DAU_list[1]/(sum(app_DAU_list[2:])/len(app_DAU_list[2:]))
#	    for i in range(len(app_DAU_list)-2):
#		app_DAU_list[i+2] = app_DAU_list[i+2]*avg_index

	# 调整本期其余周的 日均活跃量
#	if app_DAU_list[0] >= app_DAU_list_fw_revised:
#	    for i in range(len(app_DAU_list)-1):
#		app_DAU_list[i+1] += (app_DAU_list[0] - app_DAU_list_fw_revised)/(len(CurWeek)-1)
#	elif app_DAU_list[0] < app_DAU_list_fw_revised:
#	    for i in range(len(app_DAU_list)-1):
#		app_DAU_list[i+1] -= (app_DAU_list_fw_revised - app_DAU_list[0])/(len(CurWeek)-1)
	# 重新计算 本期の月均日活跃量
#	app_ADAU_CM = np.mean(app_DAU_list)

#	print 'ADAU_CM:', app_ADAU_CM
#	print 'DAU_WoWC_CurMonth_FirstWeek_Revised:', momc_dau_fw
#	for i in range(len(app_DIns_list)-1):
#	    print 'DAU_WoWC_WeeklyData' + str(i+2) + '_Revised:', app_DAU_list[i+1]/app_DAU_list[i] - 1
#	print '\n'


    try:
	momc_dins_fw = momc_dau_fw/abs(momc_dau_fw) * np.random.uniform(abs(momc_dau_fw)*0.975, abs(momc_dau_fw)*1.025) 
	momc_dins_2 = momc_dau_2/abs(momc_dau_2) * np.random.uniform(abs(momc_dau_2)*0.975, abs(momc_dau_2)*1.025) 
	momc_dins_3 = momc_dau_3/abs(momc_dau_3) * np.random.uniform(abs(momc_dau_3)*0.975, abs(momc_dau_3)*1.025) 
	momc_dins_4 = momc_dau_4/abs(momc_dau_4) * np.random.uniform(abs(momc_dau_4)*0.975, abs(momc_dau_4)*1.025) 
        if number_weeks == 5:
	    momc_dins_5 = momc_dau_5/abs(momc_dau_5) * np.random.uniform(abs(momc_dau_5)*0.975, abs(momc_dau_5)*1.025) 

	app_DIns_list[0] = app_DIns_LM_LW*(1+momc_dins_fw)
	app_DIns_list[1] = app_DIns_list[0]*(1+momc_dins_2)
	app_DIns_list[2] = app_DIns_list[1]*(1+momc_dins_3)
	app_DIns_list[3] = app_DIns_list[2]*(1+momc_dins_4)
        if number_weeks == 5:
	    app_DIns_list[4] = app_DIns_list[3]*(1+momc_dins_5)
    except Exception, e:
	print e, pk



    # 重新计算本期の DInsPR DAUPR
    if pk in AppsPackageNameForAASD:
	app_DInsPR_list = [j/i for i, j in zip(AASD_Weekly, app_DIns_list)]
	app_DAUPR_list = [j/i for i, j in zip(AASD_Weekly, app_DAU_list)]
    else:
	app_DInsPR_list = [j/i for i, j in zip(ASD_Weekly, app_DIns_list)]
	app_DAUPR_list = [j/i for i, j in zip(ASD_Weekly, app_DAU_list)]
#    print 'DInsPR_list_Revised:', app_DInsPR_list
#    print 'DAUPR_list_Revised:', app_DAUPR_list


     #将每一款应用 本期の月均日活跃量 导入到CSV文件
    lines_ADAU = [[pk, app_ADAU_CM,]]
    for line in lines_ADAU:
	writer_ADAU.writerow(line)

	
    # 将每一款应用 本期の日均安装比例 导入到CSV文件
    lines_DInsPR = [[pk,] +  app_DInsPR_list]
    for line in lines_DInsPR:
	writer_DInsPR.writerow(line)

    # 将每一款应用 本期の日均活跃比例 导入到CSV文件
    lines_DAUPR = [[pk,] + app_DAUPR_list]
    for line in lines_DAUPR:
	writer_DAUPR.writerow(line)


