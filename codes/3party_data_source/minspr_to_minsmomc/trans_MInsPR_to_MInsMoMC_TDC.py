#coding:utf-8
import pandas as pd

last_month = '2017-06'
cur_month = '2017-07'
# 3、4月 排名中心的大盘值
asd_tdc = [301.77251308143, 303.883643218064,]
working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/MInsPR_to_MInsMoMC_TDC/'
path_minspr_tdc = working_space + 'MInsPR_TDC/' + cur_month + '/MInsPR_TDC_' + cur_month + '.csv'
path_to_write = working_space + 'MInsMoMC_TDC/MInsMoMC_TDC_' + cur_month + '.csv'

# 读入数据中心里上期及本期的月安装比例
df_minspr_tdc = pd.read_csv(path_minspr_tdc, index_col=0)
#print df_minspr_tdc.columns
#print df_minspr_tdc[:10]

# 为方便根据包名（作为索引）提取值, 重构DataFrame
# 提取上期和本期的月安装比例
ss_minspr_last_month_tdc = df_minspr_tdc[last_month]
ss_minspr_cur_month_tdc = df_minspr_tdc[cur_month]
#print ss_minspr_last_month_tdc[:10]
#print ss_minspr_cur_month_tdc[:10]
#print ss_minspr_last_month_tdc.ix['com.wandoujia.phoenix2']

# 计算 TDC 中上期和本期的月安装量
ss_mins_last_month_tdc = ss_minspr_last_month_tdc * asd_tdc[-2]
ss_mins_cur_month_tdc = ss_minspr_cur_month_tdc * asd_tdc[-1]
#print ss_mins_last_month_tdc[:10]
#print ss_mins_cur_month_tdc[:10]

# 得到 TDC中 本期较上期の 月安装量的环比
df_mins_momc_tdc = pd.DataFrame(ss_mins_cur_month_tdc/ss_mins_last_month_tdc-1, columns=['MInsMoMC_TDC'])
#print df_mins_momc_tdc, type(df_mins_momc_tdc)

# 先导出所有应用的环比, 然后针对每一个应用人工核查其环比变化情况
df_mins_momc_tdc.to_csv(path_to_write)







