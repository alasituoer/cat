#coding:utf-8
import sys
import scrapy

class RankAso100Spider(scrapy.Spider):
    name = 'rank_cat_apps_aso100_spider'
    allowed_domains = ['aso100.com']
    start_urls =['https://aso100.com']
    working_space = '/Users/Alas/Documents/TD_handover/CAT/data_source/monthlydata/第三方数据/2017-08/rank_aso100/'

    # 待爬取apps_apple_appid文件
    path_to_be_crawled_apps_apple_appid = working_space +\
            'apps_apple_appid/apps_apple_appid.txt'
    # 已爬取apps_apple_appid文件
    path_crawled_apps_apple_appid = working_space +\
            'apps_apple_appid/crawled_apps_apple_appid.txt'
    # 存放爬取到的文件
    path_crawled_history_ranks_of_apps = working_space +\
            'crawled_history_ranks_of_apps/'

    def parse(self, response):
        # 打开待爬取的apps_apple_appid文件
        with open(self.path_to_be_crawled_apps_apple_appid, 'r') as f1:
            #print f1.readlines()
            # 再打开已爬取的apps_apple_appid文件
            with open(self.path_crawled_apps_apple_appid, 'r') as f2:
                # 将以爬取的苹果应用id放入一个列表中
                list_apps_apple_appid_crawled = []
                for info in f2.readlines():
                    list_apps_apple_appid_crawled.append(info.split(',')[1].strip())
                #print list_apps_apple_appid_crawled


                #if 'li' in 'limingzhi':
                #    line = f1.readline()
                # 从待爬取文件中挨个读入苹果应用id来构造爬取网址
                for line in f1.readlines():
                    app_apple_appid = line.split(',')[1].replace('\n', '')
                    # 如果该ID在已爬取文件中, 则跳过此次爬取行为(或称循环)
                    if app_apple_appid in list_apps_apple_appid_crawled:
                        continue

                    app_name = line.split(',')[0]
                    #print [app_apple_appid, app_name]

                    start_date = '2017-07-01'
                    end_date = '2017-08-31'

                    url = 'https://aso100.com/app/rankMore/' +\
                            'appid/' + app_apple_appid +\
                            '?device=iphone&country=cn&brand=free' +\
                            '&sdate=' + start_date +\
                            '&edate=' + end_date 
                    meta_source = {
                            'app_apple_appid': app_apple_appid,
                            'app_name': app_name,
                            'start_date': start_date,
                            'end_date': end_date,}
                    yield scrapy.Request(url,
                            callback=self.ToWriteCSV,
                            meta=meta_source,)

    def ToWriteCSV(self, response):
        #print response.body
        # 如果相应返回的状态是200, 则导出文件形如: '990892583_2014-04-01_2017-03-29.txt'
        if response.status == 200:
            to_write_filename = response.meta['app_apple_appid'] + '_' +\
                    response.meta['start_date'] + '_' +\
                    response.meta['end_date'] + '.txt'
            #print response.body
            
            with open(self.path_crawled_history_ranks_of_apps + to_write_filename, 'wb') as f1:
                f1.write(response.body)

            # 将成功爬取数据的apps_apple_appid存放于一个单独文件
            to_write_crawled_apple_appid = response.meta['app_name'] + ',' +\
                    response.meta['app_apple_appid'] + '\n'
            with open(self.path_crawled_apps_apple_appid, 'ab') as f1:
                f1.write(to_write_crawled_apple_appid)


