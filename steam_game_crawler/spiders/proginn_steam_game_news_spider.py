# -*- coding: utf-8 -*-
import csv
import datetime
import itertools

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamGameNews(scrapy.Spider):
    name = 'steamGameNews'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 60000,
        'DOWNLOAD_MAXSIZE': 12406585060,
        'DOWNLOAD_WARNSIZE': 0,
        'DOWNLOAD_DELAY': 0,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
    }
    batch = datetime.datetime.now()
    allowed_domains = []

    def start_requests(self):
        rows = self.get_rows(r'csv/steam_game_list.csv')
        rows.set_index(['appid'])
        history = self.get_rows(r'csv/steam_game_news_history.csv')
        history.set_index(['appid'])
        common = rows.merge(history, on=['appid'])
        inputs = rows[(~rows.appid.isin(common.appid))]
        for row in inputs.itertuples():
            appid = row.appid
            if (history['appid'].eq(str(appid))).any():
                continue
            yield scrapy.Request(
                url='https://steamcommunity.com/app/'+str(appid)+'/homecontent/?announcementsoffset=0&userreviewsoffset=0&p=1&workshopitemspage=1&readytouseitemspage=1&mtxitemspage=1&itemspage=1&screenshotspage=1&videospage=1&artpage=1&allguidepage=1&webguidepage=1&integratedguidepage=1&discussionspage=1&numperpage=10&browsefilter=trend&browsefilter=trend&appid='+str(appid)+'&appHubSubSection=14&l=english&filterLanguage=default&searchText=&maxInappropriateScore=50&forceanon=1',
                method='GET', callback=self.parse_basic_info, meta={'row': row},
                dont_filter=True)

    @inline_requests
    def parse_basic_info(self, response):
        news_list = []
        row = pandas.DataFrame(response.meta['row'])
        appid = row.iat[1, 0]
        if len(response.text) > 0:
            news_list.extend(self.parse_news(response.url, appid, response.text))
            announcementsoffset = 0
            page = 1
            while True:
                announcementsoffset = announcementsoffset + 10
                page = page + 1
                res = yield scrapy.Request(
                    url='https://steamcommunity.com/app/'+str(appid)+'/homecontent/?announcementsoffset='+str(announcementsoffset)+'&userreviewsoffset=0&p='+str(page)+'&workshopitemspage='+str(page)+'&readytouseitemspage='+str(page)+'&mtxitemspage='+str(page)+'&itemspage='+str(page)+'&screenshotspage='+str(page)+'&videospage='+str(page)+'&artpage='+str(page)+'&allguidepage='+str(page)+'&webguidepage='+str(page)+'&integratedguidepage='+str(page)+'&discussionspage='+str(page)+'&numperpage=10&browsefilter=trend&browsefilter=trend&appid='+str(appid)+'&appHubSubSection=14&l=english&filterLanguage=default&searchText=&maxInappropriateScore=50&forceanon=1',
                    dont_filter=True)
                if len(res.text) > 0 and 'id="page' in res.text:
                    news_list.extend(self.parse_news(res.url, appid, res.text))
                else:
                    break
        if len(news_list) > 0:
            self.create_csv(news_list, r'csv/steam_game_news.csv')
        self.create_csv([[appid]], r'csv/steam_game_news_history.csv')

    def parse_news(self, url, appid, news):
        news_list = []
        news = scrapy.Selector(text=news).xpath('//div[contains(@class, "Announcement_Card")]').extract()
        for new in news:
            news_title = extractStringFromSelector(scrapy.Selector(text=new).xpath(
                '//div[contains(@class, "apphub_CardContentNewsTitle")]/text()').extract(), 0).strip()
            news_date = extractStringFromSelector(scrapy.Selector(text=new).xpath(
                '//div[contains(@class, "apphub_CardContentNewsDate")]/text()').extract(), 0).strip()
            news_content = extractStringFromSelector(scrapy.Selector(text=new).xpath(
                '//div[contains(@class, "apphub_CardTextContent")]').extract(), 0).strip()
            news_rating_count = extractStringFromSelector(scrapy.Selector(text=new).xpath(
                '//div[contains(@class, "apphub_CardRating")]/text()').extract(), 0).strip()
            news_comment_count = extractStringFromSelector(scrapy.Selector(text=new).xpath(
                '//div[contains(@class, "apphub_CardCommentCount")]/text()').extract(), 0).strip()
            news_list.append([url, appid, news_title, news_date, news_content, news_rating_count, news_comment_count])
        return news_list

    def get_rows(self, path):
        rows = pd.read_csv(path, header=0, index_col=None, dtype=str)
        rows = rows.fillna('')
        return rows

    def create_csv(self, rows, path):
        with open(path, 'a') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

def closed(self, reason):
        '''
        爬虫结束时退出登录状态
        :param reason:
        :return:
        '''
        if 'finished' == reason:
            self.logger.warning('%s', '爬虫程序执行结束，即将关闭')
        elif 'shutdown' == reason:
            self.logger.warning('%s', '爬虫进程被强制中断，即将关闭')
        elif 'cancelled' == reason:
            self.logger.warning('%s', '爬虫被引擎中断，即将关闭')
        else:
            self.logger.warning('%s', '爬虫被未知原因打断，即将关闭')