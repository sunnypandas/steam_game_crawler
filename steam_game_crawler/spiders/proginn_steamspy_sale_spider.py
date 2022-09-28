# -*- coding: utf-8 -*-
import csv
import datetime
import glob
import itertools
import os

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamspySale(scrapy.Spider):
    name = 'steamspySale'
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
        yield scrapy.Request(
            url='https://www.baidu.com',
            method='GET', callback=self.parse_basic_info,
            dont_filter=True)

    def parse_basic_info(self, response):
        path = r'task/sales/'  # use your path
        files = glob.glob(os.path.join(path, "*.html"))
        files.sort()
        for filename in files:
            f = open(filename, "r")
            sales = scrapy.Selector(text=f.read()).xpath('//table[contains(@id, "tablesales")]/tbody/tr').extract()
            sale_list = []
            for sale in sales:
                game_link = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[2]/a/@href').extract(), 0).strip()
                game_name = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[2]/@data-order').extract(), 0).strip()
                before = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[3]/text()').extract(), 0).strip()
                after = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[4]/text()').extract(), 0).strip()
                sales = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[5]/text()').extract(), 0).strip()
                increase = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[6]/text()').extract(), 0).strip()
                pricesales = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[7]/text()').extract(), 0).strip()
                discount = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[8]/text()').extract(), 0).strip()
                userscoresales = extractStringFromSelector(
                    scrapy.Selector(text=sale).xpath('(//tr/td)[9]/text()').extract(), 0).strip()

                if len(game_link) > 0:
                    game_link = 'https://steamspy.com' + game_link
                    sale_list.append([game_link, game_name, before, after, sales, increase, pricesales, discount, userscoresales])
            if len(sale_list) > 0:
                self.create_csv(sale_list, r'csv/steamspy_sale_detail.csv')

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