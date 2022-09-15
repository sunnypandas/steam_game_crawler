# -*- coding: utf-8 -*-
import csv
import datetime

import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json


class SteamGameList(scrapy.Spider):
    name = 'steamGameList'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 60000,
        'DOWNLOAD_MAXSIZE': 12406585060,
        'DOWNLOAD_WARNSIZE': 0,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
    }
    batch = datetime.datetime.now()
    allowed_domains = []

    def start_requests(self):
        yield scrapy.Request(
            url='http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json',
            method='GET', callback=self.parse_basic_info,
            dont_filter=True)

    def parse_basic_info(self, response):
        apps = convertRawString2Json(response.text).get('applist').get('apps')
        rows = []
        rows.append(['appid', 'name'])
        for app in apps:
            appid = app.get('appid')
            name = app.get('name')
            rows.append([appid, name])
        self.create_csv(rows, r'csv/steam_game_list.csv')

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