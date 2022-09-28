# -*- coding: utf-8 -*-
import csv
import datetime
import itertools
import logging

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamspyOwner(scrapy.Spider):
    name = 'steamspyOwner'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 60000,
        'DOWNLOAD_MAXSIZE': 12406585060,
        'DOWNLOAD_WARNSIZE': 0,
        'DOWNLOAD_DELAY': 0.5,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
    }
    batch = datetime.datetime.now()
    allowed_domains = []

    def start_requests(self):
        headers = """
                :authority: steamspy.com
                :method: GET
                :path: /app/590380
                :scheme: https
                accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
                accept-encoding: gzip, deflate, br
                accept-language: en-US,en;q=0.9,ja-JP;q=0.8,ja;q=0.7,zh-CN;q=0.6,zh;q=0.5,zh-TW;q=0.4
                cache-control: max-age=0
                cookie: SteamSpySession=e2a86aebd6618b3c3ddea7869d0d395c31c48ad2; PHPSESSID=56s1qo3dga0nr318uda5ciq21u
                referer: https://steamspy.com/year/2018
                sec-ch-ua: "Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"
                sec-ch-ua-mobile: ?0
                sec-ch-ua-platform: "macOS"
                sec-fetch-dest: document
                sec-fetch-mode: navigate
                sec-fetch-site: same-origin
                sec-fetch-user: ?1
                upgrade-insecure-requests: 1
                user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36
                """
        rows = self.get_rows(r'task/steamspy_list.csv')
        rows.set_index(['appurl'])
        history = self.get_rows(r'csv/steamspy_owner_history.csv')
        history.set_index(['appurl'])
        common = rows.merge(history, on=['appurl'])
        inputs = rows[(~rows.appurl.isin(common.appurl))]
        for row in inputs.itertuples():
            appurl = row.appurl
            yield scrapy.Request(
                url=appurl,
                method='GET', callback=self.parse_basic_info,
                headers=convertRawString2Headers(headers),
                dont_filter=True)

    def parse_basic_info(self, response):
        texts = extractStringFromSelector(scrapy.Selector(text=response.text).xpath("//script[re:test(text(),'OwnersChartData=','i')]/text()").extract(), 0).strip().split('\r\n')
        data = ''
        for text in texts:
            if text != None and text.strip().startswith('OwnersChartData='):
                data = text.strip().replace('OwnersChartData=', '')
                if data.endswith(';'):
                    data = data[:-1]
                break
        if len(data) == 0:
            logging.warning('Request got nothing for url: %s', response.url)
        else:
            self.create_csv([[response.url, data]], r'csv/steamspy_owner_detail.csv')
        self.create_csv([[response.url]], r'csv/steamspy_owner_history.csv')

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