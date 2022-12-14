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


class SteamspyList(scrapy.Spider):
    name = 'steamspyList'
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
                :path: /year/2004
                :scheme: https
                accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
                accept-encoding: gzip, deflate, br
                accept-language: en-US,en;q=0.9,ja-JP;q=0.8,ja;q=0.7,zh-CN;q=0.6,zh;q=0.5,zh-TW;q=0.4
                cache-control: max-age=0
                cookie: SteamSpySession=e2a86aebd6618b3c3ddea7869d0d395c31c48ad2; PHPSESSID=56s1qo3dga0nr318uda5ciq21u
                sec-ch-ua: "Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"
                sec-ch-ua-mobile: ?0
                sec-ch-ua-platform: "macOS"
                sec-fetch-dest: document
                sec-fetch-mode: navigate
                sec-fetch-site: none
                sec-fetch-user: ?1
                upgrade-insecure-requests: 1
                user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 
                """
        for year in range(1983, 2022 + 1):
            yield scrapy.Request(
                url='https://steamspy.com/year/' + str(year),
                method='GET', callback=self.parse_basic_info,
                headers=convertRawString2Headers(headers),
                dont_filter=True)

    def parse_basic_info(self, response):
        game_list = scrapy.Selector(text=response.text).xpath('//table[contains(@id, "gamesbygenre")]/tbody/tr').extract()
        game_link_list = []
        for game in game_list:
            game_link = extractStringFromSelector(
                scrapy.Selector(text=game).xpath('(//tr/td)[2]/a/@href').extract(), 0).strip()
            if len(game_link) > 0:
                game_link = 'https://steamspy.com' + game_link
                game_link_list.append([game_link])
        if len(game_link_list) > 0:
            self.create_csv(game_link_list, r'csv/steamspy_list.csv')

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
        ?????????????????????????????????
        :param reason:
        :return:
        '''
        if 'finished' == reason:
            self.logger.warning('%s', '???????????????????????????????????????')
        elif 'shutdown' == reason:
            self.logger.warning('%s', '??????????????????????????????????????????')
        elif 'cancelled' == reason:
            self.logger.warning('%s', '????????????????????????????????????')
        else:
            self.logger.warning('%s', '??????????????????????????????????????????')