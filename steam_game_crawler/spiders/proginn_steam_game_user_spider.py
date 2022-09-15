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


class SteamGameUser(scrapy.Spider):
    name = 'steamGameUser'
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
        rows = self.get_rows(r'/Users/sunpanpan/Workspace/Work/Doyle/steam_game_list.csv')
        for row in rows.itertuples():
            yield scrapy.Request(
                url='https://steamcommunity.com/id/teddyradko',
                method='GET', callback=self.parse_basic_info, meta={'row': row},
                dont_filter=True)
            if True:
                break

    @inline_requests
    def parse_basic_info(self, response):
        row = pandas.DataFrame(response.meta['row'])
        appid = row.iat[1, 0]
        name = row.iat[2, 0]
        profile_private_info = extractStringFromSelector(scrapy.Selector(text=response.text).xpath('//div[contains(@class, "profile_private_info")]/text()').extract(), 0).strip()
        if len(profile_private_info) == 0:
            friend_player_level_num = extractStringFromSelector(scrapy.Selector(text=response.text).xpath(
                '//div[contains(@class, "profile_header_badgeinfo_badge_area")]//span[contains(@class, "friendPlayerLevelNum")]/text()').extract(),
                                                                0).strip()
            friend_player_level_badge = '|'.join(x.strip() for x in scrapy.Selector(text=response.text).xpath(
                '//div[contains(@class, "profile_header_badgeinfo_badge_area")]//div[contains(@class, "favorite_badge_description")]/div/text()').extract())
            friend_player_award_count = extractStringFromSelector(scrapy.Selector(text=response.text).xpath('//div[contains(@class, "responsive_count_link_area")]/div[contains(@class, "profile_awards")]//span[contains(@class, "profile_count_link_total")]/text()').extract(), 0).strip()
            friend_player_badge_count = extractStringFromSelector(scrapy.Selector(text=response.text).xpath(
                '//div[contains(@class, "responsive_count_link_area")]/div[contains(@class, "profile_badges")]//span[contains(@class, "profile_count_link_total")]/text()').extract(), 0).strip()
            profile_item_links = scrapy.Selector(text=response.text).xpath(
                '//div[contains(@class, "responsive_count_link_area")]/div[contains(@class, "profile_item_links")]//div[contains(@class, "profile_count_link")]').extract()
            friend_player_inventory_count = ''
            friend_player_workshop_item_count = ''
            friend_player_workshop_url = ''
            friend_player_follower_count = ''
            for profile_count_link in profile_item_links:
                label = extractStringFromSelector(scrapy.Selector(text=profile_count_link).xpath('//a/span[contains(@class, "count_link_label")]/text()').extract(), 0).strip()
                total = extractStringFromSelector(scrapy.Selector(text=profile_count_link).xpath('//a/span[contains(@class, "profile_count_link_total")]/text()').extract(), 0).strip().replace(',', '')
                if label == 'Inventory':
                    friend_player_inventory_count = total
                if label == 'Workshop Items':
                    friend_player_workshop_item_count = total
                    friend_player_workshop_url = extractStringFromSelector(scrapy.Selector(text=profile_count_link).xpath('//a/@href').extract(), 0).strip()
            if len(friend_player_workshop_url) > 0:
                extra_detail = yield scrapy.Request(url=friend_player_workshop_url, dont_filter=True)
                friend_player_follower_count = extractStringFromSelector(extra_detail.xpath('//div[@id="rightContents"]//div[@class="followStat"]/text()').extract(), 0)
            print(friend_player_follower_count)

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