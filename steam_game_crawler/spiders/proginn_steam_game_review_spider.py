# -*- coding: utf-8 -*-
import csv
import datetime
import itertools

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy
import urllib.parse

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamGameReview(scrapy.Spider):
    name = 'steamGameReview'
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
        history = self.get_rows(r'csv/steam_game_review_history.csv')
        history.set_index(['appid'])
        common = rows.merge(history, on=['appid'])
        inputs = rows[(~rows.appid.isin(common.appid))]
        for row in inputs.itertuples():
            appid = row.appid
            yield scrapy.Request(
                url='https://store.steampowered.com/appreviews/'+str(appid)+'?language=english&num_per_page=100&cursor=*&purchase_type=all&filter=recent&json=1',
                method='GET', callback=self.parse_basic_info, meta={'row': row},
                dont_filter=True)

    @inline_requests
    def parse_basic_info(self, response):
        review_list = []
        row = pandas.DataFrame(response.meta['row'])
        appid = row.iat[1, 0]
        detail = convertRawString2Json(response.text)
        prev_cursor = '*'
        if detail.get('success') == 1:
            next_cursor = detail.get('cursor')
            reviews = detail.get('reviews')
            review_list.extend(self.parse_review(response.url, appid, reviews))
            while True:
                if len(next_cursor) == 0 or prev_cursor == next_cursor:
                    break
                else:
                    res = yield scrapy.Request(
                        url='https://store.steampowered.com/appreviews/'+str(appid)+'?language=english&num_per_page=100&cursor='+urllib.parse.quote(str(next_cursor))+'&purchase_type=all&filter=recent&json=1',
                        dont_filter=True)
                    det = convertRawString2Json(res.text)
                    prev_cursor = next_cursor
                    next_cursor = det.get('cursor')
                    if det.get('success') == 1 and prev_cursor != next_cursor:
                        revs = det.get('reviews')
                        review_list.extend(self.parse_review(res.url, appid, revs))
        if len(review_list) > 0:
            self.create_csv(review_list, r'csv/steam_game_review.csv')
        self.create_csv([[appid]], r'csv/steam_game_review_history.csv')

    def parse_review(self, url, appid, reviews):
        review_list = []
        for review in reviews:
            votes_up = review.get('votes_up')
            votes_funny = review.get('votes_funny')
            voted_up = review.get('voted_up')
            author = review.get('author')
            author_playtime_forever = author.get('playtime_forever')
            author_playtime_at_review = author.get('playtime_at_review')
            author_steamid = author.get('steamid')
            author_num_games_owned = author.get('num_games_owned')
            timestamp_created = review.get('timestamp_created')
            timestamp_updated = review.get('timestamp_updated')
            content = review.get('review')
            review_list.append([url, appid, votes_up, votes_funny, voted_up, author_playtime_forever, author_playtime_at_review, author_steamid, author_num_games_owned,
                                timestamp_created, timestamp_updated, content])
        return review_list

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