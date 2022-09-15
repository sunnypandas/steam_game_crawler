# -*- coding: utf-8 -*-
import csv
import datetime
from collections.abc import Iterable

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamGameDetail(scrapy.Spider):
    name = 'steamGameDetail'
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
        rows = self.get_rows(r'task/steam_game_list_'+str(self.jobId)+'.csv')
        rows.set_index(['appid'])
        history = self.get_rows(r'csv/steam_game_detail_history.csv')
        history.set_index(['appid'])
        common = rows.merge(history, on=['appid'])
        inputs = rows[(~rows.appid.isin(common.appid))]
        for row in inputs.itertuples():
            appid = row.appid
            # appid = '1796140'
            yield scrapy.Request(
                url='http://store.steampowered.com/api/appdetails?appids=' + str(appid),
                method='GET', callback=self.parse_basic_info, meta={'row': row},
                dont_filter=True)
            # if True:
            #     break

    @inline_requests
    def parse_basic_info(self, response):
        row = pandas.DataFrame(response.meta['row'])
        appid = row.iat[1, 0]
        # appid = '1796140'
        name = row.iat[2, 0]
        detail = convertRawString2Json(response.text).get(str(appid)).get('data')
        if detail == None:
            # self.create_csv([[appid]], r'csv/steam_game_detail_history.csv')
            return
        short_description = detail.get('short_description')
        release_date = detail.get('release_date').get('date') if detail.get('release_date') != None else ''
        developers = '|'.join(detail.get('developers')) if isinstance(detail.get('developers'), Iterable) else ''
        publishers = '|'.join(detail.get('publishers')) if isinstance(detail.get('publishers'), Iterable) else ''
        extra_detail = yield scrapy.Request(url='https://store.steampowered.com/app/' + str(appid), dont_filter=True)
        tablet_grid = extractStringFromSelector(extra_detail.xpath('//div[@id="tabletGrid"]').extract(), 0)
        breadcrumbs = '|'.join(scrapy.Selector(text=tablet_grid).xpath('//div[@class="breadcrumbs"]/div[@class="blockbg"]/a//text()').extract())
        recent_reviews_positive = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath('(//div[@id="userReviews"]/div[@class="user_reviews_summary_row"])[1]//span[@class="game_review_summary positive"]/text()').extract(), 0).strip()
        recent_reviews_count = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath('(//div[@id="userReviews"]/div[@class="user_reviews_summary_row"])[1]//span[@class="responsive_hidden"]/text()').extract(), 0).strip()
        recent_reviews = recent_reviews_positive + recent_reviews_count
        all_reviews_positive = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath('(//div[@id="userReviews"]/div[@class="user_reviews_summary_row"])[2]//span[@class="game_review_summary positive"]/text()').extract(), 0).strip()
        all_reviews_count = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath('(//div[@id="userReviews"]/div[@class="user_reviews_summary_row"])[2]//span[@class="responsive_hidden"]/text()').extract(), 0).strip()
        all_reviews = all_reviews_positive + all_reviews_count
        tags = '|'.join(x.strip() for x in scrapy.Selector(text=tablet_grid).xpath('//div[@id="glanceCtnResponsiveRight"]//div[@class="glance_tags popular_tags"]/a[@class="app_tag"]/text()').extract())
        game_area_purchases = scrapy.Selector(text=tablet_grid).xpath('//div[@id="game_area_purchase"]/div[contains(@class, "game_area_purchase_game_wrapper")]').extract()
        game_area_purchase_list = []
        for game_area_purchase in game_area_purchases:
            game_area_purchase_title = extractStringFromSelector(scrapy.Selector(text=game_area_purchase).xpath('//div[contains(@class, "game_area_purchase_game")]/h1/text()').extract(), 0).strip()
            game_area_purchase_price = extractStringFromSelector(scrapy.Selector(text=game_area_purchase).xpath(
                    '//div[@class="game_purchase_action_bg"]//div[contains(@class, "game_purchase_price price") or contains(@class, "discount_final_price")]/text()').extract(), 0).strip()
            game_area_purchase_list.append(game_area_purchase_title + '(' + str(game_area_purchase_price) + ')')
        game_area_purchase = '|'.join(game_area_purchase_list)
        categories = '|'.join(x.get('description') for x in detail.get('categories')) if isinstance(detail.get('categories'), Iterable) else ''
        supported_languages = detail.get('supported_languages')
        game_rating_icon = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath(
            '//div[contains(@class, "shared_game_rating")]//div[contains(@class, "game_rating_icon")]/a/img/@src').extract(), 0).strip()
        game_rating_agency = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath(
            '//div[contains(@class, "shared_game_rating")]//div[contains(@class, "game_rating_agency")]/text()').extract(),
                                                            0).strip()
        genres = '|'.join(x.get('description') for x in detail.get('genres')) if isinstance(detail.get('genres'), Iterable) else ''
        metacritic = detail.get('metacritic').get('score') if detail.get('metacritic') != None else ''
        award = extractStringFromSelector(scrapy.Selector(text=tablet_grid).xpath('//div[@id="AwardsDefault"]//div[contains(@class, "game_page_autocollapse")]/img/@src').extract(), 0)
        about_the_game = detail.get('about_the_game')
        platforms = detail.get('platforms')
        pc_requirements_minimum = detail.get('pc_requirements').get('minimum') if platforms.get('windows') and type(detail.get('pc_requirements'))==dict else ''
        pc_requirements_recommended = detail.get('pc_requirements').get('recommended') if platforms.get('windows') and type(detail.get('pc_requirements'))==dict else ''
        mac_requirements_minimum = detail.get('mac_requirements').get('minimum') if platforms.get('mac') and type(detail.get('mac_requirements'))==dict else ''
        mac_requirements_recommended = detail.get('mac_requirements').get('recommended') if platforms.get('mac') and type(detail.get('mac_requirements'))==dict else ''
        linux_requirements_minimum = detail.get('linux_requirements').get('minimum') if platforms.get('linux') and type(detail.get('linux_requirements'))==dict else ''
        linux_requirements_recommended = detail.get('linux_requirements').get('recommended') if platforms.get('linux') and type(detail.get('linux_requirements'))==dict else ''
        self.create_csv([[extra_detail.url, appid, name, breadcrumbs, short_description, recent_reviews, all_reviews, release_date, developers, publishers, tags, game_area_purchase,
              categories, supported_languages, game_rating_icon, game_rating_agency, genres, metacritic, award, about_the_game, pc_requirements_minimum,
              pc_requirements_recommended, mac_requirements_minimum, mac_requirements_recommended, linux_requirements_minimum, linux_requirements_recommended]],
                        r'csv/steam_game_detail.csv')
        self.create_csv([[appid]], r'csv/steam_game_detail_history.csv')

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