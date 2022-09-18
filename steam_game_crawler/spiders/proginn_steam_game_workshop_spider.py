# -*- coding: utf-8 -*-
import csv
import datetime
import re
import time
from math import ceil

from inline_requests import inline_requests
import pandas
import pandas as pd
import scrapy

from steam_game_crawler.consts import DOWNLOADER_MIDDLEWARES_SQUID_PROXY_OFF
from steam_game_crawler.utils.httputils import convertRawString2Json, convertRawString2Headers, extractStringFromSelector


class SteamGameWorkshop(scrapy.Spider):
    name = 'steamGameWorkshop'
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
        rows = self.get_rows(r'task/steam_game_workshop_list_'+str(self.jobId)+'.csv')
        rows.set_index(['appid'])
        history = self.get_rows(r'csv/steam_game_workshop_history.csv')
        history.set_index(['appid'])
        common = rows.merge(history, on=['appid'])
        inputs = rows[(~rows.appid.isin(common.appid))]
        for row in inputs.itertuples():
            appid = row.appid
            yield scrapy.Request(
                url='https://steamcommunity.com/app/'+str(appid)+'/workshop/',
                method='GET', callback=self.parse_basic_info, meta={'row': row},
                dont_filter=True)

    @inline_requests
    def parse_basic_info(self, response):
        row = pandas.DataFrame(response.meta['row'])
        appid = row.iat[1, 0]
        tag_list = scrapy.Selector(text=response.text).xpath('//div[contains(@class, "responsive_local_menu")]/div[contains(@class, "panel")]/div[contains(@class, "filterOption")]').extract()
        tags = ''
        for tag in tag_list:
            tag_label = extractStringFromSelector(scrapy.Selector(text=tag).xpath('//label[contains(@class, "tag_label")]/text()').extract(), 0).strip()
            tag_count = extractStringFromSelector(
                scrapy.Selector(text=tag).xpath('//label[contains(@class, "tag_label")]/span[contains(@class, "tag_count")]/text()').extract(), 0).strip()
            tag_label_count = tag_label + tag_count
            tags = tags + tag_label_count + '|'
        total_counts = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?(?!\d)',extractStringFromSelector(
                scrapy.Selector(text=response.text).xpath('//div[contains(@class, "section_bottom_bar")]/a/span/text()').extract(), 0).strip())
        total_count = 0
        if len(total_counts) > 0:
            total_count = total_counts[0].replace(',', '')
        for year in range(2011, 2022 + 1):
            for half_year in range(1, 3):
                year_start = ''
                year_end = ''
                if half_year == 1:
                    year_start = str(year) + '-01-01'
                    year_end = str(year) + '-06-30'
                elif half_year == 2:
                    year_start = str(year) + '-07-01'
                    year_end = str(year) + '-12-31'
                year_start_timestamp = int(time.mktime(datetime.datetime.strptime(year_start, "%Y-%m-%d").timetuple()))
                year_end_timestamp = int(time.mktime(datetime.datetime.strptime(year_end, "%Y-%m-%d").timetuple()))
                extra_detail = yield scrapy.Request(
                    url='https://steamcommunity.com/workshop/browse/?appid='+str(appid)+'&searchtext=&childpublishedfileid=0&browsesort=mostrecent&section=readytouseitems&created_date_range_filter_start='+str(year_start_timestamp)+'&created_date_range_filter_end='+str(year_end_timestamp)+'&updated_date_range_filter_start=NaN&updated_date_range_filter_end=NaN',
                    dont_filter=True)
                entries = extractStringFromSelector(
                    scrapy.Selector(text=extra_detail.text).xpath(
                        '//div[contains(@id, "profileBlock")]//div[contains(@class, "workshopBrowsePagingInfo")]/text()').extract(), 0).strip()
                if len(entries) > 0:
                    entries_count = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?(?!\d)', entries.split('of')[1])
                    pages = 0
                    if len(entries_count) > 0:
                        pages = ceil(int(entries_count[0].replace(',', ''))/30)
                    if pages > 0:
                        for page in range(1, pages + 1):
                            workshop_browse_item_list = []
                            workshop_browse_items_response = yield scrapy.Request(
                                url='https://steamcommunity.com/workshop/browse/?appid=' + str(
                                    appid) + '&searchtext=&childpublishedfileid=0&browsesort=mostrecent&section=readytouseitems&created_date_range_filter_start=' + str(
                                    year_start_timestamp) + '&created_date_range_filter_end=' + str(
                                    year_end_timestamp) + '&updated_date_range_filter_start=NaN&updated_date_range_filter_end=NaN&actualsort=mostrecent&p=' + str(
                                    page),
                                dont_filter=True)
                            workshop_browse_items = scrapy.Selector(text=workshop_browse_items_response.text).xpath(
                                '//div[contains(@id, "profileBlock")]//div[contains(@class, "workshopBrowseItems")]/div[contains(@class, "workshopItem")]').extract()
                            for workshop_browse_item in workshop_browse_items:
                                item_link = extractStringFromSelector(
                                    scrapy.Selector(text=workshop_browse_item).xpath(
                                        '//a[contains(@class, "item_link")]/@href').extract(), 0).strip()
                                workshop_item_title = extractStringFromSelector(
                                    scrapy.Selector(text=workshop_browse_item).xpath(
                                        '//a[contains(@class, "item_link")]/div[contains(@class, "workshopItemTitle")]/text()').extract(),
                                    0).strip()
                                if len(item_link) > 0:
                                    workshop_item_detail_response = yield scrapy.Request(url=item_link,
                                                                                         dont_filter=True)
                                    workshop_item_category, workshop_item_posted, workshop_item_updated, workshop_item_change_notes_num, workshop_item_change_notes_url, \
                                    workshop_item_creator_list, workshop_item_visitor_count, workshop_item_subscriber_count, workshop_item_favorite_count, workshop_item_description, workshop_item_comment_count = \
                                        self.parse_workshop_browse_item(workshop_item_detail_response)
                                    workshop_item_creators = '|'.join(workshop_item_creator_list)
                                    workshop_item_change_notes = ''
                                    if len(workshop_item_change_notes_url) > 0:
                                        workshop_item_change_notes_response = yield scrapy.Request(
                                            url=workshop_item_change_notes_url, dont_filter=True)
                                        changelog_list = self.parse_workshop_item_change_notes(
                                            workshop_item_change_notes_response)
                                        workshop_item_change_notes = '|'.join(changelog_list)
                                    workshop_browse_item_list.append([item_link, appid, tags, total_count, workshop_item_title, workshop_item_category, workshop_item_posted,
                                          workshop_item_updated, workshop_item_change_notes_num, workshop_item_change_notes, workshop_item_creators,
                                          workshop_item_visitor_count, workshop_item_subscriber_count, workshop_item_favorite_count, workshop_item_description,
                                          workshop_item_comment_count])
                            if len(workshop_browse_item_list) > 0:
                                self.create_csv(workshop_browse_item_list, r'csv/steam_game_workshop_detail.csv')
        self.create_csv([[appid]], r'csv/steam_game_workshop_history.csv')

    def parse_workshop_browse_item(self, workshop_item_detail_response):
        workshop_item_category = extractStringFromSelector(scrapy.Selector(text=workshop_item_detail_response.text).xpath('//div[contains(@class, "responsive_local_menu")]//div[contains(@class, "rightDetailsBlock") or contains(@class, "workshopTags")]/a/text()').extract(), 0).strip()
        workshop_item_posted = extractStringFromSelector(scrapy.Selector(text=workshop_item_detail_response.text).xpath(
            '(//div[contains(@class, "responsive_local_menu")]/div[contains(@class, "rightDetailsBlock")]/div[contains(@class, "detailsStatsContainerRight")]/div[contains(@class, "detailsStatRight")])[2]/text()').extract(), 0).strip()
        workshop_item_updated = extractStringFromSelector(scrapy.Selector(text=workshop_item_detail_response.text).xpath(
            '(//div[contains(@class, "responsive_local_menu")]/div[contains(@class, "rightDetailsBlock")]/div[contains(@class, "detailsStatsContainerRight")]/div[contains(@class, "detailsStatRight")])[3]/text()').extract(), 0).strip()
        workshop_item_change_notes_num = extractStringFromSelector(scrapy.Selector(text=workshop_item_detail_response.text).xpath(
            '//div[contains(@class, "responsive_local_menu")]/div[contains(@class, "rightDetailsBlock")]/div[contains(@class, "detailsStatNumChangeNotes")]/text()').extract(), 0).strip()
        workshop_item_change_notes_url = extractStringFromSelector(
            scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '//div[contains(@class, "responsive_local_menu")]/div[contains(@class, "rightDetailsBlock")]/div[contains(@class, "detailsStatNumChangeNotes")]/span[contains(@class, "change_note_link")]/a/@href').extract(), 0).strip()
        workshop_item_creators = scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '//div[contains(@id, "rightContents")]//div[contains(@class, "panel")]//div[contains(@class, "creatorsBlock")]/div[contains(@class, "friendBlock")]').extract()
        workshop_item_creator_list = []
        for workshop_item_creator in workshop_item_creators:
            workshop_item_creator_url = extractStringFromSelector(scrapy.Selector(text=workshop_item_creator).xpath('//a[contains(@class, "friendBlockLinkOverlay")]/@href').extract(), 0).strip()
            workshop_item_creator_id = extractStringFromSelector(scrapy.Selector(text=workshop_item_creator).xpath(
                '//div[contains(@class, "friendBlockContent")]/text()').extract(), 0).strip()
            workshop_item_creator_list.append('(' + workshop_item_creator_id + ')' + workshop_item_creator_url)
        workshop_item_visitor_count = extractStringFromSelector(scrapy.Selector(text=workshop_item_detail_response.text).xpath(
            '((//div[contains(@id, "rightContents")]//div[contains(@class, "panel")]//table[contains(@class, "stats_table")]//tr)[1]/td)[1]/text()').extract(), 0).strip().replace(',', '')
        workshop_item_subscriber_count = extractStringFromSelector(
            scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '((//div[contains(@id, "rightContents")]//div[contains(@class, "panel")]//table[contains(@class, "stats_table")]//tr)[2]/td)[1]/text()').extract(),
            0).strip().replace(',', '')
        workshop_item_favorite_count = extractStringFromSelector(
            scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '((//div[contains(@id, "rightContents")]//div[contains(@class, "panel")]//table[contains(@class, "stats_table")]//tr)[3]/td)[1]/text()').extract(),
            0).strip().replace(',', '')
        workshop_item_description = extractStringFromSelector(
            scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '//div[contains(@id, "profileBlock")]//div[contains(@id, "highlightContent")]').extract(), 0).strip()
        workshop_item_comment_count = extractStringFromSelector(
            scrapy.Selector(text=workshop_item_detail_response.text).xpath(
                '//div[contains(@class, "commentthread_count")]//span[contains(@class, "commentthread_count_label")]/span/text()').extract(), 0).strip().replace(',', '')
        return workshop_item_category, workshop_item_posted, workshop_item_updated, workshop_item_change_notes_num, workshop_item_change_notes_url, \
               workshop_item_creator_list, workshop_item_visitor_count, workshop_item_subscriber_count, workshop_item_favorite_count, workshop_item_description, workshop_item_comment_count

    def parse_workshop_item_change_notes(self, workshop_item_change_notes):
        change_notes = scrapy.Selector(text=workshop_item_change_notes.text).xpath(
            '//div[contains(@id, "profileBlock")]//div[contains(@class, "workshopAnnouncement")]').extract()
        changelog_list = []
        for change_note in change_notes:
            changelog_updated = extractStringFromSelector(scrapy.Selector(text=change_note).xpath(
                '//div[contains(@class, "changelog")]/text()').extract(), 0).strip()
            changelog_content = extractStringFromSelector(scrapy.Selector(text=change_note).xpath(
                '//p[boolean(@id)]/text()').extract(), 0).strip()
            changelog_list.append('(' + changelog_updated + ')' + changelog_content)
        return changelog_list

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