# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

class ZhihuUserPipeline(object):

    def __init__(self, host, db):
        self.host = host
        self.db = db

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.host)
        self.user = self.client[self.db]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host = crawler.settings.get("MONGODB_HOST"),
            db = crawler.settings.get("MONGODB_DB")
        )

    def process_item(self, item, spider):
        self.user['user'].update({'url_token':item['url_token']},{'$set':item}, True)
        return item

    def close_spider(self, spider):
        self.client.close()