# -*- coding: utf-8 -*-
import scrapy
import json
from zhihu_user.items import ZhihuUserItem

class ZhihuSpider(scrapy.Spider):
    name = 'zhihu'
    allowed_domains = ['www.zhihu.com']

    followers_url = "https://www.zhihu.com/api/v4/members/{user}/followees?include={include}"
    followers_query = "data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={offset}&limit={limit}"

    user_url = "https://www.zhihu.com/api/v4/members/{user}?include={include}"
    user_query = "allow_message%2Cis_followed%2Cis_following%2Cis_org%2Cis_blocking%2Cemployments%2Canswer_count%2Cfollower_count%2Carticles_count%2Cgender%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics"


    def start_requests(self):
        yield scrapy.Request(url=self.user_url.format(user="excited-vczh", include=self.user_query), callback=self.parse_user)
        yield scrapy.Request(url=self.followers_url.format(user = "excited-vczh", include=self.followers_query.format(offset="0", limit="20")), callback=self.parse_follower)

    def parse_user(self, response):
        item = ZhihuUserItem()
        reult = json.loads(response.text)
        for field in item.fields:
            item[field] = reult.get(field)
        yield item
        yield scrapy.Request(url=self.followers_url.format(user=reult.get("url_token"),include=self.followers_query.format(offset="0", limit="20")), callback=self.parse_follower)

    def parse_follower(self, response):
        result = json.loads(response.text)
        if "data" in result.keys():
            for data in result['data']:
                yield scrapy.Request(url=self.user_url.format(user=data['url_token'], include=self.user_query), callback=self.parse_user)

        if result['paging'] and result['paging']['is_end'] == False:
            next = result['paging']['next']
            yield scrapy.Request(url=next, callback=self.parse_follower)
