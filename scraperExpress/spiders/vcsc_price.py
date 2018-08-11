# -*- coding: utf-8 -*-
import scrapy
import cx_Oracle
from scrapy_splash import SplashRequest


class VcscPriceSpider(scrapy.Spider):
    name = 'vcsc_price'
    download_delay = 2
    allowed_domains = ['ra.vcsc.com.vn']
    start_urls = ['http://ra.vcsc.com.vn/']

    def start_requests(self):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        stock_price_dates = [
        'from=2017-07-02&to=2018-07-02',
        'from=2016-07-02&to=2017-07-01',
        'from=2015-07-02&to=2016-07-01',
        'from=2014-07-02&to=2015-07-01',
        'from=2013-07-02&to=2014-07-01',
        'from=2012-07-02&to=2013-07-01',
        'from=2011-07-02&to=2012-07-01',]

        runletter = 'V'

        sqlstatement = 'select stock_code from vcsc_stock_entity where stock_code like (' + "'" + runletter + "%'" ')'
        curstatement = cur.execute(sqlstatement)
        stockcodes = curstatement.fetchall()
        stocklists = []

        for stockcode in stockcodes:
            stocklists.append(stockcode[0])

        for stocklist in stocklists:
            for stock_price_date in stock_price_dates:
                url = 'http://ra.vcsc.com.vn/Stock?lang=en-US&ticker=' + stocklist + '&' + stock_price_date + '&pageSize=1000'
                yield scrapy.Request(url, self.parse, meta={'splash': {'endpoint': 'render.html',}, 'runletter': runletter, 'stocklist': stocklist}, cookies={'ASP.NET_SessionId':'btrin0cb5mfb0lvh1ndjbcoq', 'KTCK_StockFeed_Stoxvn_CookieCulture':'en-US'})

        cur.close()
        con.close()

    def parse(self, response):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        runletter = response.meta.get('runletter')
        stocklist = response.meta.get('stocklist')

        f = open("vcsc-" + runletter + "-price" + ".csv", "a")

        rows = response.xpath('//table[@class="tblTransaction"]/tbody/tr[position()>1]')

        for row in rows:
            stock_date = row.xpath('./td[1]/text()').extract()[0].strip()
            stock_change = row.xpath('./td[2]/span/text()').extract()[0].strip()
            stock_open = row.xpath('./td[3]/text()').extract()[0].strip()
            stock_highest = row.xpath('./td[4]/text()').extract()[0].strip()
            stock_lowest = row.xpath('./td[5]/text()').extract()[0].strip()
            stock_close = row.xpath('./td[6]/text()').extract()[0].strip()
            stock_order_match_vol = row.xpath('./td[7]/text()').extract()[0].strip()
            stock_order_match_val = row.xpath('./td[8]/text()').extract()[0].strip()
            stock_put_through_vol = row.xpath('./td[9]/text()').extract()[0].strip()
            stock_put_through_val = row.xpath('./td[10]/text()').extract()[0].strip()
            stock_unmatch_buy_vol = row.xpath('./td[11]/text()').extract()[0].strip()
            stock_unmatch_sell_vol = row.xpath('./td[12]/text()').extract()[0].strip()
            stock_total_trade_val = row.xpath('./td[13]/text()').extract()[0].strip()
            stock_total_trade_vol = row.xpath('./td[14]/text()').extract()[0].strip()

            yield {
            'stock_code': stocklist,
            'stock_date': stock_date,
            'stock_change': stock_change,
            'stock_open': stock_open,
            'stock_highest': stock_highest,
            'stock_lowest': stock_lowest,
            'stock_close': stock_close,
            'stock_order_match_vol': stock_order_match_vol,
            'stock_order_match_val': stock_order_match_val,
            'stock_put_through_vol': stock_put_through_vol,
            'stock_put_through_val': stock_put_through_val,
            'stock_unmatch_buy_vol': stock_unmatch_buy_vol,
            'stock_unmatch_sell_vol': stock_unmatch_sell_vol,
            'stock_total_trade_val': stock_total_trade_val,
            'stock_total_trade_vol': stock_total_trade_vol,
            }
            sqlstatement = 'insert into vcsc_stock_price (stock_code, stock_date, stock_change, stock_open, stock_highest, stock_lowest, stock_close, stock_order_match_vol, stock_order_match_val, stock_put_through_vol, stock_put_through_val, stock_unmatch_buy_vol, stock_unmatch_sell_vol, stock_total_trade_val, stock_total_trade_vol) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)'
            cur.execute(sqlstatement, (stocklist, stock_date, stock_change, stock_open, stock_highest, stock_lowest, stock_close, stock_order_match_vol, stock_order_match_val, stock_put_through_vol, stock_put_through_val, stock_unmatch_buy_vol, stock_unmatch_sell_vol, stock_total_trade_val, stock_total_trade_vol))
            con.commit()

            f.write(stocklist + ', ' + stock_date + ', ' + stock_change + ', ' + stock_open + ', ' + stock_highest + ', ' + stock_lowest + ', ' + stock_close + ', ' + stock_order_match_vol + ', ' + stock_order_match_val + ', ' + stock_put_through_vol + ', ' + stock_put_through_val + ', ' + stock_unmatch_buy_vol + ', ' + stock_unmatch_sell_vol + ', ' + stock_total_trade_val + ', ' + stock_total_trade_vol + '\n')

        f.close()
        cur.close()
        con.close()
