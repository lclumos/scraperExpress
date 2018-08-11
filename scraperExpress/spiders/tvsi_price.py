# -*- coding: utf-8 -*-
import scrapy
import cx_Oracle
from scrapy_splash import SplashRequest


class TvsiPriceSpider(scrapy.Spider):
    name = 'tvsi_price'
    download_delay = 1
    allowed_domains = ['finance.tvsi.com.vn']

    def start_requests(self):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        stock_price_pages = ['1', '2', '3', '4']

        runindustry = ''

        sqlstatement = 'select stock_code from tvsi_stock_entity where stock_industry in (' + "'" + runindustry + "'" ')'
        curstatement = cur.execute(sqlstatement)
        stockcodes = curstatement.fetchall()
        stocklists = []

        for stockcode in stockcodes:
            stocklists.append(stockcode[0])

        for stocklist in stocklists:
            for stock_price_page in stock_price_pages:
                url = 'http://finance.tvsi.com.vn/Enterprises/LichsugiaSymbolPart2?symbol=' + stocklist + '&currentPage=' + stock_price_page + '&duration=m&startDate=29%2F05%2F2008&endDate=29%2F05%2F2018&_'
                yield scrapy.Request(url, self.parse, meta={'splash': {'endpoint': 'render.html',}, 'runindustry': runindustry}, cookies={'fp_tvsi_lang':'en-US'})

        cur.close()
        con.close()

    def parse(self, response):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        runindustry = response.meta.get('runindustry')
        f = open("tvsi-2-Price-" + runindustry + ".csv", "a")

        rows = response.xpath('//div[@class="table-data"]/table/tbody/tr')

        for row in rows:
            stock_code = response.url.split("?symbol=")[1].split("&currentPage=")[0]

            stock_date = row.xpath('./td[1]/text()').extract()
            if stock_date:
                stock_date = stock_date[0]
            else:
                stock_date = ''
            stock_open = row.xpath('./td[2]/text()').extract()
            if stock_open:
                stock_open = stock_open[0]
            else:
                stock_open = ''
            stock_close = row.xpath('./td[3]/text()').extract()
            if stock_close:
                stock_close = stock_close[0]
            else:
                stock_close = ''
            stock_change = row.xpath('./td[4]/span/text()').extract()
            if stock_change:
                stock_change = stock_change[0]
            else:
                stock_change = ''
            stock_percent_change = row.xpath('./td[5]/span/text()').extract()
            if stock_percent_change:
                stock_percent_change = stock_percent_change[0]
            else:
                stock_percent_change = ''
            stock_matching_volumn = row.xpath('./td[6]/text()').extract()
            if stock_matching_volumn:
                stock_matching_volumn = stock_matching_volumn[0]
            else:
                stock_matching_volumn = ''
            stock_put_through_volumn = row.xpath('./td[7]/text()').extract()
            if stock_put_through_volumn:
                stock_put_through_volumn = stock_put_through_volumn[0]
            else:
                stock_put_through_volumn = ''
            stock_total_volumn = row.xpath('./td[8]/text()').extract()
            if stock_total_volumn:
                stock_total_volumn = stock_total_volumn[0]
            else:
                stock_total_volumn = ''
            stock_matching_value = row.xpath('./td[9]/text()').extract()
            if stock_matching_value:
                stock_matching_value = stock_matching_value[0]
            else:
                stock_matching_value = ''
            stock_put_through_value = row.xpath('./td[10]/text()').extract()
            if stock_put_through_value:
                stock_put_through_value = stock_put_through_value[0]
            else:
                stock_put_through_value = ''
            stock_total_value = row.xpath('./td[11]/text()').extract()
            if stock_total_value:
                stock_total_value = stock_total_value[0]
            else:
                stock_total_value = ''

            yield {
            'stock_code': stock_code,
            'stock_date': stock_date,
            'stock_open': stock_open,
            'stock_close': stock_close,
            'stock_change': stock_change,
            'stock_percent_change': stock_percent_change,
            'stock_matching_volumn': stock_matching_volumn,
            'stock_put_through_volumn': stock_put_through_volumn,
            'stock_total_volumn': stock_total_volumn,
            'stock_matching_value': stock_matching_value,
            'stock_put_through_value': stock_put_through_value,
            'stock_total_value': stock_total_value
            }
            sqlstatement = 'insert into tvsi_stock_price (stock_code, stock_date, stock_open, stock_close, stock_change, stock_percent_change, stock_matching_volumn, stock_put_through_volumn, stock_total_volumn, stock_matching_value, stock_put_through_value, stock_total_value) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)'
            cur.execute(sqlstatement, (stock_code, stock_date, stock_open, stock_close, stock_change, stock_percent_change, stock_matching_volumn, stock_put_through_volumn, stock_total_volumn, stock_matching_value, stock_put_through_value, stock_total_value))
            con.commit()

            f.write(stock_code + ', ' + stock_date + ', ' + stock_open + ', ' + stock_close + ', ' + stock_change + ', ' + stock_percent_change + ', ' + stock_matching_volumn + ', ' + stock_put_through_volumn + ', ' + stock_total_volumn + ', ' + stock_matching_value + ', ' + stock_put_through_value + ', ' + stock_total_value + '\n')

        f.close()
        cur.close()
        con.close()
