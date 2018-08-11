# -*- coding: utf-8 -*-
import scrapy
import cx_Oracle
from scrapy_splash import SplashRequest


class VcscFinancialRatioSpider(scrapy.Spider):
    name = 'vcsc_financial_ratio'
    download_delay = 2
    allowed_domains = ['ra.vcsc.com.vn']
    start_urls = ['http://ra.vcsc.com.vn/']

    def start_requests(self):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        runletter = 'A'

        sqlstatement = 'select stock_code from vcsc_stock_entity where stock_code like (' + "'" + runletter + "%'" ')'
        curstatement = cur.execute(sqlstatement)
        stockcodes = curstatement.fetchall()
        stocklists = []

        for stockcode in stockcodes:
            stocklists.append(stockcode[0])

        for stocklist in stocklists:
            url = 'http://ra.vcsc.com.vn/Financial/PV_FinancialRatio?ticker=' + stocklist + '&filter=0&unit=1000000000'
            yield scrapy.Request(url, self.parse, meta={'splash': {'endpoint': 'render.html',}, 'runletter': runletter, 'stocklist': stocklist}, cookies={'ASP.NET_SessionId':'i0esd5fzell4l52fer5ika3a', 'KTCK_StockFeed_Stoxvn_CookieCulture':'en-US'})

        cur.close()
        con.close()

    def parse(self, response):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        runletter = response.meta.get('runletter')
        stocklist = response.meta.get('stocklist')

        f = open("vcsc-" + runletter + ".csv", "a")

        factors = response.xpath('//div[@class=financial-block]')
        rows = response.xpath('//div[@class="jcarousel"]/ul/li')

        for row in rows:
            data_year = row.xpath('./p/b/text()').extract()[0].strip()
            factor_g1p2 = response.xpath('//body/div/div[@group="1"]/p[2]/text()').extract()[1].strip()
            data_g1p2 = row.xpath('./div[@group="1"]/p[2]/text()').extract()[0].strip()
            factor_g1p3 = response.xpath('//body/div/div[@group="1"]/p[3]/text()').extract()[1].strip()
            data_g1p3 = row.xpath('./div[@group="1"]/p[3]/text()').extract()[0].strip()
            factor_g1p4 = response.xpath('//body/div/div[@group="1"]/p[4]/text()').extract()[1].strip()
            data_g1p4 = row.xpath('./div[@group="1"]/p[4]/text()').extract()[0].strip()
            factor_g1p5 = response.xpath('//body/div/div[@group="1"]/p[5]/text()').extract()[1].strip()
            data_g1p5 = row.xpath('./div[@group="1"]/p[5]/text()').extract()[0].strip()

            factor_g2p2 = response.xpath('//body/div/div[@group="2"]/p[2]/text()').extract()[1].strip()
            data_g2p2 = row.xpath('./div[@group="2"]/p[2]/text()').extract()[0].strip()
            factor_g2p3 = response.xpath('//body/div/div[@group="2"]/p[3]/text()').extract()[1].strip()
            data_g2p3 = row.xpath('./div[@group="2"]/p[3]/text()').extract()[0].strip()
            factor_g2p4 = response.xpath('//body/div/div[@group="2"]/p[4]/text()').extract()[1].strip()
            data_g2p4 = row.xpath('./div[@group="2"]/p[4]/text()').extract()[0].strip()
            factor_g2p5 = response.xpath('//body/div/div[@group="2"]/p[5]/text()').extract()[1].strip()
            data_g2p5 = row.xpath('./div[@group="2"]/p[5]/text()').extract()[0].strip()

            factor_g3p2 = response.xpath('//body/div/div[@group="3"]/p[2]/text()').extract()[1].strip()
            data_g3p2 = row.xpath('./div[@group="3"]/p[2]/text()').extract()[0].strip()
            factor_g3p3 = response.xpath('//body/div/div[@group="3"]/p[3]/text()').extract()[1].strip()
            data_g3p3 = row.xpath('./div[@group="3"]/p[3]/text()').extract()[0].strip()
            factor_g3p4 = response.xpath('//body/div/div[@group="3"]/p[4]/text()').extract()[0].strip()
            data_g3p4 = row.xpath('./div[@group="3"]/p[4]/text()').extract()[0].strip()
            factor_g3p5 = response.xpath('//body/div/div[@group="3"]/p[5]/text()').extract()[0].strip()
            data_g3p5 = row.xpath('./div[@group="3"]/p[5]/text()').extract()[0].strip()
            factor_g3p6 = response.xpath('//body/div/div[@group="3"]/p[6]/text()').extract()[0].strip()
            data_g3p6 = row.xpath('./div[@group="3"]/p[6]/text()').extract()[0].strip()
            factor_g3p7 = response.xpath('//body/div/div[@group="3"]/p[7]/text()').extract()[0].strip()
            data_g3p7 = row.xpath('./div[@group="3"]/p[7]/text()').extract()[0].strip()
            factor_g3p8 = response.xpath('//body/div/div[@group="3"]/p[8]/text()').extract()[0].strip()
            data_g3p8 = row.xpath('./div[@group="3"]/p[8]/text()').extract()[0].strip()

            factor_g4p2 = response.xpath('//body/div/div[@group="4"]/p[2]/text()').extract()[1].strip()
            data_g4p2 = row.xpath('./div[@group="4"]/p[2]/text()').extract()[0].strip()
            factor_g4p3 = response.xpath('//body/div/div[@group="4"]/p[3]/text()').extract()[0].strip()
            data_g4p3 = row.xpath('./div[@group="4"]/p[3]/text()').extract()[0].strip()
            factor_g4p4 = response.xpath('//body/div/div[@group="4"]/p[4]/text()').extract()[0].strip()
            data_g4p4 = row.xpath('./div[@group="4"]/p[4]/text()').extract()[0].strip()
            factor_g4p5 = response.xpath('//body/div/div[@group="4"]/p[5]/text()').extract()[0].strip()
            data_g4p5 = row.xpath('./div[@group="4"]/p[5]/text()').extract()[0].strip()
            factor_g4p6 = response.xpath('//body/div/div[@group="4"]/p[6]/text()').extract()[0].strip()
            data_g4p6 = row.xpath('./div[@group="4"]/p[6]/text()').extract()[0].strip()

            factor_g5p2 = response.xpath('//body/div/div[@group="5"]/p[2]/text()').extract()[0].strip()
            data_g5p2 = row.xpath('./div[@group="5"]/p[2]/text()').extract()[0].strip()
            factor_g5p3 = response.xpath('//body/div/div[@group="5"]/p[3]/text()').extract()[0].strip()
            data_g5p3 = row.xpath('./div[@group="5"]/p[3]/text()').extract()[0].strip()
            factor_g5p4 = response.xpath('//body/div/div[@group="5"]/p[4]/text()').extract()[0].strip()
            data_g5p4 = row.xpath('./div[@group="5"]/p[4]/text()').extract()[0].strip()
            factor_g5p5 = response.xpath('//body/div/div[@group="5"]/p[5]/text()').extract()[0].strip()
            data_g5p5 = row.xpath('./div[@group="5"]/p[5]/text()').extract()[0].strip()
            factor_g5p6 = response.xpath('//body/div/div[@group="5"]/p[6]/text()').extract()[0].strip()
            data_g5p6 = row.xpath('./div[@group="5"]/p[6]/text()').extract()[0].strip()
            factor_g5p7 = response.xpath('//body/div/div[@group="5"]/p[7]/text()').extract()[0].strip()
            data_g5p7 = row.xpath('./div[@group="5"]/p[7]/text()').extract()[0].strip()

            factor_g6p2 = response.xpath('//body/div/div[@group="6"]/p[2]/text()').extract()[0].strip()
            data_g6p2 = row.xpath('./div[@group="6"]/p[2]/text()').extract()[0].strip()
            factor_g6p3 = response.xpath('//body/div/div[@group="6"]/p[3]/text()').extract()[0].strip()
            data_g6p3 = row.xpath('./div[@group="6"]/p[3]/text()').extract()[0].strip()

            factor_g7p2 = response.xpath('//body/div/div[@group="7"]/p[2]/text()').extract()[0].strip()
            data_g7p2 = row.xpath('./div[@group="7"]/p[2]/text()').extract()
            if data_g7p2:
                data_g7p2 = data_g7p2[0].strip()
            else:
                data_g7p2 = ''
            factor_g7p3 = response.xpath('//body/div/div[@group="7"]/p[3]/text()').extract()[0].strip()
            data_g7p3 = row.xpath('./div[@group="7"]/p[3]/text()').extract()
            if data_g7p3:
                data_g7p3 = data_g7p3[0].strip()
            else:
                data_g7p3 = ''
            factor_g7p4 = response.xpath('//body/div/div[@group="7"]/p[4]/text()').extract()[0].strip()
            data_g7p4 = row.xpath('./div[@group="7"]/p[4]/text()').extract()
            if data_g7p4:
                data_g7p4 = data_g7p4[0].strip()
            else:
                data_g7p4 = ''

            yield {
            'stocklist': stocklist,
            'data_year': data_year,
            'factor_g1p2': factor_g1p2,
            'data_g1p2': data_g1p2,
            'factor_g1p3': factor_g1p3,
            'data_g1p3': data_g1p3,
            'factor_g1p4': factor_g1p4,
            'data_g1p4': data_g1p4,
            'factor_g1p5': factor_g1p5,
            'data_g1p5': data_g1p5,

            'factor_g2p2': factor_g2p2,
            'data_g2p2': data_g2p2,
            'factor_g2p3': factor_g2p3,
            'data_g2p3': data_g2p3,
            'factor_g2p4': factor_g2p4,
            'data_g2p4': data_g2p4,
            'factor_g2p5': factor_g2p5,
            'data_g2p5': data_g2p5,

            'factor_g3p2': factor_g3p2,
            'data_g3p2': data_g3p2,
            'factor_g3p3': factor_g3p3,
            'data_g3p3': data_g3p3,
            'factor_g3p4': factor_g3p4,
            'data_g3p4': data_g3p4,
            'factor_g3p5': factor_g3p5,
            'data_g3p5': data_g3p5,
            'factor_g3p6': factor_g3p6,
            'data_g3p6': data_g3p6,
            'factor_g3p7': factor_g3p7,
            'data_g3p7': data_g3p7,
            'factor_g3p8': factor_g3p8,
            'data_g3p8': data_g3p8,

            'factor_g4p2': factor_g4p2,
            'data_g4p2': data_g4p2,
            'factor_g4p3': factor_g4p3,
            'data_g4p3': data_g4p3,
            'factor_g4p4': factor_g4p4,
            'data_g4p4': data_g4p4,
            'factor_g4p5': factor_g4p5,
            'data_g4p5': data_g4p5,
            'factor_g4p6': factor_g4p6,
            'data_g4p6': data_g4p6,

            'factor_g5p2': factor_g5p2,
            'data_g5p2': data_g5p2,
            'factor_g5p3': factor_g5p3,
            'data_g5p3': data_g5p3,
            'factor_g5p4': factor_g5p4,
            'data_g5p4': data_g5p4,
            'factor_g5p5': factor_g5p5,
            'data_g5p5': data_g5p5,
            'factor_g5p6': factor_g5p6,
            'data_g5p6': data_g5p6,
            'factor_g5p7': factor_g5p7,
            'data_g5p7': data_g5p7,

            'factor_g6p2': factor_g6p2,
            'data_g6p2': data_g6p2,
            'factor_g6p3': factor_g6p3,
            'data_g6p3': data_g6p3,

            'factor_g7p2': factor_g7p2,
            'data_g7p2': data_g7p2,
            'factor_g7p3': factor_g7p3,
            'data_g7p3': data_g7p3,
            'factor_g7p4': factor_g7p4,
            'data_g7p4': data_g7p4,
            }

            f.write(stocklist + ', ' + factor_g1p2 + ', ' + data_year + ', ' + data_g1p2 + '\n')
            f.write(stocklist + ', ' + factor_g1p3 + ', ' + data_year + ', ' + data_g1p3 + '\n')
            f.write(stocklist + ', ' + factor_g1p4 + ', ' + data_year + ', ' + data_g1p4 + '\n')
            f.write(stocklist + ', ' + factor_g1p5 + ', ' + data_year + ', ' + data_g1p5 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g1p2, '', data_year, data_g1p2))
            cur.execute(sqlstatement, (stocklist, factor_g1p3, '', data_year, data_g1p3))
            cur.execute(sqlstatement, (stocklist, factor_g1p4, '', data_year, data_g1p4))
            cur.execute(sqlstatement, (stocklist, factor_g1p5, '', data_year, data_g1p5))
            con.commit()

            f.write(stocklist + ', ' + factor_g2p2 + ', ' + data_year + ', ' + data_g2p2 + '\n')
            f.write(stocklist + ', ' + factor_g2p3 + ', ' + data_year + ', ' + data_g2p3 + '\n')
            f.write(stocklist + ', ' + factor_g2p4 + ', ' + data_year + ', ' + data_g2p4 + '\n')
            f.write(stocklist + ', ' + factor_g2p5 + ', ' + data_year + ', ' + data_g2p5 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g2p2, '', data_year, data_g2p2))
            cur.execute(sqlstatement, (stocklist, factor_g2p3, '', data_year, data_g2p3))
            cur.execute(sqlstatement, (stocklist, factor_g2p4, '', data_year, data_g2p4))
            cur.execute(sqlstatement, (stocklist, factor_g2p5, '', data_year, data_g2p5))
            con.commit()

            f.write(stocklist + ', ' + factor_g3p2 + ', ' + data_year + ', ' + data_g3p2 + '\n')
            f.write(stocklist + ', ' + factor_g3p3 + ', ' + data_year + ', ' + data_g3p3 + '\n')
            f.write(stocklist + ', ' + factor_g3p4 + ', ' + data_year + ', ' + data_g3p4 + '\n')
            f.write(stocklist + ', ' + factor_g3p5 + ', ' + data_year + ', ' + data_g3p5 + '\n')
            f.write(stocklist + ', ' + factor_g3p6 + ', ' + data_year + ', ' + data_g3p6 + '\n')
            f.write(stocklist + ', ' + factor_g3p7 + ', ' + data_year + ', ' + data_g3p7 + '\n')
            f.write(stocklist + ', ' + factor_g3p8 + ', ' + data_year + ', ' + data_g3p8 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g3p2, '', data_year, data_g3p2))
            cur.execute(sqlstatement, (stocklist, factor_g3p3, '', data_year, data_g3p3))
            cur.execute(sqlstatement, (stocklist, factor_g3p4, '', data_year, data_g3p4))
            cur.execute(sqlstatement, (stocklist, factor_g3p5, '', data_year, data_g3p5))
            cur.execute(sqlstatement, (stocklist, factor_g3p6, '', data_year, data_g3p6))
            cur.execute(sqlstatement, (stocklist, factor_g3p7, '', data_year, data_g3p7))
            cur.execute(sqlstatement, (stocklist, factor_g3p8, '', data_year, data_g3p8))
            con.commit()

            f.write(stocklist + ', ' + factor_g4p2 + ', ' + data_year + ', ' + data_g4p2 + '\n')
            f.write(stocklist + ', ' + factor_g4p3 + ', ' + data_year + ', ' + data_g4p3 + '\n')
            f.write(stocklist + ', ' + factor_g4p4 + ', ' + data_year + ', ' + data_g4p4 + '\n')
            f.write(stocklist + ', ' + factor_g4p5 + ', ' + data_year + ', ' + data_g4p5 + '\n')
            f.write(stocklist + ', ' + factor_g4p6 + ', ' + data_year + ', ' + data_g4p6 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g4p2, '', data_year, data_g4p2))
            cur.execute(sqlstatement, (stocklist, factor_g4p3, '', data_year, data_g4p3))
            cur.execute(sqlstatement, (stocklist, factor_g4p4, '', data_year, data_g4p4))
            cur.execute(sqlstatement, (stocklist, factor_g4p5, '', data_year, data_g4p5))
            cur.execute(sqlstatement, (stocklist, factor_g4p6, '', data_year, data_g4p6))
            con.commit()

            f.write(stocklist + ', ' + factor_g5p2 + ', ' + data_year + ', ' + data_g5p2 + '\n')
            f.write(stocklist + ', ' + factor_g5p3 + ', ' + data_year + ', ' + data_g5p3 + '\n')
            f.write(stocklist + ', ' + factor_g5p4 + ', ' + data_year + ', ' + data_g5p4 + '\n')
            f.write(stocklist + ', ' + factor_g5p5 + ', ' + data_year + ', ' + data_g5p5 + '\n')
            f.write(stocklist + ', ' + factor_g5p6 + ', ' + data_year + ', ' + data_g5p6 + '\n')
            f.write(stocklist + ', ' + factor_g5p7 + ', ' + data_year + ', ' + data_g5p7 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g5p2, '', data_year, data_g5p2))
            cur.execute(sqlstatement, (stocklist, factor_g5p3, '', data_year, data_g5p3))
            cur.execute(sqlstatement, (stocklist, factor_g5p4, '', data_year, data_g5p4))
            cur.execute(sqlstatement, (stocklist, factor_g5p5, '', data_year, data_g5p5))
            cur.execute(sqlstatement, (stocklist, factor_g5p6, '', data_year, data_g5p6))
            cur.execute(sqlstatement, (stocklist, factor_g5p7, '', data_year, data_g5p7))
            con.commit()

            f.write(stocklist + ', ' + factor_g6p2 + ', ' + data_year + ', ' + data_g6p2 + '\n')
            f.write(stocklist + ', ' + factor_g6p3 + ', ' + data_year + ', ' + data_g6p3 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g6p2, '', data_year, data_g6p2))
            cur.execute(sqlstatement, (stocklist, factor_g6p3, '', data_year, data_g6p3))
            con.commit()

            f.write(stocklist + ', ' + factor_g7p2 + ', ' + data_year + ', ' + data_g7p2 + '\n')
            f.write(stocklist + ', ' + factor_g7p3 + ', ' + data_year + ', ' + data_g7p3 + '\n')
            f.write(stocklist + ', ' + factor_g7p4 + ', ' + data_year + ', ' + data_g7p4 + '\n')

            sqlstatement = 'insert into vcsc_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
            cur.execute(sqlstatement, (stocklist, factor_g7p2, '', data_year, data_g7p2))
            cur.execute(sqlstatement, (stocklist, factor_g7p3, '', data_year, data_g7p3))
            cur.execute(sqlstatement, (stocklist, factor_g7p4, '', data_year, data_g7p4))
            con.commit()

        f.close()
        cur.close()
        con.close()
