# -*- coding: utf-8 -*-
import scrapy
import cx_Oracle
from scrapy_splash import SplashRequest


class TvsiFinancialRatioSpider(scrapy.Spider):
    name = 'tvsi_financial_ratio'
    download_delay = 1
    allowed_domains = ['finance.tvsi.com.vn']

    def start_requests(self):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        stockyears = ['2018', '2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010', '2009', '2008']
        #stockyears = ['2013', '2012', '2011', '2010', '2009', '2008']

        runindustry = 'Building, Construction'

        stockperiod = '1'

        sqlstatement = 'select stock_code from tvsi_stock_entity where stock_industry in (' + "'" + runindustry + "'" ')'
        curstatement = cur.execute(sqlstatement)
        stockcodes = curstatement.fetchall()
        stocklists = []

        for stockcode in stockcodes:
            stocklists.append(stockcode[0])

        for stockyear in stockyears:
            for stocklist in stocklists:
                url = 'http://finance.tvsi.com.vn/Enterprises/chitieutaichinh?symbol=' + stocklist + '&YearView=' + stockyear + '&period=' + stockperiod + '&donvi=1000000'
                yield scrapy.Request(url, self.parse, meta={'splash': {'endpoint': 'render.html',}, 'stockyear': stockyear, 'runindustry': runindustry, 'stockperiod': stockperiod}, cookies={'fp_tvsi_lang':'en-US'})

        cur.close()
        con.close()

    def parse(self, response):

        con = cx_Oracle.connect('stock/password1@127.0.0.1/xe')
        cur = con.cursor()

        stockyear = response.meta.get('stockyear')
        runindustry = response.meta.get('runindustry')
        stockperiod = response.meta.get('stockperiod')

        f = open("tvsi-" + runindustry + "-" + stockyear + ".csv", "a")

        rows = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level=2]')

        for row in rows:
            stock_code = response.url.split("?symbol=")[1].split("&YearView=")[0]
            stock_factor = row.xpath('./td[1]/div[@class="label"]/text()').extract()[0]

            data_date1 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[3]/text()').extract()
            if data_date1:
                if stockperiod == '1':
                    data_quarter1 = data_date1[0].split(" ")[0]
                    data_year1 = data_date1[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter1 = data_date1[0].split(" ")[0]
                    data_year1 = data_date1[0].split(" ")[1]
                stock_data1 = row.xpath('./td[3]/text()').extract()
                if stock_data1:
                    stock_data1 = stock_data1[0]
                else:
                    stock_data1 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter1,
                'stock_year': data_year1,
                'stock_data': stock_data1,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter1, data_year1, stock_data1))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter1 + ', ' + data_year1 + ', ' + stock_data1 + '\n')

            data_date2 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[4]/text()').extract()
            if data_date2:
                if stockperiod == '1':
                    data_quarter2 = data_date2[0].split(" ")[0]
                    data_year2 = data_date2[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter2 = data_date2[0].split(" ")[0]
                    data_year2 = data_date2[0].split(" ")[1]
                stock_data2 = row.xpath('./td[4]/text()').extract()
                if stock_data2:
                    stock_data2 = stock_data2[0]
                else:
                    stock_data2 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter2,
                'stock_year': data_year2,
                'stock_data': stock_data2,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter2, data_year2, stock_data2))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter2 + ', ' + data_year2 + ', ' + stock_data2 + '\n')

            data_date3 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[5]/text()').extract()
            if data_date3:
                if stockperiod == '1':
                    data_quarter3 = data_date3[0].split(" ")[0]
                    data_year3 = data_date3[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter3 = data_date3[0].split(" ")[0]
                    data_year3 = data_date3[0].split(" ")[1]
                stock_data3 = row.xpath('./td[5]/text()').extract()
                if stock_data3:
                    stock_data3 = stock_data3[0]
                else:
                    stock_data3 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter3,
                'stock_year': data_year3,
                'stock_data': stock_data3,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter3, data_year3, stock_data3))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter3 + ', ' + data_year3 + ', ' + stock_data3 + '\n')

            data_date4 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[6]/text()').extract()
            if data_date4:
                if stockperiod == '1':
                    data_quarter4 = data_date4[0].split(" ")[0]
                    data_year4 = data_date4[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter4 = data_date4[0].split(" ")[0]
                    data_year4 = data_date4[0].split(" ")[1]
                stock_data4 = row.xpath('./td[6]/text()').extract()
                if stock_data4:
                    stock_data4 = stock_data4[0]
                else:
                    stock_data4 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter4,
                'stock_year': data_year4,
                'stock_data': stock_data4,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter4, data_year4, stock_data4))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter4 + ', ' + data_year4 + ', ' + stock_data4 + '\n')

            data_date5 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[7]/text()').extract()
            if data_date5:
                if stockperiod == '1':
                    data_quarter5 = data_date5[0].split(" ")[0]
                    data_year5 = data_date5[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter5 = data_date5[0].split(" ")[0]
                    data_year5 = data_date5[0].split(" ")[1]
                stock_data5 = row.xpath('./td[7]/text()').extract()
                if stock_data5:
                    stock_data5 = stock_data5[0]
                else:
                    stock_data5 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter5,
                'stock_year': data_year5,
                'stock_data': stock_data5,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter5, data_year5, stock_data5))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter5 + ', ' + data_year5 + ', ' + stock_data5 + '\n')

            data_date6 = response.xpath('//table[@id="table_cttc"]/tbody/tr[@data-level="header"]/td[8]/text()').extract()
            if data_date6:
                if stockperiod == '1':
                    data_quarter6 = data_date6[0].split(" ")[0]
                    data_year6 = data_date6[0].split(" ")[0]
                elif stockperiod == '2':
                    data_quarter6 = data_date6[0].split(" ")[0]
                    data_year6 = data_date6[0].split(" ")[1]
                stock_data6 = row.xpath('./td[8]/text()').extract()
                if stock_data6:
                    stock_data6 = stock_data6[0]
                else:
                    stock_data6 = ''
                yield {
                'stock_code': stock_code,
                'stock_factor': stock_factor,
                'stock_quarter': data_quarter6,
                'stock_year': data_year6,
                'stock_data': stock_data6,
                }
                sqlstatement = 'insert into tvsi_financial_ratio (stock_code, stock_factor, stock_quarter, stock_year, stock_data) values (:1, :2, :3, :4, :5)'
                cur.execute(sqlstatement, (stock_code, stock_factor, data_quarter6, data_year6, stock_data6))
                con.commit()

                f.write(stock_code + ', ' + stock_factor + ', ' + data_quarter6 + ', ' + data_year6 + ', ' + stock_data6 + '\n')

        f.close()
        cur.close()
        con.close()
