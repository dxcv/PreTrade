# -*- coding: utf-8 -*-
# @Time    : 2019/2/22 16:45
# @Author  : ZouJunLin
"""爬取持仓共用的API"""
import re
import csv,os,codecs

def GetDCEPosition(info,TradingDay,ExchangeID):

    sql="select InstrumentID from SettlementInfo where TradingDay='%s' and Position>20000 and ExchangeID='%s' and IsFuture=1"%(TradingDay.strftime("%Y-%m-%d"),ExchangeID)
    templist=info.mysql.ExecQueryGetList(sql)
    for i in templist:
        code = str(re.match(r"\D+", i).group())
        url="http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html?"
        url=url+"memberDealPosiQuotes.variety=%s&memberDealPosiQuotes.trade_type=0&year=%s&month=%s&day=%s&contract.contract_id=%s&contract.variety_id=%s&contract="
        url=url % (code, TradingDay.year, TradingDay.month - 1, TradingDay.day,i,code)
        info.Set_QryPosition(ExchangeID,url, code, i,TradingDay.strftime("%Y%m%d"))
        GetDCEPositionProductData(info)


def GetDCEPositionProductData(info):
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=8E650BA1F3AFEAB1611F8ADC6C186BD1; WMONID=j1TJsMZrARA; Hm_lvt_a50228174de2a93aee654389576b60fb=1547791080,1548045741,1548146303,1548147105; sssssss=516c92f7sssssss_516c92f7',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    print info.QryPositionurl
    html=info.mysplider.getUrlcontent(info.QryPositionurl,header=header)
    listdata=info.mysplider.tableTolistByNum(html,"DCE",1)

    """write to xls"""

    ListDataToExcel(info,listdata,".csv")

    # raise Exception

def GetDCEStagedTurnover(info,TradingDay,ExchangeID):

    beginmonth=TradingDay.strftime("%Y%m")
    endmonth=TradingDay.strftime("%Y%m")
    url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealCh.html?"
    url = url + "memberDealQuotes.variety = %s & memberDealQuotes.trade_type = 0& memberDealQuotes.begin_month = %s & memberDealQuotes.end_month = %s"
    sql="SELECT [InstrumentCode] FROM [PreTrade].[dbo].[ContractCode] where [ExchangeID]='DCE'"
    templist = info.mysql.ExecQueryGetList(sql)
    for i in templist:
        url = url % (i, TradingDay.month - 1, TradingDay.month - 1)
        print url



def ListDataToExcel(info,listdata,ext):
    """a public method  that list data write ext extension file"""
    parent="D:/GitData/PositionData/"

    if not os.path.exists(parent + "/" + info.QryPositionExchangeID):
        os.mkdir(parent + "/" + info.QryPositionExchangeID)
    if not os.path.exists(parent + "/" + info.QryPositionExchangeID+ "/" + info.QryPositionTradingDay):
        os.mkdir(parent+ "/" + info.QryPositionExchangeID + "/" + info.QryPositionTradingDay)
    filename=info.QryPositionTradingDay+"_"+info.QryPositionInstrumentID+ext


    # file_backup=f = codecs.open(parent+info.QryPositionExchangeID+"/"+filename,'wb','utf-8')
    csvfile = file(parent+info.QryPositionExchangeID+"/" + info.QryPositionTradingDay+ "/"+filename, 'wb')
    csvfile.write(codecs.BOM_UTF8)
    writer=csv.writer(csvfile)

    writer.writerows(listdata)
    csvfile.close()
