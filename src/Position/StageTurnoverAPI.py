# -*- coding: utf-8 -*-
# @Time    : 2019/4/4 10:51
# @Author  : ZouJunLin
import chardet
from bs4 import BeautifulSoup

def  GetCZCEStagedTurnover(info, startdate,ExchangeId):
    url="http://app.czce.com.cn/cms/QueryTradeHolding?querytype=tradeamt&beginDate=%s&endDate=%s&classid=%s"
    TradingDay=startdate.strftime("%Y-%m-%d")
    templists=list()

    codelist=info.GetExchangeProduct(ExchangeId)
    for code in codelist:
        Url=url%(TradingDay,TradingDay,code)
        temp=GetCodeTurnover(info,ExchangeId,Url,code,TradingDay)

def GetCodeTurnover(info,ExchangeId,Url,code,TradingDay):
    templist=list()
    print Url
    header = info.GetExchangeHeader(ExchangeId)
    html=info.mysplider.getUrlcontent(Url,header=header)
    table=info.mysplider.tableTolistById(html,ExchangeId,"senfe")
    for i in table:
        col=[]
        if str(i[0]).strip().find("名次")==-1 and str(i[0]).strip().find("共计")==-1 and str(i[0]).strip().find("总计")==-1:
            InstrumentID=code
            Rank=str(i[0]).strip()
            Type='0'
            ParticipantID1=str(i[1]).strip()
            ParticipantABBR1=str(i[3]).strip()
            CJ1=str(i[4]).strip()
            ParticipantID2 = str(i[6]).strip()
            ParticipantABBR2 = str(i[7]).strip()
            CJ2=str(i[8]).strip()
            col=[TradingDay, InstrumentID, ExchangeId, Rank,Type, ParticipantABBR1, CJ1, ParticipantABBR2, CJ2]
            print col,col[-2]

    # html=BeautifulSoup(html, "html.parser")
    # divdata = html.find("table",id="senfe")
    # if divdata is None:
    #     bs = BeautifulSoup(html, "lxml")
    #     divdata = bs.find("table",id="senfe")
    # try:
    #     trdata = divdata.findAll("tr")
    # except:
    #     print "网络异常，部分数据爬取失败，重新运行"
    #     return []
    # for tr in trdata:
    #     td = tr.findAll("td")
    #     col = []
    #     if len(td) > 0:
    #         for i in td:
    #             temp = str(i.text.strip().encode("utf-8")).strip().replace(",", "").replace("绝对值", '0').replace("比例值",
    #                                                                                                             '1')
    #             if temp == '-':
    #                 temp = temp.strip("-")
    #             col.append(temp)
    #
    #         col.insert(2, ExchangeId)
    #     print col