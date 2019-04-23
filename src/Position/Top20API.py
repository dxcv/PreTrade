# -*- coding: utf-8 -*-
# @Time    : 2019/4/8 17:03
# @Author  : ZouJunLin
"""阶段性排名前20期货公司"""
from bs4 import BeautifulSoup
from PositionAPI import *

def GetCFFEXPositionTop20(info, startdate, ExchangeID):
    insertsql = "INSERT INTO [dbo].[Position_Top20] ([TradingDay],[InstrumentID],[ExchangeID],[Rank],[Type],[ParticipantID1],[ParticipantABBR1],[CJ1],[CJ1_CHG],[ParticipantID2],[ParticipantABBR2]" \
                ",[CJ2],[CJ2_CHG],[ParticipantID3],[ParticipantABBR3],[CJ3],[CJ3_CHG]) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    codeList=info.GetExchangeProduct(ExchangeID)
    url="http://www.cffex.com.cn/sj/ccpm/%s/%s/%s.xml"
    tempdata=list()
    Tradingday=startdate.strftime("%Y%m%d")
    for code in codeList:
        Url=url%(Tradingday[:6],Tradingday[6:8],code)
        temp=GetCFFEXListTop20(info,startdate.strftime("%Y-%m-%d"),ExchangeID,Url)
        tempdata=tempdata+temp
    info.mysql.ExecmanysNonQuery(insertsql, tempdata)

def GetCFFEXListTop20(info,Tradingday,ExchangeID,url):
    header=info.GetExchangeHeader(ExchangeID)
    html = info.mysplider.getUrlcontent(url, header)
    tempdata=dict()
    temp=list()
    bs = BeautifulSoup(html, "xml")
    content = bs.findAll("data")
    for i in content:
        InstrumentID = str(i.find("instrumentid").text).strip()
        Rank=str(i.find("rank").text).strip()
        datatypeid=str(i.find("datatypeid").text).strip()
        partyid=str(i.find("partyid").text).strip()
        shortname=str(i.find("shortname").text).strip()
        volume=str(i.find("volume").text).strip()
        varvolume=str(i.find("varvolume").text).strip()
        if not  InstrumentID in tempdata.keys():
            tempdata[InstrumentID]=dict()
        if not  Rank in tempdata[InstrumentID].keys():
            tempdata[InstrumentID][Rank] =dict()
            tempdata[InstrumentID][Rank]=[Tradingday,InstrumentID,ExchangeID,Rank,'0','','','','','','','','','','','','']
        tempdata[InstrumentID][Rank][int(datatypeid) * 4 + 5] = partyid
        tempdata[InstrumentID][Rank][int(datatypeid) * 4 + 6] = shortname
        tempdata[InstrumentID][Rank][int(datatypeid) * 4 + 7] = volume
        tempdata[InstrumentID][Rank][int(datatypeid) * 4 + 8] = varvolume
    if len(tempdata):
        for i in tempdata.keys():
            for j in tempdata[i].keys():
                temp.append(tuple(tempdata[i][j]))
        return temp
    else:
        return []
