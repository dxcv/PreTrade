# -*- coding: utf-8 -*-
# @Time    : 2019/4/9 17:55
# @Author  : ZouJunLin
"""top 20持仓从数据库写入到excel表格"""
import datetime
from utils.InfoApi import *
from utils.TradingDay import NextTradingDay
from PositionAPI import *
saveDirector = "D:/GitData/Top20Position/"
ExchangeList=['CZCE','DCE','SHFE','CFFEX']
last_day=""

def main(startdate, mysplider,info):

    """"""
    columns = [u'名次', u'会员编号', u'会员简称', u'成交量(手)', u'增减', u'名次1', u'会员编号1', u'会员简称1', u'持买单量1', u'增减1', u'名次2', u'会员编号2', u'会员简称2', u'持卖单量2', u'增减2']
    sql="SELECT  [Rank],[ParticipantID1],[ParticipantABBR1],[CJ1],[CJ1_CHG],[Rank],[ParticipantID2],[ParticipantABBR2],[CJ2],[CJ2_CHG],[Rank] ,[ParticipantID3],[ParticipantABBR3],[CJ3],[CJ3_CHG] " \
        "FROM [PreTrade].[dbo].[Position_Top20] where TradingDay='%s' and InstrumentID='%s'"
    for i in info.PositionTop20InstrumentID.keys():
        data=info.Get2Listfromsql(sql%(startdate.strftime("%Y-%m-%d"),i))
        if len(data):
           excelDataToExcel(data,info.PositionTop20InstrumentID[i],columns,startdate.strftime("%Y%m%d"),i)

    #压缩成zip格式


if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()
    info.GetPositionTop20InstrumentID()

    t = NextTradingDay.TradingDay(info)
    startdate = datetime.datetime.strptime("20190411", "%Y%m%d")


    enddate = datetime.datetime.now()
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate,
        last_day=startdate.strftime("%Y%m%d")
        main(startdate, mysplider,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")
        # saveDirector = "D:/GitData/Top20Position/" + ExchangeID + "/" + TradingDay + "/"
        for i in ExchangeList:
            filepath=saveDirector+i + "/" + last_day + "/"
            zipDir(filepath,saveDirector+i + "/" +last_day+".zip")

    info.mysql.Disconnect()

