# -*- coding: utf-8 -*-
# @Time    : 2018/11/30 15:41
# @Author  : ZouJunLin
"""交易日获取，并且写入交易日数据库"""

from NextTradingDay import *
from utils.InfoApi import *
from utils.sqlServer import *


NoTradesql="""
    select * from [HistoryNoTdCalendar] order by NoTradingDay desc
"""

Exitsql="""
    SELECT * FROM [dbo].[HistoryTradCalendar] order by [TradingDay] desc
"""

InsertSql="""
    INSERT INTO [dbo].[HistoryTradCalendar] ([TradingDay])VALUES ('%s')
"""

startsql="""
SELECT TOP (1) [TradingDay]
  FROM [StatisticData].[dbo].[HistoryTradCalendar] order by TradingDay desc
"""
NoTradeList=list()          #保存非交易日

SqlList=list()              #保存交易日

ExitList=list()             #保存数据库已经存在的交易日


def GetTradingDay(startDay,enDay):
    while startDay<enDay:
        startDay=t.NextTradingDay(startDay,True)
        SqlList.append(startDay)
    return list(set(SqlList)-set(ExitList))



if __name__=='__main__':
    infoapi = InfoApi()
    t = TradingDay(infoapi)
    mysql=infoapi.GetDbHistoryConnect()
    tempList = mysql.ExecQuery(NoTradesql)

    for i in tempList:
        NoTradeList.append(str(i[0]).encode("utf-8").strip())

    mysql1 =infoapi.GetStaticDataconnect()
    tempList1=mysql1.ExecQuery(Exitsql)

    startDay = str(tempList1[0][0]).encode("utf-8").strip()
    startDay='20181230'
    enDay = (datetime.datetime.now()).strftime("%Y%m%d")


    for i in tempList1:
        ExitList.append(str(i[0]).encode("utf-8").strip())

    SqlList=GetTradingDay(startDay,enDay)
    mysql1.ExecmanysNonQuery(InsertSql,SqlList)