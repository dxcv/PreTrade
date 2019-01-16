# -*- coding: utf-8 -*-
# @Time    : 2018/11/23 17:39
# @Author  : ZouJunLin
"""计算平均成交量所用到的API"""
from utils.Mysplider import *
import datetime,sys
from utils.TradingDay.NextTradingDay import *

IsexistAvgsql ="SELECT top 1 [TradingDay] FROM [StatisticData].[dbo].[AvgTotalVol] where InstrumentID='%s' order by TradingDay desc"

StartDaysql="SELECT top 1  [TradingDay]  FROM [PreTrade].[dbo].[SettlementInfo] where InstrumentID='%s' order by TradingDay"

def ComputerAvgEachInstrumentID(info,InstrumentID):
    """计算每一个合约的平均成交量"""

    """首先判断数据库是否有计算出之前的合约平均成交量信息"""
    IsexistAvgday=IsExistData(IsexistAvgsql%InstrumentID,info)
    startDay = IsExistData(StartDaysql % InstrumentID,info)[0][0]
    enDay = (datetime.datetime.now()).strftime("%Y-%m-%d")
    # enDay="2018-11-30"

    if len(IsexistAvgday):
        """如果存在，取出来，接着这一天的数据继续计算"""
        if str(IsexistAvgday[0][0]).strip()==enDay:
            return []
        startDay5,startDay10,startDay20,startDay60,startDay120,startDayYear=GetNumDaysBegin(info,startDay,str(IsexistAvgday[0][0]).strip())
        """如果存在，开始计算时间和startDay5相同"""
        return GetavegVol(info,startDay5,startDay5,startDay10,startDay20,startDay60,startDay120,startDayYear,enDay,InstrumentID)

    else:
        """如果不存在，开始计算时间为该合约的起始交易时间"""
        startDay5, startDay10, startDay20, startDay60, startDay120 ,startDayYear= GetNumDaysBegin(info,startDay,0)
        return GetavegVol(info,startDay,startDay5,startDay10,startDay20,startDay60,startDay120,startDayYear,enDay,InstrumentID)


def GetavegVol(info,startDay,startDay5,startDay10,startDay20,startDay60,startDay120,startDayYear,enDay,InstrumentID):
    """根据合约的起始时间计算平均成交量"""
    avgsql="""
     select max(TradingDay) as TradingDay,max(InstrumentId) as InstrumentID,AVG([Volume]) as [Volume]  from(
	  select top %s Volume,TradingDay,InstrumentId from [PreTrade].[dbo].[SettlementInfo]  where [InstrumentId]='%s' and  [TradingDay]<='%s' order by  [TradingDay] desc) a
    """

    volitasql="""
        select max(TradingDay) as TradingDay,max(InstrumentId) as InstrumentID,AVG([Volume]) as [Volume],AVG(CSPriceChange) as cs  from(
	          select top %s Volume,CSPriceChange,TradingDay,InstrumentId from [PreTrade].[dbo].[SettlementInfo]  where [InstrumentId]='%s' and  [TradingDay]<='%s' order by  [TradingDay] desc) a
    """

    sqllist=[]
    t = TradingDay(info)
    while startDay<=enDay:
        if  startDay<startDay5:
            tradingday=startDay
            instrumentid=InstrumentID
            avg5=avg10=avg20=avg60=avg120=avgYear=volati20=volatiYear="-"
        elif startDay<startDay10:
            avg10=avg20=avg60=avg120=avgYear=volati20=volatiYear="-"
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday,instrumentid,avg5=templist[0]
        elif startDay<startDay20:
            avg20=avg60=avg120=avgYear=volati20=volatiYear="-"
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday, instrumentid, avg5 = templist[0]
            templist = IsExistData(avgsql % (10, InstrumentID, startDay),info)
            tradingday, instrumentid, avg10 = templist[0]
        elif startDay<startDay60:

            avg60=avg120=avgYear=volatiYear="-"
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday, instrumentid, avg5 = templist[0]
            templist = IsExistData(avgsql % (10, InstrumentID, startDay),info)
            tradingday, instrumentid, avg10 = templist[0]
            templist = IsExistData(volitasql % (20, InstrumentID, startDay),info)
            tradingday, instrumentid, avg20,volati20 = templist[0]
            volati20 = float('%.3f' % volati20)
        elif startDay<startDay120:
            avg120=avgYear=volatiYear="-"
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday, instrumentid, avg5 = templist[0]
            templist = IsExistData(avgsql % (10, InstrumentID, startDay),info)
            tradingday, instrumentid, avg10 = templist[0]
            templist = IsExistData(volitasql % (20, InstrumentID, startDay),info)
            tradingday, instrumentid, avg20, volati20 = templist[0]
            templist = IsExistData(avgsql % (60, InstrumentID, startDay),info)
            tradingday, instrumentid, avg60 = templist[0]
            volati20 = float('%.3f' % volati20)
        elif startDay<startDayYear:
            avgYear=volatiYear='-'
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday, instrumentid, avg5 = templist[0]
            templist = IsExistData(avgsql % (10, InstrumentID, startDay),info)
            tradingday, instrumentid, avg10 = templist[0]
            templist = IsExistData(volitasql % (20, InstrumentID, startDay),info)
            tradingday, instrumentid, avg20, volati20 = templist[0]
            templist = IsExistData(avgsql % (60, InstrumentID, startDay),info)
            tradingday, instrumentid, avg60 = templist[0]
            templist = IsExistData(avgsql % (120, InstrumentID, startDay),info)
            tradingday, instrumentid, avg120 = templist[0]
            volati20 = float('%.3f' % volati20)
        else:
            templist = IsExistData(avgsql % (5, InstrumentID, startDay),info)
            tradingday, instrumentid, avg5 = templist[0]
            templist = IsExistData(avgsql % (10, InstrumentID, startDay),info)
            tradingday, instrumentid, avg10 = templist[0]
            templist = IsExistData(volitasql % (20, InstrumentID, startDay),info)
            tradingday, instrumentid, avg20, volati20 = templist[0]
            templist = IsExistData(avgsql % (60, InstrumentID, startDay),info)
            tradingday, instrumentid, avg60 = templist[0]
            templist = IsExistData(avgsql % (120, InstrumentID, startDay),info)
            tradingday, instrumentid, avg120 = templist[0]
            templist = IsExistData(volitasql % (256, InstrumentID, startDay),info)
            tradingday, instrumentid, avgYear,volatiYear = templist[0]
            volati20=float('%.3f'%volati20)
            volatiYear=float('%.3f'%volatiYear)

        col=[startDay, instrumentid, avg5, avg10, avg20, avg60, avg120,avgYear,volati20,volatiYear]
        startDay=GetlastNumDayIn(info.mysql1, 2,str(startDay).replace("-", ""))
        startDay=datetime.datetime.strptime(startDay,"%Y%m%d").strftime("%Y-%m-%d")
        sqllist.append(tuple(col))
    return sqllist


def GetNumDaysBegin(info,startDay,beginDay):
    """计算5/10/20/60/120/240天的起始时间"""

    if beginDay:
        # startDay5 = GetNumDaysIn(str(beginDay).replace("-", ""), 1, True)
        startDay5=GetlastNumDayIn(info.mysql1,2,str(beginDay).replace("-", ""))
        startDay5 = datetime.datetime.strptime(startDay5, "%Y%m%d").strftime("%Y-%m-%d")
    else:
        startDay5 = GetlastNumDayIn(info.mysql1, 5,str(startDay).replace("-", ""))
        startDay5 = datetime.datetime.strptime(startDay5, "%Y%m%d").strftime("%Y-%m-%d")

    startDay10 =GetlastNumDayIn(info.mysql1, 10,str(startDay).replace("-", ""))
    startDay10 = datetime.datetime.strptime(startDay10, "%Y%m%d").strftime("%Y-%m-%d")
    if startDay10 < startDay5:
        startDay10 = startDay5

    startDay20 = GetlastNumDayIn(info.mysql1, 20,str(startDay).replace("-", ""))
    startDay20 = datetime.datetime.strptime(startDay20, "%Y%m%d").strftime("%Y-%m-%d")
    if startDay20 < startDay5:
        startDay20 = startDay5

    startDay60 = GetlastNumDayIn(info.mysql1, 60, str(startDay).replace("-", ""))
    startDay60 = datetime.datetime.strptime(startDay60, "%Y%m%d").strftime("%Y-%m-%d")
    if startDay60 < startDay5:
        startDay60 = startDay5

    startDay120 = GetlastNumDayIn(info.mysql1, 120, str(startDay).replace("-", ""))
    startDay120 = datetime.datetime.strptime(startDay120, "%Y%m%d").strftime("%Y-%m-%d")
    if startDay120 < startDay5:
        startDay120 = startDay5

    startDayYear = GetlastNumDayIn(info.mysql1, 256, str(startDay).replace("-", ""))
    startDayYear = datetime.datetime.strptime(startDayYear, "%Y%m%d").strftime("%Y-%m-%d")
    if startDayYear < startDay5:
        startDayYear= startDay5

    return startDay5,startDay10,startDay20,startDay60,startDay120,startDayYear

def GetlastNumDayIn(mysql1,num,tradingday):
    TradingDaysql = """
             select top %s * from [HistoryTradCalendar]  where [TradingDay]>='%s' order by [TradingDay]
         """
    temp=mysql1.ExecQuery(TradingDaysql%(num,tradingday))
    if len(temp)<num:
        t=num-len(temp)
        return (datetime.datetime.now()+datetime.timedelta(days=t)).strftime("%Y%m%d")
    return str(temp[num-1][0]).encode("utf-8").strip()