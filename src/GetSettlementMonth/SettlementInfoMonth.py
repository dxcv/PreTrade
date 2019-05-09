# -*- coding: utf-8 -*-
# @Time    : 2018/11/9 15:11
# @Author  : ZouJunLin
"""
月行情  //暂时没什么用
郑商所，中金所，上期所，大商所，结算信息，汇总
主要包括收盘价，前一天结算价，当天成交量，当天成交金额，最高价，最低价
周行情与日行情抓取方法，大致相同，唯一不同的是:查询虽然都是行情周周五去查询的，但是写入数据库的时候，周行情数据还要计算出这一周的最后一个交易日作为数据库的TradingDay
"""
import sys
import os,datetime
from multiprocessing import Process
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.InfoApi import *
from SettlementInfoMonthAPI import *
import threading
import calendar
from utils.Mysplider import *



def main(startdate, mysplider,info):
    """
    获取四大交易所的月统计信息
    :return:
    """
    # starttime = datetime.datetime.now()
    GetSettlementInfoMonth(info,startdate,"DCE")                 #大商所周统计信息
    GetSettlementInfoMonth(info,startdate,"CZCE")                #郑商所周统计信息
    GetSettlementInfoMonth(info,startdate, "SHFE")                #上期所周统计信息
    GetSettlementInfoMonth(info,startdate,"CFFEX")               #中金所周统计信息
    # endtime = datetime.datetime.now()
    # print (endtime - starttime).seconds

    # """多线程版本"""
    # starttime = datetime.datetime.now()
    # ExchangeList=info.GetAllExchange()
    # threads=[]
    # for i in ExchangeList:
    #     t=threading.Thread(target=GetSettlementInfo,args=(info,startdate,i))
    #     threads.append(t)
    # for t in threads:
    #     t.start()
    # for t in threads:
    #     t.join()
    # endtime = datetime.datetime.now()
    # print (endtime - starttime).seconds


if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = TradingDay(info)
    startdate = datetime.datetime.strptime("201801", "%Y%m")        #起始时间必须是星期五
    endyear=datetime.datetime.now().year
    endmonth=datetime.datetime.now().month-1
    if endmonth==0:
        endmonth=12
        endyear=endyear-1
    date=str(endyear)+str(endmonth)
    enddate = datetime.datetime.strptime(date,"%Y%m")
    # enddate=datetime.datetime.strptime("20190302","%Y%m%d")
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate,
        monthEndDay=startdate+datetime.timedelta(days=(calendar.monthrange(startdate.year,startdate.month)[1]-1))
        day = GetMonthLastTradingDay(monthstartdate=startdate,monthenddate=monthEndDay,t=t)
        setattr(info, 'TradingDay', day.strftime("%Y-%m-%d"))

        main(startdate, mysplider,info)
        startdate = startdate+datetime.timedelta(days=calendar.monthrange(startdate.year,startdate.month)[1])

    try:
        info.mysql.Disconnect()
    except:
        print "没有打开过数据库"






