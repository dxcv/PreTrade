# -*- coding: utf-8 -*-
# @Time    : 2018/11/9 15:11
# @Author  : ZouJunLin
"""
周行情
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
from SettlementInfoWeekAPI import *
import threading
from utils.Mysplider import *



def main(startdate, mysplider,info):
    """
    获取四大交易所的周统计信息
    :return:
    """
    # starttime = datetime.datetime.now()
    GetSettlementInfoWeek(info,startdate,"DCE")                 #大商所周统计信息
    GetSettlementInfoWeek(info,startdate,"CZCE")                #郑商所周统计信息
    GetSettlementInfoWeek(info,startdate, "SHFE")                #上期所周统计信息
    WeekFirsyDay=startdate-datetime.timedelta(days=4)
    GetSettlementInfoWeek(info,WeekFirsyDay,"CFFEX")               #中金所周统计信息
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
    startdate = datetime.datetime.strptime("20180105", "%Y%m%d")        #起始时间必须是星期五
    enddate = datetime.datetime.now()
    # enddate=datetime.datetime.strptime("20190302","%Y%m%d")
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate
        day = GetWeekLastTradingDay(startdate, t)
        setattr(info, 'TradingDay', (GetWeekLastTradingDay(startdate,t)).strftime("%Y-%m-%d"))
        main(startdate, mysplider,info)

        startdate = GetNextTradingWeekDay(startdate,t)

    try:
        info.mysql.Disconnect()
    except:
        print "没有打开过数据库"






