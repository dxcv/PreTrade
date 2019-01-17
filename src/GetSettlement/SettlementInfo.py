# -*- coding: utf-8 -*-
# @Time    : 2018/11/9 15:11
# @Author  : ZouJunLin
"""
郑商所，中金所，上期所，大商所，结算信息，汇总
主要包括收盘价，前一天结算价，当天成交量，当天成交金额，最高价，最低价
"""

from utils.TradingDay.NextTradingDay import *
from utils.InfoApi import *
from SettlementInfoAPI import *
from utils.Mysplider import *



def main(startdate, mysplider,info):
    """
    获取四大交易所的日统计信息
    :return:
    """
    GetSettlementInfo(info,startdate,"DCE")                 #大商所日统计信息
    GetSettlementInfo(info,startdate,"CZCE")                #郑商所日统计信息
    GetSettlementInfo(info,startdate, "SHFE")                #上期所日统计信息
    GetSettlementInfo(info,startdate,"CFFEX")               #中金所日统计信息

if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = TradingDay(info)
    startdate = datetime.datetime.strptime("20190114", "%Y%m%d")
    enddate = datetime.datetime.now()
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate
        main(startdate, mysplider,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()






