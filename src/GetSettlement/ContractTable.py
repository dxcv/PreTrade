# -*- coding: utf-8 -*-
# @Time    : 2018/11/13 18:24
# @Author  : ZouJunLin
"""
四大交易所合约的信息表，
"""
from utils.TradingDay.NextTradingDay import *
from utils.InfoApi import  *
from utils.Mysplider import *
from ConTractTableAPI import *


def main(startdate, mysplider):
    """
    获取四大交易所的日统计信息
    :return:
    # """
    GetContractInfo(startdate, info, "DCE")  # 获取大商所合约信息

    GetCZCEContractInfo(startdate,info)        #获取郑商所合约信息

    GetContractInfo(startdate, info, "SHFE")  # 获取上期所合约信息

    GetContractInfo(startdate, info, "CFFEX")  # 中金所合约信息



if __name__=="__main__":
    info=InfoApi()
    info.Get_Msplider()
    info.Get_BasicApi()
    t = TradingDay(info)
    startdate = datetime.datetime.now()
    enddate = datetime.datetime.now()

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        main(startdate, info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")
