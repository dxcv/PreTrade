# -*- coding: utf-8 -*-
# @Time    : 2019/2/18 15:49
# @Author  : ZouJunLin

"""主力合约数据处理脚本"""
from utils.InfoApi import *
import datetime


def CleanData(MainInstrument,info):
    pass


if __name__=='__main__':

    ProductCodeList=['ni','au','ag','rb','i','m','TA']
    StartDay="20180801"


    """Read SqlServer Get Main InstrumentID by ProductCode"""
    pass

    info = InfoApi()
    info.GetDbHistoryConnect()

    t = TradingDay(info)
    startdate = datetime.datetime.strptime(StartDay, "%Y%m%d")
    enddate = datetime.datetime.now()

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate
        MainInstrument=info.GetMainInstrumentIdByProductCode(ProductCodeList[0],startdate.strftime("%Y-%m-%d"))
        CleanData(MainInstrument,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()

