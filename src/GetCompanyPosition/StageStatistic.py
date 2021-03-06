# -*- coding: utf-8 -*-
# @Time    : 2019/2/22 16:43
# @Author  : ZouJunLin
"""获取四大交易所阶段性统计排名"""

import sys
import os,datetime
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.InfoApi import *
from utils.Mysplider import *
from CompanyPositionAPI import *
from utils.TradingDay import NextTradingDay



def main(startdate,info):
    """
    获取四大交易所阶段性统计排名
    :return:
    """

    GetDCEStagedStatistic(info,startdate,"DCE")      #阶段性统计




if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = TradingDay(info)
    startdate = datetime.datetime.strptime("20190308", "%Y%m%d")
    startdate=info.mydate.GetCurrentB(startdate)
    enddate = datetime.datetime.now()
    enddate=info.mydate.GetNextmonthB(enddate)
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <enddate.strftime("%Y%m%d"):
        print startdate
        main(startdate,info)
        startdate = info.mydate.GetNextmonthB(startdate)

    info.mysql.Disconnect()
