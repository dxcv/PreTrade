# -*- coding: utf-8 -*-
# @Time    : 2019/4/4 9:46
# @Author  : ZouJunLin
"""期货阶段成交排名表"""

import sys
import os,datetime
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.InfoApi import *
from utils.Mysplider import *
from PositionAPI import *
from utils.TradingDay import NextTradingDay
from StageTurnoverAPI import *



def main(startdate, mysplider,info):
    """
    获取四大交易所期货公司成交量持仓量信息
    :return:
    """
    GetCZCEStagedTurnover(info, startdate, "CZCE")

    # GetDCEPosition(info,startdate,"DCE")            #大商所持仓信息
    #
    # GetSHFEPosition(info, startdate, "SHFE")        # 上期所
    #
    # GetCZCEPosition(info, startdate, "CZCE")        # 郑商所


    # GetDCEStagedTurnover(info,startdate,"DCE")      #阶段性成交




if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = NextTradingDay.TradingDay(info)
    startdate = datetime.datetime.strptime("20190401", "%Y%m%d")
    # startdate =datetime.datetime.now()

    enddate = datetime.datetime.now()-datetime.timedelta(days=1)
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate,
        main(startdate, mysplider,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()

