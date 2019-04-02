# -*- coding: utf-8 -*-
# @Time    : 2019/4/2 15:21
# @Author  : ZouJunLin
"""抓取四大交易所持仓信息写入数据库"""

import sys
import os,datetime
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.InfoApi import *
from utils.Mysplider import *
from PositionAPI import *
from utils.TradingDay import NextTradingDay



def main(startdate, mysplider,info):
    """
    获取四大交易所期货公司持仓信息
    :return:
    """
    isexistsql="select distinct [ExchangeID] from [Position_Top20] where TradingDay='%s'"%startdate.strftime("%Y-%m-%d")
    templist=info.mysql.ExecQueryGetList(isexistsql)
    if not 'DCE' in templist:
        GetDCEPosition(info,startdate,"DCE")         #大商所持仓信息
    else:
        print "DCE数据已经存在"
    # GetDCEStagedTurnover(info,startdate,"DCE")      #阶段性成交




if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = TradingDay(info)
    startdate = datetime.datetime.strptime("20190401", "%Y%m%d")
    # startdate =datetime.datetime.now()

    enddate = datetime.datetime.now()
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate,
        main(startdate, mysplider,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()

