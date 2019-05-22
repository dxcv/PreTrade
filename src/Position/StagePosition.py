# -*- coding: utf-8 -*-
# @Time    : 2019/4/4 9:46
# @Author  : ZouJunLin
"""期货阶段成交排名表TOP20"""

import sys
import os,datetime
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.InfoApi import *
from utils.Mysplider import *
from PositionAPI import *
from utils.TradingDay import NextTradingDay
from StageTurnoverAPI import *
from Top20API import *

WriteExcel=True

def main(startdate, mysplider,info):
    """
    郑商所
    :return:
    """

    #######GetCZCEStagedTurnover(info, startdate, "CZCE")        #阶段性成交排名表 日
    # GetDCEStagedTurnoverBymonth(info, startdate, "DCE")


    isexistsql = "select distinct [ExchangeID] from [Position_Top20] where TradingDay='%s'" % startdate.strftime(
        "%Y-%m-%d")
    eixstlist = info.mysql.ExecQueryGetList(isexistsql)

    if not 'CFFEX' in eixstlist:
        GetCFFEXPositionTop20(info, startdate, "CFFEX")  # 获取中金所日成交top20
    else:
        print 'CFFEX', "持仓排名top20数据已经存在"

    if not 'DCE' in eixstlist:
        GetDCEPosition(info, startdate, "DCE")  # 获取大商所日成交top20
    else:
        print 'DCE', "持仓排名top20数据已经存在"

    if not 'SHFE' in eixstlist:
        GetSHFEPosition(info, startdate, "SHFE")  # 获取上期所日成交top20
    else:
        print 'SHFE', "持仓排名top20数据已经存在"
    #
    if not 'CZCE' in eixstlist:
        GetCZCEPosition(info, startdate, "CZCE")  # 获取郑商所日成交top20
    else:
        print 'CZCE', "持仓排名top20数据已经存在"


    # GetDCEPosition(info,startdate,"DCE")            #大商所持仓信息
    #
    # GetSHFEPosition(info, startdate, "SHFE")        # 上期所
    # #
    # GetCZCEPosition(info, startdate, "CZCE")        # 郑商所阶段性前20名




    # GetDCEStagedTurnover(info,startdate,"DCE")      #阶段性成交




if __name__=="__main__":
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.Get_Msplider()

    t = NextTradingDay.TradingDay(info)
    startdate = datetime.datetime.now() - datetime.timedelta(days=7)
    startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), False)
    startdate = datetime.datetime.strptime(startdate, "%Y%m%d")


    enddate = datetime.datetime.now()
    mysplider = info.mysplider

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate,
        main(startdate, mysplider,info)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()

