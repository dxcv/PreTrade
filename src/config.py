# -*- coding: utf-8 -*-
# @Time    : 2018/12/14 15:35
# @Author  : ZouJunLin
"""自动计算第二天要报价的合约"""
import pymssql
import numpy as np
from utils.InfoApi import  *

sql="""
SELECT TOP (2) 
	  TradingDay,
	  InstrumentID,
      [SettlementPrice]
  FROM [PreTrade].[dbo].[SettlementDetail] where InstrumentID='CF905' or InstrumentID='CF909'  order by TradingDay desc
"""
def GetInstrument():
    pass


if __name__=='__main__':
    PreSettle = {}
    info=InfoApi()
    resList = info.mysql.ExecQuery(sql)
    for i in resList:

        PreSettle[str(i[1]).encode("utf-8")] = int(float(i[2]))
    print PreSettle
    for key in PreSettle.keys():
        atmstrike=round(PreSettle[key]/1000.0*5)*200
        for k in np.arange(atmstrike-600, atmstrike+1400, 200):
            symbol=key+'C'+str(int(k) )
            print symbol
        for k in np.arange(atmstrike-1200, atmstrike+800, 200):
            symbol=key+'P'+str(int(k))
            print symbol
    GetInstrument()

