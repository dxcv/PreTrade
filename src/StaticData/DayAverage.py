# -*- coding: utf-8 -*-
# @Time    : 2018/11/23 8:33
# @Author  : ZouJunLin
"""计算日平均成交量"""
import sys,datetime
reload(sys)
from utils.TradingDay.NextTradingDay import *
from utils.Mysplider import *
from ComputerAPI import *
from utils.InfoApi import *
from utils.sqlServer import *
from tqdm import tqdm

"""查询当天交易的所有合约"""
Instrumentsql="""
  SELECT [InstrumentID] FROM [PreTrade].[dbo].[SettlementInfo] where  TradingDay='%s'
"""

InsertSql="""
    INSERT INTO [dbo].[AvgTotalVol]([TradingDay],[InstrumentID],[Avg5TotalVol],[Avg10TotalVol],[Avg20TotalVol],[Avg60TotalVol],[Avg120TotalVol],[AvgYearTotalVol],[Avg20Volati],[AvgYearVolati]) VALUES 
    ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
"""

def ComputerAvgVol():
    """计算某一天交易的合约平均成交量"""
    TradingDay=datetime.datetime.now().strftime("%Y-%m-%d")
    TradingDay='2019-01-15'
    info=InfoApi()
    mysql1 =info.GetStaticDataconnect()
    templist=IsExistData(Instrumentsql%TradingDay,info)

    sqllist=[]
    for i in tqdm(templist):
        #Instrumentlist.append(str(i[0]).encode("utf-8"))
        InstrumentID=str(i[0]).encode("utf-8").strip()
        """for each InstrumentID"""
        temp=ComputerAvgEachInstrumentID(info,InstrumentID)
        sqllist=sqllist+temp
    mysql1.ExecmanysNonQuery(InsertSql,sqllist)



if __name__=='__main__':

    ComputerAvgVol()