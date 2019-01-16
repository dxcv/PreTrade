# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 17:29
# @Author  : ZouJunLin
"""计算一个品种的两个不同月份的价差"""
from utils.InfoApi import *
from utils.sqlServer import *
import datetime
from ReadSignal import *
from StrageAPI import *
from pandas import DataFrame,Series
import re

sql="""
    select Time,BP1,SP1,(BP1+SP1)/2 as midPrice  from [PreTrade].[dbo].[FutureHistoryTdData] where InstrumentId='%s' and TradingDay='%s'
"""


def Get2ContractDiffer(info,date,gep,InstrumentID1,InstrumentID2,):
    code = re.search(r"[a-zA-Z]+\.?", str(InstrumentID1).strip()).group()
    sql1 = """ select [DayTradTime],[NightTradTime] from ContractCode where InstrumentCode='%s'""" % code
    TradTime = info.mysql.ExecQuery(sql1)[0]
    tempdata=info.mysql.ExecQuery(sql%(InstrumentID1,date))
    df = DataFrame([list(i) for i in tempdata],columns=['Time','bid1','ask1','midpx'])
    df=df.set_index('Time')
    tempdata1 =info.mysql.ExecQuery(sql % (InstrumentID2, date))
    print sql % (InstrumentID2, date)
    df2 = DataFrame([list(i) for i in tempdata1], columns=['Time', 'bid11', 'ask11', 'midpx1'])
    # df2.index = df2['Time']
    df2 = df2.set_index('Time')
    rs=pd.merge(df, df2,on=['Time'])

    # print rs

    df1=GetDf1(rs,gep,TradTime)
    # print df1
    df1.to_csv("./data/"+InstrumentID1+"-"+InstrumentID2+'_Correlation.csv',index=True)







if __name__=='__main__':

    TestDay="20170103"
    info=InfoApi()
    gep=5       #需要统一格式/秒
    signal=Signal().ReadContract()

    for i in signal.index:
        InstrumentID1=str(signal.at[i,'Contract1']).strip()
        InstrumentID2=str(signal.at[i,'Contract2']).strip()

        Get2ContractDiffer(info,TestDay,gep,InstrumentID1,InstrumentID2)