# -*- coding: utf-8 -*-
# @Time    : 2019/2/20 16:46
# @Author  : ZouJunLin
"""交易日历表"""
import sys
import os
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
reload(sys)
import requests
from utils.sqlServer import *
import datetime
import ConfigParser
from utils.Mysplider import *
from utils.InfoApi import *


sql="INSERT INTO [StatisticData].[dbo].[Calender]([Date],IsTradingDay,IsEveningOpen,[PreTradingDay],[NextTradingDay]) VALUES('%s',%s,%s,'%s','%s')"
existsql="select [Date]  from [StatisticData].[dbo].[Calender] order by Date desc"


def datatolist(data):
    templist=[]
    for k in data:
        content=data[k].split(",")
        templist=templist+content[::-1]
    templist=sorted(templist,reverse=False)
    return templist

if __name__=="__main__":
    info=InfoApi()
    templist=[]
    holiday=info.setting.GetHoliday()
    temp=info.mysql.ExecQueryGetList(existsql)
    t = TradingDay(info)
    if len(temp):
        startdate = datetime.datetime.strptime(temp[0], "%Y-%m-%d")
    else:
        startdate = datetime.datetime.strptime("20170101", "%Y%m%d")
    enddate=datetime.datetime.now()
    enddate=t.NextTradingDay(enddate.strftime("%Y%m%d"),True)
    enddate=datetime.datetime.strptime(enddate,"%Y%m%d")
    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        col=[]
        tempstartday=startdate.strftime("%Y%m%d").strip()
        if t.IsTradingDayS(startdate.strftime("%Y%m%d")):
            col= [startdate.strftime("%Y-%m-%d").strip(),1,t.IsEveningOpen(tempstartday)]
        else:
            col=[startdate.strftime("%Y-%m-%d"),0,0]
        preday=t.NextTradingDay(tempstartday,False)
        preday=datetime.datetime.strptime(preday,"%Y%m%d").strftime("%Y-%m-%d")
        nextday=t.NextTradingDay(tempstartday, True)
        nextday=datetime.datetime.strptime(nextday,"%Y%m%d").strftime("%Y-%m-%d")
        col.append(preday)
        col.append(nextday)
        templist.append(tuple(col))
        startdate = startdate+datetime.timedelta(days=1)
    info.mysql.ExecmanysNonQuery(sql,templist)





