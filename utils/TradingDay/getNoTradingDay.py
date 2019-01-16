# -*- coding: utf-8 -*-
# @Time    : 2018/8/24 10:52
# @Author  : ZouJunLin
"""
获取交易日,写入数据库
执行当天之前的从官网抓取的，因为抓取的数据有点问题，所以还要排除周末周日和节假日

"""
import requests
from utils.sqlServer import *
import datetime
import ConfigParser
from utils.Mysplider import *
from utils.InfoApi import *


sql="INSERT INTO [HistoryNoTdCalendar](NoTradingDay) VALUES(%s)"

def datatolist(data):
    templist=[]
    for k in data:
        content=data[k].split(",")
        templist=templist+content[::-1]
    templist=sorted(templist,reverse=False)
    return templist

def geturldata():
    """
    网上获取交易日历，有时候会出错
    """
    url="http://www.shfe.com.cn/js/calendar-data.js"
    req=requests.get(url)
    data=req.text.encode("utf-8").replace("define(function (){　return ","").replace(";});","").strip()
    data=eval(data)
    templist=datatolist(data)
    return templist


if __name__=="__main__":
    info=InfoApi()
    daylist=geturldata()
    daylist.append('20180618')
    daylist=sorted(daylist)
    nowdate=datetime.datetime.now()
    nowdate=nowdate.strftime("%Y%m%d")
    #### 20180824之前的非交易日
    for i in daylist:
        if i>=nowdate:
            length=daylist.index(i)
            break
    daylist=daylist[:length]
    mysql=info.GetDbHistoryConnect()
    sql1="SELECT [NoTradingDay] FROM [PreTrade].[dbo].[HistoryNoTdCalendar]"
    tlist=mysql.ExecQueryGetList(sql1)
    daylist=list(set(daylist)-set(tlist))
    if len(daylist):
        mysql.ExecmanysNonQuery(sql,daylist)

    mysql.Disconnect()





