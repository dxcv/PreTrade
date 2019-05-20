# -*- coding: utf-8 -*-
# @Time    : 2019/5/13 13:34
# @Author  : ZouJunLin
"""主要用来铁矿石的成交量持仓量的监控函数。同比上一交易日的成交量，持仓量数据"""
import datetime,os,sys
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.TradingDay.NextTradingDay import *
from utils.InfoApi import  *
import pandas as pd
import xlwt
SaveDirector={'i':u"D:/GitData/pignemo/PreTrade/铁矿石成交量监控/",'ni':u"D:/GitData/pignemo/PreTrade/镍持仓量监控/"}
ProductName={'i':'铁矿石成交量监控','ni':'镍持仓量监控'}
columns=[u'交易日',u'合约代码',u'成交量',u'上一交易日成交量',u'持仓量',u'上一交易日持仓量']

def dealResult(result):
    temp=[]
    for i in result:
        col=list(i)
        temp.append(col)
    return temp

def GetVolumeAndPosition(info):
    sql = """
            select a.TradingDay as 交易日,a.InstrumentID as 合约,a.Volume as 成交量,b.Volume as 上一交易日成交量,a.Position as 持仓量,b.Position as 上一交易日持仓量
           from ( select TradingDay,InstrumentID,Volume,position from SettlementInfo  where TradingDay='{TradingDay}' and (InstrumentID like '{productcode}1%' or InstrumentID like '{productcode}2%')) a,
            ( select TradingDay,InstrumentID,Volume,Position from SettlementInfo  where TradingDay='{PreTradingDay}' and (InstrumentID like '{productcode}1%' or InstrumentID like '{productcode}2%')) b  
            where a.InstrumentID=b.InstrumentID 
    """.format(TradingDay=info.TradingDay, PreTradingDay=info.PreTradingDay, productcode=info.ProductCode)
    result=info.mysql.ExecQuery(sql)
    temp=dealResult(result)
    temp=pd.DataFrame(data=temp,columns=columns,index=None)
    excelfilename = SaveDirector[info.ProductCode] + info.TradingDay.replace("-","") +ProductName[info.ProductCode]+ '.xlsx'
    writer =  pd.ExcelWriter(excelfilename, engine='xlsxwriter')
    temp.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    worksheet.set_column('A:F', 18)

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'vcenter',
        'border': 1
    })
    header_format.set_align('center')
    header_format.set_align('vcenter')
    writer.save()

if __name__ == '__main__':

    info=InfoApi()
    info.GetDbHistoryConnect()      #初始化mysql
    t = TradingDay(info)
    productlist=['i','ni']              #成交量持仓量的品种代码


    startdate='20190515'
    startdate=datetime.datetime.strptime(startdate,"%Y%m%d")
    enddate=datetime.datetime.now()
    startdate=enddate
    enddate=startdate
    while startdate<=enddate:
        print info.mysql
        Prestartdate=t.NextTradingDayFuture(startdate.strftime("%Y%m%d"),False)
        Prestartdate=Prestartdate[0:4]+"-"+Prestartdate[4:6]+"-"+Prestartdate[6:8]
        print startdate,Prestartdate
        setattr(info,'TradingDay',startdate.strftime("%Y-%m-%d"))
        setattr(info,'PreTradingDay',Prestartdate)
        for i in productlist:
            setattr(info,'ProductCode',i)
            GetVolumeAndPosition(info)
        startdate=t.NextTradingDayFuture(startdate.strftime("%Y%m%d"),True)
        startdate=datetime.datetime.strptime(startdate,"%Y%m%d")