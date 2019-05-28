# -*- coding: utf-8 -*-
# @Time    : 2019/5/17 17:47
# @Author  : ZouJunLin
import numpy as np
import pandas as pd
import datetime
import sys
import os,datetime
from multiprocessing import Process
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.TradingDay import NextTradingDay
from utils.InfoApi import *
from Level_CleanLoop import *
from widthAPI import *


def GetSourceData(info):
    #根据文件存储文件规则获取相应文件
    #找出前四个月
    sql="""
        SELECT  top  4  [InstrumentID]
        FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='SHFE' and InstrumentID like'cu%' and IsFuture=1 and TradingDay='{TradingDay}' order by InstrumentID
    """
    optionsql="""
        SELECT  
          [InstrumentID]
          ,[Volume]
      FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='SHFE' and IsFuture=0  and InstrumentID like'cu%' and TradingDay='{TradingDay}' order by InstrumentID
    """
    TOP4Instrument=info.mysql.ExecQueryGetList(sql.format(TradingDay=info.TradingDay.strftime("%Y-%m-%d")))
    optionresult=info.mysql.ExecQueryGetDict(optionsql.format(TradingDay=info.TradingDay.strftime("%Y-%m-%d")))
    optionresult=optionresult
    templist=[]
    columns = [u'合约', u'平均宽度', u'连续报价义务宽度',u'回应义务宽度']
    saveDirector = "D:/GitData/width/"
    tradingday=info.TradingDay.strftime("%Y-%m-%d")
    savafile = saveDirector + tradingday.replace("-","")+u"cu期权报价宽度.xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')
    for i in sorted(optionresult.keys(),reverse=False):
        print i
        info.cleanDatadict = [tradingday, None, i]
        filename = i + "_" + tradingday.replace("-", "") + ".csv"
        fileDiretory = "E:/options/"
        if str(i)[:6] in TOP4Instrument and  optionresult[i]>50:
            """先标准化处理，再计算,return"""
            if IsExistfile(fileDiretory+tradingday.replace("-","")+"/",filename):
                df=Level_1_Clean(filename,fileDiretory+tradingday.replace("-",""),info)
                length = len(df)
                BP = sum(df['BP1']) / length
                BPWidth = GetWidth(BP)
                width = sum(df['SP1'] - df['BP1']) / length
                temp=[i,width,BPWidth,'']
                templist.append(temp)
            else:
                print "source file missing filename named",filename
                raise Exception
        else:
            """直接计算相关数据,带return"""
            if IsExistfile(saveDirector, filename):
                temp = GetResponseAverageWidth(fileDiretory+tradingday.replace("-",""), filename)
                templist.append(temp)


    """days result data story as filename"""
    df = pd.DataFrame(data=templist, columns=columns)

    df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:C', 20)

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




def main():
    Directory="E:/options/"
    startdate="20190524"
    startdate=datetime.datetime.strptime(startdate,'%Y%m%d')
    info=InfoApi()
    info.GetDbHistoryConnect()
    info.GetStaticDataconnect()
    t=TradingDay(info)
    enddate = datetime.datetime.now()
    # if datetime.datetime.now().hour<17:
    #     enddate=datetime.datetime.now()-datetime.timedelta(days=1)
    # if not t.IsTradingDayFuture(enddate.strftime("%Y%m%d")):
    #     enddate = t.NextTradingDayFuture(enddate.strftime("%Y%m%d"), False)
    #     enddate = datetime.datetime.strptime(enddate, "%Y%m%d")
    enddate=datetime.datetime.strptime("20190524","%Y%m%d")


    while startdate<=enddate:
        print startdate
        starttime = datetime.datetime.now()
        setattr(info,'TradingDay',startdate)
        GetSourceData(info)
        startdate=t.NextTradingDayFuture(startdate.strftime("%Y%m%d"),True)
        startdate = datetime.datetime.strptime(startdate, '%Y%m%d')
        endtime=datetime.datetime.now()
        print endtime-starttime




if __name__ == '__main__':
    main()


