# -*- coding: utf-8 -*-
# @Time    : 2019/3/28 17:10
# @Author  : ZouJunLin
import pandas as pd
from MinutesCleanAPI import *
import datetime,os
import numpy as np
import pandas as pd
from utils.InfoApi import *
from utils.TradingDay import  NextTradingDay
import tqdm
import xlsxwriter
import threading

def MinutesDataClean(source_file,filename,TradeTime):
    col={'ask1':'24','bid1':'22'}
    columns=map(lambda i:str(i),np.arange(0,44))
    templist=list()
    starttime="21:05:00"
    tempindex = 1

    data=pd.read_csv(source_file+filename,encoding='gbk',header=None,names=columns,low_memory=False)
    TradingDay =str(data.at[1,'0']).strip()
    # if not os.path.exists(source_file+TradingDay):
    #     os.makedirs(source_file+TradingDay)
    if str(data.at[1,'20']).strip()<'09:00:00':
        starttime='09:05:00'

    while starttime!="":
        row=[]
        tempcol=data[data['20']==starttime].head(1)
        if not tempcol.empty:
            tempindex=data[data['20']==starttime].index[0]
            bid1 = float(tempcol[col['bid1']])
            ask1 = float(tempcol[col['ask1']])
            mid = (ask1 + bid1) / 2
            # print "tempcol[col['ask1']],tempcol[col['bid1']]",tempcol[col['ask1']],tempcol[col['bid1']],tempcol
            row = [TradingDay[:4] + "/" + TradingDay[4:6] + "/" + TradingDay[6:] + "  " + starttime, ask1, bid1, mid]

        else:
            j=tempindex
            while data.at[j,'20']<starttime:
               j+=1
            tempcol=data.iloc[j-1]
            tempindex=j-1
            bid1=float(tempcol[col['bid1']])
            ask1=float(tempcol[col['ask1']])
            mid=(ask1+bid1)/2
            row = [TradingDay[:4]+"/"+TradingDay[4:6]+"/"+TradingDay[6:]+"  "+starttime,ask1,bid1,mid]
        templist.append(row)
        starttime=theorynextdate(starttime,TradeTime)
    pddata=pd.DataFrame(templist,columns=['time', u'卖一', u'买一', 'mid'])
    return pddata

def MinutesCleanMain(t,instrumentID1,instrumentID2,startdate,enddate):

    #instrumentID1 = 'ni1906'
    #instrumentID2 = 'ni1907'
    # TradeTime = ['0', '3']
    dataframe1=pd.DataFrame()
    dataframe2=pd.DataFrame()
    saveDirector="D:/DATA/MinutesData/ones/"
    savafile = saveDirector+instrumentID2 + "-" + instrumentID1 + "spread.xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')

    # startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate
        filename1 = instrumentID1+"_"+startdate.strftime("%Y%m%d")+".csv"
        filename2 = instrumentID2 + "_" + startdate.strftime("%Y%m%d") + ".csv"
        source_file = "T:/level_1/" + startdate.strftime("%Y%m%d") + "/"
        # filename2 = "ni1907_20190326.csv"
        tempdataframe1 = MinutesDataClean(source_file, filename1, TradeTime=['0', '3'])
        tempdataframe2 = MinutesDataClean(source_file, filename2, TradeTime=['0', '3'])
        if dataframe1.empty:
        # source_file = "D:/GitData/PreTrade/src/minutesDataClean/DATA/"
            dataframe1 =tempdataframe1
            dataframe2 = tempdataframe2
        else:
            dataframe1=dataframe1.append(tempdataframe1)
            dataframe2=dataframe2.append(tempdataframe2)

        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

        # t1=threading.Thread(target=MinutesDataClean,args=(source_file, filename1,TradeTime))
        # t2 = threading.Thread(target=MinutesDataClean, args=(source_file, filename2, TradeTime))

    ask1 = dataframe2[u'卖一'] - dataframe1[u'卖一']
    bid1 = dataframe2[u'买一'] - dataframe1[u'买一']
    mid = (ask1 + bid1) / 2
    df1 = pd.DataFrame(list(zip(mid, ask1, bid1)), columns=['mid', 'ask_post', 'bid_post'])
    # df12.to_excel(writer, sheet_name='Sheet1',startrow=1,startcol=0,index=None)
    df1.to_excel(writer,sheet_name='Sheet1', startrow=1, startcol=10, index=None)
    dataframe1.to_excel(writer,sheet_name='Sheet1', startrow=1, startcol=0, index=None)
    dataframe2.to_excel(writer,sheet_name='Sheet1', startrow=1, startcol=5, index=None)

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:A', 20)
    worksheet.set_column('F:F', 20)
    worksheet.set_column('K:K', 15)
    worksheet.set_column('L:M', 10)

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'vcenter',
        'border': 1
    })
    header_format.set_align('center')
    header_format.set_align('vcenter')
    value = str(instrumentID2 + "-" + instrumentID1).strip()
    worksheet.write(0, 0, instrumentID1, header_format)
    worksheet.write(0, 5, instrumentID2, header_format)
    worksheet.write(0, 10, value, header_format)

    writer.save()

def GetSourfile(t,startdate,enddate,instrumentID1,instrumentID2):
    source_file="T:/level_1/"+startdate.strftime("%Y%m%d")+"/"
    # print source_file
    MinutesCleanMain(t,source_file, instrumentID1, instrumentID2,startdate,enddate)


if __name__=='__main__':

    InstrumentList=[['ni1906','ni1907'],['ni1906','ni1908']]
    StartDay = "20190125"
    startdate = datetime.datetime.strptime(StartDay, "%Y%m%d")
    # StartDay = "20190201"
    storeDirectory = "D:/DATA/MinutesData/ones/"

    """Read SqlServer Get Main InstrumentID by ProductCode"""

    info = InfoApi()
    info.GetDbHistoryConnect()

    t = NextTradingDay.TradingDay(info)
    # enddate = datetime.datetime.now()-datetime.timedelta(days=1)
    enddate = datetime.datetime.strptime("20190326", "%Y%m%d")
    threadList=list()
    for i in InstrumentList:
        StartDay = "20190125"
        instrumentID1=i[0]
        instrumentID2=i[1]

        # t=threading.Thread(target=MinutesCleanMain,args=(t,instrumentID1, instrumentID2, startdate, enddate))
        # t.setDaemon(True)
        # t.start()
        # threadList.append(t)
        # GetSourfile(t,startdate,enddate,instrumentID1,instrumentID2)
        MinutesCleanMain(t,instrumentID1, instrumentID2, startdate, enddate)
    # for i in threadList:
    #     i.join()

    info.mysql.Disconnect()


