# -*- coding: utf-8 -*-
# @Time    : 2019/3/28 17:10
# @Author  : ZouJunLin
import pandas as pd
from MinutesCleanAPI import *
import datetime,os
import numpy as np
import pandas as pd
import xlsxwriter

def MinutesDataClean(source_file,filename,TradeTime):
    col={'ask1':'24','bid1':'22'}
    columns=map(lambda i:str(i),np.arange(0,44))
    templist=list()
    starttime="21:05:00"
    tempindex = 1

    data=pd.read_csv(source_file+filename,encoding='gbk',header=None,names=columns,low_memory=False)
    TradingDay =str(data.at[1,'0']).strip()
    if not os.path.exists(source_file+TradingDay):
        os.makedirs(source_file+TradingDay)
    if str(data.at[1,'20']).strip()<'09:00:00':
        lasttime='09:05:00'
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





if __name__=='__main__':
    minutes=1
    instrumentID1='ni1906'
    instrumentID2='ni1907'
    savafile=instrumentID2+"-"+instrumentID1+"spread.xlsx"
    filename1="ni1906_20190326.csv"
    filename2 = "ni1907_20190326.csv"
    source_file="D:/GitData/PreTrade/src/minutesDataClean/DATA/"
    dataframe1=MinutesDataClean(source_file,filename1,TradeTime=['0','3'])
    dataframe2=MinutesDataClean(source_file,filename2,TradeTime=['0','3'])

    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')

    ask1=dataframe2[u'卖一']-dataframe1[u'卖一']
    bid1=dataframe2[u'买一']-dataframe1[u'买一']
    mid =(ask1+bid1)/2
    df1 = pd.DataFrame(list(zip(mid,ask1, bid1)), columns=['mid','ask_post', 'bid_post'])
    # df12.to_excel(writer, sheet_name='Sheet1',startrow=1,startcol=0,index=None)
    df1.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=10, index=None)
    dataframe1.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=0, index=None)
    dataframe2.to_excel(writer, sheet_name='Sheet1', startrow=1, startcol=5, index=None)

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
    value=str(instrumentID2+"-"+instrumentID1).strip()
    worksheet.write(0, 0, instrumentID1, header_format)
    worksheet.write(0, 5, instrumentID2, header_format)
    worksheet.write(0, 10, value, header_format)

    writer.save()
