# -*- coding: utf-8 -*-
# @Time    : 2019/4/4 10:51
# @Author  : ZouJunLin
import chardet
from bs4 import BeautifulSoup
import PositionAPI
import numpy as np
import pandas as pd
import os

def  GetCZCEStagedTurnover(info, startdate,ExchangeId):
    url="http://app.czce.com.cn/cms/QueryTradeHolding?querytype=tradeamt&beginDate=%s&endDate=%s&classid=%s"
    TradingDay=startdate.strftime("%Y-%m-%d")
    templists=list()

    isexistsql="SELECT  [TradingDay] from [StagePosition] where TradingDay='%s' and [ExchangeID]='%s'"%(TradingDay,ExchangeId)
    insertsql="INSERT INTO [dbo].[StagePosition]([TradingDay],[InstrumentID],[ExchangeID],[Rank],[Type],[ParticipantID1],[ParticipantABBR1] ,[CJ1],[ParticipantID2],[ParticipantABBR2],[CJ2])" \
              " values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    existlist = info.mysql.ExecQueryGetList(isexistsql)
    codelist=info.GetExchangeProduct(ExchangeId)
    for code in codelist:
        Url=url%(TradingDay,TradingDay,code)
        temp=GetCodeTurnover(info,ExchangeId,Url,code,TradingDay)
        templists=templists+temp
    if not len(existlist):
        info.mysql.ExecmanysNonQuery(insertsql, templists)
    else:
        print "CZCE阶段性数据数据库已经存在"


def GetCodeTurnover(info,ExchangeId,Url,code,TradingDay):
    templist=list()
    print Url
    header = info.GetExchangeHeader(ExchangeId)
    html=info.mysplider.getUrlcontent(Url,header=header)
    table=info.mysplider.tableTolistById(html,ExchangeId,"senfe")
    for i in table:
        col=[]
        if str(i[0]).strip().find("名次")==-1 and str(i[0]).strip().find("共计")==-1 and str(i[0]).strip().find("总计")==-1:
            if  "null" not in i:
                InstrumentID=code
                Rank=str(i[0]).strip()
                Type='0'
                ParticipantID1=str(i[1]).strip()
                ParticipantABBR1=str(i[3]).strip()
                CJ1=str(i[4]).strip()
                ParticipantID2 = str(i[6]).strip()
                ParticipantABBR2 = str(i[7]).strip()
                CJ2=str(i[8]).strip()
                col=[TradingDay, InstrumentID, ExchangeId, Rank,Type, ParticipantID1,ParticipantABBR1, CJ1, ParticipantID2,ParticipantABBR2, CJ2]
                templist.append(tuple(col))
    DataToExecl(code,TradingDay,templist,ExchangeId)
    return templist

def  DataToExecl(code,TradingDay,templist,ExchangeId):
    """dataframe 写入excel表格"""
    saveDirector = "D:/GitData/StagePosition/"+ExchangeId+"/"+TradingDay.replace("-","")+"/"
    if not os.path.exists(saveDirector):
        os.mkdir(saveDirector)
    savafile = saveDirector + TradingDay.replace("-","") + "_" +code+ ".xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')
    columns=[u'名次',u'会员号',u'会员简称',u'成交量(手)',u'名次1',u'会员号1',u'会员简称1',u'成交金额(亿元)']
    datalist=list()
    for i in templist:
        col=[int(i[3]),i[5],i[6],float(i[7]),int(i[3]),i[8],i[9],float(i[10])]
        datalist.append(col)
    df=pd.DataFrame(data=datalist,columns=columns)
    df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:G', 12)
    worksheet.set_column('H:H', 14)

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