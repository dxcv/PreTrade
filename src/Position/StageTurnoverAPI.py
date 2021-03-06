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
        print "CZCE阶段性日数据数据库已经存在"


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

"""大商所"""
def GetDCEStagedTurnoverBymonth(info,TradingDay,ExchangeID):
    beginmonth=TradingDay.strftime("%Y%m")
    endmonth=TradingDay.strftime("%Y%m")
    url = "http://www.dce.com.cn/publicweb/quotesdata/memberDealCh.html?"
    url = url + "memberDealQuotes.variety=%s&memberDealQuotes.trade_type=0&memberDealQuotes.begin_month=%s&memberDealQuotes.end_month=%s"
    sql="SELECT [InstrumentCode] FROM [PreTrade].[dbo].[ContractCode] where [ExchangeID]='%s'"%ExchangeID
    templist = info.mysql.ExecQueryGetList(sql)
    templist.append("all")
    for i in templist:
        Surl = url % (i, beginmonth, endmonth)
        print Surl
        info.Set_StagePosition(ExchangeID, Surl, i,beginmonth,endmonth)
        GetDCEStagePosition(info)

def GetDCEStagePosition(info):
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=1311697EA3395C8127FD0BB9D51B1742; WMONID=j1TJsMZrARA; Hm_lvt_a50228174de2a93aee654389576b60fb=1550114491,1550801897,1550826990,1550890979; Hm_lpvt_a50228174de2a93aee654389576b60fb=1550892897',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    html=info.mysplider.getUrlcontent(info.StagePositionurl,header=header)
    listdata=info.mysplider.tableTolistByNum(html,"DCE",0)
    templist=list()
    begindate=info.StagePositionBeginTime
    enddate=info.StagePositionEndTime
    for i in listdata:
        col = []
        if str(i[0]).strip().find("名次") == -1 and str(i[0]).strip().find("共计") == -1 and str(i[0]).strip().find("总计") == -1:
            if "null" not in i:
                InstrumentID = info.StagePositionCode
                Rank = str(i[0]).strip()
                Type = '0'
                ParticipantID1 = str(i[1]).strip()
                ParticipantABBR1 = str(i[2]).strip()
                CJ1 = str(i[3]).strip()
                CJ1_Percent=str(i[4]).strip()
                ParticipantID2 = str(i[6]).strip()
                ParticipantABBR2 = str(i[7]).strip()
                CJ2 = str(i[8]).strip()
                CJ2_Percent = str(i[9]).strip()
                col = [int(Rank),  ParticipantID1, ParticipantABBR1, int(CJ1), CJ1_Percent,int(Rank),
                       ParticipantID2, ParticipantABBR2, float(CJ2), CJ2_Percent]
                templist.append(tuple(col))
    columns=[u'名次',u'会员号',u'会员名称',u'成交量(手)',u'成交量比重',u'名次1',u'会员号1',u'会员名称1',u'成交金额(亿元)',u'成交额比重']
    df=pd.DataFrame(data=templist,columns=columns)
    saveDirector = "D:/GitData/StagePosition/" + info.StagePositionExchangeID + "/" + begindate+ "/"
    if not os.path.exists(saveDirector):
        os.mkdir(saveDirector)
    savafile = saveDirector + begindate + "_" + InstrumentID + ".xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:J', 15)

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