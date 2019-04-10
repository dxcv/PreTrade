# -*- coding: utf-8 -*-
# @Time    : 2019/2/22 16:45
# @Author  : ZouJunLin
"""爬取持仓共用的API"""
import re
import csv,os,codecs,datetime
import pandas as pd
import xlwt
import threading
import json


def ResultToDatabase(info,result,sql):
    info.mysql.ExecmanysNonQuery(sql,result)


def GetDCEPosition(info,TradingDay,ExchangeID):
    result=list()
    sql="select InstrumentID from SettlementInfo where TradingDay='%s' and Position>20000 and ExchangeID='%s' and IsFuture=1"%(TradingDay.strftime("%Y-%m-%d"),ExchangeID)
    insertsql="INSERT INTO [dbo].[Position_Top20] ([TradingDay],[InstrumentID],[ExchangeID],[Rank],[Type],[ParticipantABBR1],[CJ1],[CJ1_CHG],[ParticipantIDABBR2]" \
              ",[CJ2],[CJ2_CHG],[ParticipantABBR3],[CJ3],[CJ3_CHG]) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    isexistsql = "select distinct [ExchangeID] from [Position_Top20] where TradingDay='%s'" % TradingDay.strftime(
        "%Y-%m-%d")
    templist=info.mysql.ExecQueryGetList(sql)
    for i in templist:
        code = str(re.match(r"\D+", i).group())
        url="http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html?"
        url=url+"memberDealPosiQuotes.variety=%s&memberDealPosiQuotes.trade_type=0&year=%s&month=%s&day=%s&contract.contract_id=%s&contract.variety_id=%s&contract="
        url=url % (code, TradingDay.year, TradingDay.month - 1, TradingDay.day,i,code)
        info.Set_QryPosition(ExchangeID,url, code, i,TradingDay.strftime("%Y%m%d"))
        temp=GetDCEPositionProductData(info,i,TradingDay.strftime("%Y-%m-%d"))
        result=result+temp
    eixstlist = info.mysql.ExecQueryGetList(isexistsql)
    ResultToDatabase(info,result,insertsql)






def GetDCEPositionProductData(info,InstrumentID,TradingDay):
    templists=list()
    exclelist=list()
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=8E650BA1F3AFEAB1611F8ADC6C186BD1; WMONID=j1TJsMZrARA; Hm_lvt_a50228174de2a93aee654389576b60fb=1547791080,1548045741,1548146303,1548147105; sssssss=516c92f7sssssss_516c92f7',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    print info.QryPositionurl
    html=info.mysplider.getUrlcontent(info.QryPositionurl,header=header)
    listdata=info.mysplider.tableTolistByNum(html,"DCE",1)

    """write to database"""
    for i in listdata[1:-1]:
        # col=[]
        # excelcol=[]
        Rank=i[0]
        if str(Rank).strip()=="":
            if str(i[4]).strip() != "":
                Rank=str(i[4]).strip()
            if str(i[8]).strip() != "":
                Rank = str(i[8]).strip()
            if Rank=="":
                continue
        ExchangeID='DCE'
        ParticipantABBR1=i[1]
        CJ1=i[2]
        CJ1_CHG=i[3]
        ParticipantABBR2=i[5]
        CJ2=i[6]
        CJ2_CHG=i[7]
        ParticipantABBR3=i[9]
        CJ3=i[10]
        CJ3_CHG=i[11]
        col=[TradingDay,InstrumentID,ExchangeID,Rank,'0',ParticipantABBR1,CJ1,CJ1_CHG,ParticipantABBR2,CJ2,CJ2_CHG,ParticipantABBR3,CJ3,CJ3_CHG]
        # excelcol=[int(Rank),ParticipantABBR1,int(CJ1),int(CJ1_CHG),int(Rank),ParticipantABBR2,int(CJ2),int(CJ2_CHG),int(Rank),ParticipantABBR3,int(CJ3),int(CJ3_CHG)]
        # exclelist.append(excelcol)
        templists.append(tuple(col))
    # columns = [u'名次', u'会员简称', u'成交量(手)', u'增减', u'名次1',u'会员简称1', u'持买单量1', u'增减1',u'名次2', u'会员简称2', u'持卖单量2', u'增减2']
    # excelDataToExcel(exclelist,ExchangeID,columns,info.QryPositionTradingDay,info.QryPositionInstrumentID)
    return templists


def excelDataToExcel(datalist,ExchangeID,columns,TradingDay,InstrumentID):
    df = pd.DataFrame(data=datalist, columns=columns)
    # df[[u'成交量(手)', u'增减',u'持买单量1',u'增减1',u'持卖单量2',u'增减2']] = df[[u'成交量(手)',u'增减',u'持买单量1',u'增减1',u'持卖单量2',u'增减2']].apply(pd.to_numeric)
    saveDirector = "D:/GitData/Top20Position/" + ExchangeID + "/" + TradingDay + "/"
    if not os.path.exists(saveDirector):
        os.mkdir(saveDirector)
    savafile = saveDirector + TradingDay + "_" + InstrumentID + ".xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', startrow=0, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    if len(columns)==12:
        worksheet.set_column('A:L', 14)
    elif len(columns)==15:
        worksheet.set_column('A:O', 14)
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


def GetDCEStagedTurnover(info,TradingDay,ExchangeID):
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

    templist = list()
    begindate = info.StagePositionBeginTime
    enddate = info.StagePositionEndTime
    for i in listdata:
        col = []
        if str(i[0]).strip().find("名次") == -1 and str(i[0]).strip().find("共计") == -1 and str(i[0]).strip().find(
                "总计") == -1:
            if "null" not in i:
                InstrumentID = info.StagePositionCode
                Rank = str(i[0]).strip()
                Type = '0'
                ParticipantID1 = str(i[1]).strip()
                ParticipantABBR1 = str(i[2]).strip()
                CJ1 = str(i[3]).strip()
                CJ1_Percent = str(i[4]).strip()
                ParticipantID2 = str(i[6]).strip()
                ParticipantABBR2 = str(i[7]).strip()
                CJ2 = str(i[8]).strip()
                CJ2_Percent = str(i[9]).strip()
                col = [int(Rank), ParticipantID1, ParticipantABBR1, int(CJ1), CJ1_Percent, int(Rank),
                       ParticipantID2, ParticipantABBR2, float(CJ2), CJ2_Percent]
                templist.append(tuple(col))
    columns = [u'名次', u'会员号', u'会员名称', u'成交量(手)', u'成交量比重', u'名次1', u'会员号1', u'会员名称1', u'成交金额(亿元)', u'成交额比重']
    df = pd.DataFrame(data=templist, columns=columns)
    saveDirector = "D:/GitData/StagePosition/" + info.StagePositionExchangeID + "/" + begindate + "/"
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


def GetDCEStagedStatistic(info,TradingDay,ExchangeID):
    """阶段性统计排名"""
    beginmonth = TradingDay.strftime("%Y%m")
    url = "http://www.dce.com.cn/publicweb/quotesdata/varietyMonthYearStatCh.html?"
    url = url + "varietyMonthYearStatQuotes.trade_type=0&varietyMonthYearStatQuotes.begin_month=%s"
    url = url%beginmonth
    GetDCEStatistic(info,url,ExchangeID,beginmonth)

def  GetDCEStatistic(info,url,ExchangeID,beginmonth):
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
    html = info.mysplider.getUrlcontent(url, header=header)
    listdata = info.mysplider.tableTolistByNum(html, "DCE", 0)
    print url
    """write to xls"""
    ext = '.csv'
    parent = "D:/GitData/StagePosition/"
    if not os.path.exists(parent + ExchangeID):
        os.mkdir(parent + ExchangeID)
    if not os.path.exists(parent + ExchangeID + "/" + beginmonth):
        os.mkdir(parent + ExchangeID + "/" + beginmonth)
    filename = beginmonth+ext
    filename = parent + ExchangeID  + "/"+beginmonth+"/" + filename
    col=[]
    col0=listdata[0]
    col1=listdata[1]
    col=[col0[0],col1[0]+col0[1],col1[1],col1[2],col1[3]+col0[1],col1[4],col1[5]+col0[2],col1[6],col1[7],col1[8]+col0[2],col1[9],col1[10],col1[11],col1[12]]
    listdata=listdata[1:]
    listdata[0]=col
    ListDataToExcel(listdata, filename)


def ListDataToExcel(listdata,filename):
    """a public method  that list data write ext extension file"""

    # file_backup=f = codecs.open(parent+info.QryPositionExchangeID+"/"+filename,'wb','utf-8')
    csvfile = file(filename.decode("utf-8"), 'wb')
    csvfile.write(codecs.BOM_UTF8)
    writer=csv.writer(csvfile)
    writer.writerows(listdata)
    csvfile.close()
    df_new = pd.read_csv(filename, encoding='utf-8')
    writer = pd.ExcelWriter(filename.replace(".csv",".xlsx"))
    df_new.to_excel(writer, index=False)
    writer.save()
    os.remove(filename)

def GetSHFEPosition(info,TradingDay,ExchangeID):
    templists=[]
    url="http://www.shfe.com.cn/data/dailydata/kx/pm"+TradingDay.strftime("%Y%m%d")+".dat"
    print url
    header={

    }
    insertsql = "INSERT INTO [dbo].[Position_Top20] ([TradingDay],[InstrumentID],[ExchangeID],[Rank],[Type],[ParticipantID1],[ParticipantABBR1],[CJ1],[CJ1_CHG],[ParticipantID2],[ParticipantIDABBR2]" \
                ",[CJ2],[CJ2_CHG],[ParticipantID3],[ParticipantABBR3],[CJ3],[CJ3_CHG]) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    html=info.mysplider.getUrlcontent(url,header=header)
    data=json.loads(html)
    data=data['o_cursor']
    for i in data:
        col=[]
        if str(i['INSTRUMENTID']).find("all")==-1:
            col = [TradingDay.strftime('%Y-%m-%d'), str(i['INSTRUMENTID']).strip(), ExchangeID, str(i['RANK']).strip(),'0', str(i['PARTICIPANTID1']).strip(),str(i['PARTICIPANTABBR1']).strip(), str(i['CJ1']).strip(), str(i['CJ1_CHG']).strip(),str(i['PARTICIPANTID2']).strip(),str(i['PARTICIPANTABBR2']).strip(), str(i['CJ2']).strip(), str(i['CJ2_CHG']).strip(),str(i['PARTICIPANTID3']).strip(),str(i['PARTICIPANTABBR3']).strip(), str(i['CJ3']).strip(), str(i['CJ3_CHG']).strip()]
            if int(col[3])<=20:
                templists.append(tuple(col))
    ResultToDatabase(info,templists,insertsql)

def GetCZCEPosition(info, startdate, ExchangeID):
    top20list=map(lambda x:str(x),range(1,21,1))
    codeList=info.GetExchangeProduct(ExchangeID)
    insertsql = "INSERT INTO [dbo].[Position_Top20] ([TradingDay],[InstrumentID],[ExchangeID],[Rank],[Type],[ParticipantABBR1],[CJ1],[CJ1_CHG],[ParticipantIDABBR2]" \
                ",[CJ2],[CJ2_CHG],[ParticipantABBR3],[CJ3],[CJ3_CHG]) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    header=info.GetExchangeHeader(ExchangeID)
    url="http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm"%(startdate.strftime("%Y%m%d")[:4],startdate.strftime("%Y%m%d"))
    print url
    templists=list()
    html=info.mysplider.getUrlcontent(url,header=header)
    table =info.mysplider.tableTolist(html,ExchangeID)
    for i in table:
        if str(i[0]).strip()=="合计" or str(i[0]).strip()=="名次":
            continue
        temp= re.findall("[A-Za-z0-9]+",str(i[0]).strip())
        if len(temp)==4:
            TradeInstrument=temp[0]
            continue
        if len(temp)==1 and temp[0] in top20list:
            TradingDay=startdate.strftime("%Y-%m-%d")
            InstrumentID=TradeInstrument
            if InstrumentID in codeList:
                Type='1'
            else:
                Type='0'
            Rank=str(i[0]).strip()
            ParticipantABBR1=str(i[1]).strip()
            CJ1=str(i[3]).strip()
            CJ1_CHG=str(i[4]).strip()
            ParticipantABBR2=str(i[5]).strip()
            CJ2=str(i[6]).strip()
            CJ2_CHG=str(i[7]).strip()
            ParticipantABBR3=str(i[8]).strip()
            CJ3=str(i[9]).strip()
            CJ3_CHG=str(i[10]).strip()
            col=[TradingDay, InstrumentID, ExchangeID, Rank,Type, ParticipantABBR1, CJ1, CJ1_CHG, ParticipantABBR2, CJ2, CJ2_CHG,ParticipantABBR3, CJ3, CJ3_CHG]
            templists.append(tuple(col))
    ResultToDatabase(info, templists, insertsql)
