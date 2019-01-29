# -*- coding: utf-8 -*-
# @Time    : 2018/11/13 18:24
# @Author  : ZouJunLin
import datetime
from utils.Mysplider import *
from utils.BasicAPI import *
import re,os
import codecs,ConfigParser,json
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def GetContractInfo(tradingday, info,ExchangeID):
    if ExchangeID=="SHFE":
        GetSHFEFutureContractInfo(tradingday, info,ExchangeID)
        GetSHFEOptionContractInfo(tradingday, info,ExchangeID)
    elif ExchangeID=="DCE":
        GetDCEContractInfo(tradingday, info,ExchangeID)
    elif ExchangeID=="CFFEX":
        GetCFFEXContractInfo(tradingday, info,ExchangeID)

"""---------------------------------获取郑商所期货合约信息------------------------------------------"""
def GetCZCEContractInfo(tradingday,info):
    #delsql = "delete from dbo.ContractInfo where [EndDelivDate]='-' and ExchangeID='CZCE'"

    url="http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp?tradetype=future&__go2pageNO=%s&DtbeginDate=%s&DtendDate=%s"
    optionurl="http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp?tradetype=option&__go2pageNO=%s&DtbeginDate=%s&DtendDate=%s"
    #mysplider.updataData(delsql)
    TradingDay=tradingday.strftime("%Y-%m-%d")
    DtbeginDate=str(int(datetime.datetime.now().year)-1)+"-06-01"
    DtendDate=datetime.datetime.now()+datetime.timedelta(days=30)
    DtendDate=DtendDate.strftime("%Y-%m-%d")
    print DtbeginDate,DtendDate
    GetCZCEFutureContractInfo(TradingDay, info,DtbeginDate,DtendDate)



def GetCZCEFutureContractInfo(tradingday, info,DtbeginDate,DtendDate):
    Instrumentsql = "SELECT [InstrumentID],[IsFuture] FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='CZCE' and TradingDay='%s'"%tradingday
    print Instrumentsql
    url = "http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp?tradetype=future&__go2pageNO=%s&DtbeginDate=%s&DtendDate=%s"
    opturl = "http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp?tradetype=option&__go2pageNO=%s&DtbeginDate=%s&DtendDate=%s"

    sql = """
      INSERT INTO [PreTrade].[dbo].[ContractInfo]([InstrumentID],[InstrumentName],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate] ,[EndDelivDate])VALUES('%s','%s','%s','%s','%s','%s','%s','%s')
             """
    sqllist=[]
    #templist=GetTbalelist(mysplider,url,DtbeginDate,DtendDate)
    dataList=IsExistData(Instrumentsql,info)
    FutureList=[]
    OptionList=[]
    FutureTableList=GetTbalelist(info,url,DtbeginDate,DtendDate)
    OptionTableList=GetTbalelist(info,opturl,DtbeginDate,DtendDate)
    print OptionTableList
    for i in dataList:
        if i[1]:
            FutureList.append(str(i[0]).encode("utf-8"))
        else:
            OptionList.append(str(i[0]).encode("utf-8"))
    print FutureList
    print OptionList
    """对于期权"""
    for i in OptionList:
        """查询是否存在该合约"""
        col = []
        querysql = """SELECT [InstrumentID],[InstrumentName],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate],[EndDelivDate] FROM [PreTrade].[dbo].[ContractInfo] where InstrumentID='%s'""" % i
        Isexist = IsExistData(querysql,info)
        if len(Isexist):
            """如果存在，取出来，删除源数据，修改数据，保存到list"""
            col =list(Isexist[0])
            delsql = "delete from dbo.ContractInfo where InstrumentID='%s'" % i
            if str(col[5]).encode("utf-8").strip()== "" or str(col[6]).encode("utf-8").strip() == "":
                info.mysplider.updataData(delsql,info)
                startDay, endDay = SearchList(OptionTableList, i, 0)
                if startDay.strip() == "":
                    startDay = GetStartDay(col[0], info)
                col[5] = startDay
                col[6] = endDay
                sqllist.append(tuple(col))
        else:
            """如果不存在，新建list,保存"""
            code = str(re.match(r"\D+", i).group())
            try:
                name = info.GetDetailByTradeCode(code)
            except:
                print code
            startDay, endDay = SearchList(OptionTableList, i,0)
            if startDay.strip() == "":
                startDay = GetStartDay(i, info)
            col = [i, name[1], 'CZCE', name[0], name[2], startDay, endDay, "-"]
            sqllist.append(tuple(col))

    """对于期货合约"""
    for i in FutureList:
        """查询是否存在该合约"""
        querysql="""SELECT [InstrumentID],[InstrumentName],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate],[EndDelivDate] FROM [PreTrade].[dbo].[ContractInfo] where InstrumentID='%s'"""%i
        Isexist = IsExistData(querysql,info)
        if len(Isexist):
            """如果存在，取出来，删除源数据，修改数据，保存到list"""
            col = list(Isexist[0])
            if str(col[5]).encode("utf-8").strip()=="" or str(col[6]).encode("utf-8").strip()=="":
                info.mysplider.updataData(delsql,info)
                startDay, endDay = SearchList(FutureTableList, i, 1)
                if startDay.strip() == "":
                    startDay = GetStartDay(col[0], info)
                if endDay.strip()=="":
                    endDay=info.basicapi.Get_EndDate(info).GetEndDate(i)[0]
                col[5]=startDay
                col[6] = endDay
                sqllist.append(tuple(col))
        else:
            """如果不存在，新建list,保存"""
            tempcol = info.basicapi.Get_EndDate(info).GetEndDate(i)
            code = str(re.match(r"\D+", i).group())
            name = info.GetDetailByTradeCode(code)
            startDay,endDay=SearchList(FutureTableList,i,1)
            if startDay.strip() == "":
                startDay = GetStartDay(i, info)
            if endDay.strip() == "":
                endDay = info.basicapi.Get_EndDate(info).GetEndDate(i)[0]
            col = [i, name[1], 'CZCE', name[0], name[2],startDay,endDay,tempcol[1]]
            sqllist.append(tuple(col))
        """写入数据库"""
    info.mysplider.dataToSqlserver(sqllist, sql,info)

def GetStartDay(InstrumentID,info):
    sql="SELECT top 1 [TradingDay] FROM [PreTrade].[dbo].[SettlementInfo] where InstrumentID ='%s' order by TradingDay"%InstrumentID
    Isexist = IsExistData(sql,info)
    if len(Isexist):
        return str(list(Isexist[0])[0]).replace("-","")
    else:
        print "without this InstrumentID,Maybe you have not got it in SettlementInfo Table"
        return ""

def SearchList(TableList,t,IsFuture):
    if IsFuture:
        t1=t
        t=list(t)
        t.insert(2, '1')
        t="".join(t)
    else:
        t1 = t[:5]
        t = list(t)[:5]
        t.insert(2, '1')
        t = "".join(t)
    sDay=""
    eDay=""
    for i in TableList:
        if i[1].find(t)!=-1 or i[1].find(t1)!=-1:
            sDay=i[0]
        if i[2].find(t)!=-1 or i[2].find(t1)!=-1:
            eDay=i[0]
    return sDay.replace("-",""),eDay.replace("-","")

def GetTbalelist( info,url,DtbeginDate,DtendDate):
    sqllist = []
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Content-Encoding': 'gzip, deflate',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'UM_distinctid=166af6525622e4-0faa66e98eda2e-51422e1f-384000-166af6525631f6; JSESSIONID=wqkWOfd7CcK1OLNvQaQm2bS4-JpqEB_SZvrM5KwXFT7L7bQ6dqw4!101336818; TS014315bf=0169c5aa321a22115ea3ad8ef1cfadac3c16f27cd57aea8856b6950d93e86316a75a0d1cdfe7b3b06d1561018bd469212cd9964c6a',
        'Host': 'app.czce.com.cn',
        'Pragma': 'no-cache',
        'Referer': 'http://app.czce.com.cn/cms/pub/search/searchjyyl.jsp',
        'Upgrade-Insecure-Requests': '1'
    }
    flag=True
    temp=1
    while flag:
        print temp,DtbeginDate,DtendDate
        print url%(temp,DtbeginDate,DtendDate)
        html = info.mysplider.getUrlcontent(url%(temp,DtbeginDate,DtendDate), header)
        if html!="":
            tablelist = info.mysplider.tableTolist(html, "CZCE")
        else:
            flag=False
            break
        if len(tablelist)<2:
            flag=False
            break
        else:
            for i in tablelist[1:]:
                if not (str(i[4]).strip()=="" and str(i[5]).strip()==""):
                    col = []
                    col.append(i[1])
                    col.append(i[4])
                    col.append(i[5])
                    sqllist.append(col)
        temp+=1
    return sqllist






"""-------------------------------- 获取中金所合约信息----------------------------------------------"""
def GetCFFEXContractInfo(tradingday, info,ExchangeID):
    sql = """
    INSERT INTO [PreTrade].[dbo].[ContractInfo]([InstrumentName],[InstrumentID],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate] ,[EndDelivDate])VALUES('%s','%s','%s','%s','%s','%s','%s','%s')
           """

    delsql = "delete from dbo.ContractInfo where [EndDelivDate]='-' or EndDate>'%s' and ExchangeID='%s'" %(tradingday.strftime("%Y%m%d"), ExchangeID)
    url = 'http://www.cffex.com.cn/cp/index_6719.xml'
    selectsql = "select InstrumentID from ContractInfo where InstrumentID='%s'"
    header = {}
    info.mysplider.updataData(delsql,info)
    html = info.mysplider.getUrlcontent(url, header)

    bs = BeautifulSoup(html, "xml")
    content = bs.findAll("T_INSTRUMENTPROPERTY")
    sqllist = []
    for i in content:
        InstrumentID = str(i.find('INSTRUMENTID').text.encode('utf-8'))
        Isexist = IsExistData(selectsql%InstrumentID,info)
        if not len(Isexist):
            code = str(re.match(r"\D+", InstrumentID).group())
            name = info.GetDetailByTradeCode(code)
            endDelivedate= info.basicapi.Get_EndDate(info).GetEndDate(InstrumentID)[1] if str(i.find("ENDDELIVDATE").text).strip() == '' else str(i.find("ENDDELIVDATE").text).strip()
            sqllist.append(tuple([name[1], InstrumentID, "CFFEX", name[0], name[2],
                                  i.find("OPENDATE").text, i.find("EXPIREDATE").text,endDelivedate]))
    info.mysplider.dataToSqlserver(sqllist, sql,info)

"""--------------------------------获取大商所期货合约信息------------------------------------------"""
def  GetDCEContractInfo(tradingday, info,ExchangeID):
    url = "http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html"
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Content-Encoding': 'gzip, deflate',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=A658D95EFCC97C3893E2B1320EB67EC9; WMONID=MvxMQtFstud; Hm_lvt_a50228174de2a93aee654389576b60fb=1534927461,1534984838,1535016855,1535448681',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sql = """
    INSERT INTO [PreTrade].[dbo].[ContractInfo]([InstrumentName],[InstrumentID],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate] ,[EndDelivDate])VALUES('%s','%s','%s','%s','%s','%s','%s','%s')
           """
    TradingDay = tradingday.strftime("%Y-%m-%d")
    delsql = "delete from dbo.ContractInfo where [EndDelivDate]='-' or EndDate>'%s' and ExchangeID='%s'"%(TradingDay.replace("-",""),ExchangeID)
    Instrumentsql = "SELECT [InstrumentID] FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='DCE' and TradingDay='%s'" % TradingDay
    selectsql="select InstrumentID from ContractInfo where InstrumentID='%s' "
    DayInstrumentlist=IsExistData(Instrumentsql,info)
    templist=[]
    for i in DayInstrumentlist:
        templist.append(str(i[0]).encode("utf-8"))
    content = info.mysplider.getUrlcontent(url, header)
    datalist = info.mysplider.tableTolist(content, ExchangeID)
    info.mysplider.updataData(delsql,info)
    sqllist=[]
    temp=[]
    for i in datalist:
        """首先判断是否存在"""
        temp = list(i)
        if temp[1]  in templist:
            if len(temp[1])>=7:
                pass
            isexit =IsExistData(selectsql%temp[1],info)
            if not len(isexit):
                if temp[7]=="":
                    temp[7]=info.basicapi.Get_EndDate(info).GetEndDate(temp[1])[1]
                sqllist.append(tuple(temp))
    # for i in datalist:
    #     """首先判断是否存在"""
    #     temp = list(i)
    #     if temp[7] == "":
    #         temp[7] = "-"
    #     sqllist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqllist, sql,info)


"""---------------------------------获取上期所合约信息----------------------------------------------"""
def GetSHFEOptionContractInfo(tradingday, info,ExchangeID):
    url="http://www.shfe.com.cn/data/instrument/option/ContractBaseInfo%s.dat"% tradingday.strftime("%Y%m%d")
    print url
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'shfe_cookid=1810261604402a0306a4bf0a876962f8f9ea91cbbff6; shfe_fbl=2560*1440',
        'Host': 'www.shfe.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sql = """
        INSERT INTO [PreTrade].[dbo].[ContractInfo]([InstrumentID],[InstrumentName],[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate] ,[EndDelivDate])VALUES('%s','%s','%s','%s','%s','%s','%s','%s')
               """
    selectsql = "select InstrumentID from ContractInfo where InstrumentID='%s'"
    html = info.mysplider.getUrlcontent(url, header)
    data = json.loads(html)
    data = data['OptionContractBaseInfo']

    sqllist = []
    for i in data:
        # 查询数据是否存在
        isexit = IsExistData(selectsql % str(i['INSTRUMENTID']).strip(),info)
        if not len(isexit):
            sqllist.append(tuple(
                [str(i['INSTRUMENTID']).strip(), str(i['commodityName']).strip(), "SHFE",  str(i['TRADEUNIT']).strip(), str(i['PRICETICK']).strip(),
                 str(i['OPENDATE']),str(i['EXPIREDATE']),str(i['EXPIREDATE'])]))
    info.mysplider.dataToSqlserver(sqllist, sql,info)


def GetSHFEFutureContractInfo(tradingday, info,ExchangeID):
    url = "http://www.shfe.com.cn/data/instrument/ContractBaseInfo%s.dat" % tradingday.strftime("%Y%m%d")
    print url
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'shfe_cookid=180824110220b34cbb20ae74edd52deea05ac6be61fd; shfe_fbl=2560*1440',
        'Host': 'www.shfe.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sql = """
    INSERT INTO [PreTrade].[dbo].[ContractInfo]([InstrumentID],[InstrumentName] ,[ExchangeID],[VolumeMultiple],[PriceTick],[StartDate],[EndDate],[EndDelivDate])VALUES('%s','%s','%s','%s','%s','%s','%s','%s')
    """
    selectsql="select InstrumentID from ContractInfo where InstrumentID='%s'"

    tradingday = tradingday.strftime("%Y%m%d")
    html = info.mysplider.getUrlcontent(url, header)
    data = json.loads(html)
    data = data['ContractBaseInfo']
    cf = ConfigParser.ConfigParser()
    dirpath = os.path.abspath(os.path.join(os.getcwd(), "./.."))
    f = codecs.open(dirpath + "\\" + 'InstrumentCode.ini', mode='r+', encoding="utf-8-sig")
    cf.readfp(f)
    delsql = "delete from dbo.ContractInfo where [EndDelivDate]='-' or EndDate>'%s' and ExchangeID='%s'" % (tradingday, ExchangeID)
    info.mysplider.updataData(delsql,info)
    sqllist = []
    for i in data:
        #查询数据是否存在
        isexit=IsExistData(selectsql%str(i['INSTRUMENTID']).strip(),info)
        if not len(isexit):
            code =  str(re.match(r"\D+", i['INSTRUMENTID']).group())
            name=info.GetDetailByTradeCode(code)
            sqllist.append(tuple(
                [str(i['INSTRUMENTID']).strip(), name[1], "SHFE", name[0], name[2],
                 str(i['OPENDATE']), str(i['EXPIREDATE']), str(i['ENDDELIVDATE'])]))
    info.mysplider.dataToSqlserver(sqllist, sql,info)