# -*- coding: utf-8 -*-
# @Time    : 2018/11/12 16:51
# @Author  : ZouJunLin
from utils.Mysplider import *
import codecs,re,json
import sys,os,ConfigParser
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

def GetSettlementInfoWeek(info,TradingDay,ExchangeID):
    selectsql = """
          select  [TradingDay] from [PreTrade].[dbo].[SettlementInfoWeek] where TradingDay='%s' and ExchangeID='%s' and IsFuture=1
    """
    # list1 = IsExistData(selectsql %(TradingDay.strftime("%Y-%m-%d"),ExchangeID))

    list1=info.mysql.ExecQuery(selectsql %(info.TradingDay,ExchangeID))
    if len(list1) == 0:
        ###爬取期货信息
        if ExchangeID=="DCE":
            GetDCEFutureWeekInfo(TradingDay,info)
        elif ExchangeID=="CZCE":
            GetCZCEFutureWeekInfo(TradingDay,info)
        elif ExchangeID=="SHFE":
            GetSHFEFutureWeekInfo(TradingDay,info)
        elif ExchangeID=="CFFEX":
            GetCFFEXFutureWeekInfo(TradingDay,info)
    else:
        print TradingDay, ExchangeID + "期货周统计数据已经存在"

    optselectsql = """
        select TradingDay from [dbo].[SettlementInfoWeek] where TradingDay='%s' and ExchangeID='%s' and IsFuture=0
    """

        # 爬取期权信息
    list1 =info.mysql.ExecQuery(optselectsql %(info.TradingDay,ExchangeID))
    if len(list1) == 0:
        if ExchangeID=="DCE":
            GetDCEOptionWeekInfo(TradingDay, info)
        elif ExchangeID=="CZCE":
            GetCZCEOptionWeekInfo(TradingDay, info)
        elif ExchangeID=="SHFE" and TradingDay.strftime("%Y-%m-%d")>"2018-09-20":
            GetSHFEOptionWeekInfo(TradingDay, info)
        elif ExchangeID=="CFFEX":
            GetCFFEXOptionDayInfo(TradingDay, info)
    else:
        print TradingDay,ExchangeID+ "期权周统计数据已经存在"


"""-------------------------------------------------中金所周统计数据------------------------------------------------------------------"""
def GetCFFEXOptionDayInfo(TradingDay, mysplider):
    """"中金所暂时没有期权"""
    pass


def GetCFFEXFutureWeekInfo(TradingDay, info):
    url = "http://www.cffex.com.cn/sj/hqsj/ztj/%s/%s/index.xml" % (TradingDay.strftime("%Y%m%d")[0:6], TradingDay.strftime("%Y%m%d")[6:8])
    print url
    header = {
    }
    sql = """
           INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
           ([TradingDay],[InstrumentID],[OpenPrice],[ExchangeID],[IsFuture],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],[Volume],[Position],[PositionChange],[Turnover])
             VALUES('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')
           """
    html = info.mysplider.getUrlcontent(url, header)
    bs = BeautifulSoup(html, "xml")
    content = bs.findAll("weeklydata")
    tradingday = TradingDay.strftime("%Y-%m-%d")
    sqllist = []
    for i in content:
        temp=[]
        CSPriceChange = float(str(i.find("pricechange").text).strip())
        temp=[info.TradingDay,str(i.find("instrumentid").text).strip(),i.find("openprice").text,"CFFEX", 1,
                              i.find("highestprice").text, i.find("lowestprice").text, i.find("closeprice").text,
                               i.find("settlementprice").text, CSPriceChange,i.find("volume").text,
                                str(int(float(i.find("openinterest").text))), str(int(float(i.find("openinterestchange").text))),str(int(float(i.find("turnover").text)))]
        if temp[8] != "" and temp[4] == "" and temp[5] == "" and temp[6] == "" and temp[2]=="":
            temp[7] = temp[8]
            temp[5] = temp[8]
            temp[6] = temp[8]
            temp[2] = temp[8]
        sqllist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqllist, sql,info)


"""-------------------------------------------------上期所-----------------------------------------------------------------------------"""
def GetSHFEOptionWeekInfo(TradingDay, info):
    url="http://www.shfe.com.cn/data/dailydata/option/kx/kx%s.dat"% (TradingDay.strftime("%Y%m%d"))
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
        INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
        ([TradingDay],[InstrumentID],[OpenPrice],[ExchangeID],[IsFuture],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],[Volume],[Position],[PositionChange],[Turnover])
          VALUES('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')
        """
    # tradingday = TradingDay.strftime("%Y-%m-%d")
    html = info.mysplider.getUrlcontent(url, header)
    if html=="":
        return
    data = json.loads(html)
    contentdata = data['o_curinstrument']
    sqllist = []
    for i in contentdata:
        temp = []
        if str(i['PRODUCTNAME'].encode("utf-8")).strip().find("计") == -1 and str(i['SETTLEMENTPRICE']).strip() != "":
            InstrumentID = str(i['INSTRUMENTID']).strip()
            temp = [info.TradingDay, InstrumentID, i['OPENPRICE'], 'SHFE', 0, i['HIGHESTPRICE'], i['LOWESTPRICE'],
                    i['CLOSEPRICE'], i['SETTLEMENTPRICE'], i['ZD1_CHG'], i['VOLUME'], i['OPENINTEREST'],
                    i['OPENINTERESTCHG'], i['TURNOVER']]
            if temp[8] != "" and temp[4] == "" and temp[5] == "" and temp[6] == "" and temp[2] == "":
                temp[2] = temp[8]
                temp[5] = temp[8]
                temp[6] = temp[8]
                temp[7] = temp[8]
            sqllist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqllist, sql,info)

def  GetSHFEFutureWeekInfo(TradingDay,info):
    url = "http://www.shfe.com.cn/data/dailydata/%s.dat" % (TradingDay.strftime("%Y%m%d"))
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
              INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
                          ([TradingDay],[InstrumentID],[OpenPrice],[ExchangeID],[IsFuture],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],[Volume],[Position],[PositionChange],[Turnover])
                       VALUES('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')
            """
    tradingday = TradingDay.strftime("%Y-%m-%d")
    html = info.mysplider.getUrlcontent(url, header)
    data = json.loads(html)
    contentdata = data['o_cursor']
    sqllist = []
    for i in contentdata:
        temp = []
        # if str(i['PRODUCTNAME'].encode("utf-8")).strip().find("计")==-1 and str(i['SETTLEMENTPRICE']).strip()!="":
        #     InstrumentID = str(i['PRODUCTID']).strip().replace("_f", "") + str(i['DELIVERYMONTH']).strip()
        InstrumentID=i['INSTRUMENTID']
        temp=[info.TradingDay,InstrumentID,i['OPENPRICE'],'SHFE',1, i['HIGHESTPRICE'], i['LOWESTPRICE'],
             i['CLOSEPRICE'], i['SETTLEMENTPRICE'], i['PRICECHG'],i['VOLUME'], i['OPENINTEREST'],i['OPENINTERESTCHG'],i['TURNOVER']]
        if temp[8] != "" and temp[4] == "" and temp[5] == "" and temp[6] == "" and temp[2]=="":
            temp[2] = temp[8]
            temp[5] = temp[8]
            temp[6] = temp[8]
            temp[7] = temp[8]
        sqllist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqllist, sql,info)

""""------------------------------------郑商所------------------------------------------------------------------------------------------"""
def GetCZCEOptionWeekInfo(TradingDay, info):
    url="http://www.czce.com.cn/cn/DFSStaticFiles/Option/%s/%s/OptionDataWeek.htm"% (TradingDay.strftime("%Y%m%d")[0:4], TradingDay.strftime("%Y%m%d"))
    print url
    header = {
        'Host': 'www.czce.com.cn',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'UM_distinctid=166af6525622e4-0faa66e98eda2e-51422e1f-384000-166af6525631f6; TS014ada8c=0169c5aa32fe35dc537b50f6666cbb9c144dec5a7442b52e72b0ed6e1cd40919533bdef5c5; CNZZDATA1264458526=1107363939-1540538968-null%7C1542093310'
    }
    sql = """
            INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
                       ([TradingDay],[InstrumentID],[OpenPrice],[ExchangeID],[IsFuture],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],[Volume],[Position],[PositionChange],[Turnover])
                    VALUES('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')
            """
    html = info.mysplider.getUrlcontent(url, header)
    datalist =info.mysplider.tableTolist(html, "CZCE")
    sqllist = []
    if len(datalist)<8:
        return
    # tradingday = TradingDay.strftime("%Y-%m-%d")
    for i in datalist[1:-1]:
        temp = list(i)
        if not (temp[0] == "小计" or temp[0] == "总计"or str("".join(temp)).find("合计")!=-1 or temp[0]=="品种代码"):
            temp.insert(0, info.TradingDay)
            temp.pop(14)
            temp.insert(4, 0)
            temp.pop(10)
            if temp[5]=="0.00" and temp[6]=="0.00" and temp[7]=="0.00" and temp[2]=="0.00":
                temp[5]=temp[8]
                temp[6]=temp[8]
                temp[7]=temp[8]
                temp[8]=temp[8]
            sqllist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqllist, sql,info)



def GetCZCEFutureWeekInfo(TradingDay,info):
    url = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataWeek.htm" % (TradingDay.strftime("%Y%m%d")[0:4], TradingDay.strftime("%Y%m%d"))
    print url
    header = {
        'Host': 'www.czce.com.cn',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'BIGipServerwww_cbd=842836160.23067.0000; TS014ada8c=0169c5aa3230adb3c09220355703fac1145e05cd70e2a68cc18a0b35cba8d75ef05dd1e2d0'
    }
    sql = """
        INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
                   ([TradingDay],[InstrumentID],[OpenPrice],[ExchangeID],[IsFuture],[HighestPrice],[LowestPrice],[ClosePrice],[CSPriceChange],[Position],[PositionChange],[SettlementPrice],[Volume],[Turnover])
                VALUES('%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')
        """
    html = info.mysplider.getUrlcontent(url, header)
    datalist = info.mysplider.tableTolist(html, "CZCE")
    sqllist = []
    tradingday = info.TradingDay
    for i in datalist[1:-1]:
        temp = list(i)
        if not (temp[0] == "小计" or temp[0] == "总计" or temp[0]=="品种月份" or str("".join(temp)).find("合计")!=-1):
            temp.insert(0, tradingday)
            temp.pop(8)
            temp.insert(4,1)
            if temp[5]=="0.00" and temp[6]=="0.00" and temp[7]=="0.00" and temp[2]=="0.00":
                temp[5]=temp[11]
                temp[6]=temp[11]
                temp[7]=temp[11]
                temp[2]=temp[11]
            sqllist.append(tuple(temp))

    info.mysplider.dataToSqlserver(sqllist, sql,info)



"""------------------------------大商所--------------------------------------------------------------------------------------------------"""
def GetDCEOptionWeekInfo(TradingDay, info):
    url = "http://www.dce.com.cn/publicweb/quotesdata/weekQuotesCh.html?weekQuotes.variety=all&weekQuotes.trade_type=1&year=%s&month=%s&day=%s"
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=1708B5775DE8AF0F906A1B42D2EF5297; WMONID=MvxMQtFstud; Hm_lvt_a50228174de2a93aee654389576b60fb=1535448681,1535535741,1535589732,1535596877',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    optsql = """
        INSERT INTO [dbo].[SettlementInfoWeek]([TradingDay],[InstrumentID],[ExchangeID],[IsFuture],[OpenPrice],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],
        [Volume],[Position],[PositionChange],[Turnover])
         VALUES('%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
    """

    Optionurl = url % ( TradingDay.year, TradingDay.month - 1, TradingDay.day)
    print Optionurl
    html = info.mysplider.getUrlcontent(Optionurl, header)
    datalist = info.mysplider.tableTolist(html, "DCE")
    sqlist = []
    for i in datalist[:-2]:
        if str(i[0]).encode("utf-8").find("计")!=-1 or str(i[1]).encode("utf-8").strip()=="":
            continue
        temp = list(i)[1:]
        temp.insert(0, info.TradingDay)
        temp.insert(3,0)
        del (temp[14])
        if temp[7]!="" and temp[4]=="" and temp[5]=="" and temp[6]=="":
            temp[4]=temp[7]
            temp[5] = temp[7]
            temp[6] = temp[7]
        sqlist.append(tuple(temp))
    info.mysplider.dataToSqlserver(sqlist, optsql,info)




def GetDCEFutureWeekInfo(TradingDay, info):
    url="http://www.dce.com.cn/publicweb/quotesdata/weekQuotesCh.html?weekQuotes.variety=all&weekQuotes.trade_type=0&year=%s&month=%s&day=%s"
    # url = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html?dayQuotes.variety=all&dayQuotes.trade_type=%s&year=%s&month=%s&day=%s"
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

    sql = """
    INSERT INTO [PreTrade].[dbo].[SettlementInfoWeek]
               ([InstrumentID],[TradingDay],[ExchangeID],[IsFuture],[OpenPrice],[HighestPrice],[LowestPrice],[ClosePrice],[SettlementPrice],[CSPriceChange],[Volume],[Position],[PositionChange],[Turnover])
            VALUES('%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
    """

    Futureurl = url % (TradingDay.year, TradingDay.month - 1, TradingDay.day)
    print Futureurl
    html = info.mysplider.getUrlcontent(Futureurl, header)
    datalist = info.mysplider.tableTolist(html, "DCE")

    sqllist = dataclean(info,datalist, TradingDay)
    info.mysplider.dataToSqlserver(sqllist,sql,info)

def dataclean(info,datalist,tradingday):
    """
    数据清洗
    :param datalist:
    :return:
    """
    sqllist=[]
    dirpath = os.path.abspath(os.path.join(os.getcwd(), "../."))
    cf = ConfigParser.ConfigParser()
    f=codecs.open(dirpath + "\\" +'InstrumentCode.ini',mode='r+',encoding="utf-8-sig")
    cf.readfp(f)
    re_words = re.compile(u"[\u4e00-\u9fa5]+")
    tradingday=tradingday.strftime("%Y-%m-%d")
    for i in datalist[:-1]:
        i=list(i)
        col=[]
        s=str(i[0]).decode("utf-8")
        if str(i[1]).decode("utf-8")=="":
            continue
        if s.find("小计")==-1:
            s=re_words.search(s).group()
            InstrumentId=cf.get("DCE",s)
            if InstrumentId.startswith("sum"):
                for j in range(3,11):
                    i[j]=""
            else:
                InstrumentId = cf.get("DCE", s)+i[1]
            i[1] = info.TradingDay
            i[0]=InstrumentId
            i.insert(3,1)
            # del(i[14])
            if i[7]!="" and i[4]=="" and i[5]=="" and i[6]=="":
                i[4]=i[7]
                i[5]=i[7]
                i[6]=i[7]
            sqllist.append(tuple(i))
    f.close()
    return sqllist

def TradingDayIsWeekLDay(day):
    """判断是否为这一周的最后一天"""
    if day.isoweekday()=='5':
        return True
    else:
        return False

def GetWeekLDay(day):
    """找到每一周的最后一个交易日"""
    return day+datetime.timedelta(days=(5-day.isoweekday))

def IsTradingWeek(day,t):
    """判断这一周是否有交易"""
    startday=day-datetime.timedelta(days=4)
    while startday<day:
        if t.IsTradingDayFuture(day.strftime("%Y%m%d")):
            return True
        startday=startday+datetime.timedelta(days=1)
    return False


def GetNextTradingWeekDay(day,t):
    """找出下一个有交易日的周五"""
    day = day + datetime.timedelta(days=7)
    while not IsTradingWeek(day,t):
        day = day + datetime.timedelta(days=7)
    return day

def GetWeekLastTradingDay(day,t):
    """
    找出每周的最后一个，函数思想:交易所周六周日不交易，既可以从每周五往前计算，直到找到一个交易日，返回与IsTradingWeek合并使用
    """
    startday = day - datetime.timedelta(days=4)
    endday = day
    while endday >= startday:
        if t.IsTradingDayFuture(endday.strftime("%Y%m%d")):
            return endday
        endday = endday - datetime.timedelta(days=1)
    raise Exception