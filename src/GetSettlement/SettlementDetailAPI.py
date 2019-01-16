# -*- coding: utf-8 -*-
# @Time    : 2018/11/5 15:57
# @Author  : ZouJunLin

from utils.Mysplider import *
import codecs,ConfigParser,json
from utils.sqlServer import *
from utils.InfoApi import *
import sys
reload(sys)

def CZCESettlementDetail(TradingDay,infoapi):
    """郑商所的结算保证金，手续费的获取"""
    sql1 = """
        select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='CZCE'
    """

    sql1Opt = """
        select TradingDay from [dbo].[SettlementInfoOpt] where TradingDay='%s' and ExchangeID='CZCE'
    """

    list1 = IsExistData(sql1 % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        ###爬取期货信息
        GetCZCEFutureSettlementDeatil(TradingDay, infoapi, "futuresettle")
    else:
        print TradingDay, "郑商所期货结算续费数据已经存在"

def GetCZCEFutureSettlementDeatil(TradingDay,infoapi,type):
    """
    获取郑商所期货结算详细信息
    :param TradingDay:
    :param type:
    :return:
    """

    sql = """
         INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],
         [MarginHedgingSale],[SettlementMethod],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[DeliveMethod],[DeliveCharge],[CloseTodayMethod],[CloseTodayPrice])
     VALUES('%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
     """

    # sql = """
    #     INSERT INTO [dbo].[CZCESettlementDetail]([TradingDay],[InstrumentID],[SettlementPrice] ,[MarginBuy],[MarginSale],[TradingCharge],[DeliveCharge],[ClosePrice])VALUES('%s','%s',%s,%s,%s,%s,%s,%s)
    # """

    header = {
        'Host': 'www.czce.com.cn',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Content-Encoding': 'gzip',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'BIGipServerwww_cbd=842836160.23067.0000; JSESSIONID=kYM9bphG26lQ6p25GXxyVRbkFw815nqCmJmTYxzGxxxvJnVTblhb!-1757810877; TS014ada8c=0169c5aa326a6aa66d379d396a2033d86a84f272d2b5c2f5d7b7d668355885aa62597926b41ead5b31a9f62f3252ecfb1e2c33422b',
        'Referer': 'http://www.zce.cn/portal/jysj/qqjysj/qqmrhq/A09112102index_1.htm'
    }

    filename =type+TradingDay.strftime("%Y%m%d") + ".txt"
    futureurl = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataClearParams.txt" % (TradingDay.strftime("%Y%m%d")[0:4], TradingDay.strftime("%Y%m%d"))
    print futureurl
    html=infoapi.mysplider.getUrlcontent(futureurl,header=header)
    file = open("./data/1.txt", "wb")
    #print filename,file
    file.write(html)
    file.flush()
    file.close()

    j = 0
    sqllist = []
    for line in open("./data/1.txt", "r"):
        col = []
        #print "line",line
        if j != 0 and j != 1 and j != "" and len(line) != 1:
            for k in line.strip().split('|'):

                col.append(k.replace(",", "").strip())
            col.insert(0, TradingDay.strftime("%Y-%m-%d"))
            col.pop(3)
            col.pop(3)
            col[3]=str(float(col[3])/100.0)
            col[4] = str(float(col[4]) / 100.0)
            col.insert(5,col[3])
            col.insert(6,col[4])
            col.insert(2,"CZCE")
            col.insert(4,1)
            col.insert(5,0)
            col.insert(6,1)
            col.insert(11,0)
            col.insert(13, col[12])
            col.insert(14, col[12])
            col.insert(15, col[12])
            col.insert(16,0)
            col.insert(18,0)
            sqllist.append(tuple(col))
        j += 1
    infoapi.mysplider.dataToSqlserver(sqllist,sql,infoapi)




def DCESettlementDetail(TradingDay, infoapi):
    sql1 = """
            select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='DCE' and IsFutures=1
        """

    list1 = IsExistData(sql1 % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        ###爬取期货信息
        GetDCEFutureSettlementDetail(TradingDay, infoapi)
    else:
        print TradingDay, "大商所期货保证金，手续费相关数据已经存在"

    """查询期大商所期权数据是否存在"""
    sql1Opt="""
            select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='DCE' and IsFutures=0
    """
    list1 = IsExistData(sql1Opt % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        GetDCEOptionSettlementDetail(TradingDay, infoapi)
    else:
        print TradingDay, "期权数据已经存在"

def GetDCEOptionSettlementDetail(TradingDay, infoapi):
    """
        获取期权的手续费基本信息
        :return:
        """
    Fourl = 'http://www.dce.com.cn/publicweb/businessguidelines/queryFutAndOptSettle.html?variety=all&trade_type=%s&year=%s&month=%s&day=%s'
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=5C1180D80CDBC1F61E550F5B7B1F9864; WMONID=MxRKrfMhnv4; Hm_lvt_a50228174de2a93aee654389576b60fb=1534468904,1534468904',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sqlOpt = """
            INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[SettlementMethod],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort]
            ,[DeliveMethod],[DeliveCharge],[MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],[MarginHedgingSale])
        VALUES('%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    Settlementsql="""
        SELECT  [InstrumentID],[SettlementPrice] FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='DCE' and TradingDay='%s' and IsFuture=0
    """%TradingDay.strftime("%Y-%m-%d")
    settlementPrice={}
    settlement=IsExistData(Settlementsql,infoapi)
    for i in settlement:
        settlementPrice[i[0]]=i[1]
    url = Fourl % (1, TradingDay.year, TradingDay.month - 1, TradingDay.day)
    html = infoapi.mysplider.getUrlcontent(url, header=header)
    table = infoapi.mysplider.tableTolist(html, "DCE")
    sqllist = []
    for i in table:
        col = []
        col = (list(i))[1:]
        del col[1]
        col.insert(0, TradingDay.strftime("%Y-%m-%d"))
        col.insert(2,"DCE")
        col.insert(3,settlementPrice[col[1]])
        col.insert(4,0)
        col.insert(5,1)
        col.insert(6,0)
        col.insert(10,0)
        col.insert(13,0)
        col.insert(15,col[14])
        col.insert(16,col[14])
        col.insert(17,col[14])
        sqllist.append(tuple(col))
    infoapi.mysplider.dataToSqlserver(sqllist, sqlOpt,infoapi)



def GetDCEFutureSettlementDetail(TradingDay,infoapi):
    """
    获取期货的手续费基本信息
    :return:
    """
    Fourl = 'http://www.dce.com.cn/publicweb/businessguidelines/queryFutAndOptSettle.html?variety=all&trade_type=%s&year=%s&month=%s&day=%s'
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=5C1180D80CDBC1F61E550F5B7B1F9864; WMONID=MxRKrfMhnv4; Hm_lvt_a50228174de2a93aee654389576b60fb=1534468904,1534468904',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sql = """
            INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[SettlementMethod],
            [MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],[MarginHedgingSale],[DeliveMethod],[DeliveCharge])
        VALUES('%s','%s','%s','%s',%s,%s,'%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s)
        """

    url=Fourl%(0,TradingDay.year,TradingDay.month-1,TradingDay.day)
    print url
    html=infoapi.mysplider.getUrlcontent(url,header=header)
    table=infoapi.mysplider.tableTolist(html,"DCE")
    sqllist=[]
    for i in table:
        col=[]
        col=(list(i))[1:]
        del col[1]
        for i in range(7,11):
            col[i]=float(str(col[i]).strip('%'))/100.0
        col.insert(0,TradingDay.strftime("%Y-%m-%d"))
        col.insert(2,"DCE")
        col.insert(4,1)
        col.insert(5,1)

        col.insert(11,1)
        col.insert(16,0)
        temp=GetDCEDelive(col[1])
        col.insert(17,int(temp))
        sqllist.append(tuple(col))
    infoapi.mysplider.dataToSqlserver(sqllist,sql,infoapi)

def GetDCEDelive(tmp):
    num = re.search(r"\d+\.?", str(tmp).strip()).group()
    code = str(tmp).strip().replace(num, "")
    cf = ConfigParser.ConfigParser()
    dirpath = os.path.abspath(os.path.join(os.getcwd(), "../."))
    f = codecs.open(dirpath + "\\" + 'deliveConfig.ini', mode='r+', encoding="utf-8-sig")
    cf.readfp(f)
    return  cf.get("DCEdeliveryCosts",code)

def SHFESettlementDetail(TradingDay, infoapi):
    sql1 = """
               select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='SHFE' and IsFutures=1
           """

    list1 = IsExistData(sql1 % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        ###爬取期货信息
        GetSHFEutureSettlementDetail(TradingDay, infoapi)
    else:
        print TradingDay, "上期所期货结算数据已经存在"

    """
        爬取上期所期权信息
    """
    sqlOpt = """
                 select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='SHFE' and IsFutures=0
             """
    list1 = IsExistData(sqlOpt % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        GetSHFEOptionSettlementDetail(TradingDay, infoapi)
    else:
        print TradingDay, "期权数据已经存在"

def GetSHFEOptionSettlementDetail(TradingDay, infoapi):
    sql = """
                INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[SettlementMethod],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[DeliveMethod],[DeliveCharge],
                [MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],[MarginHedgingSale])
            VALUES('%s','%s','%s','%s',%s,%s,%s,'%s','%s','%s','%s',%s,'%s',%s,'%s','%s','%s','%s')
            """
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
    Ourl = "http://www.shfe.com.cn/data/instrument/option/Settlement%s.dat"
    url = Ourl % (TradingDay.strftime("%Y%m%d"))
    print url
    html = infoapi.mysplider.getUrlcontent(url, header=header)
    jsondata = eval(html)["OptionSettlement"]
    print jsondata
    sqllist = []
    for i in jsondata:
        col = []
        col.append(i["UPDATE_DATE"].split(" ")[0])
        col.append(i['INSTRUMENTID'])
        col.append("SHFE")
        col.append(str(float(i["SETTLEMENTPRICE"])))
        col.append(0)
        col.append(1)

        ratio = str(float(i["TRADEFEERATIO"]))
        if ratio == '0.00':
            col.append(0)
        else:
            col.append(1)
            ratio=str(float(i["TRADEFEEUNIT"]))
        col.append(ratio)
        col.insert(8,col[7])
        col.insert(9,col[7])
        col.insert(10,col[7])

        ratio = str(float(i["STRIKEFEERATIO"]))
        if ratio == '0.00':
            col.append(0)
        else:
            col.append(1)
            ratio = str(float(i["STRIKEFEEUNIT"]))
        col.append(ratio)

        col.insert(13,0)
        col.append(str(float(i["STRADEUNITMARGIN"])))
        col.insert(15,col[14])

        col.append(str(float(i["HTRADEUNITMARGIN"])))
        col.insert(17,col[16])
        sqllist.append(tuple(col))
    infoapi.mysplider.dataToSqlserver(sqllist, sql,infoapi)

def GetSHFEutureSettlementDetail(TradingDay, infoapi):
    sql = """
            INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[SettlementMethod],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[DeliveMethod],[DeliveCharge],
            [MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],[MarginHedgingSale])
        VALUES('%s','%s','%s','%s',%s,%s,%s,'%s','%s','%s','%s',%s,'%s',%s,'%s','%s','%s','%s')
        """
    Furl = 'http://www.shfe.com.cn/data/instrument/Settlement%s.dat'
    #Ourl = "http://www.shfe.com.cn/data/instrument/option/Settlement%s.dat"
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
    url = Furl % (TradingDay.strftime("%Y%m%d"))
    print url
    html = infoapi.mysplider.getUrlcontent(url, header=header)
    if html.strip() == "":
        return
    else:
        jsondata = eval(html)["Settlement"]
        sqllist = []
        for i in jsondata:
            col = []
            col.append(i["UPDATE_DATE"].split(" ")[0])
            InstrumentID=i['INSTRUMENTID']
            col.append(InstrumentID)
            col.append(str(float(i["SETTLEMENTPRICE"])))
            ratio = str(float(i["TRADEFEERATION"])*10000)
            if float(ratio)== 0.0:
                # col.append(str(0.0))
                col.insert(6,0)
                ratio=str(float(i["TRADEFEEUNIT"]))
            else:
                col.insert(6,1)
                ratio=ratio
            col.insert(7,ratio)
            col.insert(8,ratio)
            col.insert(9,ratio)
            col.insert(10,ratio)
            deliveratio=float(i["COMMODITYDELIVERYFEERATION"])
            if float(deliveratio)==0.0:
                col.insert(11, 0)
                deliveratio=str(float(i["COMMODITYDELIVERYFEEUNIT"]))
            else:
                col.insert(11,1)
            col.insert(12,deliveratio)

            col.insert(13,1)

            col.append(str(float(i["SPEC_LONGMARGINRATIO"])))
            col.append(str(float(i["SPEC_SHORTMARGINRATIO"])))
            col.append(str(float(i["LONGMARGINRATIO"])))
            col.append(str(float(i["SHORTMARGINRATIO"])))
            col.insert(2,"SHFE")
            col.insert(4,1)
            col.insert(5,0)
            sqllist.append(tuple(col))
        infoapi.mysplider.dataToSqlserver(sqllist, sql,infoapi)

# def GetSHFEMutli(InstrumentID):
#     sql="""
#         SELECT [VolumeMultiple] FROM [PreTrade].[dbo].[ContractInfo] where InstrumentID='%s'
#     """
#     info=InfoApi()
#     server,user,password,database=info.GetDbHistoryDataAccount()
#     mysql = Mysql(server,user,password,database)
#     templist=mysql.ExecQuery(sql%InstrumentID)
#     if not len(templist):
#         print "请先更新大商所期货合约信息数据库-ContractInfo!"
#         raise Exception
#     print int(templist[0][0])
#     return int(templist[0][0])

def GetCFFEXDayInstrument(TradingDay,infoapi):
    sql="""
        SELECT   [InstrumentID] FROM [PreTrade].[dbo].[SettlementInfo] where ExchangeID='CFFEX' and  TradingDay='%s'
    """
    datalist = []
    templist = IsExistData(sql % TradingDay,infoapi)
    print templist
    if len(templist) == 0:
        print "请先爬取该天结算数据", TradingDay
        raise Exception
    for i in templist:
        datalist.append(i[0])
    return datalist

def GetCFFEXFutureSettlementPrice(TradingDay,infoapi):
    sql="""
        SELECT [InstrumentID],SettlementPrice  FROM [PreTrade].[dbo].[SettlementInfo]  where TradingDay='%s' and ExchangeID='CFFEX'
    """
    print sql
    datalist={}
    templist=IsExistData(sql%TradingDay,infoapi)
    if len(templist)==0:
        print "请先爬取该天结算数据",TradingDay
    for i in templist:
        datalist[i[0]]=i[1]
    return datalist