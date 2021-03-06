# -*- coding: utf-8 -*-
# @Time    : 2018/9/18 15:36
# @Author  : ZouJunLin
"""
config 配置文件数据
获取账户信息，数据库信息等等
"""
import ConfigParser
import sys,os
from utils.sqlServer import *
from utils.BasicAPI import *
from  data.setting import *
from utils.Mysplider import *
from utils.ThreadSqlServer import *
from MyDate import *
import calendar


class InfoApi:
    def __init__(self):
        self.HistoryDbconn=None
        self.StaticDbconn=None
        self.setting=setting()
        self.mysql=self.GetDbHistoryConnect()
        self.mysql1=None
        self.mysplider = None
        self.basicapi = None
        """to save tradingDay's Instrument """
        self.tradingDayInstrument=dict()
        self.NameCode=dict()
        """Memory to Save all kinds Data"""
        self.tempdata=dict()
        self.cleanDatadict=list()
        self.mydate=MyDate()

        self.PositionTop20InstrumentID=dict()

    def Get_Msplider(self):
        self.mysplider=MySplider()
        return self.mysplider

    def Set_QryPosition(self,ExchangeID,url,code,InstrumentID,TradingDay):
        self.QryPositionExchangeID=ExchangeID
        self.QryPositionurl=url
        self.QryPositionCode=code
        self.QryPositionInstrumentID=InstrumentID
        self.QryPositionTradingDay=TradingDay

    def Set_StagePosition(self,ExchangeID, Surl, code,begin,end):
        self.StagePositionExchangeID=ExchangeID
        self.StagePositionurl=Surl
        self.StagePositionCode = code
        self.StagePositionBeginTime = begin
        self.StagePositionEndTime=end

    def Get_BasicApi(self):
        self.basicapi=BasicAPI()
        return self.basicapi

    def GetDbHistoryDataAccount(self):
        """
        依次返回历史行情数据库地址，账号，密码,数据库名字
        :return:
        """
        return self.setting.HistoryDbInfo()

    def GetDbHistoryConnect(self):
        """
        获取历史行情数据库链接
        :return:
        """

        temp=self.setting.HistoryDbInfo()
        self.HistoryDbconn=SqlServer(temp[0],temp[1],temp[2],temp[3])
        self.mysql=self.HistoryDbconn
        return self.mysql

    def DisConnectDbHistory(self):
        self.HistoryDbconn = None
        self.mysql.Disconnect()
        self.mysql=None

    def GetStaticDataAccount(self):
        """
        依次返回盘后统计数据库地址，账号，密码,数据库名字
        :return:
        """
        return self.setting.StaticDbInfo()


    def GetStaticDataconnect(self):
        """
        返回统计数据库链接
        :return:
        """
        if self.StaticDbconn is None:
            temp=self.GetStaticDataAccount()
            self.StaticDbconn = Mysql(temp[0], temp[1], temp[2], temp[3])
            self.mysql1=self.StaticDbconn
        return self.mysql1

    def DisConnectStaticData(self):
        self.StaticDbconn=None
        self.mysql1.Disconnect()
        self.mysql1=None

    def GetHolidayList(self):
        """
        返回一个节假日的list,目前只有2017,2018和2019年
        :return:
        """
        return self.setting.GetHoliday()

    def GetExchangeWebsite(self,ExchangeID):
        """
        返回相应交易所的官网地址
        :param ExchangeID:
        :return:
        """
        try:
            return str(self.setting.GetExchangeWeb(ExchangeID)).strip()
        except:
            print "please enter right ExchangeID"

    def GetDetailByTradeCode(self,TradeCode):
        """获取标准合约，仅供合约信息表使用"""
        sql="select [VolumeMultiple],[ProductName],[PriceTick] from [PreTrade].[dbo].[StandContract] where [TradeCode]='%s'"%TradeCode
        self.mysql = self.GetDbHistoryConnect()
        templist = self.mysql.ExecQuery(sql)
        return self.basicapi.GetResultList(templist)[0]

    def GetAnotherInstrumentByInstrument(self,instrumentID):
        """获取另一个正在交易的合约，去你补部分数据缺失的问题"""
        a= str(instrumentID).strip()[:2]
        sql="select top 2 [InstrumentID] from [PreTrade].[dbo].[SettlementInfo] where InstrumentID like'"+a+"%'  and [IsFuture]='1' order by TradingDay desc"
        if self.mysql is None:
            self.GetDbHistoryConnect()
        templist = self.mysql.ExecQueryGetList(sql)
        return templist[1] if templist[0]==instrumentID else templist[0]

    def GetRelaviteInstrumentBycode(self,code):
        """获取相关合约通过product code"""
        date=str(datetime.datetime.now().year)
        if self.mysql is None:
            self.GetDbHistoryConnect()
        ExchangeId=self.GetExchangeByCode(code)
        if ExchangeId=='CZCE':
            return code+date[3]
        else:
            return code+date[2]


    def GetExchangeByCode(self,code):
        sql="SELECT  [ExchangeID] FROM [PreTrade].[dbo].[ContractCode] where InstrumentCode='%s'"%code
        if self.mysql is None:
            self.GetDbHistoryConnect()
        ExchangeID=self.mysql.ExecQueryGetList(sql)
        return ExchangeID[0]

    def GetAllTradeInstrumentByTradingDay(self,tradingDay):
        """获取某一个交易日的所有正在交易的合约"""
        if tradingDay in self.tradingDayInstrument.keys():
            return self.tradingDayInstrument[tradingDay]
        else:
            self.tradingDayInstrument=dict()
            tradingDay=datetime.datetime.strptime(tradingDay,"%Y%m%d")
            if self.mysql is None:
                self.GetDbHistoryConnect()
            tempfuture={}
            templist = self.mysql.ExecQuery("select [TradeCode],[ExchangeID],[ProductName] from  [PreTrade].[dbo].[StandContract] order by ExchangeID")
            templist = BasicAPI().GetResultList(templist)
            for i in templist:
                InstrumentId = i[0]
                ExchangeId = i[1]
                temp=self.Get_BasicApi().GetInstrumentMonth(self, InstrumentId, ExchangeId, tradingDay)
                for i in temp:
                    tempfuture[i]=ExchangeId
            self.tradingDayInstrument[tradingDay.strftime("%Y%m%d")]=tempfuture
            return tempfuture

    def GetCodeByName(self,name):
        """Get ProductCode by ProductNme"""
        sql="select [InstrumentCode],[InstrumentName],ExchangeID from [ContractCode]"
        if len(self.NameCode)==0:
            self.GetDbHistoryConnect()
            templist=self.mysql.ExecQuery(sql)
            for i in templist:
                self.NameCode[i[1].encode("utf-8")]=[i[0].encode("utf-8"),i[2].encode("utf-8")]
        return self.NameCode[name]

    def GetDetailByInstrumentID(self,instrumentID,ExchangeID):
        """通过合约代码获取code,以及年月"""
        code = str(re.match(r"\D+", instrumentID).group())
        num = str(instrumentID).strip().replace(code, "")
        if ExchangeID=='CZCE':
            nyear=datetime.datetime.now().year
            if str(nyear)[-1:]=='9':
                if num[:1]=='0':
                    num=str(nyear+1)[2:3]+str(num)
                else:
                    num=str(nyear)[2:3]+str(num)
        return code,num

    def GetInstrumentIDByCodeAndDate(self,ExchangeID,code,date):
        """
        通过品种代码和年月日获取标准的交易合约，检验交易代码的大小写以及郑商所合约后面只保留三位数字(期货)
        :param code: ProductCode
        :param date:1904/1908
        :return:
        """
        if ExchangeID in ['CZCE','CFFEX']:
            code=str(code).strip().upper()
        elif ExchangeID in ['SHFE','DCE','INE']:
            ode=str(code).strip().lower()
        if ExchangeID == 'CZCE':
            date = ''.join(list(date)[1:4])
        else:
            date=''.join(list(date)[:4])
        return code+date




    def GetMainInstrumentIdByProductCode(self,code,tradingday):
        """Get main InstrumentID by ProductCode """
        relInstrument=self.GetRelaviteInstrumentBycode(code)
        relInstrument=" '"+relInstrument+"%'"
        Mainsql = "SELECT top 1 [InstrumentID] FROM [PreTrade].[dbo].[SettlementInfo] where InstrumentID like"+relInstrument+"and TradingDay='%s' order by Position desc"%tradingday
        if self.mysql is None:
            self.GetDbHistoryConnect()
        instrument=self.mysql.ExecQueryGetList(Mainsql)
        if len(instrument):
            return instrument[0]
        else:
            print "code or  traingday error",code,tradingday
            raise Exception

    def GetAllExchange(self):
        """
        获取所有的交易所代码
        :return:
        """
        return self.setting.ExchangeList


    def GetExchangeHeader(self,ExchangeID):
        """
        返回header头部信息
        :return:
        """
        temp={}
        temp['CZCE'] = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cookie': 'BIGipServerwww_cbd=842836160.23067.0000; JSESSIONID=kYM9bphG26lQ6p25GXxyVRbkFw815nqCmJmTYxzGxxxvJnVTblhb!-1757810877; TS014ada8c=0169c5aa326a6aa66d379d396a2033d86a84f272d2b5c2f5d7b7d668355885aa62597926b41ead5b31a9f62f3252ecfb1e2c33422b',
            'Host': 'www.czce.com.cn',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1'
        }
        temp['DCE'] = {
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
        temp['SHFE']= {
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
        temp['CFFEX']= {
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Host': 'www.cffex.com.cn',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1'
        }
        return temp[ExchangeID]

    def GetExchangeProduct(self,ExchangeID):
        sql="select [InstrumentCode] FROM [PreTrade].[dbo].[ContractCode] where ExchangeID='%s'"%ExchangeID
        if self.HistoryDbconn is None:
            self.GetDbHistoryConnect()
        templist=self.HistoryDbconn.ExecQueryGetList(sql)
        return templist

    def IsInstrumentMonth(self,Tradecode,month):
        """判断month是否为该品种的合约月份，格式:直接传入月份01/02/.../10/11/12"""
        pass
        sql="""select [Delivemonth] from [PreTrade].[dbo].[StandContract] where [TradeCode]='%s'"""%Tradecode
        temp=self.mysql.ExecQueryGetList(sql)
        if temp.find("|")!=-1:
            temp=temp.split("|")
            temp = map(lambda x: x.zfill(2), temp)
            if month  in temp:
                return True
        elif temp.find("&"):
            pass

    def GetFutureInstrumentLits(self,a):
        """获取tradingday当日的数据库里面的SettlementInfo的交易的合约"""
        tempdict = {}
        a=a[:4].zfill(4) + "-" + a[5:6].zfill(2) + "-" + a[-2:].zfill(2)
        sql="""select [InstrumentID],[ExchangeID] from [PreTrade].[dbo].[SettlementInfo] where TradingDay='%s'  and [IsFuture]='1' order by ExchangeID"""%a
        if self.mysql is None:
            self.GetDbHistoryConnect()
        tlist=self.mysql.ExecQuery(sql)
        for i in tlist:
            tempdict[i[0].encode("utf-8")]=i[1].encode("utf-8")
        return tempdict


    def GetAllProduct(self):
        """获取所有的期货品种名称"""
        sql="select [InstrumentName]   FROM [PreTrade].[dbo].[ContractCode] order by ExchangeID"
        mysql=self.GetDbHistoryConnect()
        templist=mysql.ExecQuery(sql)
        return self.Get_BasicApi().GetResultList(templist)

    def GetChineseToEnglish(self):
        temp={"交易所":"ExchangeID","交易代码":"InstrumentCode","最小变动价位":"PriceTick","合约乘数":"VolumeMultiple","合约月份":"InstrumentMonth","最低交易保证金":"MinMargin","最后交易日":""}


    def GetPositionTop20InstrumentID(self):
        """获取交易所持仓top20 合约以及对应的交易所"""
        sql="SELECT distinct [InstrumentID],[ExchangeID] FROM [PreTrade].[dbo].[Position_Top20]"
        if self.mysql is None:
            self.GetDbHistoryConnect()
        tlist = self.mysql.ExecQuery(sql)
        for i in tlist:
            self.PositionTop20InstrumentID[i[0].encode("utf-8")] = i[1].encode("utf-8")

    def Get2Listfromsql(self,sql):
        """存数据库中获取数据到二维list"""
        templist=[]
        if self.mysql is None:
            self.GetDbHistoryConnect()
        tlist = self.mysql.ExecQuery(sql)

        for i in tlist:
            col=[]
            for j in i:
                col.append(j)
            templist.append(col)
        return templist