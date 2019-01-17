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

    def Get_Msplider(self):
        self.mysplider=MySplider()
        return self.mysplider

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
        if self.HistoryDbconn is None:
            temp=self.setting.HistoryDbInfo()
            self.HistoryDbconn=Mysql(temp[0],temp[1],temp[2],temp[3])
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
        return self.basicapi.GetResultList(templist)

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
        templist=self.HistoryDbconn.ExecQuery(sql)
        return self.Get_BasicApi().GetResultList(templist)

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


    def GetAllProduct(self):
        """获取所有的期货品种名称"""
        sql="select [InstrumentName]   FROM [PreTrade].[dbo].[ContractCode] order by ExchangeID"
        mysql=self.GetDbHistoryConnect()
        templist=mysql.ExecQuery(sql)
        return self.Get_BasicApi().GetResultList(templist)

    def GetChineseToEnglish(self):
        temp={"交易所":"ExchangeID","交易代码":"InstrumentCode","最小变动价位":"PriceTick","合约乘数":"VolumeMultiple","合约月份":"InstrumentMonth","最低交易保证金":"MinMargin","最后交易日":""}

