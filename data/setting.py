# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 9:50
# @Author  : ZouJunLin
"""配置文件py化"""
import ConfigParser

class setting:
     def __init__(self):
         self.server = 'localhost:1433'
         self.user = 'sa'
         self.password = 'Laystbz20!*'
         self.ExchangeList=['CZCE','DCE','SHFE','CFFEX']
         self.Website="http://www.%s.com.cn"
         self.Holiday=dict()
         self.lastCode = {"eg": 'eg1906', "sp": 'sp1906', "wr": 'wr1903'}
         self.ProductName={'两年期国债':'2年期国债','十年期国债':'10年期国债','五年期国债':'5年期国债','沪深300股指':'沪深300','上证50股指':'上证50','中证500股指':'中证500','油菜籽':'菜籽','晚籼稻':'晚稻','菜籽油':'菜油','菜籽粕':'菜粕'}
         self.level5=['a','b','bb','c','cs','fb','i','j','jd','jm','l','m','p','pp','v','y']


     def HistoryDbInfo(self):
        """盘前分析数据库"""
        self.database='PreTrade'
        return self.server,self.user,self.password,self.database

     def StaticDbInfo(self):
         """盘后统计数据库"""
         self.database="StatisticData"
         return self.server, self.user, self.password, self.database

     def GetExchangeWeb(self,ExchangeID):
         return self.Website%str(ExchangeID).lower()

     def GetHoliday(self):
         self.Holiday['2017']="20170101,20170102,20170127,20170128,20170129,20170130,20170131,20170201,20170202,20170402,20170403,20170404,20170429,20170430,20170501,20190528,20170529,20170530,20171001,20171002,20171003,20171004,20171005,20171006,20171007,20171008,20171230,20171231"
         self.Holiday['2018']="20180101,20180215,20180216,20180217,20180218,20180219,20180220,20180221,20180405,20180406,20180407,20180429,20180430,20180501,20180616,20180617,20180618,20180922,20190923,20180924,20180930,20181001,20181002,20181003,20181004,20181005,20181006,20181007,20181230,20181231"
         self.Holiday['2019']="20190101,20190204,20190205,20190206,20190207,20190208,20190209,20190210,20190405,20190406,20190407,20190501,20190607,20190608,20190609,20190913,20190914,20190915,20191001,20191002,20191003,20191004,20191005,20191006,20191007"
         self.Holiday['2020']="20200101,20200124,20200127,20200128,20200129,20200130,20200404,20200501,20200625,20201001,20201002,20201005,20201006,20201007"
         return self.Holiday['2017'].split(",")+self.Holiday['2018'].split(",")+self.Holiday['2019'].split(",")+self.Holiday['2020'].split(",")



