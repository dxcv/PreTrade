# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 9:50
# @Author  : ZouJunLin
"""配置文件py化"""
import ConfigParser

class setting:
     def __init__(self):
         self.server = 'localhost:53350'
         self.user = 'sa'
         self.password = 'aa85258584'
         self.ExchangeList=['CZCE','DCE','SHFE','CFFEX']
         self.Website="http://www.%s.com.cn"
         self.Holiday=dict()


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
         self.Holiday['2017']="20170102,20170127,20170130,20170131,20170201,20170202,20170403,20170404,20170501,20170529,20170530,20171002,20171003,20171004,20171005,20171006"
         self.Holiday['2018']="20180101,20180215,20180216,20180217,20180218,20180219,20180220,20180221,20180405,20180406,20180407,20180429,20180430,20180501,20180618,20180924,20180930,20181001,20181002,20181003,20181004,20181005,20181006,20181231"
         self.Holiday['2019']="20190101,20190204,20190205,20190206,20190207,20190208,20190405,20190501,20190607,20190913,20191001,20191002,20191003,20191004,20191007"
         return self.Holiday['2017'].split(",")+self.Holiday['2018'].split(",")+self.Holiday['2019'].split(",")


