# -*- coding: utf-8 -*-
# @Time    : 2018/8/24 15:59
# @Author  : ZouJunLin
"""
输入一个日期获取到下一个交易日
最下方使用事例
"""

from utils.InfoApi import *

sql="""
    select * from [PreTrade].[dbo].[HistoryNoTdCalendar] where NoTradingDay=%s
"""

class TradingDay:
    def __init__(self,info):
        self.info=info
        self.mysql =self.info.GetDbHistoryConnect()
        self.Holiday=InfoApi().GetHolidayList()

    def NextTradingDay(self,day,mark):
        """
        获取2002年之后的此交易日的下一个交易日
        day String
        :return:
        flag  1      下一个交易日          0  上一个交易日
        """
        self.day=str(day).strip()
        self.day,self.falg=NextDay(self.day) if mark else PreviousDay(self.day)
        data=self.mysql.ExecQuery(sql%self.day)
        while len(data)==1:
            self.day,self.falg=NextDay(self.day) if mark else PreviousDay(self.day)
            data = self.mysql.ExecQuery(sql % self.day)

        ## 除去节假日
        while self.day in self.Holiday:
            self.day,self.falg=NextDay(self.day) if mark else PreviousDay(self.day)

        ## 是否为周六周天
        while datetime.datetime.strptime(self.day,"%Y%m%d").weekday()==6 or datetime.datetime.strptime(self.day,"%Y%m%d").weekday()==5:
            self.day,self.falg=NextDay(self.day) if mark else PreviousDay(self.day)
        return self.day

    def IsTradingDay(self,day):
        """
        判断是否为交易日
        :param day:String
        :return: True or False
        """
        self.day=day
        data = self.mysql.ExecQuery(sql % self.day)
        if len(data)==1:
            return False
        ## 查看配置文件   除去一些特殊的日子
        if self.day in self.Holiday:
            return False
        ## 是否为周六周天
        if datetime.datetime.strptime(self.day, "%Y%m%d").weekday() == 6 or datetime.datetime.strptime(self.day,"%Y%m%d").weekday() == 5:
            return False
        return True

    def NextTradingDayFuture(self,day,mark):
        """
               获取2002年之后的此交易日的下一个交易日
               day String
               :return:
               flag  1      下一个交易日          0  上一个交易日
        """
        HolidayFlag=False
        weekFlag=False
        day = str(day).strip()
        while  not HolidayFlag and  not weekFlag :
            day, falg = NextDay(day) if mark else PreviousDay(day)
            while day in self.Holiday:
                day, falg = NextDay(day) if mark else PreviousDay(day)
                weekFlag=False
                HolidayFlag=False
            else:
                weekFlag=True
            while datetime.datetime.strptime(day, "%Y%m%d").weekday() == 6 or datetime.datetime.strptime(day,"%Y%m%d").weekday() == 5:
                day, falg = NextDay(day) if mark else PreviousDay(day)
                HolidayFlag=False
                weekFlag=False
            else:
                HolidayFlag=True
        return day



    def IsTradingDayFuture(self,day):
        """
        判断未来某一天是否为交易日，通用版本，需要节假日文件,否则计算不准确
        :param day:
        :return:
        """
        if day in self.Holiday:
            return False
        if datetime.datetime.strptime(day, "%Y%m%d").weekday() == 6 or datetime.datetime.strptime(day,"%Y%m%d").weekday() == 5:
            return False
        return True


    def GetLastNumTradingday(self,tradingday,num):
        """
            计算某个交易日的后几个交易日时间
            :param tradingday:
            :param daynum:
            :return:
            """
        daylist=[]
        daylist.append(tradingday)
        while len(daylist)<=num:
            tradingday=self.NextTradingDay(tradingday,True)
            daylist.append(tradingday)
        return daylist

    def GetNumTradingday(self,tradingday,num):
        """
        计算某个交易日的前几个交易日时间
        :param tradingday:
        :param num:
        :return:
        """
        daylist = []
        daylist.append(tradingday)
        while len(daylist) <= num:
            tradingday = self.NextTradingDay(tradingday, False)
            daylist.append(tradingday)
        return daylist

    def GetNumDaysIn(self,tradingday,num,flag):
        """
        获取5天前后的日期，flag True 5天后 Flase 5天前
        :param tradingday:
        :param num:
        :return:
        """
        while num:
            tradingday = self.NextTradingDay(tradingday,flag)
            num-=1
        return tradingday

    def GetNumDaysInFuture(self,tradingday,num,flag):
        """
        获取5天前后的日期，flag True 5天后 Flase 5天前
        :param tradingday:
        :param num:
        :return:
        """
        while num:
            tradingday = self.NextTradingDayFuture(tradingday,flag)
            num-=1
        return tradingday



def NextDay(day):
    nextday=datetime.datetime.strptime(day,"%Y%m%d")+datetime.timedelta(days=1)
    return nextday.strftime("%Y%m%d"),True if datetime.datetime.strptime(day,"%Y%m%d").year<nextday.year else False

def PreviousDay(day):
    preday=datetime.datetime.strptime(day,"%Y%m%d")-datetime.timedelta(days=1)
    return preday.strftime("%Y%m%d"), True if datetime.datetime.strptime(day, "%Y%m%d").year > preday.year else False


"""
    使用事例
"""
# if __name__=='__main__':
#     t=TradingDay(InfoApi())
#     day=t.NextTradingDayFuture("20190204",True)
#     # day=t.NextTradingDay("20181228",True)
#     # print day
#     # day=t.NextTradingDay("20181231",False)
#     # print t.IsTradingDay("20181231")
#     # print day
#     print day