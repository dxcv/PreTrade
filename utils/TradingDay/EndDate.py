# -*- coding: utf-8 -*-
# @Time    : 2019/1/10 16:50
# @Author  : ZouJunLin
"""根据期货合约信息表计算出最后交易日，已经最后交割日"""
from utils.InfoApi import *
import re,sys
reload(sys)
from utils.TradingDay.NextTradingDay  import TradingDay
import calendar

class EndDate():
    def __init__(self,info):
        self.info=info
        self.basicapi=self.info.Get_BasicApi()
        self.nextTraday=TradingDay(self.info)
        self.holiday=self.info.GetHolidayList()
        self.mysql=self.info.GetDbHistoryConnect()
        self.endDatesql="select [EndDate],[EndDelivDate],[ExchangeID] from [PreTrade].[dbo].[StandContract] where [TradeCode]='%s'"

    def GetEndDeliveDateAPI(self,DeliveList,EndDate):
        if DeliveList[0] == '0':
            if self.nextTraday.IsTradingDayFuture(self.tempday):
                EndDeliveDate = self.nextTraday.GetNumDaysInFuture(self.tempday, int(DeliveList[1]) - 1, 1)
            else:
                EndDeliveDate = self.nextTraday.GetNumDaysInFuture(self.tempday, int(DeliveList[1]), 1)
        elif DeliveList[0]=='1':
            EndDeliveDate = self.nextTraday.GetNumDaysInFuture(EndDate, int(DeliveList[1]),1)
        elif DeliveList[0]=='2':
            EndDeliveDate=self.YearMonth+str(DeliveList[1])
            if not EndDeliveDate:
                EndDeliveDate=self.nextTraday.NextTradingDay(EndDeliveDate,1)
        elif DeliveList[0]=='3':
            EndDeliveDate=self.monthLastDay
            if not self.nextTraday.IsTradingDayFuture(EndDeliveDate):
                EndDeliveDate=self.nextTraday.NextTradingDayFuture(EndDeliveDate,0)
        return EndDeliveDate

    def GetEndDate(self,InstrumentID):
        self.code = str(re.match(r"\D+", InstrumentID).group())
        temp=self.mysql.ExecQuery(self.endDatesql%self.code)
        EndDatetemp=temp[0][0].encode("utf-8").split("|")
        EndDeliveDatetemp=temp[0][1].encode("utf-8").split("|")
        self.ExchangeID=temp[0][2].encode("utf-8")
        year,month=self.basicapi.GetInstrumentYearMonth(InstrumentID,self.ExchangeID)
        self.YearMonth=year+month
        self.tempday=year+month+"01"
        days_num = calendar.monthrange(int(year), int(month))[1]
        self.monthLastDay=year+month+str(days_num)
        if EndDatetemp[0]=='0':
            if self.nextTraday.IsTradingDayFuture(self.tempday):
                EndDate=self.nextTraday.GetNumDaysInFuture(self.tempday,int(EndDatetemp[1])-1,1)
            else:
                EndDate = self.nextTraday.GetNumDaysInFuture(self.tempday, int(EndDatetemp[1]), 1)
        elif EndDatetemp[0]=='1':
            EndDate=year+month+"15"
            if not  self.nextTraday.IsTradingDayFuture(EndDate):
                EndDate=self.nextTraday.NextTradingDayFuture(EndDate,1)
        elif EndDatetemp[0]=='2':
            EndDate=self.nextTraday.NextTradingDayFuture(self.tempday,0)
        elif EndDatetemp[0]=='3':
            if self.nextTraday.IsTradingDayFuture(self.monthLastDay):
                EndDate = self.nextTraday.GetNumDaysInFuture(self.monthLastDay, int(EndDatetemp[1]) - 1, 0)
            else:
                EndDate = self.nextTraday.GetNumDaysInFuture(self.monthLastDay, int(EndDatetemp[1]), 0)
        elif EndDatetemp[0]=='4':
            c = calendar.Calendar(firstweekday=calendar.SUNDAY)
            monthcal = c.monthdatescalendar(int(year),int(month))
            EndDate = [day for week in monthcal for day in week if \
                            day.weekday() == calendar.FRIDAY and \
                            day.month == int(month)][int(EndDatetemp[1])-1].strftime("%Y%m%d")
        if not self.nextTraday.IsTradingDayFuture(EndDate):
            EndDate=self.nextTraday.NextTradingDayFuture(EndDate,1)
        EndDeliveDate = self.GetEndDeliveDateAPI(EndDeliveDatetemp[0:2], EndDate)
        if len(EndDeliveDatetemp)==4:
            EndDeliveDate1=self.GetEndDeliveDateAPI(EndDeliveDatetemp[2:4],EndDate)
            EndDeliveDate=EndDeliveDate+"/"+EndDeliveDate1

        return EndDate,EndDeliveDate


    def GetStartDay(self,InstrumentID):
        """上一个合约的最后交易日第二天上新合约"""
        EndDate=self.GetEndDate(InstrumentID)[0]
        return self.nextTraday.NextTradingDayFuture(EndDate,1)

# if __name__=='__main__':
#     EndDate,EndDeliveDate=EndDate().GetEndDate("m1901")
#     print EndDate,EndDeliveDate