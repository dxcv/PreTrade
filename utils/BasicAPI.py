# -*- coding: utf-8 -*-
# @Time    : 2019/1/7 11:04
# @Author  : ZouJunLin
"""
一些常用的方法集合
"""
import re
import datetime
from utils.TradingDay.EndDate import *
from data.setting import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class BasicAPI:
    def __init__(self):
        self.data=None
        self.enddate=None

    def Get_EndDate(self, info):
        self.enddate = EndDate(info)
        return self.enddate

    def GetResultList(self, result):
        """
        将数据库查询结果转换成list
        :param result:
        :return:
        """
        temp = []
        col=[]
        for i in result:
            for j in range(len(i)):
                col.append(str(i[j].encode("utf-8")).strip())
            temp.append(col)
            col=[]
        return temp

    def StrinList(self,str,tempList):
        """判断某个str是否在List中"""
        if str in tempList:
            return True
        else:
            return False

    def GetExchangeProductCode(self,code,ExchangeID):
        """根据交易所返回代码大小写"""
        if ExchangeID=='CZCE' or ExchangeID=='CFFEX':
            return code.upper()
        elif ExchangeID=='DCE' or ExchangeID=='SHFE':
            return code.lower()

    def StrIntoNumber(self,str):
        templist={'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
                            '十': 10,}
        return templist[str]

    def MyPrint(self,templist):
        """打印一个List"""
        j=0
        for i in templist:
            length=len(i)
            while j<length:
                print i[j],"|||",j,"\t\t",
                j+=1
            j=0
            print "\n"

    def GetInstrumentYearMonth(self,InstrumentID,ExchangeID):
        """根据合约以及交易所命名规则获取合约的年，月"""

        code = str(re.match(r"\D+", InstrumentID).group())
        InstrumentID = list(InstrumentID.replace(code, ""))
        if ExchangeID=="CZCE":
            year=InstrumentID[0]
            if year!='0':
                month=InstrumentID[1:3]
                year = "".join(year)
                month = "".join(month)
                now = list(str(datetime.datetime.now().year))
                year = "".join(now[:3]) + year
            else:
                month = InstrumentID[1:3]
                year = "".join(year)
                month = "".join(month)
                now = list(str(int(datetime.datetime.now().year)+1))
                year = "".join(now[:3]) + year
        else:
            year = InstrumentID[0:2]
            month=InstrumentID[2:4]
            year="".join(year)
            month="".join(month)
            now=list(str(datetime.datetime.now().year))
            year="".join(now[:2])+year
        return  year,month

    def BinarySeach(self,left,right,num,tempList):
        """二分查找算法进化形式"""
        while left<=right:
            mid = int((right + left) / 2)
            if tempList[mid]==num:
                return tempList.index(num)
            elif tempList[mid]>num:
                return self.BinarySeach(left,mid-1,num,tempList)
            elif tempList[mid]<num:
                return self.BinarySeach(mid+1,right,num,tempList)
        return left

    def GetContinueNumsmonth(self,info,TradeCode,num,now):
        """当前月份开始的num个连续该品种的合约"""
        col=[]
        temp=map(lambda x: str(x).zfill(2), range(1, 13))
        for i in range(num+1):
            if i + temp.index(self.month) < len(temp):
                col.append(TradeCode + self.year + temp[(i + temp.index(self.month)) % len(temp)])
            else:
                col.append(TradeCode + self.nextyear + temp[(i + temp.index(self.month)) % len(temp)])

        if self.InstrumentIdIsTrading(info,col[0],now):
            return col[:-1]
        else:
            return col[1:]

    def InstrumentIdIsTrading(self,info,InstrumentID,now):
        """判断InstrumentId是否正在交易"""
        now=now.strftime("%Y%m%d")
        a,b=self.Get_EndDate(info).GetEndDate(InstrumentID)
        return False if a<now else True

    def GetInstrumentMonth(self,info,TradeCode,ExchangeID,now):
        """输入交易代码，即可查询该品种最近正在交易的合约"""
        self.lastCode =setting().lastCode
        sql="""select Delivemonth from [PreTrade].[dbo].[StandContract] where [TradeCode]='%s'"""%TradeCode
        temp=info.mysql.ExecQueryGetList(sql)[0]
        col = []
        self.year = list(str(now.year))
        self.nextyear =list(str(now.year + 1))
        if ExchangeID=='CZCE':
            self.year="".join(self.year[-1:]).strip()
            self.nextyear = "".join(self.nextyear[-1:]).strip()
        else:
            self.year="".join(self.year[2:]).zfill(2)
            self.nextyear = "".join(self.nextyear[2:]).zfill(2)
        self.month=str(now.month).zfill(2)
        if  temp.find("|")!=-1 and temp.find("&")==-1:
            temp=temp.split("|")
            tempdata = map(lambda x: x.zfill(2), temp)
            if self.month in tempdata :
                delta=tempdata.index(self.month)
                if not self.InstrumentIdIsTrading(info,str(TradeCode + self.year +self.month).strip(),now):
                    delta=delta+1
            else:
                delta=0
            for i in range(len(tempdata)):
                if i+delta<len(tempdata):
                    col.append(TradeCode+self.year+tempdata[(i+delta)%len(tempdata)])
                else:
                    col.append(TradeCode+self.nextyear+tempdata[(i + delta) % len(tempdata)])
        elif temp.find("&")!=-1:
            temp = temp.split("&")
            temp1=temp[1]
            continuemonth=self.GetContinueNumsmonth(info,TradeCode,int(temp[0]),now)
            if temp1.find("/")!=-1:
                temp1=temp1.split("/")
                tempList=self.GetLastNumMonth(int(temp1[0]),ExchangeID,now)
                tempList=filter(lambda x:int("".join(list(x)[-2:]))%int(temp1[1])==0,tempList[tempList.index("".join(list(continuemonth[-1])[-4:]))+1:])
                tempList=map(lambda x:TradeCode+x,tempList)
                tempList=continuemonth+tempList
                return tempList
            elif temp1.find("*")!=-1:
                temp1 = temp1.split("*")
                tempList = self.GetLastNumMonth(int(temp1[0])*int(temp1[1])+int(temp[0]), ExchangeID,now)
                tempList = filter(lambda x: int("".join(list(x)[-2:])) % int(temp1[1]) == 0,
                                  tempList[tempList.index("".join(list(continuemonth[-1])[-4:])) + 1:])
                tempList = map(lambda x: TradeCode + x, tempList)
                col= continuemonth + tempList[:int(temp1[0])]
                return col

        elif temp.find("*")!=-1:
            temp = temp.split("*")
            monthList=["03","06","09","12"]
            index=self.BinarySeach(0,len(monthList)-1,self.month,monthList)
            for i in range(int(temp[0])):
                if i + index < len(monthList):
                    col.append(TradeCode + self.year + monthList[i+index])
                else:
                    col.append(TradeCode + self.nextyear +monthList[i+index])
        if self.lastCode.has_key(TradeCode):
            col=col[col.index(self.lastCode[TradeCode]):]
        return col


    def GetLastNumMonth(self,num,ExchangeID,now):
        """获取最近num个月份"""
        col=[]
        for i in range(num):
            year = now.year
            month = now.month
            days_num = calendar.monthrange(int(year), int(month))[1]
            now=now+datetime.timedelta(days=days_num)
            if ExchangeID=='CZCE':
                col.append(now.strftime("%Y%m%d")[3:6].zfill(4))
            else:
                col.append(now.strftime("%Y%m%d")[2:6].zfill(4))
        return col

    def TwoList2Dict(self,tlist):
        """双层list转换成key"""
        tkey=dict()
        col=tlist[0][1:]
        for i in tlist[1:]:
            if not i[0] in tkey.keys():
                tkey[i[0]]=dict()
            for j in  range(len(i[1:])):
                if not col[j] in tkey[i[0]].keys():
                    tkey[i[0]][col[j]] = dict()
                tkey[i[0]][col[j]]=i[j+1]
        for i in tkey.keys():
            sorted(tkey[i].keys(), lambda x, y: cmp(x[1], y[1]))
        return tkey
