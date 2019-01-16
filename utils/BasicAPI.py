# -*- coding: utf-8 -*-
# @Time    : 2019/1/7 11:04
# @Author  : ZouJunLin
"""
一些常用的方法集合
"""
import re
import datetime
from utils.InfoApi import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')


class BasicAPI:
    def __init__(self):
        self.data=None

    def GetResultList(self, result):
        """
        将数据库查询结果转换成list
        :param result:
        :return:
        """
        temp = []
        for i in result:
            for j in range(len(i)):
                temp.append(str(i[j].encode("utf-8")).strip())
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
                print i[j],"\t",
                j+=1
            j=0
            print "\n"

    def GetInstrumentYearMonth(self,InstrumentID,ExchangeID):
        """根据合约以及交易所命名规则获取合约的年，月"""
        code = str(re.match(r"\D+", InstrumentID).group())
        InstrumentID = list(InstrumentID.replace(code, ""))
        if ExchangeID=="CZCE":
            year=InstrumentID[0]
            month=InstrumentID[1:3]
            year = "".join(year)
            month = "".join(month)
            now = list(str(datetime.datetime.now().year))
            year = "".join(now[:3]) + year
        else:
            year = InstrumentID[0:2]
            month=InstrumentID[2:4]
            year="".join(year)
            month="".join(month)
            now=list(str(datetime.datetime.now().year))
            year="".join(now[:2])+year
        return  year,month


    def GetInstrumentMonth(self,info,TradeCode,ExchangeID):
        """输入交易代码，即可查询该品种最近正在交易的合约"""
        sql="""select Delivemonth from [PreTrade].[dbo].[StandContract] where [TradeCode]='%s'"""%TradeCode
        temp=info.mysql.ExecQueryGetList(sql)[0]
        print temp
        year = list(str(datetime.datetime.now().year))
        nextyear =list(str(datetime.datetime.now().year + 1))
        if ExchangeID=='CZCE':
            year=year[-1:]
            nextyear = nextyear[-1:]
        else:
            year="".join(year[2:]).zfill(2)
            nextyear = "".join(nextyear[2:]).zfill(2)
        month=str(datetime.datetime.now().month).zfill(2)
        if  temp.find("|")!=-1:
            temp=temp.split("|")
            temp = map(lambda x: x.zfill(2), temp)
        col=[]
        for i in range(len(temp)):
            if i+temp.index(month)<len(temp):
                col.append(TradeCode+year+temp[(i+temp.index(month))%len(temp)])
            else:
                col.append(TradeCode+nextyear+temp[(i + temp.index(month)) % len(temp)])
        return col

