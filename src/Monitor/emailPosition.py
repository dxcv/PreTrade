# -*- coding: utf-8 -*-
# @Time    : 2019/5/20 14:24
# @Author  : ZouJunLin
"""镍持仓量和首次或者第五个交易日持仓超过15万手自动发送邮件提示"""
import datetime,os,sys
dir_path=os.path.dirname(os.path.abspath(".."))
sys.path.append(dir_path)
from utils.TradingDay.NextTradingDay import *
from utils.InfoApi import  *
import pandas as pd
from utils.Email.MyEmail import *

columns=[u'交易日',u'合约代码',u'成交量',u'上一交易日成交量',u'持仓量',u'上一交易日持仓量']
Directory=u"D:/GitData/pignemo/PreTrade/镍持仓量监控/"
MAX_POSITION=150000

def dealResult(result):
    templist=list()
    for i in result:
        if int(i[4])>MAX_POSITION:
            templist.append(str(i[1].encode('utf-8')).strip())
    return templist

def GetPosition(info):
    """1、查询数据库判断当天是否有合约持仓量超过15万手,返回超过最大持仓的list"""
    sql = """
                select a.TradingDay as 交易日,a.InstrumentID as 合约,a.Volume as 成交量,b.Volume as 上一交易日成交量,a.Position as 持仓量,b.Position as 上一交易日持仓量
               from ( select TradingDay,InstrumentID,Volume,position from SettlementInfo  where TradingDay='{TradingDay}' and (InstrumentID like '{productcode}1%' or InstrumentID like '{productcode}2%')) a,
                ( select TradingDay,InstrumentID,Volume,Position from SettlementInfo  where TradingDay='{PreTradingDay}' and (InstrumentID like '{productcode}1%' or InstrumentID like '{productcode}2%')) b  
                where a.InstrumentID=b.InstrumentID 
        """.format(TradingDay=info.TradingDay, PreTradingDay=info.PreTradingDay, productcode=info.ProductCode)
    result = info.mysql.ExecQuery(sql)
    temp = dealResult(result)
    return temp

def GetDaysInstrument(info,positionlist):
    """计算合约持仓量超过阈值的天数，返回一个字典"""
    tempdict={}
    sql="""select InstrumentID as 合约,[TradingDay] as 交易日,[Position] as 持仓量,[Volume] as 成交量  from [SettlementInfo] where InstrumentID='{InstrumentID}' and Position>{position} order by TradingDay desc"""
    for i in positionlist:
        tempsql=sql.format(InstrumentID=i,position=MAX_POSITION)
        result = info.mysql.ExecQuery(tempsql)
        if len(result)==5 or len(result)==1:
            tempdict[i]=len(result)
    return tempdict

if __name__ == '__main__':

    info = InfoApi()
    info.GetDbHistoryConnect()  # 初始化mysql
    t = TradingDay(info)

    startdate=datetime.datetime.now()
    Prestartdate = t.NextTradingDayFuture(startdate.strftime("%Y%m%d"), False)
    Prestartdate = Prestartdate[0:4] + "-" + Prestartdate[4:6] + "-" + Prestartdate[6:8]
    setattr(info, 'TradingDay', startdate.strftime("%Y-%m-%d"))
    setattr(info, 'PreTradingDay', Prestartdate)
    setattr(info, 'ProductCode', 'ni')

    positionlist=GetPosition(info)

    if len(positionlist):
        """2、超过15万手的合约判断是否是第一天或者第五天，返回一个字典(keys：作为合约，value:作为持仓量超过15万手的天数)"""
        value=GetDaysInstrument(info,positionlist)
        if len(value):
            """3、根据2中返回的字典拼接邮件发送的内容，附件以当天持仓量监控的内容"""
            sender = "zjl@jingyoutech.com"
            password = "Aa85258584"
            receiver = ["zhuhaiqiang@jingyoutech.com","1927007992@qq.com","jt@jingyoutech.com"]
            receiverPwd = "lvadbzsqpjxpdjhf"
            subject = u"镍持仓量监控"
            doc=Directory+startdate.strftime("%Y%m%d")+u"镍持仓量监控.xlsx"
            myemail = MyEmail(sender, password=password, receiver=receiver, subject=subject, Type="qiye", doc=doc)
            myemail.Login()
            content=""
            print value
            for k,v in value.items():
                if v==1:
                    tempcontent = u"%s合约首次持仓超过15万张,\n" % (k)
                else:
                    tempcontent = u"%s合约连续%s天持仓超过15万张,\n"%(k,v)
                content=content+tempcontent
            content=content+u"镍当天持仓成交数据请见附件,历史持仓数据见github镍持仓量监控文件夹(邮件自动监控脚本发送，如有bug，请及时提出)。"
            print content
            myemail.SetSendContent(content)
            myemail.Send()