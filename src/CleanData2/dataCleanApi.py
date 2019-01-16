# -*- coding: utf-8 -*-
# @Time    : 2018/9/6 16:39
# @Author  : ZouJunLin
"""
数据清洗全用到的方法
"""
import datetime


# columns0={'交易日':'TradingDay','合约代码':'InstrumentId','最新价':'Price','上次结算价':'Price3','昨收盘':'LastClose','昨持仓量':'LastTotalVol','今开盘':'Open','最高价':'High','最低价':'Low','数量':'Volume','成交金额':'Amount','持仓量':'OpenInt','今收盘':'ClosePrice ','本次结算价':'Price3','涨停板价':'HighLimitPrice','跌停板价':'LowLimitPrice','昨虚实度':'PreDelta','今虚实度':'Delta',
#           '后修改时间':'Time','申买价一':'BP1','申买量一':'BV1','申卖价一':'SP1','申卖量一':'SV1','申买价二':'BP2','申买量二':'BP2','申卖价二':'SP2','申卖量二':'SV2','申买价三':'BP3','申买量三':'BV3','申卖价三':'SP3','申卖量三':'SV3','申买价四':'BP4','申买量四':'BV4','申卖价四':'SP4','申卖量四':'SV4','申买价五':'BP5',
#          '申买量五':'BV5','申卖价五':'SP5','申卖量五':'SV5','当日均价':'AveragePrice','业务日期':'TradingDay'}
columns=['Time','Price','Volume','Amount','OpenInt','TotalVol','TotalAmount','Price2','Price3','LastClose','Open','High','Low','SP1','SP2','SP3','SP4','SP5','SV1','SV2','SV3','SV4','SV5','BP1','BP2','BP3','BP4','BP5','BV1','BV2','BV3','BV4','BV5','isBuy']

class MyDataApi:
    def IshaveFile(self,name, fileDirectory, close, method):
        """
       判断文件是否存在
       :param name: 
       :param fileDirectory: 
       :param close: 
       :param method: 
       :return: 
        """""
        fileDirectory = fileDirectory.split("\\")
        fileDirectory[fileDirectory.index("DATA")] = "cleanedData"
        fileDirectory = "\\".join(fileDirectory)
        filename=fileDirectory + "\\" + name
        try:
            file_backup = open(filename, method)
        except Exception, e:
            file_backup = open(filename, method)
            print e.message
        if close:
            file_backup.close()
        return file_backup

    def isintimerange(hms):
        """
        判断时间是否在建议范围之类
        :return:
        """
        try:
            if mystrptime(hms) >= mystrptime("090000") and mystrptime(hms) <= mystrptime("101500"):
                return True
            elif mystrptime(hms) >= mystrptime("103000") and mystrptime(hms) <= mystrptime("113000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000") and mystrptime(hms) <= mystrptime("150000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000") and mystrptime(hms) <= mystrptime("233000"):
                return True
            else:
                return False
        except Exception, e:
            # print e.message,hms,type(hms),"循环里面时间溢出，正常情况"
            return False

def isintimerange(hms,TradTime):


    try:
        if TradTime[0]=='0' and TradTime[1]=='0':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='1':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("230000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='2':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("233000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='3':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("235959500"):
                return True
            elif mystrptime(hms) >= mystrptime("000000000") and mystrptime(hms) <= mystrptime("010000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='4':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("235959500"):
                return True
            elif mystrptime(hms) >= mystrptime("000000000") and mystrptime(hms) <= mystrptime("023000000"):
                return True
            else:
                return False
        elif TradTime[0]=='1' and TradTime[1]=='0':
            if mystrptime(hms) >= mystrptime("091500000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("151500000"):
                return True
            else:
                return False
        elif TradTime[0] == '2' and TradTime[1] == '0':
            if mystrptime(hms) >= mystrptime("093000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            else:
                return False
    except Exception, e:
        # print e.message,hms,type(hms),"循环里面时间溢出，正常情况"
        return False

def mystrptime(hms):
    return datetime.datetime.strptime(hms.strip(),"%H%M%S%f")

# def Myappend(csv_data,i,col,TradingDay,InstrumentId):
#     newcol=[]
#     # newcol.append(TradingDay)
#     # newcol.append(InstrumentId)
#     for column in columns:
#         newcol.append(csv_data.at[i,column])
#     col.append(tuple(newcol))


def theorynextdate(lasthms,TradTime):
    """
    #理论上下一个时间点的数,当然有时间范围限制
    :param lasthms:
    :return:
    """
    time=str('%09d'%(int(lasthms)+500))
    if time[2:4]=="60" or time[4:6]=="60":
        hour=int(time[:2])
        minute=int(time[2:4])
        second=int(time[4:6])
        if second==60:
            minute=str('%02d'%(minute+1))
            second=str("00")
            if minute=="60":
                hour = str('%02d' % (hour + 1))
                minute = str("00")
                if (hour == "24"):
                    hour = str("00")

        if minute==60:
            hour=str('%02d'%(hour+1))
            minute=str("00")
            if (hour == "24"):
                hour = str("00")
        if(hour==24):
            hour=str("00")
        hour='%02d'%int(hour)
        minute = '%02d'%int(minute)
        second='%02d'%int(second)
        time=str(hour)+str(minute)+str(second)+time[-3:]

    if isintimerange(time,TradTime):
        return time
    else:
        time=nextdurationTime(time,TradTime)
        return time

def nextdurationTime(time,TradTime):
    """计算时间断层问题，将时间链接起来"""
    dict1=dict()
    dict0={'0':{"101500500":'103000000','113000500':'133000000'},'1':{'113000500':'133000000'},'2':{'113000500':'133000000'}}
    list0=["090000000",'091500000','093000000']

    dict1['0']={}
    dict1['1']={'230000500':list0[int(TradTime[0])]}
    dict1['2']={'233000500':list0[int(TradTime[0])]}
    dict1['3']={'010000500':list0[int(TradTime[0])]}
    dict1['4']={'023000500':list0[int(TradTime[0])]}

    tempdict=dict(dict0[TradTime[0]].items()+dict1[TradTime[1]].items())
    try:
        return tempdict[time]
    except:
        return ""



def  GetSEndHms(TradTime):
    """获取合约的交易开始时间"""
    startime={'0':['090000000','091500000','093000000']}
    endtime=['150000000','151500000','150000000']
    if TradTime[1]=='0':
        return startime['0'][int(TradTime[0])],endtime[int(TradTime[0])]
    else:
        return '210000000',endtime[int(TradTime[0])]

def specialdata(hms,TradTime):
    temp=dict()
    temp['0']=['090000000','103000000','133000000']
    temp['1']=['091500000','130000000']
    temp['2']=['093000000','130000000']
    if hms in temp[TradTime[0]]:
        return True
    else:
        return False

