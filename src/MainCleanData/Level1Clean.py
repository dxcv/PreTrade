# -*- coding: utf-8 -*-
# @Time    : 2019/2/19 15:36
# @Author  : ZouJunLin
"""一档行情的数据处理"""
from CleanAPI import *
import pandas as pd
import sys,csv,datetime

columnsNum1=['最后修改时间','最新价','持仓量','数量','成交金额','本次结算价','上次结算价','昨收盘','今开盘','最高价','最低价','申卖价一','申卖价二','申卖价三','申卖价四','申卖价五'
    , '申卖量一','申卖量二','申卖量三','申卖量四','申卖量五','申买价一','申买价二','申买价三','申买价四','申买价五','申买量一','申买量二','申买量三','申买量四',
         '申买量五']       ##缺少  Volume：当前成交量    Amount：当前成交金额       isBuy：是否为主动性买单

columns0={'最后修改时间':'Time','最新价':'Price','持仓量':'OpenInt','数量':'TotalVol','成交金额':'TotalAmount','本次结算价':' Price2','上次结算价':' Price3','昨收盘':' LastClose','今开盘':'Open','最高价':'High','最低价':'Low','申卖价一':'SP1','申卖价二':'SP2','申卖价三':'SP3','申卖价四':'SP4','申卖价五':'SP5'
    , '申卖量一':'SV1','申卖量二':'SV2','申卖量三':'SV3','申卖量四':'SV4','申卖量五':'SV5','申买价一':'BP1','申买价二':'BP2','申买价三':'BP3','申买价四':'BP4','申买价五':'BP5','申买量一':'BV1','申买量二':'BV2','申买量三':'BV3','申买量四':'BV4',
         '申买量五':'BV5'}       ##缺少  Volume：当前成交量    Amount：当前成交金额       isBuy：是否为主动性买单

cols=['交易日','合约代码','交易所代码','合约在交易所的代码','最新价','上次结算价','昨收盘','昨持仓量','今开盘','最高价','最低价','数量','成交金额','持仓量','今收盘','本次结算价','涨停板价','跌停板价','昨虚实度','今虚实度','最后修改时间','最后修改毫秒','申买价一','申买量一','申卖价一','申卖量一','申买价二','申买量二',
      '申卖价二','申卖量二','申买价三','申买量三','申卖价三','申卖量三','申买价四','申买量四','申卖价四','申卖量四','申买价五','申买量五','申卖价五','申卖量五','当日均价','业务日期']



def Myappend(csv_data, i, templist,InstrumentID,TradingDay):
    newcol = []
    newcol.append(TradingDay)
    newcol.append(InstrumentID)
    for column in columnsNum1:
        newcol.append(csv_data.at[i, column])
    # newcol.insert(4,'')
    # newcol.insert(5,'')
    # newcol.insert(35,'')
    templist.append(tuple(newcol))


def Level_1_Clean(filename, fileDirectory, info):
    """
    读取数据并清洗
    :param filename: 源文件名
    :param fileDirectory: 源文件路径
    :return:
    """
    templist = []
    TotalAmount = 0
    temp = ""  #记录已经完成的上一个数据点
    lasthms = ""
    InstrumentId = info.cleanDatadict[2]
    TradingDay = info.cleanDatadict[0]
    code = info.cleanDatadict[1]
    sql = """ select [DayTradTime],[NightTradTime] from ContractCode where InstrumentCode='%s'"""%code
    TradTime=info.mysql.ExecQuery(sql)[0]
    try:
        csv_data=pd.read_csv(fileDirectory+"\\"+filename,encoding='gbk',header=0,names=cols)
    except Exception,e:
        print e.message
        print "文件读取异常，安全退出"
        sys.exit(0)
    length = len(csv_data)-1
    firstdata = datetime.datetime.strptime(str(csv_data.at[0, 'Time']), '%Y-%m-%d %H:%M:%S').strftime("%H:%M:%S")
    starthms, endhms = GetSEndHms(TradTime,firstdata)
    backup_name = info.cleanDatadict[1] + "_" + info.cleanDatadict[0] + ".csv"
    backup_csvfile = IshaveFile(backup_name, info.cleanDatadict[3], False, "wb")
    writer = csv.writer(backup_csvfile)
    col=[]
    col.append('InstrumentId')
    col.append('TradingDay')
    for i in columnsNum1:
        col.append(columns0[i])
    writer.writerow(col)

    for i in csv_data.index:
        date = datetime.datetime.strptime(str(csv_data.at[i, '最后修改时间']), '%H:%M:%S')
        hms = date.strftime("%H%M%S")
        if temp==hms:
            continue
        if isintimerange(hms+"000",TradTime):
            if starthms!="":
                if starthms[:6]==hms:
                    if int(csv_data.at[i, '申卖价一'])==0:
                        csv_data.at[i, '申卖价一']=csv_data.at[i,'昨收盘']
                    if int(csv_data.at[i, '申买价一']) == 0:
                        csv_data.at[i, '申买价一'] = csv_data.at[i, '昨收盘']
                    lasthms=theorynextdate(starthms,TradTime)
                    starthms=""
                    csv_data.at[i, '最后修改时间'] = str('%09d' % int(hms + "000"))
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                else:
                    """补开盘时缺失的数据"""
                    while starthms!=hms+'000':
                        if i==0:
                            if int(csv_data.at[i, '申卖价一']) == 0:
                                csv_data.at[i, '申卖价一'] = csv_data.at[i, '昨收盘']
                            if int(csv_data.at[i, '申买价一']) == 0:
                                csv_data.at[i, '申买价一'] = csv_data.at[i, '昨收盘']
                            csv_data.at[i, '最后修改时间'] =starthms
                            Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                            starthms=theorynextdate(starthms,TradTime)
                        else:
                            if int(csv_data.at[i-1, '申卖价一']) == 0:
                                csv_data.at[i-1, '申卖价一'] = csv_data.at[i, '昨收盘']
                            if int(csv_data.at[i-1, '申买价一']) == 0:
                                csv_data.at[i-1, '申买价一'] = csv_data.at[i, '昨收盘']
                            csv_data.at[i-1, '最后修改时间'] = starthms
                            Myappend(csv_data, i-1, templist, InstrumentId, TradingDay)
                            starthms = theorynextdate(starthms, TradTime)
                    csv_data.at[i, '最后修改时间'] = starthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    if lasthms == endhms:
                        daylist = [InstrumentId, TradingDay, csv_data.at[i, '数量'], csv_data.at[i, '成交金额'],csv_data.at[i, '本次结算价'], csv_data.at[i, '最新价'], csv_data.at[i, '上次结算价'],csv_data.at[i, '昨收盘']]
                        break
                    lasthms = theorynextdate(starthms, TradTime)
                    starthms=""
            elif hms !=temp:
                if hms!=lasthms[:6] :
                    while lasthms !=hms + "000" and lasthms!=endhms:
                        csv_data.at[i-1, '最后修改时间'] = lasthms
                        Myappend(csv_data, i-1, templist, InstrumentId, TradingDay)
                        lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                            temp = hms
                    csv_data.at[i, '最后修改时间'] = lasthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    if lasthms == endhms:
                        daylist = [InstrumentId, TradingDay, csv_data.at[i, '数量'], csv_data.at[i, '成交金额'],csv_data.at[i, '本次结算价'], csv_data.at[i, '最新价'], csv_data.at[i, '上次结算价'],csv_data.at[i, '昨收盘']]
                        break
                    lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                        temp = hms
                elif lasthms==hms+"000":
                    csv_data.at[i, '最后修改时间'] = lasthms
                    Myappend(csv_data, i, templist,InstrumentId,TradingDay)
                    lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                        temp=hms
                elif lasthms==hms+"500":
                    csv_data.at[i, '最后修改时间'] = lasthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    lasthms = theorynextdate(lasthms, TradTime)
                    temp=hms

        "结束循环的两种情况"
        if lasthms==endhms:
            daylist = [InstrumentId, TradingDay, csv_data.at[i, '数量'], csv_data.at[i, '成交金额'],csv_data.at[i, '本次结算价'], csv_data.at[i, '最新价'], csv_data.at[i, '上次结算价'], csv_data.at[i, '昨收盘']]
            break
        if i ==length and lasthms!=endhms:
            while lasthms != endhms and lasthms!="":
                csv_data.at[i, '最后修改时间'] = lasthms
                Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                lasthms = theorynextdate(lasthms, TradTime)
            csv_data.at[i, '最后修改时间'] = lasthms
            Myappend(csv_data, i, templist, InstrumentId, TradingDay)
            if lasthms=="":
                break
            lasthms = theorynextdate(lasthms, TradTime)
    while lasthms!="":
        csv_data.at[i, 'Time'] = lasthms
        Myappend(csv_data, i, templist, InstrumentId, TradingDay)
        lasthms = theorynextdate(lasthms, TradTime)
    writer.writerows(templist)
