# encoding:utf-8
import pandas as pd
import os,sys
directory=os.getcwd()+"\DATA"
from dataCleanApi import *
import csv,datetime
from collections import OrderedDict
from tqdm import tqdm
import sys,re
reload(sys)
import codecs
from utils.InfoApi import *
from utils.sqlServer import *
sys.setdefaultencoding('utf-8')

insertFuturesql="""
        INSERT INTO FutureHistoryTdData
           (TradingDay,InstrumentId,Time,Price,OpenInt,TotalVol,TotalAmount,Price2,Price3,LastClose,[Open],High,Low,SP1,SP2,SP3,SP4,SP5,SV1,SV2,SV3 ,SV4,SV5 ,BP1,BP2,BP3,BP4,BP5,BV1,BV2,BV3,BV4,BV5)
        VALUES ('%s','%s',%s,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d)
    """

insertDayFuturesql="""
        INSERT INTO FutureHistoryTdDayData
           (InstrumentId,TradingDay,TotalVol,[TotalAmount],SettlementPrice,ClosePrice,LastSettlementPrice,[LastClosePrice])
        VALUES ('%s','%s',%d,%d,'%s','%s','%s','%s')
    """

columns0=OrderedDict()
columns1=OrderedDict()
# columns0={'交易日':'TradingDay','合约代码':'InstrumentId','最后修改时间':'Time','最新价':'Price','今开盘':'Open','最高价':'High','最低价':'Low','昨收盘':'LastClose','涨停板价':'HighLimitPrice','跌停板价':'LowLimitPrice','数量':'Volume','成交金额':'Amount','持仓量':'OpenInt','昨持仓量':'LastTotalVol','本次结算价':'Price2','今收盘':'ClosePrice ','上次结算价':'Price3','昨虚实度':'PreDelta','今虚实度':'Delta'
#     ,'申买价一':'BP1','申买量一':'BV1','申卖价一':'SP1','申卖量一':'SV1','申买价二':'BP2','申买量二':'BV2','申卖价二':'SP2','申卖量二':'SV2','申买价三':'BP3','申买量三':'BV3','申卖价三':'SP3','申卖量三':'SV3','申买价四':'BP4','申买量四':'BV4','申卖价四':'SP4','申卖量四':'SV4','申买价五':'BP5',
#          '申买量五':'BV5','申卖价五':'SP5','申卖量五':'SV5','当日均价':'AveragePrice'}

columnsNum=['交易日','合约代码','最后修改时间','最新价','今开盘','最高价','最低价','涨停板价','跌停板价','数量','成交金额','持仓量','昨持仓量','本次结算价','今收盘','上次结算价','昨收盘','昨虚实度','今虚实度'
    ,'申买价一','申买量一','申卖价一','申卖量一','申买价二','申买量二','申卖价二','申卖量二','申买价三','申买量三','申卖价三','申卖量三','申买价四','申买量四','申卖价四','申卖量四','申买价五',
         '申买量五','申卖价五','申卖量五','当日均价']

columns0={'最后修改时间':'Time','最新价':'Price','持仓量':'OpenInt','数量':'TotalVol','成交金额':'TotalAmount','本次结算价':' Price2','上次结算价':' Price3','昨收盘':' LastClose','今开盘':'Open','最高价':'High','最低价':'Low','申卖价一':'SP1','申卖价二':'SP2','申卖价三':'SP3','申卖价四':'SP4','申卖价五':'SP5'
    , '申卖量一':'SV1','申卖量二':'SV2','申卖量三':'SV3','申卖量四':'SV4','申卖量五':'SV5','申买价一':'BP1','申买价二':'BP2','申买价三':'BP3','申买价四':'BP4','申买价五':'BP5','申买量一':'BV1','申买量二':'BV2','申买量三':'BV3','申买量四':'BV4',
         '申买量五':'BV5'}       ##缺少  Volume：当前成交量    Amount：当前成交金额       isBuy：是否为主动性买单

cols=['交易日','合约代码','交易所代码','合约在交易所的代码','最新价','上次结算价','昨收盘','昨持仓量','今开盘','最高价','最低价','数量','成交金额','持仓量','今收盘','本次结算价','涨停板价','跌停板价','昨虚实度','今虚实度','最后修改时间','最后修改毫秒','申买价一','申买量一','申卖价一','申卖量一','申买价二','申买量二',
      '申卖价二','申卖量二','申买价三','申买量三','申卖价三','申卖量三','申买价四','申买量四','申卖价四','申卖量四','申买价五','申买量五','申卖价五','申卖量五','当日均价','业务日期']

columnsNum1=['最后修改时间','最新价','持仓量','数量','成交金额','本次结算价','上次结算价','昨收盘','今开盘','最高价','最低价','申卖价一','申卖价二','申卖价三','申卖价四','申卖价五'
    , '申卖量一','申卖量二','申卖量三','申卖量四','申卖量五','申买价一','申买价二','申买价三','申买价四','申买价五','申买量一','申买量二','申买量三','申买量四',
         '申买量五']       ##缺少  Volume：当前成交量    Amount：当前成交金额       isBuy：是否为主动性买单

columns1=['InstrumentId', 'Price', 'BP5', 'SP2', 'Volume', 'Delta', 'Open', 'SV5', 'SV2', 'LowLimitPrice', 'BP1', 'SV3', 'PreDelta', 'LastClose', 'Low', 'OpenInt', 'SV1', 'BP2', 'SP3', 'BV2', 'BV4', 'LastTotalVol', 'Price3', 'SV4', 'BV3', 'BP3', 'SP1', 'HighLimitPrice', 'SP4', 'SP5', 'BV1', 'Amount', 'AveragePrice', 'Price2', 'BP4', 'Time', 'BV5', 'High', 'ClosePrice ', 'TradingDay']
mydataApi=MyDataApi()

def dataclean(mysql):
    # ##读取文件到内存
    abspath = os.path.abspath(directory)
    for filenames in os.walk(abspath):
        print filenames
        if not os.path.exists("./cleanedData"):
            os.mkdir("./cleanedData")
        ## 新建清洗后的数据的存储路径
        for i in filenames[1]:
            path = filenames[0].replace(abspath, "") + "\\" + i + "\\"
            path = os.getcwd() + "\\cleanedData" + path
            if not os.path.exists(path):
                os.mkdir(path)
        fileDirectory = filenames[0]
        for filename in filenames[2]:
            filename=filename.decode(encoding='gbk')
            if str(filename).endswith("csv"):
                data=pd.read_csv(fileDirectory+"\\"+filename)
                ReadData(filename,fileDirectory,mysql)
        


    # ##清洗数据




    # ##写入csv文件或者数据库




def ReadData(filename,fileDirectory,mysql):
    """
    读取数据并清洗
    :param filename: 文件名
    :param fileDirectory: 文件路径
    :return:
    """
    # test="150000000"
    # print theorynextdate(test,['0','3'])
    # raise Exception
    templist = []
    TotalAmount = 0
    temp = ""  #记录已经完成的上一个数据点
    lasthms = ""
    InstrumentId=filename.split("_")[0]
    TradingDay = filename.split("_")[1].split(".")[0]
    isExist="  select top 1 * from [PreTrade].[dbo].[FutureHistoryTdData] where InstrumentId='%s' and TradingDay='%s'  "%(InstrumentId,TradingDay)
    t=mysql.ExecQuery(isExist)
    if len(t):
        # print InstrumentId,TradingDay,"数据已经存在"
        return
    code = re.search(r"[a-zA-Z]+\.?", str(InstrumentId).strip()).group()
    sql = """ select [DayTradTime],[NightTradTime] from ContractCode where InstrumentCode='%s'"""%code
    TradTime=mysql.ExecQuery(sql)[0]
    starthms,endhms = GetSEndHms(TradTime)
    # print starthms,endhms
    # raise Exception
    try:
        csv_data=pd.read_csv(fileDirectory+"\\"+filename,encoding='gbk',header=0,names=cols)
    except Exception,e:
        print e.message
        print "文件读取异常，安全退出"
        sys.exit(0)
    length = len(csv_data)-1
    backup_name=filename.replace(".csv","_backup.csv")
    backup_csvfile = mydataApi.IshaveFile(backup_name, fileDirectory, False, "wb")
    writer = csv.writer(backup_csvfile)
    col=[]
    col.append('InstrumentId')
    col.append('TradingDay')
    for i in columnsNum1:
        col.append(columns0[i])
    writer.writerow(col)

    for i in tqdm(csv_data.index):
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
    # InsertFutureData(templist, tuple(daylist),insertFuturesql,insertDayFuturesql)
    if len(daylist) > 0:
        mysql.ExecmanyNonQuery(insertDayFuturesql, tuple(daylist))
    if len(templist) > 0:
        mysql.ExecmanysNonQuery(insertFuturesql, templist)
    # writer.writerows(templist)



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

if __name__=='__main__':
    temp=InfoApi().GetDbHistoryDataAccount()
    mysql=Mysql(temp[0],temp[1],temp[2],temp[3])
    dataclean(mysql)