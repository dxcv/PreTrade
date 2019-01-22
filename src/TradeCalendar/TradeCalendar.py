#-*-coding:utf-8-*-
# @Time    : 2019/1/17 15:44
# @Author  : ZouJunLin
"""获取最近所有品种正在交易的合约，最后交易日倒计时，和即将到来的合约"""

from utils.InfoApi import *
from utils.MyXlwt import *
from utils.BasicAPI import  *
import xlwt




def main(info):
    columnsIndex=dict()
    nowTime="20190117"
    now=datetime.datetime.strptime(nowTime,"%Y%m%d")
    filename=str(now.strftime("%Y%m%d")+'交易日历'+".xls").strip()
    basicapi=BasicAPI()
    largmonth="".join(list(basicapi.GetInstrumentMonth(info,'sc','SHFE')[-1:][0])[-2:])
    largyear= "".join(list(datetime.datetime.now().strftime("%Y%m%d"))[:2])+"".join(list(basicapi.GetInstrumentMonth(info, 'sc', 'SHFE')[-1:][0])[-4:-2])
    print largmonth,largyear
    largMonthList =GetContinueM(largyear,largmonth)
    # print largMonthList
    columns=[u"交易所",u"交易品种",u"名称"]+largMonthList
    style=MyXlwt().GetMystyleTitle()
    redstyle=MyXlwt().GetstyleRed()
    # filename=datetime.datetime.now().strftime("%Y%m%d")+u"交易日历"+".xlsx"
    templist = info.mysql.ExecQuery("select [TradeCode],[ExchangeID],[ProductName] from  [PreTrade].[dbo].[StandContract] order by ExchangeID")
    templist = BasicAPI().GetResultList(templist)

    wbk = xlwt.Workbook(encoding="utf-8")
    sheet = wbk.add_sheet('sheet Test')
    # indexing is zero based, row then column
    InstrumentIDList = info.mysql.ExecQueryGetList( "select [InstrumentID]  FROM [PreTrade].[dbo].[SettlementInfo] where TradingDay='2019-01-17'  and [IsFuture]='1'")
    for i in range(len(columns)):
        sheet.row(i).height_mismatch = True
        sheet.row(i).height = 1000
        sheet.col(i).width = (20 * 240)
        sheet.write(0,i,columns[i],style)
        columnsIndex[columns[i]]=i
    print columnsIndex
    for i in range(len(templist)):
        sheet.row(i+1).height_mismatch = True
        sheet.row(i+1).height = 1000
        sheet.col(i+1).width = (20 * 240)
        InstrumentId=templist[i][0]
        ExchangeId=templist[i][1]
        ProductName=templist[i][2]
        temp = BasicAPI().GetInstrumentMonth(info, InstrumentId, ExchangeId)
        sheet.write(i+1, 0, ExchangeId, style)
        sheet.write(i+1, 1, InstrumentId, style)
        sheet.write(i+1, 2, ProductName, style)
        # sheet.handle_writeaccess(temp)
        if set(temp) <= set(InstrumentIDList):
            InstrumentIDList = list(set(InstrumentIDList) - set(temp))
        else:
            print temp
            print "**********************************"
        lastCode = temp[0]
        for j in temp:
            print "j",j
            if ExchangeId=='CZCE':
                InstrumentIdtemp = "".join(list(nowTime)[:3]) + "".join(list(j)[-3:])
                try:
                    test=columnsIndex[InstrumentIdtemp]
                except:
                    InstrumentIdtemp = str(int("".join(list(nowTime)[:3]))+1).zfill(3) + "".join(list(j)[-3:])
            else:
                InstrumentIdtemp="".join(list(nowTime)[:2])+"".join(list(j)[-4:])
            print "InstrumentIdtemp",InstrumentIdtemp
            if j!=temp[0]:
                sheet.write(i + 1,columnsIndex[InstrumentIdtemp],j, style)
            else:
                day = BasicAPI().Get_EndDate(info).GetEndDate(lastCode)[0]
                day = (datetime.datetime.strptime(day, "%Y%m%d") - now).days
                content=str(j+"\n"+"合约倒计时:"+str(day) +"天").strip()
                if day<=15:
                    sheet.write(i + 1, columnsIndex[InstrumentIdtemp], content, redstyle)
                else:
                    sheet.write(i + 1, columnsIndex[InstrumentIdtemp], content, style)

        # day = BasicAPI().Get_EndDate(info).GetEndDate(lastCode)[0]
        # day = (datetime.datetime.strptime(day, "%Y%m%d") - now).days
        # print lastCode
        # if day <= 100:
        #     print "合约倒计时:", day, "天"

    print "----------------------------------------------------------"
    print InstrumentIDList

    wbk.save(filename.decode('utf-8'))

def GetContinueM(largyear,largmonth):
    """获取到最大年月日的所有月份"""
    col=[]
    now = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y%m")+"01","%Y%m%d")
    largm=datetime.datetime.strptime(largyear+largmonth,"%Y%m")
    while now<=largm:
        year = now.year
        month = now.month
        col.append(now.strftime("%Y%m"))
        days_num = calendar.monthrange(int(year), int(month))[1]
        now=now+datetime.timedelta(days=days_num)
    return col




if __name__=='__main__':
    info=InfoApi()
    main(info)