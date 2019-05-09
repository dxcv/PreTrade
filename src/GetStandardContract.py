# -*- coding: utf-8 -*-
# @Time    : 2019/1/4 14:39
# @Author  : ZouJunLin
"""获取官网标准合约信息，并更新相关数据"""
from utils.BasicAPI import *
from utils.InfoApi import *


LimitUpDown=["每日价格最大波动限制","每日价格波动限制" ,"涨跌停板幅度*","涨跌停板幅度"]
InstrumentCODE=["交易代码"]
VolumeMULTIPLE=["交割单位","合约乘数","交易单位"," 交易单位","交易"]



class ProductDetail:
    def __init__(self):
        self.imgContract=["沥青","天然橡胶","原油"]
        self.cf = ConfigParser.ConfigParser()
        self.cf.read("InstrumentCode.ini")
        self.errorName=[]
        self.ContractTable=dict()
        self.DeliveryMethod={"实物交割":3,"期货交割":2,"现金交割":1}
        self.mysplider=MySplider()
        self.templist={'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,'十': 10,"十一":11,"十二":12,"十三":13,"十四":14}
        self.CZCEHeader= {
            'Host': 'www.czce.com.cn',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Content-Encoding': 'gzip',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cookie': 'BIGipServerwww_cbd=842836160.23067.0000; JSESSIONID=kYM9bphG26lQ6p25GXxyVRbkFw815nqCmJmTYxzGxxxvJnVTblhb!-1757810877; TS014ada8c=0169c5aa326a6aa66d379d396a2033d86a84f272d2b5c2f5d7b7d668355885aa62597926b41ead5b31a9f62f3252ecfb1e2c33422b',
            'Referer': 'http://www.zce.cn/portal/jysj/qqjysj/qqmrhq/A09112102index_1.htm'
        }
        self.DCEHeader={
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=5C1180D80CDBC1F61E550F5B7B1F9864; WMONID=MxRKrfMhnv4; Hm_lvt_a50228174de2a93aee654389576b60fb=1534468904,1534468904',
        'Host': 'www.dce.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
        }
        self.SHFEHeader={
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'shfe_fbl=2560*1440; shfe_fbl=2560*1440; shfe_cookid=1810261604402a0306a4bf0a876962f8f9ea91cbbff6; shfe_fbl=2560*1440',
        'Host': 'www.shfe.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
        }
        self.CFFEXHeader={
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Host': 'www.cffex.com.cn',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1'
        }

    def GetContractTable(self):
        """VolumeMultiple,PriceTick,LimitUpDown,MinMargin,DelivMethod,EndDate,EndDelivDate,Delivemonth"""
        col = []
        temp = eval(self.cf.get(self.ExchangeID, self.InstrumentCode))
        EndDate = str(temp["EndDateM"]) + "|" + str(temp["EndDate"])
        EndDelivDate = str(temp["EndDelivDateM"]) + "|" + str(temp["EndDelivDate"])

        return temp["Name"], self.InstrumentCode, self.ExchangeID, temp['VolumeMultiple'], temp['PriceTick'], temp[
            "LimitUpDown"], EndDate, EndDelivDate, temp["MinMargin"], temp["DelivMethod"], temp["Delivemonth"]

    def explaintempEndDelivDate(self,tempEndDelivDate):
        """文本解析，抽取最后交割日数据信息"""
        EndDatestr1=""
        EndDate1=""
        if tempEndDelivDate.find("板交割")!=-1:
            a=tempEndDelivDate.split(" ")
            if tempEndDelivDate.index("仓单交割")==0:
                tempEndDelivDate=a[0]
                tempEndDelivDate1=a[1]
            else:
                tempEndDelivDate = a[1]
                tempEndDelivDate1 = a[0]

            if tempEndDelivDate1.find("次月")!=-1:
                EndDatestr1='2'
            elif tempEndDelivDate1.find("合约交割月份的最后")!=-1:
                EndDatestr1='3'
            elif tempEndDelivDate1.find("第")!=-1:
                EndDatestr1 = '0'
            EndDate1 = (re.findall(ur'[一二三四五六七八九十]+|[0-9]+', tempEndDelivDate1.decode("utf-8"))[0]).encode("utf-8")
            EndDate1="|"+EndDatestr1 + "|" + str(EndDate1).strip()
        if tempEndDelivDate.find("合约交割月份的第")!=-1 or tempEndDelivDate.find("合约交割月份第")!=-1:
            EndDatestr = '0'
        elif tempEndDelivDate.find("最后交易日后第")!=-1 or tempEndDelivDate.find("最后交易日后连续")!=-1 or tempEndDelivDate.find("最后交易日后的第")!=-1:
            EndDatestr='1'

        if tempEndDelivDate.find("同最后交易日")!=-1:
            EndDate='1|0'
        else:
            EndDate = (re.findall(ur'[一二三四五六七八九十]+|[0-9]+', tempEndDelivDate.decode("utf-8"))[0]).encode("utf-8")
            if EndDate in self.templist.keys():
                EndDate = str(self.templist[EndDate])
            EndDate = EndDatestr + "|" + str(EndDate).strip()+EndDate1
        return EndDate


    def explainEnddate(self,tempEndDate):
        """抽取最后交易日数据信息"""
        if tempEndDate.find("个星期五") != -1 or tempEndDate.find("周五") != -1:
            EndDatestr = '4'
            tempEndDate.replace("个星期五","").replace("周五","")
        elif tempEndDate.find("倒数") != -1:
            EndDatestr = '3'
        elif tempEndDate.find("第") != -1:
            EndDatestr = '0'
        elif tempEndDate.find("合约月份") != -1:
            EndDatestr = '1'
        elif tempEndDate.find("前一月份的最后")!=-1:
            EndDatestr='2'
        tempEndDate=tempEndDate.replace("前一个月份","")
        EndDate = (re.findall(ur'[一二三四五六七八九十]+|[0-9]+', tempEndDate.decode("utf-8"))[0]).encode("utf-8")
        if EndDate in self.templist.keys():
            EndDate = str(self.templist[EndDate])
        EndDate = EndDatestr + "|" + str(EndDate).strip()
        return  EndDate

    def explainDelivemonth(self,Delivemonth):
        if  Delivemonth.find("3月、6月、9月、12月中的最近三个月循环")!=-1:
            return "3*3"
        elif Delivemonth.find("当月、下月及随后两个季月")!=-1:
            return "2&2*3"
        elif Delivemonth.find("-")!=-1 or Delivemonth.find("－")!=-1 or Delivemonth.find("～")!=-1 :
            monthList=re.findall(r'\d+', Delivemonth)
            monthList=range(int(monthList[0]),int(monthList[1])+1)
        elif Delivemonth.find("、")!=-1:
            monthList=re.findall(r'\d+', Delivemonth)
            monthList=map(eval,monthList)
        monthList=map(lambda x:str(x),monthList)
        monthList="|".join(monthList)
        return monthList

    def abstractData(self,td, data):
        t=dict()
        if td in LimitUpDown:
            Limitupdown = float(re.findall(r'([0-9.]+)[ ]*', data)[0]) / 100
            # if float(Limitupdown)==0.0:
            #     Limitupdown = re.findall(r'([0-9.]+)[ ]*%', data)
            t["LimitUpDown"]=Limitupdown
            return  t
        elif td in InstrumentCODE:
            InstrumentCode = BasicAPI().GetExchangeProductCode(data, self.ExchangeID)
            t["TradeCode"]=InstrumentCode
            return t
        elif td in VolumeMULTIPLE:
            print "data", data
            VolumeMultiple=re.findall(r'\d+',data)[0]
            print "VolumeMultiple",VolumeMultiple

    def GetProductDetail(self,Url,ExchangeID,name,code):
        print Url
        self.Url=Url
        self.ExchangeID=ExchangeID
        self.name=name
        self.InstrumentCode=code
        if ExchangeID=='CZCE':
            col=self._ProductDetail__GetCZCEProductDetail()
        elif ExchangeID=='DCE':
            col=self._ProductDetail__GetDCEProductDetail()
        elif ExchangeID=='SHFE':
            col=self._ProductDetail__GetSHFEProductDetail()
        elif ExchangeID=='CFFEX':
            col=self._ProductDetail__GetCFFEXProductDetail()
        return col

    def _ProductDetail__GetCFFEXProductDetail(self):
        table=[]
        html = self.mysplider.getUrlcontent(self.Url, self.CFFEXHeader)
        bs = BeautifulSoup(html, "lxml")
        divdata = bs.find("table")
        trdata = divdata.findAll("tr")
        for tr in trdata:
            td = tr.findAll("td")
            if len(td)==4:
                col1=[td[0].get_text().encode("utf-8").strip(),td[1].get_text().encode("utf-8").strip()]
                col2=[td[2].get_text().encode("utf-8").strip(),td[3].get_text().encode("utf-8").strip()]
                table.append(col1)
                table.append(col2)
        if self.name=="10年期国债" or self.name=="2年期国债" or self.name=="5年期国债":
            VolumeMultiple=10000
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[1][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[6][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[11][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[3][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[9][1]]
            tempEndDate=table[5][1]
            tempEndDelivDate=table[7][1]

        else:
            VolumeMultiple = re.findall(r'\d+', table[2][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[12][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[6][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[9][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[1][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[7][1]]
            tempEndDate=table[3][1]
            tempEndDelivDate=table[5][1]
        Delivemonth=self.explainDelivemonth(table[8][1])

        EndDate = self.explainEnddate(tempEndDate)
        EndDelivDate=self.explaintempEndDelivDate(tempEndDelivDate)

        return self.name, InstrumentCode, self.ExchangeID, VolumeMultiple, PriceTick, LimitUpDown, EndDate, EndDelivDate, MinMargin, DelivMethod,Delivemonth



    def _ProductDetail__GetSHFEProductDetail(self):
        html = self.mysplider.getUrlcontent(self.Url, self.SHFEHeader)
        table = self.mysplider.tableTolist(html, self.ExchangeID)
        if len(table)>6:
            VolumeMultiple = re.findall(r'\d+', table[1][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[4][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[3][1])[0]
            if self.name in ["白银","黄金","铅","燃料油","纸浆"]:
                InstrumentCode = BasicAPI().GetExchangeProductCode(table[14][1], self.ExchangeID)
            else:
                InstrumentCode = BasicAPI().GetExchangeProductCode(table[15][1], self.ExchangeID)
            # MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[11][1])[0]) / 100
            if self.name in ["纸浆","燃料油","铅","黄金","白银"]:
                MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[11][1])[0]) / 100
            else:
                MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[12][1])[0]) / 100
            if self.name in ["白银","黄金","铅","燃料油","纸浆"]:
                DelivMethod = self.DeliveryMethod[table[12][1]]
            else:
                DelivMethod = self.DeliveryMethod[table[13][1]]
            Delivemonth=table[5][1]
            EndDate=self.explainEnddate(table[7][1])
            tempEndDelivDate = table[8][1]
            if self.name=="黄金":
                Delivemonth="3&13/2"
            else:
                Delivemonth=self.explainDelivemonth(Delivemonth)
            EndDelivDate = self.explaintempEndDelivDate(tempEndDelivDate)
        else:
            if self.name in self.imgContract:
                # temp=InfoApi().GetContractTable(self.ExchangeID,self.InstrumentCode)
                temp=self.GetContractTable()
                VolumeMultiple=temp[3]
                LimitUpDown = temp[5]
                PriceTick = temp[4]
                InstrumentCode = temp[1]
                MinMargin = temp[8]
                DelivMethod=temp[9]
                EndDate = temp[6]
                EndDelivDate = temp[7]
                Delivemonth=temp[10]
        return self.name, InstrumentCode, self.ExchangeID, VolumeMultiple, PriceTick, LimitUpDown, EndDate, EndDelivDate, MinMargin, DelivMethod,Delivemonth

    def _ProductDetail__GetDCEProductDetail(self):
        html = self.mysplider.getUrlcontent(self.Url, self.DCEHeader)
        table = self.mysplider.tableTolistP(html, self.ExchangeID)
        if self.name=="玉米":
            VolumeMultiple = re.findall(r'\d+', table[1][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[4][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[3][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[13][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[11][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[12][1]]
            EndDate = (re.findall(ur'[一二三四五六七八九十]+|[0-9]+', table[8][1].decode("utf-8"))[0]).encode("utf-8")
            tempEndDate=table[7][1]
            tempEndDelivDate=table[8][1]
            Delivemonth=table[5][1]
        else:
            VolumeMultiple = re.findall(r'\d+', table[1][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[4][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[3][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[13][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[11][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[12][1]]
            EndDate = (re.findall(ur'[一二三四五六七八九十]+|[0-9]+', table[7][1].decode("utf-8"))[0]).encode("utf-8")
            tempEndDate = table[7][1]
            tempEndDelivDate=table[8][1]
            Delivemonth=table[5][1]
        EndDate = self.explainEnddate(tempEndDate)
        EndDelivDate=self.explaintempEndDelivDate(tempEndDelivDate)
        Delivemonth=self.explainDelivemonth(Delivemonth)
        return self.name, InstrumentCode, self.ExchangeID, VolumeMultiple, PriceTick, LimitUpDown, EndDate, EndDelivDate, MinMargin, DelivMethod,Delivemonth


    def _ProductDetail__GetCZCEProductDetail(self):
        html=self.mysplider.getUrlcontent(self.Url,self.CZCEHeader)
        table=self.mysplider.tableTolist(html,"CZCE")
        if self.name in ['红枣']:
            VolumeMultiple = re.findall(r'\d+', table[1][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[4][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*', table[3][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[13][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[5][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[12][1]]
            # ur'([一二三四五六七八九零十百千万亿]+|[0-9]+[,]*[0-9]+.[0-9]+)'
            tempEndDate = table[8][1]
            tempEndDelivDate = table[9][1]
            Delivemonth = table[6][1]
        elif self.name in ['甲醇']:
            VolumeMultiple = re.findall(r'\d+', table[3][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*',table[6][1] )[0]) / 100
            PriceTick =re.findall(r'([0-9.]+)[ ]*',table[5][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[15][1], self.ExchangeID)
            MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[7][1])[0]) / 100
            DelivMethod = self.DeliveryMethod[table[14][1]]
            # ur'([一二三四五六七八九零十百千万亿]+|[0-9]+[,]*[0-9]+.[0-9]+)'
            tempEndDate=table[10][1]
            tempEndDelivDate=table[11][1]
            Delivemonth=table[8][1]
        else:
            VolumeMultiple = re.findall(r'\d+', table[2][1])[0]
            LimitUpDown = float(re.findall(r'([0-9.]+)[ ]*', table[5][1])[0]) / 100
            PriceTick = re.findall(r'([0-9.]+)[ ]*',table[4][1])[0]
            InstrumentCode = BasicAPI().GetExchangeProductCode(table[14][1], self.ExchangeID)
            if self.name in ["白糖","棉花"]:
                MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[12][1])[0]) / 100
                tempEndDate = table[8][1]
                tempEndDelivDate=table[9][1]
                Delivemonth = table[6][1]
            else:
                MinMargin = float(re.findall(r'([0-9.]+)[ ]*', table[6][1])[0]) / 100
                tempEndDate = table[9][1]
                tempEndDelivDate=table[10][1]
                Delivemonth = table[7][1]
            DelivMethod = self.DeliveryMethod[table[13][1]]
        EndDate =self.explainEnddate(tempEndDate)
        EndDelivDate = self.explaintempEndDelivDate(tempEndDelivDate)
        Delivemonth=self.explainDelivemonth(Delivemonth)
        return self.name,InstrumentCode,self.ExchangeID,VolumeMultiple,PriceTick,LimitUpDown,EndDate,EndDelivDate,MinMargin,DelivMethod,Delivemonth


if __name__=='__main__':
    sql="INSERT INTO [dbo].[StandContract]([ProductName],[TradeCode],[ExchangeID] ,[VolumeMultiple],[PriceTick] ,[LimitUpDown],[EndDate],[EndDelivDate],[MinMargin],[DelivMethod],[Delivemonth])VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    selectsql="select InstrumentName,ExchangeID,InstrumentCode,[Website]  FROM [PreTrade].[dbo].[ContractCode]  order by ExchangeID"
    existsql="SELECT  [ProductName] FROM [PreTrade].[dbo].[StandContract]"
    updatesql="update  [PreTrade].[dbo].[StandContract] set VolumeMultiple='%s',PriceTick='%s',LimitUpDown='%s',EndDate='%s',EndDelivDate='%s',MinMargin='%s',DelivMethod='%s',Delivemonth='%s' where ProductName='%s'"
    delsql="delete from [PreTrade].[dbo].[StandContract]"
    info=InfoApi()
    templist=info.mysql.ExecQuery(selectsql)
    existlist=info.mysql.ExecQuery(existsql)
    existlist=BasicAPI().GetResultList(existlist)
    tempcol=[]
    for i in templist:
        print i[0].encode("utf-8")
        product=ProductDetail()
        col=product.GetProductDetail(i[3].encode("utf-8"),i[1].encode("utf-8"),i[0].encode("utf-8"),i[2].encode("utf-8"))
        print col
        tempcol.append(col)
    BasicAPI().MyPrint(tempcol)
    if  len(tempcol)==len(templist) and len(tempcol)>55:
        info.mysql.ExecNonQuery(delsql)
        info.mysql.ExecmanysNonQuery(sql,tempcol)