# -*- coding: utf-8 -*-
# @Time    : 2018/8/28 17:53
# @Author  : ZouJunLin
"""
自定义爬虫封装,从网获取到table表格信息获取到最后的写入数据库
"""
import urllib2
import chardet
from bs4 import BeautifulSoup
from time import sleep


class MySplider:
    def __init__(self):
        pass
    def getUrlcontent(self,url,header):
       """
       获取某个url的html内容,并且按照编码格式解码
       :param url: String
       :param header:dict
       :return:
       """
       sleep(1.5)
       try:
           req=urllib2.Request(url,headers=header)
           html=urllib2.urlopen(req,timeout=20).read()
       except Exception,e:
           print "url有误",e.message
           return ""
       webcode=chardet.detect(html)['encoding']
       html = html.decode(webcode).encode("utf-8")
       return html

    def tableTolist(self,content,exchangeID):
        """
        将网页的table里面的数据抽取到list中,并且去掉空格等
        :param content: html
        :return: list
        """
        tablelist=[]
        bs = BeautifulSoup(content, "html.parser")
        divdata = bs.find("table")
        if divdata is None:
            bs = BeautifulSoup(content, "lxml")
            divdata = bs.find("table")
        try:
            trdata = divdata.findAll("tr")
        except:
            print "网络异常，部分数据爬取失败，重新运行"
            return []
        for tr in trdata:
            td=tr.findAll("td")
            col = []
            if len(td)>0:
                for i in td:
                    temp=str(i.text.strip().encode("utf-8")).strip().replace(",", "").replace("绝对值", '0').replace("比例值", '1')
                    if temp=='-':
                        temp=temp.strip("-")
                    col.append(temp)


                col.insert(2,exchangeID)
                tablelist.append(tuple(col))
        return tablelist

    def tableTolistByNum(self,content,exchangeID,num):
        """
        通过第几个num将网页的table里面的数据抽取到list中,并且去掉空格等
        主要是抓取持仓信息时用
        """
        tablelist=[]
        bs = BeautifulSoup(content, "html.parser")
        divdata = bs.findAll("table")[num]
        if divdata is None:
            bs = BeautifulSoup(content, "lxml")
            divdata = bs.find("table")
        try:
            trdata = divdata.findAll("tr")
        except:
            print "网络异常，部分数据爬取失败，重新运行"
            return []
        for tr in trdata:
            td=tr.findAll("td")
            col = []
            a=len(td)
            if len(td)>0:
                for i in td:
                    temp=str(i.text.strip().encode("utf-8")).strip().replace(",", "").replace("绝对值", '0').replace("比例值", '1')
                    if temp=='-':
                        temp=temp.strip("-")
                    col.append(temp)
                # col.insert(2,exchangeID)
            elif len(tr.findAll("th")) > 0:
                td = tr.findAll("th")
                for i in td:
                    temp=str(i.text.strip().encode("utf-8")).strip().replace(",", "").replace("绝对值", '0').replace("比例值", '1')
                    if temp=='-':
                        temp=temp.strip("-")
                    col.append(temp)
            tablelist.append(tuple(col))
        return tablelist

    def tableTolistP(self,content,exchangeID):
        """
        将网页的table里面的数据抽取到list中,与tableTolist不去逗号等内容
        :param content: html
        :return: list
        """
        tablelist=[]
        bs = BeautifulSoup(content, "html.parser")
        divdata = bs.find("table")
        if divdata is None:
            bs = BeautifulSoup(content, "lxml")
            divdata = bs.find("table")
        try:
            trdata = divdata.findAll("tr")
        except:
            print "网络异常，部分数据爬取失败，重新运行"
            return []
        for tr in trdata:
            td=tr.findAll("td")
            col = []
            if len(td)>0:
                for i in td:
                    temp=str(i.text.strip().encode("utf-8")).strip().replace("，","、").replace(",","、")
                    if temp=='-':
                        temp=temp.strip("-")
                    col.append(temp)


                col.insert(2,exchangeID)
                tablelist.append(tuple(col))
        return tablelist

    def dataToSqlserver(self, datalist,sql,info):
        """
        将获取的list写入数据库
        :param datalist:
        :param sql:
        :return:
        """
        info.mysql.ExecmanysNonQuery(sql, datalist)


    def deleteTableData(self, sql,info):
        """
        清空结算数据
        :param sql:
        :return:
        """
        info.mysql.ExecNonQuery(sql)

    def updataData(self,sql,info):
        """
        更新数据库信息
        :param sql:
        :return:
        """
        ##删除部分没有更新的数据
        info.mysql.ExecNonQuery(sql)

def IsExistData(sql,info):
    """
    查询数据是否存在
    :param sql:
    :param info:
    :return:
    """
    list=info.mysql.ExecQuery(sql)
    return list



