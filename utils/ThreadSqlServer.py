# -*- coding: utf-8 -*-
# @Time    : 2019/3/1 16:47
# @Author  : ZouJunLin
"""sqlServer 数据库连接池"""
import pymssql
from DBUtils.PooledDB import PooledDB

class SqlServer:
    def __init__(self,server,user,password,database):
        """server,user,password,database"""
        self.server=str(server).strip().split(":")[0]
        self.port=str(server).strip().split(":")[1]
        self.user=str(user).strip()
        self.password=str(password).strip()
        self.database=str(database).strip()
        self.conn = None
        self.cur = None
        self.limit_count = 4
        """
        1. mincached，最少的空闲连接数，如果空闲连接数小于这个数，pool会创建一个新的连接
        2. maxcached，最大的空闲连接数，如果空闲连接数大于这个数，pool会关闭空闲连接
        3. maxconnections，最大的连接数，
        """
        self.pool = PooledDB(creator=pymssql, mincached=2, maxcached=5,maxconnections=10, blocking=True,
                        host=self.server, port=self.port, user=self.user, password=self.password, database=self.database, charset="utf8")

    def GetConnect(self):
        """
        得到连接的返回信息
        返回：conm.cursor()
        """
        self.conn=self.pool.connection()
        self.cur = self.conn.cursor()

    def Disconnect(self):
        self.cur.close()
        self.conn.close()

    def deleteTable(self,tableName):
        """
        删除表
        :param tableName:
        :return:
        """
        self.GetConnect()
        try:
            self.cur.execute("delete  from %s" % tableName)
        except Exception,e:
            print e.message
        self.conn.commit()
        self.Disconnect()

    def ExecQuery(self, sql):
        """
        执行查询语句，返回的是一个包含tuple的list,list记录行，tuple为每行记录的字段
         ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        self.GetConnect()
        self.cur.execute(sql)
        resList = self.cur.fetchall()
        self.Disconnect()
        return resList

    def ExecQueryGetList(self,sql):
        """
         执行查询语句，返回的是一个list，仅适用查询一个字段
        :param sql:
        :return:
        """
        self.GetConnect()
        self.cur.execute(sql)
        resList = self.cur.fetchall()
        self.Disconnect()
        return self.GetResultList(resList)

    def  ExecQueryGetDict(self,sql):
        """
        执行查询语句，返回一个dict，仅适用两个字段的查询
        :param sql:
        :return:
        """
        self.GetConnect()
        self.cur.execute(sql)
        resList = self.cur.fetchall()
        self.Disconnect()
        return self.GetResultDict(resList)



    def ExecNonQuery(self,sql):
        """
        执行非查询语句
        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        self.GetConnect()
        self.cur.execute(sql)
        self.conn.commit()
        self.Disconnect()

    def ExecmanysNonQuery(self,sql,list1):
        """
        插入sql语句集，一次执行多个sql查询语句
        :param sqllist:
        :return:
        """
        self.GetConnect()
        try:
            for i in list1:
                i=tuple(i)
                self.cur.execute(sql%i)
            self.conn.commit()
        except Exception, e:
            print "数据已经存在", e.message
            print "异常",sql,i
            print sql%i
        self.Disconnect()

    def ExecmanyNonQuery(self,sql,list1):
        """
        插入sql语句集，一次执行多个sql非查询语句
        :param sqllist:
        :return:
        """
        self.GetConnect()
        self.cur.execute(sql%list1)
        self.conn.commit()
        self.Disconnect()

    def UpdateMarginExample(self, templist,num):
        """update  MarginExample 数据表"""
        self.GetConnect()
        if len(templist)==0:
            return
        # delsql="delete from [PreTrade].[dbo].[MarginExample] where InstrumentId ='%s'"
        isExist="select InstrumentID from [PreTrade].[dbo].[MarginExample] where [TradingDay]='%s'"
        insertsql="INSERT INTO [dbo].[MarginExample] ([TradingDay],[InstrumentID],[ExchangeID],Margintation"+str(num).strip()+") VALUES('%s','%s','%s','%s')"
        updatesql="update MarginExample set Margintation"+str(num).strip()+"='%s' where InstrumentId ='%s'and  TradingDay='%s'"
        isExist=self.ExecQueryGetList(isExist%templist[0][0])
        self.GetConnect()
        for i in templist:
            if i[1] in isExist:
                self.cur.execute(updatesql%(i[3],i[1],i[0]))
            else:
                self.cur.execute(insertsql%(tuple(i)))
        self.conn.commit()
        self.Disconnect()


    def GetResultList(self,result):
        """
        将数据库查询结果转换成list
        :param result:
        :return:
        """
        temp=[]
        for i in result:
            temp.append(str(i[0].encode("utf-8")).strip())
        return temp

    def GetResultDict(self,result):
        """
        将查询的结果转换为dict字典
        :param result:
        :return:
        """
        temp=dict()
        if len(result):
            for i in result:
                try:
                    data=float(i[1])
                    temp[str(i[0].encode("utf-8"))] = i[1]
                except:
                    temp[str(i[0].encode("utf-8"))] = str(i[1].encode("utf-8"))
        return temp


