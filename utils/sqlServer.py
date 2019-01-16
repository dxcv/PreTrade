# encoding:utf-8
import pymssql
from time import *
##将sqlserver 封装成mysql的样子

class Mysql:
    def __init__(self,server,user,password,database):
        """server,user,password,database"""
        self.server=str(server).strip()
        self.user=str(user).strip()
        self.password=str(password).strip()
        self.database=str(database).strip()
        self.conn = None
        self.cur = None

    def GetConnect(self):
        """
        得到连接的返回信息
        :返回：conm.cursor()
        """
        if self.conn is None or self.cur is None:
            self.conn=pymssql.connect(host=self.server,user=self.user,password=self.password,database=self.database,charset='utf8')
            self.cur=self.conn.cursor()

    def Disconnect(self):
        if not  self.conn is None:
            self.cur.close()
            self.conn.close()
        self.cur=None
        self.conn=None


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

    def ExecQuery(self,sql):
        """
        执行查询语句，返回的是一个包含tuple的list,list记录行，tuple为每行记录的字段
         ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        self.GetConnect()
        self.cur.execute(sql)
        resList=self.cur.fetchall()
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
        return self.GetResultList(resList)


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

    def ExecmanysNonQuery(self,sql,list1):
        """
        插入sql语句集，一次执行多个sql查询语句
        :param sqllist:
        :return:
        """
        self.GetConnect()
        try:
            for i in list1:
                self.cur.execute(sql%i)
            self.conn.commit()
        except Exception, e:
            print "数据已经存在", e.message
            print "异常",sql,i
            print sql%i

    def ExecmanyNonQuery(self,sql,list1):
        """
        插入sql语句集，一次执行多个sql非查询语句
        :param sqllist:
        :return:
        """
        self.GetConnect()
        self.cur.execute(sql%list1)

        self.conn.commit()



    # def ExecmanysNonQuery(self,sql,list1):
    #     """
    #     插入sql语句集，一次执行多个sql查询语句
    #     :param sqllist:
    #     :return:
    #     """
    #
    #    # print type(list),list
    #     #print list
    #     cur = self.__GetConnect()
    #     #cur.execute(sql,list)
    #     for i in list1:
    #         print sql % i
    #         cur.execute(sql%i)
    #


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