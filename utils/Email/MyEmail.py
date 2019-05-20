# -*- coding: utf-8 -*-
# @Time    : 2019/5/20 9:24
# @Author  : ZouJunLin
"""python自动发送邮件"""
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from email.header import Header
from email.utils import formataddr
import os

class MyEmail():

    def __init__(self,sender,password,receiver,subject,doc,Type):
        self.sender=sender
        self.password=password
        self.receiver=";".join(receiver)
        self.subject=subject
        self.doc=doc
        self.content=None
        self.Type=Type
        self.lam_format_addr = lambda name, addr: formataddr((Header(name, 'utf-8').encode(), addr))


    def Login(self):
        self.msg = MIMEMultipart("mixed")
        self.msg['From'] = self.sender
        self.msg['To'] = self.receiver
        self.msg['Subject'] = self.subject


        if self.Type=='QQ':
            self.smtp = smtplib.SMTP_SSL('smtp.exmail.qq.com',465)

        else:
            self.smtp=smtplib.SMTP('smtp.exmail.qq.com',25)
        self.smtp.set_debuglevel(1)
        self.smtp.login(self.sender,self.password)

    def SetSubject(self,subject):
        """设置邮件标题"""
        self.subject=subject        #腾讯邮箱忽略会导致邮件被屏蔽

    def SetSenderName(self,name):
        """设置发件人姓名"""
        self.sender=name

    def get_attach(self):
        """构造邮件"""
        attach=MIMEMultipart()
        if self.subject is not None:
            attach['Subject']=self.subject
        if self.sender is not None:
            attach['From']=self.sender
        if self.receiver:
            attach['To']=self.receiver
        if self.doc:
            # 估计任何文件都可以用base64，比如rar等
            # 文件名汉字用gbk编码代替
            name = os.path.basename(self.doc).encode("gbk")
            f = open(self.doc, "rb")
            doc = MIMEText(f.read(), "base64", "gb2312")
            doc["Content-Type"] = 'application/octet-stream'
            doc["Content-Disposition"] = 'attachment; filename="' + name + '"'
            attach.attach(doc)
            f.close()
        if self.content:
            content = MIMEText(self.content, 'plain', 'utf-8')
        attach.attach(content)
        return attach



    def SetSendContent(self,content):
        """设置正文内容"""
        # self.msg=MIMEText(text,'plain','utf-8')
        self.content=content

    def Send(self):
        try:

            self.smtp.sendmail("<%s>" % self.sender, self.receiver, self.get_attach().as_string())
            self.smtp.quit()
            print("send email successful")
        except Exception as e:
            print("send email failed %s" % e)


if __name__ == '__main__':
    sender="zjl@jingyoutech.com"
    password="Aa85258584"
    receiver=["1927007992@qq.com",]
    receiverPwd="lvadbzsqpjxpdjhf"
    subject="镍持仓量监控"
    myemail=MyEmail(sender,password=password,receiver=receiver,subject=subject,Type="a",doc=None)
    myemail.Login()
    content="%s合约持仓超过15万张"
    InstrumentID=['ni1907','ni1909']
    day=5
    content="%s合约连续%s天持仓超过15万张，持仓成交详细数据请见附件或者github(邮件自动监控脚本发送，如有bug，请及时提出)。"%(','.join(InstrumentID),day)
    print content
    myemail.SetSendContent(content)

    myemail.Send()