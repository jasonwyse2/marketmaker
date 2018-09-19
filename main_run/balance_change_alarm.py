import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from marketmaker.UserInfo_Conf import UserName_UserId_dict, UserId_UserName_dict
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from marketmaker.MongoOps import Mongo
from marketmaker.tool import get_local_datetime
from email.header import Header
from email.mime.text import MIMEText
import smtplib

receiver_list = ['zhangyaogong@lingjuninvest.com',
                 'yupengzhi@lingjuninvest.com',
                 'lizhe@lingjuninvest.com'
                 ]

def email(subject, receiver, content):

    sender = 'zhangyaogong@lingjuninvest.com'#'auto@lingjuninvest.com'
    smtpserver = 'smtp.exmail.qq.com'
    username = 'zhangyaogong@lingjuninvest.com'#''auto@lingjuninvest.com'
    password = 'Qti78Jai9ntNenos' #'Auto123!'
    msg = MIMEText(content,_subtype='plain',_charset='gb2312')
    msg['Subject'] = Header(subject, charset='UTF-8')
    msg['From'] = sender
    msg['To'] = ";".join(receiver)

    smtp = smtplib.SMTP_SSL(smtpserver,465)
    smtp.login(username, password)
    try:
        smtp.sendmail(sender, receiver, msg.as_string())
        smtp.quit()
        print("通知邮件发送成功")
    except:
        print("邮件服务器异常，发送失败")

userId_list = [UserName_UserId_dict['maker_lj1'],#maker_lj1
                   UserName_UserId_dict['maker_lj2'],
                   UserName_UserId_dict['maker_lj3']
                   ]
balance_BTC_new_list = [0.]*len(userId_list)
balance_BTC_old_list = [0.]*len(userId_list)
account_old_datetime_list = ['']*len(userId_list)
account_new_datetime_list = ['']*len(userId_list)

def balance_change_alarm(balance_obj_list, idx):
    balance_obj = balance_obj_list[idx]
    docs = balance_obj.find(record_num=1)
    for doc in docs:
        balancdInfo_new_dict = doc
        break
    account_new_list = balancdInfo_new_dict['account']
    account_new_datetime_list[idx] = balancdInfo_new_dict['datetime']
    for i in range(len(account_new_list)):
        account_new = account_new_list[i]
        if (account_new['currency'] == 'BTC'):
            new_balance_BTC_str = account_new['balance']
            balance_BTC_new_list[idx] = float(new_balance_BTC_str)
            break
    userName = UserId_UserName_dict[balance_obj.userId]
    if balance_BTC_old_list[idx] != 0. and balance_BTC_new_list[idx] != balance_BTC_old_list[idx]:

        subject = 'Real Trade, BTC balance changes for user:%s' % (userName)
        content = 'old datetime: %s' % account_old_datetime_list[idx] + \
                  '; old balance: %f' % balance_BTC_old_list[idx] + '\n' + \
                  'new datetime: %s' % account_new_datetime_list[idx] + \
                  '; new balance: %f' % balance_BTC_new_list[idx] + '\n'
        email(subject, receiver_list, content)
        print(subject)
        print(content)
    else:
        local_datetime = get_local_datetime()
        print('no trade for user:',userName, local_datetime)

    balance_BTC_old_list[idx] = balance_BTC_new_list[idx]
    account_old_datetime_list[idx] = account_new_datetime_list[idx]

def checkAllAcount():
    for i in range(len(userId_list)):
        userId = userId_list[i]
        dealApi = user_obj.get_dealApi(userId)
        balance_obj = Mongo.Balance(mongodb_balanceTable, userId, dealApi)
        balance_obj_list.append(balance_obj)
        balance_change_alarm(balance_obj_list, i)
    print()


if __name__ == "__main__":
    executor = ThreadPoolExecutor(10)
    mongodb_name = 'bitasset'
    mongodb_orderTable_name = 'order'
    mongodb_balanceTable_name = 'balance'
    mongodb_userTable_name = 'user'
    mongodb_exchangeTable_name = 'exchange'

    balance_obj_list = []
    mongo_obj = Mongo()
    mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name, mongodb_userTable_name)
    mongodb_balanceTable = mongo_obj.get_mongodb_table(mongodb_name, mongodb_balanceTable_name)
    mongodb_exchangeTable = mongo_obj.get_mongodb_table(mongodb_name, mongodb_exchangeTable_name)

    sched = BlockingScheduler()  # timer

    # ----------------- update account balance  --------------------------
    user_obj = Mongo.User(mongodb_userTable)

    sched.add_job(checkAllAcount, 'interval', seconds=5, start_date='2018-08-13 14:00:00',
                  end_date='2118-12-13 14:00:10')
    # ----------------- save order detail  --------------------------
    executor.submit(sched.start)



