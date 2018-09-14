import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from marketmaker.dbOperation.UserInfo_Conf import *
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from apscheduler.schedulers.blocking import BlockingScheduler
from marketmaker.MongoOps import Mongo
import time
from email.header import Header
from email.mime.text import MIMEText
import smtplib

receiver_list = ['zhangyaogong@lingjuninvest.com',
                 # 'yupengzhi@lingjuninvest.com',
                 # 'lizhe@lingjuninvest.com'
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

def shutdown_marketmaker(userName):
    screenName = UserName_ScreenName_dict[userName]
    cmd = 'screen -X -S %s quit'%screenName
    os.system(cmd)
    if userName == 'test005':
        from marketmaker.market_maker_0913_test005 import cancel_all_orders
    elif userName == 'test006':
        from marketmaker.market_maker_0913_test006 import cancel_all_orders
    elif userName == 'test004':
        from marketmaker.market_maker_0913_test004 import cancel_all_orders
    cancel_all_orders()


def check_balance_safety(balance_obj, threthold=0.2):
    minutes_ago = 60*1
    balance_BTC_new_str, balance_BTC_old_str, account_new_datetime, account_old_datetime = balance_obj.diff(minutes_ago)
    balance_BTC_new,balance_BTC_old = float(balance_BTC_new_str),float(balance_BTC_old_str)
    change = 0.
    userName = UserId_UserName_dict[balance_obj.userId]
    if balance_BTC_new>0 and balance_BTC_old>0:
        change_pct = float(balance_BTC_new)/float(balance_BTC_old)-1
        # exceed threthold,close the program
        if abs(change_pct)>threthold:
            shutdown_marketmaker(userName)
            change = round(change_pct*100,4)
            subject = 'program closed!, BTC balance changes %s %% for user:%s'%(str(change),userName)
            content = 'old datetime: %s'%account_old_datetime+\
                      '; old balance %s'%balance_BTC_old_str+'\n'+ \
                      'new datetime: %s' % account_new_datetime + \
                      '; new balance %s' % balance_BTC_new_str + '\n'

            email(subject, receiver_list, content)
            print(subject)
            print(content)
        else:
            print('balance changes %f %% in today so far for user:%s.'%(change,userName))
    else:
        print('something go wrong for user:',balance_obj.userId,' in check_balance()')
        print('balance_BTC_new:',balance_BTC_new,'balance_BTC_old:',balance_BTC_old)

    print('checking balance is over for user:',userName,'the BTC balance changes:%s %%'%str(change))


# def check_balance_concurrence(balance_obj_list):
#     futures = []
#     for i in range(len(balance_obj_list)):
#         balance_obj = balance_obj_list[i]
#         futures.append(executor.submit(check_balance2,balance_obj))
#     wait(futures)

userId_list = [UserName_UserId_dict['test004'],
                   UserName_UserId_dict['test005'],
                   UserName_UserId_dict['test006']
                   ]


if __name__ == "__main__":
    executor = ThreadPoolExecutor(50)
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
    while True:
        for i in range(len(userId_list)):
            userId = userId_list[i]
            dealApi = user_obj.get_dealApi(userId)
            balance_obj = Mongo.Balance(mongodb_balanceTable, userId, dealApi)
            balance_obj_list.append(balance_obj)
            check_balance_safety(balance_obj_list[i])
        print('----------------------------------------------------------')
        time.sleep(5)

