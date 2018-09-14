import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from marketmaker.dbOperation.Sqlite3 import Sqlite3
from concurrent.futures import ThreadPoolExecutor
from marketmaker.dbOperation.UserInfo_Conf import UserName_UserId_dict, UserId_UserName_dict,StatusName_StatusCode_dict
from apscheduler.schedulers.blocking import BlockingScheduler
from marketmaker.MongoOps import Mongo
from marketmaker.order_helper import saveOrder


if __name__ == "__main__":
    executor = ThreadPoolExecutor(20)

    mongodb_name = 'bitasset'
    mongodb_orderTable_name = 'order'
    mongodb_balanceTable_name = 'balance'
    mongodb_userTable_name = 'user'
    mongodb_exchangeTable_name = 'exchange'
    sql3_datafile= '/mnt/data/bitasset/bitasset0906.sqlite'
    sql3_obj = Sqlite3(dataFile=sql3_datafile)

    userId_list = [UserName_UserId_dict['test004'],
                   UserName_UserId_dict['test005'],
                   UserName_UserId_dict['test006']
                   ]
    balanceOps_obj_list = []
    mongo_obj = Mongo()
    mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name, mongodb_userTable_name)
    mongodb_balanceTable = mongo_obj.get_mongodb_table(mongodb_name, mongodb_balanceTable_name)
    mongodb_exchangeTable = mongo_obj.get_mongodb_table(mongodb_name,mongodb_exchangeTable_name)

    sched = BlockingScheduler() #timer

    # ----------------- update account balance  --------------------------
    user_obj = Mongo.User(mongodb_userTable)
    for i in range(len(userId_list)):
            userId = userId_list[i]
            dealApi = user_obj.get_dealApi(userId)
            balanceOps_obj = Mongo.Balance(mongodb_balanceTable, userId,dealApi)
            balanceOps_obj_list.append(balanceOps_obj)
    # update 'balance' for each userId account (userId is in userId_list),
    for i in range(len(userId_list)):
        sched.add_job(balanceOps_obj_list[i].update, 'interval', seconds=5, start_date='2018-08-13 14:00:00',
                  end_date='2122-12-13 14:00:10')
    # ----------------- update account balance  --------------------------

    # ----------------- save order detail  --------------------------
    sched.add_job(saveOrder, 'interval', seconds=13, start_date='2018-08-13 14:00:07',
                  end_date='2118-12-13 14:00:10', args=[userId_list,sql3_obj])
    # ----------------- save order detail  --------------------------
    executor.submit(sched.start)
