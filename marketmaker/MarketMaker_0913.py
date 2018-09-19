from marketmaker.MarketMakerBasic import MarketMakerBasic,WebSocketBasic,OrderBasic,PriorityQueue
from multiprocessing import Process, Queue, Manager
from concurrent.futures import ThreadPoolExecutor, wait
from marketmaker.Sqlite3 import Sqlite3
import gzip,json
from datetime import datetime
import websocket
import os
import time
# from marketmaker.const import *
from apscheduler.schedulers.blocking import BlockingScheduler
import signal
from marketmaker.UserInfo_Conf import UserName_UserId_dict
from marketmaker import tool
from marketmaker.tool import get_config_parameter_dict


class WebSocket(WebSocketBasic):
    def __init__(self, *args, **kwargs):
        super(WebSocket, self).__init__(*args, **kwargs)

class Order(OrderBasic):
    # def __init__(self,orderQueue, sell_price_order_dict, buy_price_order_dict,
    #      sql3, CONTRACT_ID, userId, dealApi, executor_cancel, DEPTH, QUANTITY, SPREAD):
    #     super(Order, self).__init__(orderQueue, sell_price_order_dict, buy_price_order_dict,
    #      sql3, CONTRACT_ID, userId, dealApi, executor_cancel, DEPTH, QUANTITY, SPREAD)
    def __init__(self,*args, **kwargs):super(Order, self).__init__(*args, **kwargs)

class MarketMaker(MarketMakerBasic):

    def __init__(self,*args, **kwargs):
        # executor_cancel = self.executor_cancel
        # sqlite3_file = kwargs['sqlite3_file']
        # self.sql3 = Sqlite3(sqlite3_file)
        # username = kwargs['username']
        # self.userId = UserName_UserId_dict[username]
        # self.QUANTITY = kwargs['quantity']
        # self.THICK_DEPTH = kwargs['thick_depth']
        super(MarketMaker, self).__init__(*args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    def orderTask_listener(self,pid):
        orderT = self.orderT
        priceQueue = self.priceQueue
        # pwdb = self.pwdb
        print("OrderTask is alive:", orderT.is_alive())
        if orderT.is_alive() == False:
            print(orderT.is_alive())
            os.kill(pid, signal.SIGKILL)
            os.kill(os.getpid(), signal.SIGKILL)
        MAX_QUEUE_SIZE = self.kwargs['max_queue_size']
        if (priceQueue.qsize() > MAX_QUEUE_SIZE):
            print('EXIT!')
            os.kill(pid, signal.SIGKILL)
            os.kill(orderT.pid, signal.SIGKILL)
            os.kill(os.getpid(), signal.SIGKILL)

    def orderTask_listener_schedule(self,pid):
        sched = BlockingScheduler()
        # 'interval', minutes = 2, start_date = '2017-12-13 14:00:01', end_date = '2017-12-13 14:00:10'
        SCHEDULE_TIME = self.kwargs['schedule_time']
        sched.add_job(self.orderTask_listener, 'interval', seconds=SCHEDULE_TIME, start_date='2018-08-13 14:00:56',
                      end_date='2118-12-13 14:00:10',args=[pid])
        sched.start()

    def run(self):
        self.cancel_all_orders()
        print("ALL orders canceled at the very beginning of the program!")
        order_obj = Order(*self.args, **self.kwargs)
        pwdb = Process(target=order_obj.writeOrderIdtoDBTask)
        orderT = Process(target=order_obj.orderTask)

        pwdb.start()
        orderT.start()
        self.executor_cancel.submit(self.orderTask_listener_schedule, pwdb.pid)

        websocket.enableTrace(False)
        websocket.setdefaulttimeout(self.kwargs['websocket_timeout'])

        websock_obj = WebSocket(*self.args, **self.kwargs)
        websock_obj.start_websocket_connect()
        pwdb.join()
        orderT.join()


if __name__ == "__main__":
    # listen_host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    # listenPair_ContractId = '10' #CONTRACT_ID_dict = {'ETH/BTC':'10','BCH/BTC':'11','LTC/BTC':'12'}
    # QUANTITY = 0.001 ## ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
    # sql3_dataFile = "/mnt/data/bitasset/bitasset0906.sqlite"
    #
    # # test004 @ bitasset.com
    # userName = 'test004'
    # userId = UserName_UserId_dict[userName]
    # mongo_obj = Mongo()
    # Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset',mongoTable_name='user')
    # user_obj = Mongo.User(Mongodb_userTable)
    # dealApi = user_obj.get_dealApi(UserName_UserId_dict[userName])

    config_file = 'marketmaker-maker_lj1-ETHBTC.ini'
    config_parameter_dict = get_config_parameter_dict(config_file)

    mkt_mkr = MarketMaker(**config_parameter_dict)
    mkt_mkr.run()
    # mkt_mkr = MarketMaker(listen_host, listenPair_ContractId, userId, sql3_dataFile, dealApi, DEPTH,QUANTITY,SPREAD)
    # mkt_mkr.run()





