from marketmaker.MarketMakerBasic import MarketMakerBasic,WebSocketBasic,OrderBasic,PriorityQueueBuy,PriorityQueueSell
from multiprocessing import Process, Queue, Manager
from concurrent.futures import ThreadPoolExecutor, wait
from marketmaker.dbOperation.Sqlite3 import Sqlite3
import gzip,json
from datetime import datetime
import websocket
import os
import time
from marketmaker.const import *
from apscheduler.schedulers.blocking import BlockingScheduler
import signal
from marketmaker.MongoOps import Mongo
from marketmaker.dbOperation.UserInfo_Conf import UserName_UserId_dict
from marketmaker.dbOperation import tool


class WebSocket(WebSocketBasic):
    def __init__(self, priceQueue, host, CONTRACT_ID, executor_cancel):
        super(WebSocket, self).__init__(host,priceQueue,CONTRACT_ID)
        self.executor_cancel = executor_cancel


    def on_message(self, ws, message):
        data = gzip.decompress(message)  # data decompress
        data_dict = json.loads(data.decode())
        # print(data_dict)
        ws.send('{ping:' + str(time.time()) + '}')
        if 'ping' in data_dict:
            ws.send('{pong:' + str(time.time()) + '}')
            print('pong:')
        elif 'subbed' in data_dict:
            print('receive subbed status message:')
            print(data_dict)

        if 'tick' in data_dict:
            # print(data_dict['tick'])
            bid = (data_dict['tick']['bids'][0][0])
            ask = (data_dict['tick']['asks'][0][0])
            print('new coming ask:%f, bit:%f'%(ask, bid))
            price = (ask, bid)
            if ask != self.last_ask or bid != self.last_bid:
                print('(ask,bid),priceQueue.qsize():',price, self.priceQueue.qsize())
                self.priceQueue.put(price)
            self.last_ask = ask
            self.last_bid = bid

    def on_open(self,ws):
        # subscribe okcoin.com spot ticker
        CONTRACT_ID = self.CONTRACT_ID
        print("ON Open:")
        if CONTRACT_ID == '10':
            ws.send('{"sub": "market.ethbtc.depth.step0","id": "id10"}')
        if CONTRACT_ID == '11':
            ws.send('{"sub": "market.bchbtc.depth.step0","id": "id10"}')
        if CONTRACT_ID == '12':
            ws.send('{"sub": "market.ltcbtc.depth.step0","id": "id10"}')
        print('---------------- on_open complete ---------------')
            # self.send('{"sub": "market.tickers","id": "id10"}')

class Order(OrderBasic):
    def __init__(self,orderQueue, sell_price_order_dict, buy_price_order_dict,
         sql3, CONTRACT_ID, userId, dealApi, executor_cancel, DEPTH, QUANTITY, SPREAD):
        super(Order, self).__init__(orderQueue, sell_price_order_dict, buy_price_order_dict,
         sql3, CONTRACT_ID, userId, dealApi, executor_cancel, DEPTH, QUANTITY, SPREAD)

    def self_trade(self,bid, ask, quantity):
        ts = time.time() * 1000
        ts_minute = int(ts / 1000 / 60) * 1000 * 60

        price = round((bid + ask) / 2., 6)

        self.latest_price = price
        if self.last_integer_minute != ts_minute:
            self.open, self.high, self.low, self.close = price, price, price, price
            self.trade_helper(price, quantity)
            self.last_integer_minute = ts_minute
        else:  # last_integer_minute==ts_minute
            if ts - ts_minute > 50 * 1000:
                self.close = price
                self.trade_helper(price, quantity)
            if (price > self.high):
                self.high, self.close = price, price
                self.trade_helper(price, quantity)
            elif (price < self.low):
                self.low, self.close = price, price
                self.trade_helper(price, quantity)

class MarketMaker(MarketMakerBasic):

    orderQueue = Queue(100)
    priceQueue = Queue()

    buy_price_order_dict = Manager().dict()
    sell_price_order_dict = Manager().dict()
    executor_cancel = ThreadPoolExecutor(100)

    def __init__(self,host,CONTRACT_ID, userId,sql3_dataFile, dealApi,DEPTH,QUANTITY,SPREAD):
        executor_cancel = self.executor_cancel
        self.sql3 = Sqlite3(dataFile=sql3_dataFile)
        self.userId = userId
        self.QUANTITY = QUANTITY
        super(MarketMaker, self).__init__(CONTRACT_ID,dealApi,executor_cancel)

        self.order_obj = Order(self.orderQueue, self.sell_price_order_dict, self.buy_price_order_dict,
                                    self.sql3, CONTRACT_ID, userId,dealApi, executor_cancel,DEPTH,QUANTITY,SPREAD)

        self.cancel_all_orders()

        self.order_obj.sell_q = PriorityQueueSell(self.orderQueue,self.sell_price_order_dict,dealApi,executor_cancel,DEPTH,CONTRACT_ID)
        self.order_obj.buy_q = PriorityQueueBuy(self.orderQueue,self.buy_price_order_dict,dealApi,executor_cancel,DEPTH,CONTRACT_ID)

        # priceQueue, host, CONTRACT_ID
        self.websock_obj = WebSocket(self.priceQueue,host,CONTRACT_ID,executor_cancel)

    def writeOrderIdtoDBTask(self):
        orderQueue = self.orderQueue
        local_datetime = tool.get_local_datetime()
        while True:
            orderList = []
            orderList.append(orderQueue.get())
            self.sql3.insert(self.userId,orderList)
    def orderTask(self):
        priceQueue = self.priceQueue
        order_obj = self.order_obj
        executor = ThreadPoolExecutor(100)
        while True:
            # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Order Task queue size:", priceQueue.qsize())
            while priceQueue.qsize() > 1:
                priceQueue.get()
            futures = []
            price = priceQueue.get()
            ask,bid = price[0],price[1]
            print('Before Cancel!', price)
            order_obj.cancel_old_orders(ask, bid)
            print('After Cancel!', price)
            futures.append(executor.submit(order_obj.self_trade(bid, ask, self.QUANTITY)))
            futures.append(executor.submit(order_obj.adjust_sell_orders, ask))
            futures.append(executor.submit(order_obj.adjust_buy_orders, bid))
            # adjust buy orders
            wait(futures)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Process end!', price)

    def orderTask_listener(self):
        orderT = self.orderT
        priceQueue = self.priceQueue
        print("XXXXXX:orderTask is alive:", orderT.is_alive())
        if orderT.is_alive() == False:
            print(orderT.is_alive())
            os.kill(os.getpid(), signal.SIGKILL)
            # os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)
        if (priceQueue.qsize() > MAX_QUEUE_SIZE):
            print('EXIT!')
            os.kill(os.getpid(), signal.SIGKILL)
            # os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)

    def oderTask_listener_schedule(self):
        sched = BlockingScheduler()
        # 'interval', minutes = 2, start_date = '2017-12-13 14:00:01', end_date = '2017-12-13 14:00:10'
        sched.add_job(self.orderTask_listener, 'interval', seconds=SCHEDULE_TIME, start_date='2018-08-13 14:00:56',
                      end_date='2118-12-13 14:00:10')
        sched.start()

    def run(self):

        print("ALL orders canceled at the very beginning of the program!")
        pwdb = Process(target=self.writeOrderIdtoDBTask)
        orderT = Process(target=self.orderTask)
        self.orderT = orderT
        pwdb.start()
        orderT.start()

        websocket.enableTrace(False)
        websocket.setdefaulttimeout(WEBSOCKET_TIMEOUT)
        websock_obj = self.websock_obj
        self.executor_cancel.submit(self.oderTask_listener_schedule)
        websock_obj.start_websocket_connect()
        pwdb.join()
        orderT.join()



if __name__ == "__main__":
    listen_host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    listenPair_ContractId = '10' #CONTRACT_ID_dict = {'ETH/BTC':'10','BCH/BTC':'11','LTC/BTC':'12'}
    QUANTITY = 0.001 ## ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
    sql3_dataFile = "/mnt/data/bitasset/bitasset0906.sqlite"

    # test004 @ bitasset.com
    userName = 'test004'
    userId = UserName_UserId_dict[userName]
    mongo_obj = Mongo()
    Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset',mongoTable_name='user')
    user_obj = Mongo.User(Mongodb_userTable)
    dealApi = user_obj.get_dealApi(UserName_UserId_dict[userName])

    DEPTH = 15
    SPREAD = 0.1

    mkt_mkr = MarketMaker(listen_host, listenPair_ContractId, userId, sql3_dataFile, dealApi, DEPTH,QUANTITY,SPREAD)
    mkt_mkr.run()





