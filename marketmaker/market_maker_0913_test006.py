import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import websocket
import time
import sys
import json
import hashlib
import zlib
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import heapq
import gzip
import random
from marketmaker.BitAssetAPI import BitAssetMarketAPI, BitAssetDealsAPI
from multiprocessing import Process, Queue, Manager
from marketmaker.dbOperation.Sqlite3 import Sqlite3
from apscheduler.schedulers.blocking import BlockingScheduler
from websocket import WebSocketException,WebSocketConnectionClosedException,WebSocketTimeoutException
from marketmaker.dbOperation.UserInfo_Conf import UserName_UserId_dict
import signal
import datetime
import sys
import os

'''
Paras Need to Change!
'''
SQLITE3_FILE = "/mnt/data/bitasset/bitasset0906.sqlite"
DEPTH = 5
THICK_DEPTH = 15
SPREAD = 0.1
DEPTH_SPREAD = 5
QUANTITY = 0.001  # ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
AMOUNT_DECIMAL = 3  # ETH/BTC:3 BCH/BTC:3  LTC/BTC:2
PRICE_DECIMAL = 6  # ETH/BTC:6 BCH/BTC:6  LTC/BTC:6
CONTRACT_ID = '10'  # ETH/BTC:10 BCH/BTC:11  LTC/BTC:12
MAX_QUEUE_SIZE = 50
SCHEDULE_TIME = 20
WEBSOCKET_TIMEOUT = 10

###username:maker_lj3@bitasset.com
###APIKEY = 'ak7aae03c570844966'
###SECRETKEY = 'f8890fee0b3a451da1f58dcd02cabcc3'
# username:maker_lj1@bitasset.com
# test005: 'ak4703b894b34d4873','SECRETKEY': 'c8116d48f0c941efae4dd19b4bd95f20'
# test006: 'aka7d579788d384c60','SECRETKEY': '8f7aae7545574dc1846c0a10d76dc0bc'
APIKEY = 'aka7d579788d384c60'
SECRETKEY = '8f7aae7545574dc1846c0a10d76dc0bc'
USERID=UserName_UserId_dict['test006']
###username:1381110955
###APIKEY = 'ak7b68c889d04045e5'
###SECRETKEY = 'b44f7c6a6ef44d9c84d7b4da2e02c8a1'
###RESTURL = 'http://api.bitasset.com'
RESTURL = 'http://tapi.bitasset.cc:7005/'
# APIKEY='ak178f82404a714188'
# SECRETKEY='1ce9a832919d403a839ff4293cea10d8'
# RESTURL = 'http://tapi.bitasset.cc:7005/'

dealApi = BitAssetDealsAPI(RESTURL, APIKEY, SECRETKEY)
executor_cacel = ThreadPoolExecutor(100)
sql3 = Sqlite3(dataFile=SQLITE3_FILE)


def writeOrderIdtoDBTask():
    while True:
        orderId = orderQueue.get()
        sql3.insert(USERID, orderId)


def orderTask_listener():
    print("OrderTask is alive:", orderT.is_alive())
    if orderT.is_alive() == False:
        print(orderT.is_alive())
        os.kill(pwdb.pid, signal.SIGKILL)
        os.kill(os.getpid(), signal.SIGKILL)
    if (priceQueue.qsize() > MAX_QUEUE_SIZE):
        print('EXIT!')
        os.kill(pwdb.pid, signal.SIGKILL)
        os.kill(orderT.pid, signal.SIGKILL)
        os.kill(os.getpid(), signal.SIGKILL)


def oderTask_listener_schedule():
    sched = BlockingScheduler()
    # 'interval', minutes = 2, start_date = '2017-12-13 14:00:01', end_date = '2017-12-13 14:00:10'
    sched.add_job(orderTask_listener, 'interval', seconds=SCHEDULE_TIME, start_date='2018-08-13 14:00:56',
                  end_date='2022-12-13 14:00:10')
    sched.start()


def trade_helper(price, quantity):
    quantity = round(quantity * random.randint(50, 150), AMOUNT_DECIMAL)
    result1 = dealApi.trade(CONTRACT_ID, side['buy'], price, quantity, '1')
    try:
        dict_data = json.loads(result1)
    except ValueError:
        print('JSON ERROR:', result1)
        return
    orderid1 = ''
    orderid2 = ''
    if dict_data['code'] == 0:
        orderid1 = dict_data['msg']
        orderQueue.put(orderid1)
        result2 = dealApi.trade(CONTRACT_ID, side['sell'], price, quantity, '1')
        try:
            dict_data = json.loads(result2)
        except ValueError:
            dealApi.cancel(orderid1, CONTRACT_ID)
            print('JSON ERROR', result2)
            return
        if dict_data['code'] == 0:
            orderid2 = dict_data['msg']
            orderQueue.put(orderid2)
        else:
            dealApi.cancel(orderid1, CONTRACT_ID)
            print('Trade Error2:', result2, price, quantity)
    else:
        print('Trade Error1:', result1, price, quantity)
    if orderid1 != '':
        dealApi.cancel(orderid1, CONTRACT_ID)
    if orderid2 != '':
        dealApi.cancel(orderid2, CONTRACT_ID)


def on_open(self):
    # subscribe okcoin.com spot ticker
    print("ON Open:")
    if CONTRACT_ID == '10':
        self.send('{"sub": "market.ethbtc.depth.step0","id": "id10"}')
    if CONTRACT_ID == '11':
        self.send('{"sub": "market.bchbtc.depth.step0","id": "id10"}')
    if CONTRACT_ID == '12':
        self.send('{"sub": "market.ltcbtc.depth.step0","id": "id10"}')
        # self.send('{"sub": "market.tickers","id": "id10"}')
    executor_cacel.submit(oderTask_listener_schedule)


def cancel_all_orders():
    futures = []
    dict_orders = {}
    all_orders = dealApi.get_all_orders(CONTRACT_ID)
    try:
        dict_orders = (all_orders)
    except ValueError:
        print('Get All Orders Error:', all_orders)
        return
    if dict_orders['code'] == 0:
        for order in dict_orders['data']:
            futures.append(executor_cacel.submit(dealApi.cancel, order['orderId'], CONTRACT_ID))
    print(wait(futures))


class PriorityQueueSell:
    def __init__(self, depth, isThick=False):
        self._queue = []
        self._index = 0
        self._depth = depth
        self._isThick = isThick

    def push(self, item, priority):
        heapq.heappush(self._queue, (priority, self._index, item))
        self._index += 1
        orderQueue.put(item)
        print("PUSH sell:", self._depth, len(self._queue))
        while len(self._queue) > self._depth:
            tobeCanceledOrderId = self._queue[-1][2]
            executor_cacel.submit(dealApi.cancel, tobeCanceledOrderId, CONTRACT_ID)
            # print(dealApi.cancel(tobeCanceledOrderId, CONTRACT_ID))
            tobeCanceledOrderPrice = self._queue[-1][0]
            if self._isThick == False:
                sell_price_order_dict.pop(tobeCanceledOrderPrice)
            else:
                depth_sell_price_order_dict.pop(tobeCanceledOrderPrice)
            self._queue.pop()

    def top(self):
        return self._queue[0][0]

    def pop(self):
        return heapq.heappop(self._queue)[-1]

    def empty(self):
        if self._queue:
            return False
        else:
            return True


class PriorityQueueBuy:
    def __init__(self, depth, isThick=False):
        self._queue = []
        self._index = 0
        self._depth = depth
        self._isThick = isThick

    def push(self, item, priority):
        heapq.heappush(self._queue, (-priority, self._index, item))
        self._index += 1
        orderQueue.put(item)
        print("PUSH buy:", self._depth, len(self._queue))
        while len(self._queue) > self._depth:
            tobeCanceledOrderId = self._queue[-1][2]
            tobeCanceledOrderPrice = -self._queue[-1][0]
            if self._isThick == False:
                buy_price_order_dict.pop(tobeCanceledOrderPrice)
            else:
                depth_buy_price_order_dict.pop(tobeCanceledOrderPrice)
            executor_cacel.submit(dealApi.cancel, tobeCanceledOrderId, CONTRACT_ID)
            # print(dealApi.cancel(tobeCanceledOrderId, CONTRACT_ID))
            self._queue.pop()

    def top(self):
        return -self._queue[0][0]

    def pop(self):
        return heapq.heappop(self._queue)[-1]

    def empty(self):
        if self._queue:
            return False
        else:
            return True


def add_order(contractId, side, price, quantity, orderType):
    print('ADD:', price)
    quantity = round(quantity * random.randint(1, 10), AMOUNT_DECIMAL)
    result = dealApi.trade(contractId, side, price, quantity, orderType)
    try:
        dict_data = json.loads(result)
    except:
        print('Trade error:', result)
        return
    orderId = dict_data['msg']
    errorCode = dict_data['code']
    if errorCode == 0:
        if side == '-1':
            sell_price_order_dict[price] = orderId
            sell_q.push(orderId, price)
        elif side == '1':
            buy_price_order_dict[price] = orderId
            buy_q.push(orderId, price)
        else:
            print("ERROR side:" + side)


def add_depth_order(contractId, side, price, quantity, orderType):
    print('DEPTH ADD:', price)
    quantity = round(5 * quantity * random.randint(1, 10), AMOUNT_DECIMAL)
    result = dealApi.trade(contractId, side, price, quantity, orderType)
    try:
        dict_data = json.loads(result)
    except:
        print('Trade error:', result)
        return
    orderId = dict_data['msg']
    errorCode = dict_data['code']
    if errorCode == 0:
        if side == '-1':
            depth_sell_price_order_dict[price] = orderId
            depth_sell_q.push(orderId, price)
        elif side == '1':
            depth_buy_price_order_dict[price] = orderId
            depth_buy_q.push(orderId, price)
        else:
            print("ERROR side:" + side)


def cancel_old_orders(ask, bid):
    futures = []
    while (sell_q.empty() == False and ask * (
            1 + SPREAD / 100) > sell_q.top()):  # or len(sell_price_order_dict) > DEPTH:
        sell_price_order_dict.pop(sell_q.top())
        oldorderId = sell_q.pop()
        # result = dealApi.cancel(oldorderId, CONTRACT_ID)
        futures.append(executor_cacel.submit(dealApi.cancel, oldorderId, CONTRACT_ID))
    while (buy_q.empty() == False and bid * (1 - SPREAD / 100) < buy_q.top()):  # or len(buy_price_order_dict) > DEPTH:
        buy_price_order_dict.pop(buy_q.top())
        oldorderId = buy_q.pop()
        # result = dealApi.cancel(oldorderId, CONTRACT_ID)
        futures.append(executor_cacel.submit(dealApi.cancel, oldorderId, CONTRACT_ID))
    if depth_sell_q.empty() == False and ask * (1 + DEPTH_SPREAD / 200) > depth_sell_q.top():
        while (depth_sell_q.empty() == False and ask * (
                1 + DEPTH_SPREAD / 100) > depth_sell_q.top()):  # or len(sell_price_order_dict) > DEPTH:
            depth_sell_price_order_dict.pop(depth_sell_q.top())
            oldorderId = depth_sell_q.pop()
            futures.append(executor_cacel.submit(dealApi.cancel, oldorderId, CONTRACT_ID))
    if depth_buy_q.empty() == False and bid * (1 - DEPTH_SPREAD / 200) < depth_buy_q.top():
        while (depth_buy_q.empty() == False and bid * (
                1 - DEPTH_SPREAD / 100) < depth_buy_q.top()):  # or len(buy_price_order_dict) > DEPTH:
            depth_buy_price_order_dict.pop(depth_buy_q.top())
            oldorderId = depth_buy_q.pop()
            futures.append(executor_cacel.submit(dealApi.cancel, oldorderId, CONTRACT_ID))
    wait(futures)


def adjust_sell_orders(ask):
    ask_price = round(ask * (1 + SPREAD / 100), PRICE_DECIMAL)
    print(ask_price)
    if sell_q.empty() == False:
        if ask_price == sell_q.top():
            print("ASK ENQUL:", ask_price)
            pass
        elif ask_price < sell_q.top():
            add_order(CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
        else:
            while sell_q.empty() == False and ask_price > sell_q.top():
                print(">>>> CANCEL:", sell_q.top())
                sell_price_order_dict.pop(sell_q.top())
                oldorderId = sell_q.pop()
                result = dealApi.cancel(oldorderId, CONTRACT_ID)
                # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
            if ask_price > sell_q.top():
                add_order(CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
    else:
        add_order(CONTRACT_ID, '-1', ask_price, QUANTITY, '1')


def adjust_buy_orders(bid):
    bid_price = round(bid * (1 - SPREAD / 100), PRICE_DECIMAL)
    if buy_q.empty() == False:
        if bid_price == buy_q.top():
            print("BID ENQUL:", bid_price)
            pass
        elif bid_price > buy_q.top():
            add_order(CONTRACT_ID, '1', bid_price, QUANTITY, '1')
        else:
            while buy_q.empty() == False and bid_price < buy_q.top():
                print(buy_price_order_dict)
                buy_price_order_dict.pop(buy_q.top())
                oldorderId = buy_q.pop()
                result = dealApi.cancel(oldorderId, CONTRACT_ID)
                # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
                print(">>>> CANCEL:" + result)
            if bid_price > buy_q.top():
                add_order(CONTRACT_ID, '1', bid_price, QUANTITY, '1')
    else:
        add_order(CONTRACT_ID, '1', bid_price, QUANTITY, '1')


ts_now = time.time() * 1000  # default is seconds, not milliseconds
ts_now_m = int(ts_now / 1000 / 60) * 1000 * 60
last_integer_minute = int(ts_now_m)
open, high, low, close = 0.0, 0.0, 0.0, 0.0
side = {'buy': '1', 'sell': '-1'}


def self_trade(bid, ask, quantity):
    ts = time.time() * 1000
    ts_minute = int(ts / 1000 / 60) * 1000 * 60
    global last_integer_minute
    global open, high, low, close
    price = round((bid * (1 - SPREAD / 100) + ask * (1 + SPREAD / 100)) / 2., PRICE_DECIMAL)
    global latest_price
    latest_price = price
    if last_integer_minute != ts_minute:
        open, high, low, close = price, price, price, price
        trade_helper(price, quantity)
        last_integer_minute = ts_minute
    else:  # last_integer_minute==ts_minute
        if ts - ts_minute > 50 * 1000:
            close = price
            trade_helper(price, quantity)
        if (price > high):
            high, close = price, price
            trade_helper(price, quantity)
        elif (price < low):
            low, close = price, price
            trade_helper(price, quantity)


def adjust_depth_sell_orders(ask):
    if depth_sell_q.empty() == False and ask * (1 + DEPTH_SPREAD / 200) <= depth_sell_q.top():
        return;
    ask_price = round(ask * (1 + DEPTH_SPREAD / 100), PRICE_DECIMAL)
    if depth_sell_q.empty() == False:
        if ask_price == depth_sell_q.top():
            print("ASK ENQUL:", ask_price)
            pass
        elif ask_price < depth_sell_q.top():
            add_depth_order(CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
        else:
            while depth_sell_q.empty() == False and ask_price > depth_sell_q.top():
                print(">>>>DEPTH CANCEL:", depth_sell_q.top())
                depth_sell_price_order_dict.pop(depth_sell_q.top())
                oldorderId = depth_sell_q.pop()
                result = dealApi.cancel(oldorderId, CONTRACT_ID)
                # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
            if ask_price > depth_sell_q.top():
                add_depth_order(CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
    else:
        count = 1
        while count <= THICK_DEPTH:
            depth_price = round(ask_price * (1 + DEPTH_SPREAD * count / 100), PRICE_DECIMAL)
            add_depth_order(CONTRACT_ID, '-1', depth_price, QUANTITY, '1')
            count = count + 1


def adjust_depth_buy_orders(bid):
    if depth_buy_q.empty() == False and bid * (1 - DEPTH_SPREAD / 200) >= depth_buy_q.top():
        return;
    bid_price = round(bid * (1 - DEPTH_SPREAD / 100), PRICE_DECIMAL)
    if depth_buy_q.empty() == False:
        if bid_price == depth_buy_q.top():
            print("DEPTH BID ENQUL:", bid_price)
            pass
        elif bid_price > depth_buy_q.top():
            add_depth_order(CONTRACT_ID, '1', bid_price, QUANTITY, '1')
        else:
            while depth_buy_q.empty() == False and bid_price < depth_buy_q.top():
                print(depth_buy_price_order_dict)
                depth_buy_price_order_dict.pop(depth_buy_q.top())
                oldorderId = depth_buy_q.pop()
                result = dealApi.cancel(oldorderId, CONTRACT_ID)
                # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
                print(">>>>DEPTH CANCEL:" + result)
            if bid_price > depth_buy_q.top():
                add_depth_order(CONTRACT_ID, '1', bid_price, QUANTITY, '1')
    else:
        count = 1
        while count <= THICK_DEPTH:
            depth_price = round(bid_price * (1 - DEPTH_SPREAD * count / 100), PRICE_DECIMAL)
            add_depth_order(CONTRACT_ID, '1', depth_price, QUANTITY, '1')
            count = count + 1


def orderTask():
    executor = ThreadPoolExecutor(100)
    while True:
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Order Task queue size:", priceQueue.qsize())
        while priceQueue.qsize() > 1:
            priceQueue.get()
        futures = []
        price = priceQueue.get()
        ask = price[0]
        bid = price[1]
        print('Before Cancel!', sell_price_order_dict)
        cancel_old_orders(ask, bid)
        print('After Cancel!', price)
        futures.append(executor.submit(self_trade(bid, ask, QUANTITY)))
        futures.append(executor.submit(adjust_sell_orders, ask))
        futures.append(executor.submit(adjust_buy_orders, bid))
        # added 20180905
        futures.append(executor.submit(adjust_depth_sell_orders, ask))
        futures.append(executor.submit(adjust_depth_buy_orders, bid))
        # adjust buy orders
        wait(futures)
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Process end!', sell_price_order_dict)


def on_message(self, evt):
    data = gzip.decompress(evt)  # data decompress
    dict_data = json.loads(data.decode())

    self.send('{ping:' + str(time.time()) + '}')
    if 'ping' in dict_data:
        self.send('{pong:' + str(time.time()) + '}')
        print('pong:')
        # Max Order Connt is 100
        # orderCount = len(buy_price_order_dict)+len(sell_price_order_dict)
        # if orderCount >= 90:
        #    clearAll()
    elif 'subbed' in dict_data:
        print('receive subbed status message:')
        print(dict_data)
    elif 'tick' in dict_data:
        # executor.submit(cancel_all_orders)
        # print(dict_data['tick'])
        bid = dict_data['tick']['bids'][0][0]
        ask = dict_data['tick']['asks'][0][0]
        price = (ask, bid)
        global last_ask, last_bid
        if ask != last_ask or bid != last_bid:
            print(price, priceQueue.qsize())
            priceQueue.put(price)
        last_bid, last_ask = bid, ask
        # adjust sell orders
    return


def on_error(ws, error):
    print("############## On_Error ##################")

    # getting all the error information
    print('On_Error os.sys.exc_info()[0:2]')
    print(os.sys.exc_info()[0:2])
    print('Error info: %s' % (error))
    print(error)

    if ("timed" in str(error)):
        print("WebSocket Connenction is getting timed out: Please check the netwrork connection")
        print("WebSocket Connenction is getting timed out: Please check the netwrork connection")
    elif ("getaddrinfo" in str(error)):
        print("Network connection is lost: Cannot connect to the host. Please check the network connection ")
        print("Network connection is lost: Cannot connect to the host. Please check the network connection ")
    elif ("unreachable host" in str(error)):
        print("Cannot establish connetion with B6-Web: Network connection is lost or host is not running")
        print("Cannot establish connetion with B6-Web: Network connection is lost or host is not running")

    # for recreatng the WebSocket connection
    if (ws is not None):
        ws.close()
        ws.on_message = None
        ws.on_open = None
        ws.close = None
        print(' deleting ws')
        del ws

    # Forcebly set ws to None
    ws = None

    count = 0
    print("Websocket Client trying  to re-connect")
    print("Websocket Client trying  to re-connect")
    print("Websocket Client trying  to re-connect")

    while (True):
        print('Inside while %s' % (count))
        try:
            ws = websocket.WebSocketApp(host,
                                        on_message=on_message,
                                        on_error=on_error,
                                        on_close=on_close)
            # time.sleep(10)
            print('On_Error: After Creation-1')
            if (ws is not None):
                print('After Creation -  inside on_error : on_open')
                # (hostname, port, resource, is_secure) = websocket._parse_url(host)
                # print ('hostname: %s, port: %s, resource: %s, is_secure: %s' %(hostname, port, resource, is_secure))
                ws.on_open = on_open
                ws.run_forever()
                print(' WS is created after the error - successfully')
                break
        except WebSocketException as e:
            print(
                "WebSocketException: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                    host))
            print(e)
            print(os.sys.exc_info()[0:2])
            del ws
            ws = None
            # time.sleep(10)
        except WebSocketConnectionClosedException as e:
            print(
                "WebSocketConnectionClosedException:Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                    host))
            print(e)
            print(os.sys.exc_info()[0:2])
            del ws
            ws = None
            # time.sleep(10)
        except WebSocketTimeoutException as e:
            print(
                "WebSocketTimeoutException: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                    host))
            print(e)
            print(os.sys.exc_info()[0:2])
            del ws
            ws = None
            # time.sleep(10)
        except Exception as e:
            print(
                "Exception: Failed to recreat connection to hos, please ensure network connection to host: %s" % (host))
            print(e)
            print(os.sys.exc_info()[0:2])
            del ws
            ws = None


def on_close(self):
    print("### websocket closed ###")
    print("### websocket closed ###")
    print("### websocket closed ###")
    localtime = time.asctime(time.localtime(time.time()))
    print('websocket closed time:', localtime)
    start_websocket_connect()


def start_websocket_connect():
    try:
        ws = websocket.WebSocketApp(host,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_open=on_open)
        ws.run_forever()
    except WebSocketException as e:
        print(
            "WebSocketException: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                host))
        print(e)
        print(os.sys.exc_info()[0:2])
    except WebSocketConnectionClosedException as e:
        print(
            "WebSocketConnectionClosedException:Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                host))
        print(e)
        print(os.sys.exc_info()[0:2])
    except WebSocketTimeoutException as e:
        print(
            "WebSocketTimeoutException: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                host))
        print(e)
        print(os.sys.exc_info()[0:2])
    except Exception as e:
        print("Exception: Failed to recreat connection to hos, please ensure network connection to host: %s" % (host))
        print(e)
        print(os.sys.exc_info()[0:2])


if __name__ == "__main__":
    host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    # api_key='2IRP5f83Vax2Vgqu7DuV7kX3'
    # secret_key = "Z2wxqAbDhEKQnxivNDtNfxeN1PgbgLeTOFGRoLs7HKA_693k"

    # At the beginning,cancel all orders.
    latest_price = 0
    last_bid = 0
    last_ask = 0
    cancel_all_orders()
    # time.sleep(10)
    print("ALL orders canceled!")

    sell_q = PriorityQueueSell(DEPTH)
    buy_q = PriorityQueueBuy(DEPTH)
    buy_price_order_dict = {}
    sell_price_order_dict = {}

    depth_sell_q = PriorityQueueSell(THICK_DEPTH, True)
    depth_buy_q = PriorityQueueBuy(THICK_DEPTH, True)
    depth_buy_price_order_dict = {}
    depth_sell_price_order_dict = {}

    orderQueue = Queue(100)
    priceQueue = Queue()

    pwdb = Process(target=writeOrderIdtoDBTask)
    orderT = Process(target=orderTask)
    pwdb.start()
    orderT.start()

    websocket.enableTrace(False)
    websocket.setdefaulttimeout(WEBSOCKET_TIMEOUT)
    start_websocket_connect()
    pwdb.join()
    orderT.join()
