import websocket
import time
import json
from concurrent.futures import wait
import heapq
import gzip
import random
from websocket import WebSocketException,WebSocketConnectionClosedException,WebSocketTimeoutException

# sql3 = Sqlite3(dataFile="/mnt/data/bitasset/bitasset.sqlite")
# sql3 = Sqlite3()
import os


# DEPTH = 15
# SPREAD = 0.1
# QUANTITY = 0.001  # ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
# MAX_RETREAT = 10
# CONTRACT_ID = '10'  # ETH/BTC:10 BCH/BTC:11  LTC/BTC:12
# MAX_QUEUE_SIZE = 50
# SCHEDULE_TIME = 20
# WEBSOCKET_TIMEOUT = 10


class WebSocketBasic:
    # host = "wss://api.huobi.pro/ws"
    # CONTRACT_ID = '10'
    last_ask,last_bid = 0., 0.
    def __init__(self,host,priceQueue=None,CONTRACT_ID='10'):
        self.priceQueue = priceQueue
        self.host = host
        self.CONTRACT_ID = CONTRACT_ID
        pass

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
        # elif 'tick' in data_dict:
        # self.on_message_handler(data_dict)
        if 'tick' in data_dict:
            priceQueue = self.priceQueue
            print(data_dict['tick'])
            bid = data_dict['tick']['bids'][0][0]
            ask = data_dict['tick']['asks'][0][0]
            price = [ask, bid]
            if ask != self.last_ask or bid != self.last_bid:
                print(price, priceQueue.qsize())
                priceQueue.put(price)
                # self.priceQueue = price
            self.last_ask = ask
            self.last_bid = bid
        # return
    def on_error(self, ws, error):
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
        host = self.host
        while (True):
            print('Inside while %s' % (count))
            try:
                ws = websocket.WebSocketApp(host,
                                            on_message=self.on_message,
                                            on_error=self.on_error,
                                            on_close=self.on_close,
                                            on_open=self.on_open)
                time.sleep(1)
                print('On_Error: After Creation-1')
                if (ws is not None):
                    print('After Creation -  inside on_error : on_open')
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
                time.sleep(10)
            except WebSocketConnectionClosedException as e:
                print(
                    "WebSocketConnectionClosedException:Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                        host))
                print(e)
                print(os.sys.exc_info()[0:2])
                del ws
                ws = None
                time.sleep(10)
            except WebSocketTimeoutException as e:
                print(
                    "WebSocketTimeoutException: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                        host))
                print(e)
                print(os.sys.exc_info()[0:2])
                del ws
                ws = None
                time.sleep(10)
            except Exception as e:
                print(
                    "Exception: Failed to recreat connection to hos, please ensure network connection to host: %s" % (
                        host))
                print(e)
                print(os.sys.exc_info()[0:2])
                del ws
                ws = None
                time.sleep(10)

    def on_close(self):
        print("### websocket closed ###")
        print("### websocket closed ###")
        print("### websocket closed ###")
        localtime = time.asctime(time.localtime(time.time()))
        print('websocket closed time:', localtime)
        self.start_websocket_connect()
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
        print('--------- on open complete !----------')

    def start_websocket_connect(self,):
        try:
            host = self.host
            ws = websocket.WebSocketApp(host,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close,
                                        on_open=self.on_open)
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
            print(
                "Exception: Failed to recreat connection to hos, please ensure network connection to host: %s" % (host))
            print(e)
            print(os.sys.exc_info()[0:2])

class PriorityQueueSell:
    def __init__(self,orderQueue,sell_price_order_dict,dealApi,executor_cancel,DEPTH,CONTRACT_ID):
        self._queue = []
        self._index = 0
        self.orderQueue, self.sell_price_order_dict = orderQueue,sell_price_order_dict
        self.dealApi, self.executor_cancel = dealApi,executor_cancel
        self.DEPTH, self.CONTRACT_ID = DEPTH,CONTRACT_ID


    def push(self, item, priority):
        dealApi = self.dealApi
        executor_cancel = self.executor_cancel
        heapq.heappush(self._queue, (priority, self._index, item))
        self._index += 1
        self.orderQueue.put(item)
        if len(self._queue) > self.DEPTH:
            tobeCanceledOrderId = self._queue[-1][2]
            executor_cancel.submit(dealApi.cancel, tobeCanceledOrderId, self.CONTRACT_ID)
            tobeCanceledOrderPrice = self._queue[-1][0]
            self.sell_price_order_dict.pop(tobeCanceledOrderPrice)
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
    def __init__(self,orderQueue,buy_price_order_dict,dealApi,executor_cancel,DETPH,CONTRACT_ID):
        self._queue = []
        self._index = 0

        self.orderQueue, self.buy_price_order_dict = orderQueue,buy_price_order_dict
        self.dealApi, self.executor_cancel = dealApi,executor_cancel
        self.DETPH, self.CONTRACT_ID = DETPH,CONTRACT_ID
    def push(self, item, priority):
        dealApi = self.dealApi
        executor_cancel = self.executor_cancel
        heapq.heappush(self._queue, (-priority, self._index, item))
        self._index += 1
        self.orderQueue.put(item)
        if len(self._queue) > self.DEPTH:
            tobeCanceledOrderId = self._queue[-1][2]
            tobeCanceledOrderPrice = -self._queue[-1][0]
            self.buy_price_order_dict.pop(tobeCanceledOrderPrice)
            executor_cancel.submit(dealApi.cancel, tobeCanceledOrderId, self.CONTRACT_ID)
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

class OrderBasic:
    last_integer_minute = 0
    open, high, low, close =0., 0., 0., 0.
    latest_price = 0.

    def __init__(self,orderQueue, sell_price_order_dict, buy_price_order_dict,
                               sql3, CONTRACT_ID, userId, dealApi,executor_cancel,DEPTH,QUANTITY,SPREAD):
        self.orderQueue = orderQueue
        self.sell_price_order_dict, self.buy_price_order_dict = sell_price_order_dict, buy_price_order_dict
        self.sql3, self.CONTRACT_ID, self.dealApi = sql3, CONTRACT_ID, dealApi
        self.executor_cancel = executor_cancel
        self.QUANTITY = QUANTITY
        self.SPREAD = SPREAD
        # orderQueue, sell_price_order_dict, dealApi, executor_cancel
        self.sell_q = PriorityQueueSell(orderQueue, sell_price_order_dict, dealApi, executor_cancel,DEPTH,CONTRACT_ID)
        self.buy_q = PriorityQueueBuy(orderQueue, buy_price_order_dict, dealApi, executor_cancel,DEPTH,CONTRACT_ID)


    def add_order(self,contractId, side, price, quantity, orderType):
        dealApi = self.dealApi

        print('ADD:', price)
        quantity = quantity * random.randint(0, 10)
        result = dealApi.trade(contractId, side, price, quantity, orderType)
        dict_data = json.loads(result)
        orderId = dict_data['msg']
        errorCode = dict_data['code']
        if errorCode == 0:
            if side == '-1':
                self.sell_price_order_dict[price] = orderId
                self.sell_q.push(orderId, price)
            elif side == '1':
                self.buy_price_order_dict[price] = orderId
                self.buy_q.push(orderId, price)
            else:
                print("ERROR side:" + side)

    def trade_helper(self,price, quantity):
        print('quantity',quantity)
        CONTRACT_ID = self.CONTRACT_ID
        dealApi = self.dealApi
        quantity = quantity * random.randint(1, 100)

        side = {'buy': '1', 'sell': '-1'}
        result1 = dealApi.trade(CONTRACT_ID, side['buy'], price, quantity, '1')
        data1_dict = json.loads(result1)
        print('result1',result1)
        data2_dict = {'code':-1}
        if data1_dict['code'] == 0:
            orderid1 = data1_dict['msg']
            self.orderQueue.put(data1_dict['msg'])
            result2 = dealApi.trade(CONTRACT_ID, side['sell'], price, quantity, '1')
            data2_dict = json.loads(result2)
            print('result2',result2)
            orderid2 = data2_dict['msg']
            if data2_dict['code'] != 0:
                res = dealApi.cancel(data2_dict['msg'], CONTRACT_ID)
                print('order2 cancel', res)
            else:
                self.orderQueue.put(data2_dict['msg'])
                pass
        # else:
        #     dealApi.cancel(orderid1, CONTRACT_ID)
        time.sleep(1)
        if data1_dict['code'] == 0:
            res = dealApi.cancel(orderid1, CONTRACT_ID)
            print('order1 cancel, result status:',res)
        if data2_dict['code'] == 0:
            res = dealApi.cancel(orderid2, CONTRACT_ID)
            print('order2 cancel, result status:', res)

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

    def cancel_old_orders(self,ask, bid):
        dealApi = self.dealApi
        sell_q,buy_q = self.sell_q,self.buy_q
        executor_cancel = self.executor_cancel
        futures = []
        while (sell_q.empty() == False and ask > sell_q.top()):  # or len(sell_price_order_dict) > DEPTH:
            self.sell_price_order_dict.pop(sell_q.top())
            oldorderId = sell_q.pop()
            # result = dealApi.cancel(oldorderId, CONTRACT_ID)
            futures.append(executor_cancel.submit(dealApi.cancel, oldorderId, self.CONTRACT_ID))
        while (buy_q.empty() == False and bid < buy_q.top()):  # or len(buy_price_order_dict) > DEPTH:
            self.buy_price_order_dict.pop(buy_q.top())
            oldorderId = buy_q.pop()
            # result = dealApi.cancel(oldorderId, CONTRACT_ID)
            futures.append(executor_cancel.submit(dealApi.cancel, oldorderId, self.CONTRACT_ID))
        wait(futures)

    def adjust_sell_orders(self,ask):
        QUANTITY = self.QUANTITY
        dealApi = self.dealApi
        sell_q = self.sell_q
        ask_price = round(ask * (1 + self.SPREAD / 100), 6)
        print(ask_price)
        if sell_q.empty() == False:
            if ask_price == sell_q.top():
                print("ASK ENQUL:", ask_price)
                pass
            elif ask_price < sell_q.top():
                self.add_order(self.CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
            else:
                while sell_q.empty() == False and ask_price > sell_q.top():
                    print(">>>> CANCEL:", sell_q.top())
                    self.sell_price_order_dict.pop(sell_q.top())
                    oldorderId = sell_q.pop()
                    result = dealApi.cancel(oldorderId, self.CONTRACT_ID)
                    # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
                if ask_price > sell_q.top():
                    self.add_order(self.CONTRACT_ID, '-1', ask_price, QUANTITY, '1')
        else:
            self.add_order(self.CONTRACT_ID, '-1', ask_price, QUANTITY, '1')

    def adjust_buy_orders(self,bid):
        QUANTITY = self.QUANTITY
        dealApi = self.dealApi
        buy_q = self.buy_q
        bid_price = round(bid * (1 - self.SPREAD / 100), 6)
        if buy_q.empty() == False:
            if bid_price == buy_q.top():
                print("BID ENQUL:", bid_price)
                pass
            elif bid_price > buy_q.top():
                self.add_order(self.CONTRACT_ID, '1', bid_price, QUANTITY, '1')
            else:
                while buy_q.empty() == False and bid_price < buy_q.top():
                    print(self.buy_price_order_dict)
                    self.buy_price_order_dict.pop(buy_q.top())
                    oldorderId = buy_q.pop()
                    result = dealApi.cancel(oldorderId, self.CONTRACT_ID)
                    # executor_cacel.submit(dealApi.cancel,oldorderId, CONTRACT_ID)
                    print(">>>> CANCEL:" + result)
                if bid_price > buy_q.top():
                    self.add_order(self.CONTRACT_ID, '1', bid_price, QUANTITY, '1')
        else:
            self.add_order(self.CONTRACT_ID, '1', bid_price, QUANTITY, '1')


    # def cancel_all_orders(self):
    #     CONTRACT_ID = self.CONTRACT_ID
    #     dealApi = self.dealApi
    #     executor_cancel = self.executor_cancel
    #     futures = []
    #     all_orders = dealApi.get_all_orders(CONTRACT_ID)
    #     print('orders going to cancel:',all_orders)
    #     if 'error' not in all_orders:
    #         if all_orders['code'] == 0:
    #             print('all_orders:',all_orders['data'])
    #             for order in all_orders['data'][::-1]:
    #                 print('cancel order info',order)
    #                 futures.append(executor_cancel.submit(dealApi.cancel, order['orderId'], CONTRACT_ID))
    #     print(wait(futures))

class MarketMakerBasic:
    def __init__(self,CONTRACT_ID,dealApi,executor_cancel):
        self.CONTRACT_ID = CONTRACT_ID
        self.dealApi = dealApi
        self.executor_cancel = executor_cancel

    def cancel_all_orders(self):
        CONTRACT_ID = self.CONTRACT_ID
        dealApi = self.dealApi
        executor_cancel = self.executor_cancel
        futures = []
        all_orders = dealApi.get_all_orders(CONTRACT_ID)
        print('orders going to cancel:',all_orders)
        if 'error' not in all_orders:
            if all_orders['code'] == 0:
                print('all_orders:',all_orders['data'])
                for order in all_orders['data'][::-1]:
                    print('cancel order info',order)
                    futures.append(executor_cancel.submit(dealApi.cancel, order['orderId'], CONTRACT_ID))
        print(wait(futures))







if __name__ == "__main__":
    host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi


