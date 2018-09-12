import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from marketmaker.MongoOps import Mongo,WebSocket

if __name__ == "__main__":
    executor = ThreadPoolExecutor(10)
    listen_host = "wss://api.huobi.pro/ws"
    listen_exchangeName = 'huobi'
    mongodb_name = 'bitasset'
    mongodb_exchangeTable_name = 'exchange'

    exchangeRate_dict = {}
    # ----  receiving exchange rate data, and store it in exchangeRate_dict -----
    websock_obj = WebSocket(listen_host, exchangeRate_dict)
    executor.submit(websock_obj.start_websocket_connect)
    mongo_obj = Mongo()
    mongodb_exchangeTable = mongo_obj.get_mongodb_table(mongodb_name,mongodb_exchangeTable_name)

    sched = BlockingScheduler() #timer
    # ----------------- update exchange rate --------------------------
    exchange_obj = Mongo.Exchange(listen_exchangeName, mongodb_exchangeTable)
    sched.add_job(exchange_obj.update, 'interval', seconds=5, start_date='2018-08-13 14:00:03',
                  end_date='2122-12-13 14:00:10', args=[exchangeRate_dict])
    # ----------------- update exchange rate --------------------------
    executor.submit(sched.start)