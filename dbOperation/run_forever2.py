from MarketMakerBasic import WebSocketBasic
from websocket import WebSocketException,WebSocketConnectionClosedException,WebSocketTimeoutException
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from pymongo import MongoClient
import time
from configparser import ConfigParser
from apscheduler.schedulers.blocking import BlockingScheduler
import gzip,json
conn = MongoClient("mongodb://localhost:27017/")
executor = ThreadPoolExecutor(100)
ethbtc_price,bchbtc_price,ltcbtc_price = 0.,0.,0.
btcusdt_price,ethusdt_price,bchusdt_price,ltcusdt_price = 0.,0.,0.,0.
price_list = [0]*7
# price_dict={'price':price_list}

executor = ThreadPoolExecutor(100)
class BitassetWebSocket(WebSocketBasic):
    def __init__(self,price_dict):
        self.price_dict = price_dict
        # self.price_list = price_dict
    def on_message(self, ws, message):
        data = gzip.decompress(message)  # data decompress
        data_dict = json.loads(data.decode())
        print(data_dict)
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
            data_list = data_dict['tick']['data']
            coin_pair = data_dict['ch'].split('.')[1]
            print('coin_pair', coin_pair)
            print('price', data_list[0]['price'])
            self.price_dict[coin_pair] = data_list[0]['price']

    def on_open(self,ws):
        # subscribe okcoin.com spot ticker
        tradeStr_list = ['{"sub": "market.ethbtc.trade.detail", "id": "id10"}',
                         '{"sub": "market.bchbtc.trade.detail", "id": "id10"}',
                         '{"sub": "market.ltcbtc.trade.detail", "id": "id10"}',
                         '{"sub": "market.btcusdt.trade.detail", "id": "id10"}',
                         '{"sub": "market.ethusdt.trade.detail", "id": "id10"}',
                         '{"sub": "market.bchusdt.trade.detail", "id": "id10"}',
                         '{"sub": "market.ltcusdt.trade.detail", "id": "id10"}',
                         ]
        # tradeStr = '{"sub": "market.ethbtc.trade.detail", "id": "id10"}'
        for i in range(len(tradeStr_list)):
            ws.send(tradeStr_list[i])
        print('--------- on open complete !----------')

    # def on_open_handler(self,ws):
    #     tradeStr_list = ['{"sub": "market.ethbtc.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.bchbtc.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.ltcbtc.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.btcusdt.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.ethusdt.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.bchusdt.trade.detail", "id": "id10"}',
    #                      '{"sub": "market.ltcusdt.trade.detail", "id": "id10"}',
    #                      ]
    #     # tradeStr = '{"sub": "market.ethbtc.trade.detail", "id": "id10"}'
    #     for i in range(len(tradeStr_list)):
    #         ws.send(tradeStr_list[i])

def write_price_to_mongo(mongo_db,mongo_table,price_dict):

    timestamp10 = time.time()
    tl = time.localtime(timestamp10)
    format_time = time.strftime("%Y-%m-%d %H", tl)    # print(format_time)
    # price_list = price_dict['price']
    p_dict = price_dict #price_dict['price']
    timestamp13 = int(time.time()*1000)
    # values = {'datetime':format_time, 'timestamp':timestamp13,'ethbtc': price_list[0],'bchbtc':price_list[1],'ltcbtc':price_list[2],
    #           'btcusdt':price_list[3],'ethusdt':price_list[4],'bchusdt':price_list[5],'ltcusdt':price_list[6]
    #           }
    values = {'datetime':format_time, 'timestamp':timestamp13,'ethbtc': p_dict['ethbtc'],'bchbtc':p_dict['bchbtc'],
              'ltcbtc':p_dict['ltcbtc'],
              'btcusdt':p_dict['btcusd'],'ethusdt':p_dict['ethusd'],'bchusdt':p_dict['bchusd'],'ltcusdt':p_dict['ltcusd']
              }
    print(values)
    query = {'datetime': format_time}
    mongo_db[mongo_table].update(query, values, True, False)

if __name__ == "__main__":

    # collect_order()
    # cfg = ConfigParser()
    # cfg.read('config.ini')
    mongo_db = conn['lingjun']
    mongo_table_name = 'bitasset_RealTrade_price'
    price_dict = {}
    ws = BitassetWebSocket(price_dict)
    ws.host = 'wss://api.huobi.pro/ws'#cfg.get('website', 'host_huobi')
    # ws.CONTRACT_ID = '10'
    # ws.start_websocket_connect()
    executor.submit(ws.start_websocket_connect)

    # sched = BlockingScheduler()
    # # 'interval', minutes = 2, start_date = '2017-12-13 14:00:01', end_date = '2017-12-13 14:00:10'
    # sched.add_job(write_price_to_mongo, 'interval', seconds=10, start_date='2018-08-13 14:00:00',
    #               end_date='2022-12-13 14:00:10')
    # executor.submit(sched.start)
    print('-----------------has start--------------')