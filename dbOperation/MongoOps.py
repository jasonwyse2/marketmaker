from BitAssetAPI import BitAssetMarketAPI,BitAssetDealsAPI
import json
import time
import hashlib
from dbOperation.Sqlite3 import *
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from MarketMakerBasic import WebSocketBasic
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import gzip
from dbOperation.userInfo_conf import UserInfo_dict, UserName_UserId_dict
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
# data = 'This a md5 test!'
# hash_md5 = hashlib.md5(data)
# hash_md5.hexdigest()
class MongoOps:
    # mongo_db = conn['lingjun']
    # mongo_table = 'bitasset_RealTrade_order'
    def __init__(self, sql3_datafile):

        self.conn = MongoClient("mongodb://localhost:27017/")
        self.sql3 = Sqlite3(dataFile=sql3_datafile)
        # self.userOps_obj = self.UserOps(self.mongodb_userTable)
        # self.orderOps_obj = self.OrderOps(self.mongodb_orderTable)
        # self.balanceOps_obj = self.BalanceOps(self.mongodb_balanceTable)
        # self.mongodb = self.conn[mongodb_name]
        # self.mongodb_orderTable = self.mongodb[mongodb_orderTable_name]
        # self.mongodb_balanceTable = self.mongodb[mongodb_balanceTable_name]
        # self.mongodb_userTable = self.mongodb[mongodb_userTable_name]

    # @classmethod
    def get_mongodb(self,mongodb_name):
        return self.conn[mongodb_name]
    def get_mongodb_table(self,mongodb_name,mongoTable_name):
        return self.conn[mongodb_name][mongoTable_name]

    class UserOps:
        def __init__(self,mongodb_userTable):
            self.mongodb_userTable = mongodb_userTable

        def update(self, userId, APIKEY, APISECRET):
            query = {'userId': userId}
            values = {'$set': {'APIKEY': APIKEY, 'APISECRET': APISECRET}}
            self.mongodb_userTable.update(query, values)
            print('update user info (userName:%s) successfully' % (userId))

        def insert(self, userId, userName, APIKEY, APISECRET):
            query = {'userId': userId}
            values = {'$set': {'userId': userId, 'userName': userName, 'APIKEY': APIKEY, 'APISECRET': APISECRET}}
            self.mongodb_userTable.update(query, values, True, False)
            # self.mongo_userTable.update_one(query,values)
            print('add user (userName:%s) successfully' % (userName))
        def find_one(self, userId):
            query = {'userId': userId}
            userInfo = self.mongodb_userTable.find_one(query)
            return userInfo
        def delete(self, userId):
            query = {'userId': userId}
            self.mongodb_userTable.delete_one(query)

        def get_dealApi(self, userId):
            # print(self.test)
            userInfo_dict = self.find_one(userId)
            # print(userInfo_dict['RESTURL'])
            dealApi = BitAssetDealsAPI(userInfo_dict['RESTURL'], userInfo_dict['APIKEY'],
                                            userInfo_dict['SECRETKEY'])
            return dealApi

    class OrderOps:
        def __init__(self,mongodb_orderTable, dealApi):
            self.mongodb_orderTable = mongodb_orderTable
            self.dealApi = dealApi
    class BalanceOps():
        def __init__(self,mongodb_balanceTable, dealApi):
            self.mongodb_balanceTable = mongodb_balanceTable
            self.dealApi = dealApi

        def update(self, userId):
            query = {'userId': userId}
            timestamp10 = time.time()
            tl = time.localtime(timestamp10)
            format_time = time.strftime("%Y-%m-%d %H%M%S", tl)

            balance_info = self.dealApi.accounts_balance()
            print(balance_info)

            # if (balance_info['code'] == 0):
            #     timestamp13 = int(round(timestamp10 * 1000))
            #     values = {'datetime': format_time, 'account': balance_info['data'], 'timestamp': timestamp13}
            #     query = {'userId': userId}
            #     balance_table = self.get_balance_table(userId)
            #     self.mongodb_balanceTable.update(query, values, True, False)
            #     print('insert %s balance successfully' % userId)
        def find_from_remote(self,):
            pass
        def record_balance(self, userId):
            db = self.mongodb
            timestamp10 = time.time()
            tl = time.localtime(timestamp10)
            format_time = time.strftime("%Y-%m-%d %H%M%S", tl)
            # print(format_time)
            dealApi = self.get_dealapi_by_symbol(userId)
            if dealApi == None:
                return
            balance_info = dealApi.accounts_balance()
            if (balance_info['code'] == 0):
                timestamp13 = int(round(timestamp10 * 1000))
                values = {'datetime': format_time, 'account': balance_info['data'], 'timestamp': timestamp13}
                query = {'datetime': format_time}
                balance_table = self.get_balance_table(userId)
                db[balance_table].update(query, values, True, False)
                print('insert %s balance successfully' % userId)

        def add(self, userId, userName, APIKEY, APISECRET):
            query = {'userId': userId}
            values = {'$set': {'userId': userId, 'userName': userName, 'APIKEY': APIKEY, 'APISECRET': APISECRET}}
            self.mongodb_userTable.update(query, values, True, False)
            # self.mongo_userTable.update_one(query,values)
            print('add user (userName:%s) successfully' % (userName))
        def find(self, userId):
            query = {'userName': userId}
            userInfo = self.mongodb_userTable.find_one(query)
            return userInfo
        def delete(self, userId):
            query = {'userName': userId}
            self.mongodb_userTable.delete_one(query)

    class ExchangePriceOps():
        def __init__(self,mongodb_exchangeTable):
            self.mongodb_exchangeTable = mongodb_exchangeTable

    def collect_data_from_sql3_into_mongodb(self, symbol, num_to_delete_sql3):
        mongo_db = self.mongodb
        mongo_table = self.mongo_table
        dealapi = self.get_dealapi_by_symbol(symbol)

        sql3 = self.sql3
        orders = sql3.fetch_specific_num(num=num_to_delete_sql3)
        df = pd.DataFrame(list(orders))

        if df.shape[0]==0:
            return
        # print(df)
        orders_list = df.ix[:,0].tolist()
        # print(len(orders_list))
        # max_msg = max(orders_list)
        while True:
            orders_info_str = dealapi.get_orders_info(orders_list)
            orders_info = json.loads(orders_info_str)
            code = orders_info['code']
            if code==0:
                data = orders_info['data']
                orderInfo_df = pd.DataFrame(data)
                # select the done order out, 2:full ,3:cancel
                tmp2 = orderInfo_df['status'] == 2
                tmp3 = orderInfo_df['status'] == 3
                tmp = (orderInfo_df['status'] == 2 or orderInfo_df['status'] == 3)
                order_done_df = orderInfo_df[tmp]
                done_data = order_done_df.values
                if len(done_data)>0:
                    # print(data)
                    mongo_db[mongo_table].insert(done_data)
                orderid_series = done_data['uuid']
                # sql3.delete_orders_before_timestamp(max_msg)
                sql3.delete_orders_by_id(list(orderid_series))
                orders = sql3.fetchall()
                df_all = pd.DataFrame((orders))
                print(df_all)
                break
        print('collect data from sql3 into mongodb successfully!')

    def record_balance(self, userId):
        db = self.mongodb
        timestamp10 = time.time()
        tl = time.localtime(timestamp10)
        format_time = time.strftime("%Y-%m-%d %H%M%S", tl)
        # print(format_time)
        dealapi = self.get_dealapi_by_symbol(userId)
        if dealapi==None:
            return
        balance_info = dealapi.accounts_balance()
        if(balance_info['code']==0):
            timestamp13 = int(round(timestamp10*1000))
            values = {'datetime': format_time,'account':balance_info['data'],'timestamp':timestamp13}
            query = {'datetime':format_time}
            balance_table = self.get_balance_table(userId)
            db[balance_table].update(query,values,True,False)
            print('insert %s balance successfully' % userId)

    def write_price_to_mongo(mongo_db, mongo_table, price_dict):

        timestamp10 = time.time()
        tl = time.localtime(timestamp10)
        format_time = time.strftime("%Y-%m-%d %H", tl)  # print(format_time)
        # price_list = price_dict['price']
        p_dict = price_dict  # price_dict['price']
        timestamp13 = int(time.time() * 1000)
        # values = {'datetime':format_time, 'timestamp':timestamp13,'ethbtc': price_list[0],'bchbtc':price_list[1],'ltcbtc':price_list[2],
        #           'btcusdt':price_list[3],'ethusdt':price_list[4],'bchusdt':price_list[5],'ltcusdt':price_list[6]
        #           }
        query = {'datetime': format_time}
        values = {'datetime': format_time, 'timestamp': timestamp13, 'ethbtc': p_dict['ethbtc'],
                  'bchbtc': p_dict['bchbtc'],
                  'ltcbtc': p_dict['ltcbtc'],
                  'btcusdt': p_dict['btcusd'], 'ethusdt': p_dict['ethusd'], 'bchusdt': p_dict['bchusd'],
                  'ltcusdt': p_dict['ltcusd']
                  }
        print(values)
        mongo_db[mongo_table].update(query, values, True, False)
    # def get_contractId_by_symbol(self,symbol):
    #     symbol_dict =self.marketApi.symbols()
    #     data_list = symbol_dict['data']
    #     contractId = ''
    #     for it in data_list:
    #         if it['name']==symbol:
    #             contractId = it['id']
    #             break
    #     # print(data_list)
    #     return contractId

class WebSocket(WebSocketBasic):
    def __init__(self,price_dict):
        self.price_dict = price_dict
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

if __name__ == "__main__":

    mongodb_name = 'bitasset'
    mongodb_orderTable_name = 'order'
    mongodb_balanceTable_name = 'balance'
    mongodb_userTable_name = 'user'
    sql3_datafile= '/mnt/data/bitasset/bitasset0906.sqlite'
    dbOps_obj = MongoOps(sql3_datafile)
    mongo_userTable = dbOps_obj.get_mongodb_table(mongodb_name, mongodb_userTable_name)

    userId = UserName_UserId_dict['test001']
    userOps_obj = MongoOps.UserOps(mongo_userTable)

    dealApi = userOps_obj.get_dealApi(userId)
    mongodb_balanceTable = dbOps_obj.get_mongodb_table(mongodb_name, mongodb_balanceTable_name)
    balanceOps_obj = MongoOps.BalanceOps(mongodb_balanceTable,dealApi)
    balanceOps_obj.update(userId)
    # dbOps_obj.balanceOps_obj.update(userId=123)
    # dbOps_obj = MongoOps.UserOps()

    # dbOps_obj.mongodb_userTable.insert_many(userInfo_dict)
