import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from marketmaker.BitAssetAPI import BitAssetDealsAPI

import json
from marketmaker.dbOperation.tool import *
from pymongo import MongoClient
import pandas as pd
from marketmaker.MarketMakerBasic import WebSocketBasic
from concurrent.futures import ThreadPoolExecutor
import gzip
from marketmaker.dbOperation.UserInfo_Conf import UserName_UserId_dict, UserId_UserName_dict,StatusName_StatusCode_dict
from apscheduler.schedulers.blocking import BlockingScheduler


class Mongo:
    conn = MongoClient("mongodb://localhost:27017/")
    def __init__(self, ):
        pass

    def get_mongodb(self,mongodb_name):
        return self.conn[mongodb_name]
    def get_mongodb_table(self,mongodb_name,mongoTable_name):
        return self.conn[mongodb_name][mongoTable_name]

    class User:
        def __init__(self,mongodb_userTable):
            self.mongodb_userTable = mongodb_userTable

        def update(self, userId, APIKEY, APISECRET):
            query = {'userId': userId}
            values = {'$set': {'APIKEY': APIKEY, 'SECRETKEY': APISECRET}}
            self.mongodb_userTable.update(query, values)
            print('update user info (userName:%s) successfully' % (userId))

        def insert(self, userId, userName, APIKEY, APISECRET,RESTURL):
            query = {'userId': userId}
            values = {'userId': userId, 'userName': userName, 'APIKEY': APIKEY, 'SECRETKEY': APISECRET,'RESTURL':RESTURL}
            self.mongodb_userTable.update(query, values, True, False)
            # self.mongo_userTable.update_one(query,values)
            print('add user (userName:%s) successfully into mongodb_user' % (userName))
        def find_one(self, userId):
            query = {'userId': userId}
            userInfo = self.mongodb_userTable.find_one(query)
            return userInfo
        def delete(self, userId):
            query = {'userId': userId}
            self.mongodb_userTable.delete_one(query)

        def get_dealApi(self, userId):
            userInfo_dict = self.find_one(userId)
            dealApi = BitAssetDealsAPI(userInfo_dict['RESTURL'], userInfo_dict['APIKEY'],
                                            userInfo_dict['SECRETKEY'])
            return dealApi

    class Balance:
        def __init__(self,mongodb_balanceTable, dealApi):
            self.mongodb_balanceTable = mongodb_balanceTable
            self.dealApi = dealApi

        def update(self, userId):
            balance_info = self.dealApi.accounts_balance()
            print(balance_info)
            if (balance_info['code'] == 0):
                # print('get balance successfully from remote website server for user:%s'%UserId_UserName_dict[userId])
                format_time = get_local_datetime(format_str="%Y-%m-%d %H:%M:%S")
                format_time_hour = format_time[0:13] #precise to hour
                query = {'userId': userId,'datetime':{'$regex':format_time_hour}}
                values = {'userId': userId, 'datetime': format_time, 'account': balance_info['data'],'change':0}

                new_account = balance_info['data']
                df_newBTCBalance = pd.DataFrame(new_account)
                newBTCValue = df_newBTCBalance.loc[df_newBTCBalance['currency'] == 'BTC', 'balance'].values[0]


                doc_lastRecord = self.find(userId,record_num=1)
                last_account = ''
                for doc in doc_lastRecord:
                    last_account = doc['account']
                    break
                # last_account = list(doc_lastRecord)[0]['account']
                if last_account!='':
                    df_lastBTCBalance = pd.DataFrame(last_account)
                    lastBTCValue = df_lastBTCBalance.loc[df_lastBTCBalance['currency']=='BTC','balance'].values[0]

                    if(float(lastBTCValue)!=float(newBTCValue)):
                        values = {'userId':userId,'datetime': format_time, 'account': balance_info['data'],'change':1}

                self.mongodb_balanceTable.update(query, values, True, False)
                print('insert balance successfully into mongodb for user:%s ' % UserId_UserName_dict[userId])
            else:
                print('get balance from remote server go wrong:',balance_info)

        def find(self,userId,record_num):
            query = {'userId': userId}  # db.foo.find().sort({_id:1}).limit(50);
            docs = self.mongodb_balanceTable.find(query).sort('_id', -1).limit(record_num)
            # print(list(docs))
            return docs

        def delete(self, userId):
            pass

    class Order:
        def __init__(self,mongodb_orderTable, dealApi):
            self.mongodb_orderTable = mongodb_orderTable
            self.dealApi = dealApi
        def insert(self, orderId_list):
            if(len(orderId_list))==0:
                return
            else:
                while True:
                    orders_info_str = self.dealApi.get_orders_info(orderId_list)
                    orders_info = json.loads(orders_info_str)
                    code = orders_info['code']
                    if code == 0:
                        data = orders_info['data']
                        orderInfo_df = pd.DataFrame(data)
                        # select the done order out, 2:full ,3:cancel
                        full_code = StatusName_StatusCode_dict['full']
                        cancel_code = StatusName_StatusCode_dict['cancel']
                        order_full_df = orderInfo_df[orderInfo_df['status'] == full_code]

                        order_cancel_df =  orderInfo_df[(orderInfo_df['status'] == cancel_code)]
                        order_partCancel_df = order_cancel_df.loc[(orderInfo_df['filledQuantity'].values>0) & (orderInfo_df['canceledQuantity'].values>0)]
                        order_partCancel_df['status'] = StatusName_StatusCode_dict['part-cancel']


                        order_cancel_df = order_cancel_df.copy()
                        order_cancel_df.loc[(orderInfo_df['filledQuantity'] > 0) & (orderInfo_df['canceledQuantity'] > 0),'status']\
                            =StatusName_StatusCode_dict['part-cancel']


                        order_done_df = pd.concat([order_full_df,order_cancel_df])
                        order_done_df.reset_index(inplace=True)
                        done_order_list = order_done_df.to_dict(orient='records')

                        if len(done_order_list) > 0:
                            self.mongodb_orderTable.insert(done_order_list)
                        break

            insert_orderId_list = order_done_df['uuid'].values.tolist()
            return insert_orderId_list

        def saveOrder(self,mongo_obj,sql3_obj,userId_list):

            mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset', mongoTable_name='user')
            mongodb_orderTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset', mongoTable_name='order')
            num_read_from_sql3 = 300
            for i in range(len(userId_list)):
                userId = userId_list[i]
                orderId_list0 = sql3_obj.fetch_specific_num(userId, num=num_read_from_sql3)
                if orderId_list0:
                    orderId_list = pd.DataFrame(orderId_list0).iloc[:, 0].values.tolist()
                    user_obj = Mongo.User(mongodb_userTable)
                    dealApi = user_obj.get_dealApi(userId)
                    order_obj = Mongo.Order(mongodb_orderTable, dealApi)
                    insert_orderId_list = order_obj.insert(orderId_list)
                    print('insert into mongodb items', len(insert_orderId_list))
                    sql3_obj.delete_by_userId_orderIdlist(userId, insert_orderId_list)
            print('------------- save order is over. --------------')

        def find(self,num):
            pass
    class Exchange():
        def __init__(self,exchangeName,mongodb_exchangeTable):
            self.mongodb_exchangeTable = mongodb_exchangeTable
            self.exchangeName = exchangeName
        def update(self, price_dict):
            print('going to update "price_dict" in mongodb:',price_dict)
            format_time = get_local_datetime(format_str="%Y-%m-%d %H:%M:%S")
            format_time_hour = format_time[0:13]  # precise to hour
            query = {'datetime': {'$regex':format_time_hour}}
            values = {'datetime': format_time,'exchangeName':self.exchangeName}
            exchangeRate_dict = {'exchangePrice':price_dict}
            values.update(exchangeRate_dict)
            print(' "price_dict" going to write in mongodb:',values)
            self.mongodb_exchangeTable.update(query, values, True, False)
        def find(self,record_num,exchangeName='huobi'):
            query = {'exchangeName':exchangeName}
            # self.mongodb_exchangeTable.update(query, values, True, False)
            docs = self.mongodb_exchangeTable.find(query).sort('_id', -1).limit(record_num)
            return docs


class WebSocket(WebSocketBasic):
    def __init__(self,host,price_dict):
        super(WebSocket,self).__init__(host)
        self.price_dict = price_dict
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
            data_list = data_dict['tick']['data']
            coin_pair = data_dict['ch'].split('.')[1]
            print('new trade %s price:'%coin_pair, data_list[0]['price'])
            self.price_dict[coin_pair] = data_list[0]['price']
            print(self.price_dict)
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

    listen_host = "wss://api.huobi.pro/ws"
    listen_exchangeName = 'huobi'
    mongodb_name = 'bitasset'
    mongodb_orderTable_name = 'order'
    mongodb_balanceTable_name = 'balance'
    mongodb_userTable_name = 'user'
    mongodb_exchangeTable_name = 'exchange'
    sql3_datafile= '/mnt/data/bitasset/bitasset.sqlite'
    dbOps_obj = Mongo()
    mongo_userTable = dbOps_obj.get_mongodb_table(mongodb_name, mongodb_userTable_name)

    user_obj = Mongo.User(mongo_userTable)
    # userOps_obj.insert(userId = 123, userName='maker_lj1', APIKEY='aka9a8c9efdaeb44b3',
    #                    APISECRET='db4d3d557d554910b7ba6b1d6a9db9a9',RESTURL='http://api.bitasset.com/')
    executor = ThreadPoolExecutor(20)
    # update balance table for userId (dealApi)

    # update exchange, including receiving data and store it in mongo
    price_dict = {}
    # ----  receiving data
    websock_obj = WebSocket(listen_host, price_dict)
    executor.submit(websock_obj.start_websocket_connect)
    # ----  store data in mongo
    sched = BlockingScheduler()
    mongodb_exchangeTable = dbOps_obj.get_mongodb_table(mongodb_name, mongodb_exchangeTable_name)

    exchange_obj = Mongo.Exchange(listen_exchangeName,mongodb_exchangeTable)

    mongodb_balanceTable = dbOps_obj.get_mongodb_table(mongodb_name, mongodb_balanceTable_name)


    userId_list = [UserName_UserId_dict['maker_lj1'],
                   UserName_UserId_dict['maker_lj2'],
                   UserName_UserId_dict['maker_lj3']
                   ]
    balanceOps_obj_list = []
    for i in range(len(userId_list)):
        userId = userId_list[i]
        dealApi = user_obj.get_dealApi(userId)
        balanceOps_obj = Mongo.Balance(mongodb_balanceTable, dealApi)
        balanceOps_obj_list.append(balanceOps_obj)
    # 'exchange rate' update in every seconds
    sched.add_job(exchange_obj.update, 'interval', seconds=5, start_date='2018-08-13 14:00:00',
                  end_date='2122-12-13 14:00:10',args=[price_dict])
    # update 'balance' for each userId account,
    for i in range(len(userId_list)):
        sched.add_job(balanceOps_obj_list[i].update, 'interval', seconds=5, start_date='2018-08-13 14:00:00',
                  end_date='2122-12-13 14:00:10', args=[userId_list[i]])
    executor.submit(sched.start)
    print('main process end')


