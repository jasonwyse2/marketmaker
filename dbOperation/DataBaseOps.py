from BitAssetAPI import BitAssetMarketAPI,BitAssetDealsAPI
import json
import time
import hashlib
from dbOperation.Sqlite3 import *
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
data = 'This a md5 test!'
hash_md5 = hashlib.md5(data)
hash_md5.hexdigest()
class DataBaseOps:
    # marketApi = BitAssetMarketAPI(RESTURL, APIKEY, SECRETKEY)
    # dealApi = BitAssetDealsAPI(RESTURL, APIKEY, SECRETKEY)
    # dealApi1 = BitAssetDealsAPI(RESTURL_real1, APIKEY_real1, SECRETKEY_real1)
    # dealApi2 = BitAssetDealsAPI(RESTURL_real2, APIKEY_real2, SECRETKEY_real2)
    # dealApi3 = BitAssetDealsAPI(RESTURL_real3, APIKEY_real3, SECRETKEY_real3)
    # dataFile = ''
    # sql3 = Sqlite3(dataFile='/mnt/data/bitasset/bitasset.sqlite')

    conn = MongoClient("mongodb://localhost:27017/")
    # mongo_db = conn['lingjun']
    # mongo_table = 'bitasset_RealTrade_order'
    def __init__(self, mongodb_name,mongo_orderTable_name,
                 mongo_balanceTable_name, mongo_userTable_name, sql3_datafile):
        # self.dealapi = BitAssetDealsAPI(RESTURL, APIKEY, SECRETKEY)

        self.mongo_db = self.conn[mongodb_name]
        self.mongo_orderTable = self.mongo_db[mongo_orderTable_name]
        self.mongo_balanceTable = self.mongo_db[mongo_balanceTable_name]
        self.mongo_userTable = self.mongo_db[mongo_userTable_name]
        self.sql3 = Sqlite3(dataFile=sql3_datafile)

    def collect_data_from_sql3_into_mongodb(self, symbol, num_to_delete_sql3):
        mongo_db = self.mongo_db
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

    def update_user(self,userId,userName,APIKEY,APISECRET):
        pass
    def add_user(self,userId,userName,APIKEY,APISECRET):
        query = {'userId': userId}
        values = {'userId': userId, 'userName': userName, 'APIKEY': APIKEY,'APISECRET':APISECRET}

        self.mongo_userTable.update(query, values, True, False)
        print('insert user (userID:%s) successfully' % (userId))
        pass
    def record_balance(self, userId):
        db = self.mongo_db
        timestamp10 = time.time()
        tl = time.localtime(timestamp10)
        format_time = time.strftime("%Y-%m-%d %H", tl)
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

    def get_history_balance(self,dt=''):
        db = self.mongo_db
        if dt=='':
            tl = time.localtime(time.time())
            time_format = '%Y-%m-%d'
            time_str = time.strftime(time_format, tl)
            curr = datetime.strptime(time_str, time_format)
            deltaTime = -1
            forward = (curr + relativedelta( days=+deltaTime))
            history_time = forward.strftime(time_format)
            # print(history_time)
            query = {'datetime':history_time+' 23'}
        else:
            query = {'datetime': dt + ' 23'}
        res = db.bitasset_balance.find(query)
        for doc in res:
            df = pd.DataFrame(doc['account'])[['currency', 'balance']]
            # print(df)
            break
        return df

    def get_current_balance(self,):
        db = self.mongo_db
        res = db.bitasset_balance.find().sort("datetime", -1).limit(1)
        for doc in res:
            df = pd.DataFrame(doc['account'])[['currency', 'balance']]
            break
        return df

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

if __name__ == "__main__":
    symbol = 'ETH/BTC'
    orderid_list = ['1535548063787068','1535548063787069'] #'1535548063786492'
    orderid2 = '1535548063787068'
    orderid3 = '1535548063787069'
    contractId = '10'

    bitasset = DataBaseOps()
    bitasset.record_balance(symbol)
    bitasset.collect_data_from_sql3_into_mongodb(symbol,100)
    # res = bitasset.sql3.fetchall()
    # print(res)
    # ts = time.time()
    # print(ts)