import json
from pymongo import MongoClient
# from monitor.models.bitasset.BitAssetInterface import *
# from monitor.models.bitasset.bitasset_util import BitAsset
from dbOperation.tool import *
from configparser import ConfigParser
import pandas as pd
# bitasset = BitAsset()
# Create your views here.
conn = MongoClient("mongodb://localhost:27017/")
mongo_db = conn.lingjun
mongo_table_order = 'bitasset_RealTrade_order'
cfg = ConfigParser()
# test_path = settings.TEST_CONF_DIR
cfg.read('config.ini')
symbolPair_contractId_dict = {'ETH/BTC': 10, 'BCH/BTC': 11, 'LTC/BTC': 12}
contractId_symbolPair_dict = {10: 'ETH/BTC', 11: 'BCH/BTC', 12: 'LTC/BTC'}
minutes_ago_num = 60
mongo_table_balance_list = ['tradeBalance_ethbtc', 'tradeBalance_bchbtc', 'tradeBalance_ltcbtc']
mongo_table_balance_dict = {'ETH/BTC': 'tradeBalance_ethbtc', 'BCH/BTC': 'tradeBalance_bchbtc',
                            'LTC/BTC': 'tradeBalance_ltcbtc'}
mongo_table_price = 'bitasset_RealTrade_price'

def get_lastday_lasthour_str(hours=-24):

    lastday_lasthour = 23
    while True:
        hours_ago_time = datetime.now() + timedelta(hours=hours)
        # hours_ago_time_str = hours_ago_time.strftime('%Y-%m-%d %H')
        hours_ago_time_str = hours_ago_time.strftime('%Y-%m-%d')+' %s'%lastday_lasthour
        doc = mongo_db[mongo_table_balance_list[0]].find_one({'datetime': hours_ago_time_str})
        if doc!=None:
            break
        else:
            lastday_lasthour= lastday_lasthour-1
    return hours_ago_time_str

def get_update_data():
    tl = time.time()
    now_time = datetime.fromtimestamp(tl).strftime('%Y-%m-%d %H')
    # get exchange rate
    price_doc = mongo_db[mongo_table_price].find_one({'datetime': now_time})  # {'datetime':now_time}
    timestamp13 = price_doc['timestamp']
    exchangePrice_recordTime = from_timestamp10_to_localtime(timestamp13 / 1000.)
    price_list = [price_doc['ethbtc'], price_doc['bchbtc'], price_doc['ltcbtc'],
                  price_doc['btcusdt'], price_doc['ethusdt'], price_doc['bchusdt'],
                  price_doc['ltcusdt']]
    price_dict = {'ETH/BTC': price_doc['ethbtc'], 'BCH/BTC': price_doc['bchbtc'], 'LTC/BTC': price_doc['ltcbtc'],
                  'BTC/USDT': price_doc['btcusdt'], 'ETH/USDT': price_doc['ethusdt'], 'BCH/USDT': price_doc['bchusdt'],
                  'LTC/USDT': price_doc['ltcusdt']
                  }
    df_price = pd.DataFrame([price_dict])
    df_price_dict = {}
    df_price_dict = json.loads(df_price.to_json(orient='split'))
    hours_ago_time_str = get_lastday_lasthour_str(hours=-24)
    account_old_list = []
    account_now_list = []
    account_recordTime_list = []
    balanceTable_dict = {}

    def transfor_currency_into_BTC(x):
        currency_in_BTC = 0.0
        currency, balance = x[0], x[1]
        if currency != 'BTC':
            price_key = currency.upper() + '/BTC'
            exchange_rate = price_dict[price_key]
            if exchange_rate != None:
                currency_in_BTC = price_dict[price_key] * float(balance)
        else:
            currency_in_BTC = float(balance)
        return currency_in_BTC

    for key in mongo_table_balance_dict:
        doc_now = mongo_db[mongo_table_balance_dict[key]].find_one({'datetime': now_time})
        account_now, timestamp13 = doc_now['account'], doc_now['timestamp']
        account_now_list.append(account_now)
        account_record_time = from_timestamp10_to_localtime(timestamp13 / 1000.)
        account_recordTime_list.append(account_record_time)
        df_now = pd.DataFrame(account_now)
        # print(df_now)
        doc_old = mongo_db[mongo_table_balance_dict[key]].find_one({'datetime': hours_ago_time_str})
        account_old = doc_old['account']
        account_old_list.append(account_old)
        df_old = pd.DataFrame(account_old)
        df_old.rename(columns={'available': 'available_old', 'balance': 'balance_old', 'frozen': 'frozen_old'},
                      inplace=True)
        # print(df_old)
        df_now_old = pd.merge(df_now, df_old, on='currency', how='left')
        # print(df_now_old)

        df_now_old['balance_in_BTC'] = df_now_old[['currency', 'balance']].apply(transfor_currency_into_BTC, axis=1)
        df_now_old['lastDayBalance_in_BTC'] = df_now_old[['currency', 'balance_old']].apply(transfor_currency_into_BTC,
                                                                                            axis=1)
        df_now_old['PNL'] = (df_now_old['balance_in_BTC'].values) / df_now_old['lastDayBalance_in_BTC'].values - 1
        df_return = df_now_old[['currency', 'frozen', 'available', 'balance', 'balance_in_BTC', 'PNL']]
        df_return.rename(
            columns={'currency': 'Currency', 'frozen': 'Frozen', 'available': 'Available', 'balance': 'Balance',
                     'balance_in_BTC': 'BTC Value'}, inplace=True)

        balanceTable_dict[key] = json.loads(df_return.to_json(orient='split'))

    data_list = [df_price_dict, exchangePrice_recordTime, balanceTable_dict, account_recordTime_list]
    return data_list