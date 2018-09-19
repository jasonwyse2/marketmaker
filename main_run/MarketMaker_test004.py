import pandas as pd
from marketmaker.tool import get_config_parameter_dict
from marketmaker.MongoOps import Mongo
from marketmaker.MarketMaker_0913 import MarketMaker
from marketmaker.UserInfo_Conf import UserName_UserId_dict
def foo(x,**kargs):
    print(x)
    # print(args)
    print(kargs)
if __name__ == "__main__":
    config_file = 'marketmaker-maker_lj1-ETHBTC.ini'
    config_parameter_dict = get_config_parameter_dict(config_file)

    mkt_mkr = MarketMaker(**config_parameter_dict)
    mkt_mkr.run()





    # mongo_obj = Mongo()
    # Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset',mongoTable_name='user')
    # user_obj = Mongo.User(Mongodb_userTable)
    # userId = UserName_UserId_dict[userName]
    # dealApi = user_obj.get_dealApi(userId)
    # DEPTH = 15
    # SPREAD = 0.1
    # THICK_DEPTH = 15
    # mkt_mkr = MarketMaker(listen_host, listenPair_ContractId, userId,sql3_dataFile,
    #                       dealApi, DEPTH,QUANTITY,SPREAD,THICK_DEPTH)
    # mkt_mkr.run()