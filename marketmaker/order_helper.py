
from marketmaker.MongoOps import Mongo
import pandas as pd
def saveOrder(userId_list,sql3_obj,num_read_from_sql3=300):

    mongo_obj = Mongo()
    mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset', mongoTable_name='user')
    mongodb_orderTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset', mongoTable_name='order')

    for i in range(len(userId_list)):
        userId = userId_list[i]
        orderId_list0 = sql3_obj.fetch_specific_num(userId, num=num_read_from_sql3)
        if orderId_list0:
            orderId_list = pd.DataFrame(orderId_list0).iloc[:, 0].values.tolist()
            user_obj = Mongo.User(mongodb_userTable)
            dealApi = user_obj.get_dealApi(userId)
            order_obj = Mongo.Order(mongodb_orderTable, dealApi)
            insert_orderId_list = order_obj.insert(orderId_list)
            print('insert into mongodb items',len(insert_orderId_list))
            sql3_obj.delete_by_userId_orderIdlist(userId, insert_orderId_list)
    print('------------- save order is over. --------------')