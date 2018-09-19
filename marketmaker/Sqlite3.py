import sqlite3
from marketmaker.tool import get_local_datetime
import pandas as pd
class Sqlite3:
    tableName = 'bitasset_order'
    def __init__(self, dataFile=None):
        if dataFile is None:
            self.conn = sqlite3.connect(":memory:")
        else:
            try:
                self.conn = sqlite3.connect(dataFile,check_same_thread=False)
            except sqlite3.Error as e:
                print("连接sqlite数据库失败:", e.args[0])
        self.create_table()

    def getcursor(self):
        return self.conn.cursor()

    def drop(self, table):
        '''
        if the table exist,please be carefull
        '''
        if table is not None and table != '':
            cu = self.getcursor()
            sql = 'DROP TABLE IF EXISTS ' + table
            try:
                cu.execute(sql)
            except sqlite3.Error as why:
                print("delete table failed:", why.args[0])
                return
            self.conn.commit()
            print("delete table successful!")
            cu.close()
        else:
            print("table does not exist！")

    def create_table(self,):
        '''
        create databaseOperation table
        :param sql:
        :return:
        '''
        tableName = self.tableName
        sql = 'create table if not exists '+tableName+' (' \
                              'userId Int,' \
                              'orderId CHAR(100),' \
                                                      'datetime CHAR(50)' \
                                                      ')'
        if sql is not None and sql != '':
            cu = self.getcursor()
            try:
                cu.execute(sql)
            except sqlite3.Error as why:
                print("create table failed:", why.args[0])
                return
            self.conn.commit()
            print("create table successful!")
            cu.close()
        else:
            print("sql is empty or None")

    def insert(self, userId,orderId):
        '''
        insert data to the table
        :param sql:
        :param data:
        :return:
        '''
        datetime = get_local_datetime()
        sql = 'INSERT INTO ' + self.tableName + '(userId,orderId,datetime) values (?,?,?)'
        if sql is not None and sql != '':
            if orderId !='':
                cu = self.getcursor()
                try:
                    # for orderId in orderId_list:
                    cu.execute(sql, [userId,orderId,datetime]) #if 'd' is an element, you should use [d]
                    self.conn.commit()
                except sqlite3.Error as why:
                    print("insert data failed:", why.args[0])
                cu.close()
        else:
            print("sql is empty or None")

    def fetch_specific_num(self,userId, num,sql=''):
        '''
                query all data
                :param sql:
                :return:
                '''
        if sql == '':
            sql = 'SELECT orderId FROM ' + self.tableName +' where userId=? order by datetime limit ?'
        if sql is not None and sql != '':
            cu = self.getcursor()
            content = None
            try:
                cu.execute(sql,[userId,num])
                content = cu.fetchall()
            except sqlite3.Error as why:
                print("fetchall data failed:", why.args[0])
            cu.close()
            return content
        else:
            print("sql is empty or None")
    def fetch_by_userId(self,userId, num,sql=''):
        '''
                query all data
                :param sql:
                :return:
                '''
        if sql == '':
            sql = 'SELECT * FROM ' + self.tableName +' where userId=? order by orderId limit ?'
        if sql is not None and sql != '':
            cu = self.getcursor()
            content = None
            try:
                cu.execute(sql,[userId,num])
                content = cu.fetchall()
            except sqlite3.Error as why:
                print("fetchall data failed:", why.args[0])
            cu.close()
            return content
        else:
            print("sql is empty or None")
    def fetchall(self, sql=''):
        '''
        query all data
        :param sql:
        :return:
        '''
        if sql=='':
            # sql = 'SELECT * FROM ' + self.tableName+ ' order by orderId'
            sql = 'SELECT * FROM ' + self.tableName
        if sql is not None and sql != '':
            cu = self.getcursor()
            content = None
            try:
                cu.execute(sql)
                content = cu.fetchall()

            except sqlite3.Error as why:
                print("fetchall data failed:", why.args[0])
            cu.close()
            return content
        else:
            print("sql is empty or None")

    def update(self, sql, data):
        '''
        update the data
        :param sql:
        :param data:
        :return:
        '''
        if sql is not None and sql != '':
            if data is not None:
                cu = self.getcursor()
                try:
                    for d in data:
                        cu.execute(sql, d)
                        self.conn.commit()
                except sqlite3.Error as why:
                    print("update data failed:", why.args[0])
                cu.close()
        else:
            print ("sql is empty or None")

    def delete(self, sql, data=None):
        '''
        delete the data
        :param sql:
        :param data:
        :return:
        '''
        if sql is not None and sql != '':
            cu = self.getcursor()
            if data is not None:
                try:
                    for d in data:
                        cu.execute(sql, d)
                        self.conn.commit()
                except sqlite3.Error as why:
                    print("delete data failed:", why.args[0])
            else:
                try:
                    cu.execute(sql)
                    self.conn.commit()
                except sqlite3.Error as why:
                    print ("delete data failed:", why.args[0])
            cu.close()
        else:
            print ("sql is empty or None")

    def delete_by_userId_orderIdlist(self, userId, orderId_list):
        sql = 'delete from ' + self.tableName + ' where userId=? and orderId= ?'
        for i in range(len(orderId_list)):
            cu = self.getcursor()
            try:
                cu.execute(sql, [userId, orderId_list[i]])
                self.conn.commit()
            except sqlite3.Error as why:
                print("delete data failed:", why.args[0])

    def delete_all(self,):
        sql = 'delete from '+self.tableName
        cu = self.getcursor()
        cu.execute(sql)
        self.conn.commit()
        cu.close()

    def __del__(self):
        self.conn.close()

if __name__ == "__main__":
    # sql3 = Sqlite3(dataFile='/mnt/data/bitasset/bitasset.sqlite')
    sql3 = Sqlite3(dataFile="/mnt/data/bitasset/bitasset.sqlite")
    # userId = 123
    # orderId_list = ['33444','2234']
    # datetime = '3232-23-23 12:34:23'
    # sql3.insert(userId,orderId_list,datetime)

    res = sql3.fetchall()
    # res = sql3.fetch_specific_num(userId=666849,num=10)
    df = pd.DataFrame(res)
    print(df)
    print('fetch number',len(res))
    # print('before delete',res)
    # sql3.delete_orders_by_userIdOrderId(userId=userId, orderId_list=['33444'])
    # res = sql3.fetchall()
    # print('after delete',res)

