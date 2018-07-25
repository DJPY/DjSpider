#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import logging
import pymysql
import redis
import pymongo
import json

from ..DjSpider.DBconfig import *


class Mysql_crud(object):
    def __init__(self):
        try:
            self.mysql_conn = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd, db=mysql_db, charset=mysql_charset)
            info = 'mysql connect Success'
            # print(info)
        except Exception as e:
            print(e)
            print('----------------------')
            info = 'mysql is not connect!'
            print(info)
            t = time.strftime('%Y/%m/%d-%H:%M:%S')
            logging.basicConfig(filename='settings.log', filemode='a', level=logging.INFO)
            logging.info('\n')
            logging.info('----------------------------------------------------------------')
            logging.info('TIME:' + t)
            logging.warning(info)
            print('----------------------')

    def mysql_insert_data(self, table, data):

        if isinstance(data, dict):
            conn = self.mysql_conn
            cursor = conn.cursor()
            for k in list(data.keys()):
                if data[k] is None or data[k] == '':
                    data.pop(k)
            fieldSize = len(data)
            field = tuple(data)
            value = tuple(data[i] for i in data)
            try:
                cursor.execute('insert into %s (%s) values (%s)' % (table, ('%s,' * fieldSize % field)[:-1], ('"%s",' * fieldSize % value)[:-1]))
                return 1
            except Exception as e:
                print(e)
                print('insert data ERROR:', table, data)
                conn.rollback()
            finally:
                conn.commit()
                #cursor.close()
                #conn.close()
        else:
            print('ERROR:data must be dict')

    def mysql_output_data(self, sql):
        conn = self.mysql_conn
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            if data is None or type(data) is tuple:
                return None
            else:
                return data
        except Exception as e:
            print(e)
            print('fetch data ERROR')

    def mysql_query_data(self, sql):
        conn = self.mysql_conn
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            data = cursor.execute(sql)
            return data
        except Exception as e:
            print(e)
            print('query data ERROR')

    def mysql_delete_data(self, sql):
        conn = self.mysql_conn
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            print('已删除')
        except Exception as e:
            print(e)
            print('delete data ERROR')
        finally:
            conn.commit()

    # 更新mysql数据不能set主键，所以set要去除主键,这里是将主键单独分开
    def mysql_update_data(self, table, data, pr, key, val):

        if isinstance(data, dict):
            conn = self.mysql_conn
            cursor = conn.cursor()
            data.pop(pr)
            field = tuple(i for i in data)
            value = tuple(data[i] for i in data)
            try:
                cursor.execute('update %s set %s where %s = "%s"' % (table, (('="%s",'.join(field) + '="%s"') % value), key, val))
            except Exception as e:
                print(e)
                print('update data ERROR')
                conn.rollback()
            finally:
                conn.commit()
                #cursor.close()
                #conn.close()
        else:
            print('ERROR:data must be dict')


class Redis_crud(object):
    def __init__(self):
        try:
            self.redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db, password=redis_passwd, decode_responses=True)
            info = 'redis connect Success'
            # print(info)
        except Exception as e:
            print(e)
            info = 'redis is not connect!'
            print(logging.warning('----------------------'))
            print(info)
            t = time.strftime('%Y/%m/%d-%H:%M:%S')
            logging.basicConfig(filename='settings.log', filemode='a', level=logging.INFO)
            logging.info('\n')
            logging.info('----------------------------------------------------------------')
            logging.info('TIME:' + t)
            logging.warning(info)
            print(logging.warning('----------------------'))

    # set都是字符串

    def set_add(self, setName, data):
        if isinstance(data, dict):
            value = json.dumps(data)
            conn = self.redis_conn
            conn.sadd(setName, value)
        elif isinstance(data, str):
            conn = self.redis_conn
            conn.sadd(setName, data)
        else:
            print('ERROR:data type must be str or dict')        

    def set_pop(self, setName):
        conn = self.redis_conn
        val = conn.spop(setName)
        if val:
            value = json.loads(val)
            return value
        else:
            print('set is empty now')

    def list_lpush(self, listName, data):
        if isinstance(data, dict):
            value = json.dumps(data)
            conn = self.redis_conn
            conn.lpush(listName, value)
        elif isinstance(data, str):
            conn = self.redis_conn
            conn.lpush(listName, data)
        else:
            print('ERROR:data type must be str or dict')

    def list_blpop(self, listName):
        conn = self.redis_conn
        val = conn.blpop(listName)
        if val:
            value = json.loads(val)
            return value
        else:
            print('list is empty now')

    def list_rpush(self, listName, data):
        if isinstance(data, dict):
            value = json.dumps(data)
            conn = self.redis_conn
            conn.rpush(listName, value)
        elif isinstance(data, str):
            conn = self.redis_conn
            conn.rpush(listName, data)
        else:
            print('ERROR:data type must be str or dict')

    def list_brpop(self, listName):
        conn = self.redis_conn
        val = conn.brpop(listName)
        if val:
            value = json.loads(val)
            return value
        else:
            print('list is empty now')


class Mongo_crud(object):
    def __init__(self):
        try:
            self.mongo_conn = pymongo.MongoClient(host=mongo_host, port=mongo_port)
            db = self.mongo_conn[mongo_db]
            db.authenticate(mongo_user, mongo_passwd)
            info = 'mongo connect Success'
            # print(info)
        except Exception as e:
            print(e)
            print('----------------------')
            info = 'mongo is not connect!'
            print(info)
            t = time.strftime('%Y/%m/%d-%H:%M:%S')
            logging.basicConfig(filename='settings.log', filemode='a', level=logging.INFO)
            logging.info('\n')
            logging.info('----------------------------------------------------------------')
            logging.info('TIME:' + t)
            logging.warning(info)
            print('----------------------')

    def mongo_insert_data(self, type, value, mongo_collection):

        #if isinstance(value, dict):
            #value = json.dumps(value).encode().decode('unicode-escape')
        db = self.mongo_conn[mongo_db]
        collection = db[mongo_collection]
        data={}
        data['type'] = type
        data['value'] = [value]
        try:
            collection.insert(data)
        except Exception as e:
            print(e)
            print('insert data ERROR')

    def mongo_update_data(self, type, value, mongo_collection):
        #if isinstance(value, dict):
            #value = json.dumps(value).encode().decode('unicode-escape')
        db = self.mongo_conn[mongo_db]
        collection = db[mongo_collection]
        try:
            collection.update({'type': type}, value, {'upsert': 'false'})
        except Exception as e:
            print(e)
            print('update data ERROR')

    def mongo_output_data(self, type, mongo_collection):
        db = self.mongo.mongo_conn[mongo_db]
        collection = db[mongo_collection]
        data={}
        data['type'] = type
        try:
            data = collection.find(data)
            if data:
                return data
            else:
                print('can not find data')
        except Exception as e:
            print(e)
            print('fetch data ERROR')


class Switch_db(object):
    def __init__(self, *args):
        for db in args:
            if db == 'none':
                print('db is not use')
            elif db == "mysql":
                self.mysql = Mysql_crud()
            elif db == "redis":
                self.redis = Redis_crud()
            elif db == "mongo":
                self.mongo = Mongo_crud()
            else:
                self.mysql = Mysql_crud()
                self.redis = Redis_crud()
                self.mongo = Mongo_crud()

    def redis_to_mysql(self, setName, table):
        data = self.redis.set_pop(setName)
        self.mysql.mysql_insert_data(table, data)

    def redis_to_mongo(self, setName, type, mongo_collection):
        value = self.redis.set_pop(setName)
        self.mongo.mongo_insert_data(type, value, mongo_collection)

    def mysql_to_redis(self, sql, setName):
        conn = self.mysql.mysql_conn
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            for i in data:
                self.redis.set_add(setName, i)
        except Exception as e:
            print(e)
            print('fetch data ERROR')

    def mysql_to_mongo(self, sql, type, mongo_collection):
        conn = self.mysql.mysql_conn
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            for i in data:
                self.mongo.mongo_insert_data(type, i, mongo_collection)
        except Exception as e:
            print(e)
            print('fetch data ERROR')

    def mongo_to_redis(self, type, mongo_collection, setName):
        db = self.mongo.mongo_conn[mongo_db]
        collection = db[mongo_collection]
        data={}
        data['type'] = type
        try:
            data = collection.find(data)
            if data:
                for i in data:
                    i.pop('_id')
                    i.pop('type')
                    value = i['value']
                    self.redis.set_add(setName, value)
            else:
                print('can not find data')
        except Exception as e:
            print(e)
            print('fetch data ERROR')

    def mongo_to_mysql(self, type, mongo_collection, table):
        db = self.mongo.mongo_conn[mongo_db]
        collection = db[mongo_collection]
        data={}
        data['type'] = type
        try:
            data = collection.find(data)
            if data:
                for i in data:
                    i.pop('_id')
                    i.pop('type')
                    data = i['value']
                    self.mysql.mysql_insert_data(table, data)
            else:
                print('can not find data')
        except Exception as e:
            print(e)
            print('fetch data ERROR')


if __name__ == "__main__":
    print('okok')




