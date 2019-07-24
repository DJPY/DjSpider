#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
import logging
import pymysql
import redis
import pymongo
import json

from DjSpider.DBconfig import *


class Mysql_crud(object):
    def __init__(self,host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd, db=mysql_db, charset=mysql_charset):
        try:
            self.mysql_conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
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
            return 1
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
                return 1
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
    def __init__(self, host=redis_host, port=redis_port, db=redis_db, password=redis_passwd, decode_responses=True):
        try:
            self.redis_conn = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=decode_responses)
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

    # set都是字符串 set_add flg:1 添加成功 flg:0 重复数据

    def set_add(self, setName, data):
        # success flg:1
        if isinstance(data, dict):
            value = json.dumps(data)
            conn = self.redis_conn
            flg = conn.sadd(setName, value)
        elif isinstance(data, str):
            conn = self.redis_conn
            flg = conn.sadd(setName, data)
        else:
            print(setName,data)
            print('ERROR:data type must be str or dict')
            return False
        return flg

    def set_pop(self, setName):
        conn = self.redis_conn
        val = conn.spop(setName)
        if val:
            value = json.loads(val)
            return value

    def set_count(self, setName):
        conn = self.redis_conn
        count = conn.scard(setName)
        return count

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

    def hash_set(self, hashName, key, val):
        if isinstance(val, dict):
            val = json.dumps(val)
            conn = self.redis_conn
            flg = conn.hset(hashName, key, val)
        elif isinstance(val, str):
            conn = self.redis_conn
            flg = conn.hset(hashName, key, val)
        else:
            print('ERROR:data type must be str or dict')
            return False
        return flg

    def hash_get(self, hashName, key):
        conn = self.redis_conn
        flg = conn.hget(hashName, key)
        return flg

    def hash_mset(self, hashName, data):
        conn = self.redis_conn
        flg = conn.hmset(hashName, data)
        return flg

    def hash_mget(self, hashName, data):
        conn = self.redis_conn
        flg = conn.hmget(hashName, data)
        return flg

    def hash_del(self, hashName, key):
        conn = self.redis_conn
        flg = conn.hdel(hashName, key)
        return flg

    def hash_len(self, hashName):
        conn = self.redis_conn
        count = conn.hlen(hashName)
        return count

    def hash_hexists(self, hashName, key):
        # flg: True False
        conn = self.redis_conn
        flg = conn.hexists(hashName, key)
        return flg


class Mongo_crud(object):
    def __init__(self, host=mongo_host, port=mongo_port):
        try:
            self.mongo_conn = pymongo.MongoClient(host=host, port=port)
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
        db = self.mongo_conn[mongo_db]
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
    def __init__(self):
            # if db is None:
            #     print('db is not use')
            # elif db == "mysql2redis" or db == "redis2mysql":
            #     self.mysql = Mysql_crud()
            #     self.redis = Redis_crud()
            # elif db == "mysql2mongo" or db == "mongo2mysql":
            #     self.mysql = Mysql_crud()
            #     self.mongo = Mongo_crud()
            # elif db == "redis2mongo" or db == "mongo2redis":
            #     self.redis = Redis_crud()
            #     self.mongo = Mongo_crud()
            # else:
            self.mysql = Mysql_crud()
            self.redis = Redis_crud()
            self.mongo = Mongo_crud()

    def redis_to_mysql(self, setName, table):
        data = self.redis.set_pop(setName)
        flg = self.mysql.mysql_insert_data(table, data)
        return flg

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



