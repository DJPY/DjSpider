#!/usr/bin/python3
# -*- coding: utf-8 -*-
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from gevent import monkey
monkey.patch_all()
import gevent
from gevent.pool import Pool

from DjSpider.DBcrud import Mysql_crud, Redis_crud, Mongo_crud, Switch_db


class DjSpider(object):
    def __init__(self, db=None):
        if db is None:
            print("db is not use")
        elif db == "mysql":
            self.mysql = Mysql_crud()
        elif db == "redis":
            self.redis = Redis_crud()
        elif db == "mongo":
            self.mongo = Mongo_crud()
        elif db == "mysql2redis" or "redis2mysql":
            self.mysql = Mysql_crud()
            self.redis = Redis_crud()
        elif db == "switch":
            self.switch = Switch_db()
        else:
            self.mysql = Mysql_crud()
            self.redis = Redis_crud()
            self.mongo = Mongo_crud()
            self.switch = Switch_db()

    def mysql_insert(self, table, data):
        return self.mysql.mysql_insert_data(table, data)

    def mysql_output(self, sql):
        return self.mysql.mysql_output_data(sql)

# 更新mysql数据不能set主键，所以set要去除主键,这里是将主键单独分开
    def mysql_update(self, table, data, pr, key, val):
        self.mysql.mysql_update_data(table, data, pr, key, val)

    def redis_set_add(self, setName, data):
        self.redis.set_add(setName, data)

    def redis_set_pop(self, setName):
        return self.redis.set_pop(setName)

    def mongo_insert(self, type, value, mongo_collection):
        self.mongo.mongo_insert_data(type, value, mongo_collection)

    def mongo_update(self, type, value, mongo_collection):
        self.mongo.mongo_update_data(type, value, mongo_collection)

    def mongo_output(self, type, mongo_collection):
        return self.mongo.mongo_output_data(type, mongo_collection)

    def mysql_to_redis(self, sql, setName):
        self.switch.mysql_to_redis(sql, setName)

    def mysql_to_mongo(self, sql, type, mongo_collection):
        self.switch.mysql_to_mongo(sql, type, mongo_collection)

    def redis_to_mysql(self, setName, table):
        self.switch.redis_to_mysql(setName, table)

    def redis_to_mongo(self, setName, type, mongo_collection):
        self.switch.redis_to_mongo(setName, type, mongo_collection)

    def mongo_to_redis(self, type, mongo_collection, setName):
        self.switch.mongo_to_redis(type, mongo_collection, setName)

    def mongo_to_mysql(self, type, mongo_collection, table):
        self.switch.mongo_to_mysql(type, mongo_collection, table)

    def process_pool(self, task, val=None, pool=4):
        p = ProcessPoolExecutor(pool)
        v = len(val) if isinstance(val, list) else pool
        for i in range(v):
            p.submit(task, i)
        p.shutdown()

    def thread_pool(self, task, val=None, pool=4):
        t = ThreadPoolExecutor(pool)
        v = len(val) if isinstance(val, list) else pool
        for i in range(v):
            t.submit(task, i)
        t.shutdown()

    def coroutine(self, task, val=None, pool=4):
        p = Pool(pool)
        jobs = []
        v = len(val) if isinstance(val, list) else pool
        for i in range(v):
            job = p.spawn(task, i)
            jobs.append(job)
        gevent.joinall(jobs)

    def get(self, url, headers=None, data=None, cookies=None, proxies=None, status_code=True, again=3):
        times = 0
        while times < again:
            try:
                resp = requests.get(url, headers=headers, data=data, cookies=cookies, proxies=proxies)
                if status_code is True:
                    if resp.status_code == 200:
                        return resp
                    else:
                        raise TimeoutError('can not open')
                else:
                    return resp
            except Exception as e:
                print(e)
                time.sleep(random.randint(1, 5))
                times += 1
                print('try --%d-- times' % times)
                if times == again:
                    break

    def post(self, url, headers=None, data=None, json=None, cookies=None, proxies=None, status_code=True, again=3):
        times = 0
        while times < again:
            try:
                resp = requests.post(url, headers=headers, data=data, json=json, cookies=cookies, proxies=proxies)
                if status_code is True:
                    if resp.status_code == 200:
                        return resp
                    else:
                        raise TimeoutError('can not open')
                else:
                    return resp
            except Exception as e:
                print(e)
                time.sleep(random.randint(1, 5))
                times += 1
                print('try --%d-- times' % times)
                if times == again:
                    break

    def Request(self, url, callback=None, method='GET', headers=None, data=None, json=None, cookies=None, proxies=None, status_code=True, again=3):
        if callback is not None and not callable(callback):
            raise TypeError('callback must be a callable, got %s' % type(callback).__name__)
        method = str(method).upper()
        if method == 'GET':
            resp = self.get(url, headers, data, cookies, proxies, status_code, again)
            if callback:
                return callback(resp)
            else:
                return resp
        if method == 'POST':
            resp = self.post(url, headers, data, json, cookies, proxies, status_code, again)
            if callback:
                return callback(resp)
            else:
                return resp


if __name__ == "__main__":
    print("you can choose these:")
    print("DjSpider(db=mysql)")
    print("DjSpider(db=redis)")
    print("DjSpider(db=mongo)")
    print("DjSpider(db=switch)")
    print("DjSpider(db=all)")
    print("all is mysql+redis+mongo+switch")


