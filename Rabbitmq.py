#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pika
import json

from ent_pollotion.DjSpider.DBconfig import *
from ..DjSpider import DBcrud as db


class Rabbitmq(object):
    def __init__(self, routing_key):
        if not isinstance(routing_key, str):
            raise TypeError('routing_key should be str')
        self.routing_key = routing_key
        try:
            self.mongo = db.Mongo_crud()
            credentials = pika.PlainCredentials(rabbit_user, rabbit_passwd)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, rabbit_port, rabbit_vhost, credentials))
            print('rabbit connect success')
        except Exception as e:
            print(e)
            print('rabbit is not connect!')

    def consumer_callback(self, ch, method, properties, body):
        self.mongo.mongo_insert_data(self.routing_key, body.decode(), self.routing_key)

    def producer(self, exchange, exchange_type, routing_key, msg):
        if not isinstance(exchange, str):
            raise TypeError('exchange should be str')

        if not isinstance(exchange_type, str):
            raise TypeError('exchange_type should be str')

        if not isinstance(routing_key, str):
            raise TypeError('routing_key should be str')

        if isinstance(msg, dict):
            msg = json.dumps(msg)

        try:
            channel = self.connection.channel()
            channel.queue_declare(queue=routing_key, durable=True)
            channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
            channel.basic_publish(body=msg, exchange=exchange, routing_key=routing_key)
        except Exception as e:
            print(e)
            print('publish error')

    def consumer(self, exchange, exchange_type, routing_key):
        if not isinstance(exchange, str):
            raise TypeError('exchange should be str')

        if not isinstance(exchange_type, str):
            raise TypeError('exchange_type should be str')

        if not isinstance(routing_key, str):
            raise TypeError('routing_key should be str')
        
        try:
            channel = self.connection.channel()
            channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
            channel.queue_bind(exchange=exchange, queue=routing_key, routing_key=routing_key)
            channel.basic_consume(self.consumer_callback, queue=routing_key)
            channel.start_consuming()
        except Exception as e:
            print(e)
            print('recv error')

    def close(self):
        self.connection.close()


if __name__=="__main__":
    print('okok')
