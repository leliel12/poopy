#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Implementation of execution node of AMPoopQ

"""


#==============================================================================
# IMPORTS
#==============================================================================

import time
import multiprocessing

import pika

from . import conf


#==============================================================================
# CONSTANTS
#==============================================================================

PONG_E = "pong_exchange"


#==============================================================================
# CLASS
#==============================================================================

class PongCallback(object):

    def __init__(self, connection, conf):
        self.connection = conf
        self.lconf = conf
        self.nodes = {}

        # connection
        connection.exchange_consume(PONG_E, self)

    def is_alive(self, uuid):
        if uuid in self.nodes and self.nodes[uuid][0] <= self.lconf.TTL:
            return True

    def get(self, uuid):
        if self.is_alive(uuid):
            return self.nodes[uuid][1]

    def __call__(self, ch, method, properties, body):
        remote_conf = conf.loads(body)
        self.nodes[remote_conf.UUID] = (time.time(), remote_conf)



class PongPublisher(multiprocessing.Process):

    def __init__(self, connection, conf, *args, **kwargs):
        super(PongPublisher, self).__init__(*args, **kwargs)
        self.connection = conf
        self.lconf = conf

    def run(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=PONG_E, type='fanout')
        body = conf.dumps(self.conf)
        while True:
            channel.basic_publish(
                exchange=PONG_E, routing_key='', body=body
            )
            time.sleep(self.lconf.SLEEP)






