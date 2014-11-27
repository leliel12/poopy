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
from Queue import Empty

import pika

from . import conf


#==============================================================================
# CONSTANTS
#==============================================================================

PONG_E = "pong_exchange"


#==============================================================================
# CLASS
#==============================================================================

class MPUUIDs(object):

    def __init__(self, queue):
        self.queue = queue
        self._buff = {}

    def _reload(self):
        try:
            while True:
                ruuid, itime, rconf = self.queue.get()
                self._buff[ruuid] = (itime, rconf)
        except Empty:
            pass

    def is_node_alive(self, uuid):
        self._reload()
        if uuid in self._buff and self._buff[uuid][0] <= self.lconf.TTL:
            return True

    def get_node_conf(self, uuid):
        if self.is_node_alive(uuid):
            return self._buff[uuid][1]


class PongListerner(multiprocessing.Process):

    def __init__(self, connection, conf, queue, *args, **kwargs):
        super(PongListener, self).__init__(*args, **kwargs)

        self.connection = connection
        self.lconf = conf
        self.queue = queue

    def run(self):
        def callback(ch, method, properties, body):
            remote_conf = conf.loads(body)
            self.queue.put_nowait((remote_conf.UUID, time.time(), remote_conf))
        connection.exchange_consume(PONG_E, callback)


class PongPublisher(multiprocessing.Process):

    def __init__(self, connection, conf, *args, **kwargs):
        super(PongPublisher, self).__init__(*args, **kwargs)
        self.connection = connection
        self.lconf = conf

    def run(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=PONG_E, type='fanout')
        body = conf.dumps(self.lconf)
        while True:
            channel.basic_publish(exchange=PONG_E, routing_key='', body=body)
            time.sleep(self.lconf.SLEEP)






