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

from . import conf, serializer


#==============================================================================
# CONSTANTS
#==============================================================================

PONG_E = "pong_exchange"


#==============================================================================
# CLASS
#==============================================================================

class PongSubscriber(multiprocessing.Process):

    def __init__(self, connection, conf, *args, **kwargs):
        super(PongSubscriber, self).__init__(*args, **kwargs)
        self.connection = connection
        self.lconf = conf
        self._queue = multiprocessing.Queue()
        self._oldtimes = []
        self._buff = {}

    def reload_buff(self):
        while self._queue.qsize():
            itime, data = self._queue.get_nowait().split("::", 1)
            rconf = conf.conf(**serializer.loads(data))
            self._buff[rconf.UUID] = (float(itime), rconf)

    def uuids(self):
        self.reload_buff()
        return [k for k in self._buff.keys() if self.is_node_alive(k)]

    def is_node_alive(self, uuid):
        self.reload_buff()
        return (
            uuid in self._buff and
            time.time() - self._buff[uuid][0] < self.lconf.TTL
        )

    def get_node_conf(self, uuid):
        if self.is_node_alive(uuid):
            return self._buff[uuid][1]

    # callback
    def _callback_pong(self, ch, method, properties, body):
        if self._queue.qsize() == 0:
            self._oldtimes = []
        for oldtime in tuple(self._oldtimes):
            if time.time() - oldtime > self.lconf.TTL:
                self._queue.get_nowait()
                self._oldtimes.remove(oldtime)
        itime = time.time()
        self._queue.put_nowait("{}::{}".format(itime, body))
        self._oldtimes.append(itime)

    def run(self):
        self.connection.exchange_consume(PONG_E, self._callback_pong)


class PongPublisher(multiprocessing.Process):

    def __init__(self, connection, conf, *args, **kwargs):
        super(PongPublisher, self).__init__(*args, **kwargs)
        self.connection = connection
        self.lconf = conf

    def run(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=PONG_E, type='fanout')
        body = serializer.dumps(self.lconf._asdict())
        while True:
            channel.basic_publish(exchange=PONG_E, routing_key='', body=body)
            time.sleep(self.lconf.SLEEP)
