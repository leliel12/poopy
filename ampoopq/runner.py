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

import os
import runpy
import uuid
import codecs
import multiprocessing

try:
    import cPickle as pickle
except ImportError:
    import pickle

import pika


#==============================================================================
# CLASS
#==============================================================================

class NodeSubscriber(multiprocessing.Process):

    def __init__(self, connection, conf, *args, **kwargs):
        super(PongSubscriber, self).__init__(*args, **kwargs)
        self.connection = connection
        self.lconf = conf
        self._queue = multiprocessing.Queue()
        self._buff = {}

    def reload_buff(self):
        while self._queue.qsize():
            itime, data = self._queue.get_nowait().split("::", 1)
            rconf = conf.loads(data)
            self._buff[rconf.UUID] = (float(itime), rconf)

    def uuids(self):
        self.reload_buff()
        return [k for k in self._buff.keys() if self.is_node_alive(k)]

    def is_node_alive(self, uuid):
        self.reload_buff()
        if uuid in self._buff and self._buff[uuid][0] <= self.lconf.TTL:
            return True

    def get_node_conf(self, uuid):
        if self.is_node_alive(uuid):
            return self._buff[uuid][1]

    def run(self):
        def callback(ch, method, properties, body):
            self._queue.put_nowait("{}::{}".format(time.time(), body))
        self.connection.exchange_consume(PONG_E, callback)




class ExecutionNode(multiprocessing.Process): pass
