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

from . import connection, serializer, conf


#==============================================================================
# CONSTANTS
#==============================================================================

SCRIPT_E = "script_exchange"

logger = conf.getLogger("Scripts")


#==============================================================================
# CLASS
#==============================================================================

class ScriptSuscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(ScriptSuscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        if not os.path.isdir(conf.SCRIPTS):
            os.makedirs(conf.SCRIPTS)

    def _callback(self, ch, method, properties, body):
        data = serializer.loads(body)
        fname = data["filename"]
        ifname = data["ifilename"]
        src = data["src"]
        logger.info("Receiving {} ({})".format(fname, ifname))
        fpath = os.path.join(self.lconf.SCRIPTS, ifname)
        with open(fpath, "w") as fp:
            fp.write(src)

    def run(self):
        conn = connection.AMPoopQConnection(self.conn)
        conn.exchange_consume(SCRIPT_E, self._callback)


class ScriptPublisher(multiprocessing.Process):

    def __init__(self, conn, conf, filepath, ifilename, *args, **kwargs):
        super(ScriptPublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.filepath = filepath
        self.ifilename = ifilename


    def run(self):
        logger.info("Deploying script '{}'".format(self.filepath))

        with open(self.filepath, "rb") as fp:
            src = fp.read()
        body = serializer.dumps({
            "filename": os.path.basename(self.filepath),
            "ifilename": self.ifilename, "src": src
        })
        conn = connection.AMPoopQConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=SCRIPT_E, type='fanout')
        channel.basic_publish(exchange=SCRIPT_E, routing_key='', body=body)
