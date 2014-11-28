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

PREFIX_POOPFS = "poopFS://"

POOPFS_E = "poopFS_exchange"

logger = conf.getLogger("poopFS")

#==============================================================================
# CLASS
#==============================================================================

class PoopFSSuscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(PoopFSSuscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        if not os.path.isdir(conf.POOP_FS):
            os.makedirs(conf.POOP_FS)

    def _callback_poopfs(self, ch, method, properties, body):
        data = serializer.loads(body)
        fname = data["poopFSpath"]
        src = data["src"]
        logger.info("Receiving {}{}".format(PREFIX_POOPFS, fname))
        fpath = os.path.join(self.lconf.POOP_FS, fname)
        dirname = os.path.dirname(fpath)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        with open(fpath, "w") as fp:
            fp.write(src)

    def run(self):
        conn = connection.AMPoopQConnection(self.conn)
        conn.exchange_consume(POOPFS_E, self._callback_poopfs)


class PoopFSPublisher(multiprocessing.Process):

    def __init__(self, conn, conf, filepath, poopFSpath, *args, **kwargs):
        super(PoopFSPublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.filepath = filepath
        self.poopFSpath = poopFSpath

    def _clean_poopfs_filename(self, poopFSpath):
        if not poopFSpath.startswith(PREFIX_POOPFS):
            raise ValueError(
                "poopFS path must start with '{}'".format(PREFIX_POOPFS)
            )
        return poopFSpath.replace(PREFIX_POOPFS, "", 1)

    def run(self):
        logger.info(
            "Upoloading '{}' to '{}'".format(self.filepath, self.poopFSpath)
        )
        conn = connection.AMPoopQConnection(self.conn)
        to_path = self._clean_poopfs_filename(self.poopFSpath)
        channel = conn.channel()
        channel.exchange_declare(exchange=POOPFS_E, type='fanout')
        with open(self.filepath, "rb") as fp:
            src = fp.read()
        body = serializer.dumps({"poopFSpath": to_path, "src": src})
        channel.basic_publish(exchange=POOPFS_E, routing_key='', body=body)
