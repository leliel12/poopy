#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This module copy and retrieves the files over a poopFS distributed file
system

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

PREFIX_POOPY_FS = "poopyFS://"

POOPY_FS_E = "poopyFS_exchange"

logger = conf.getLogger("poopyFS")

#==============================================================================
# CLASS
#==============================================================================

class PoopyFSSuscriber(multiprocessing.Process):

    def __init__(self, conn, conf, *args, **kwargs):
        super(PoopyFSSuscriber, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        if not os.path.isdir(conf.POOPY_FS):
            os.makedirs(conf.POOPY_FS)

    def _callback(self, ch, method, properties, body):
        data = serializer.loads(body)
        fname = data["poopyFSpath"]
        src = data["src"]
        logger.info("Receiving {}{}".format(PREFIX_POOPY_FS, fname))
        fpath = os.path.join(self.lconf.POOPY_FS, fname)
        dirname = os.path.dirname(fpath)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        with open(fpath, "w") as fp:
            fp.write(src)

    def run(self):
        conn = connection.PoopyConnection(self.conn)
        conn.exchange_consume(POOPY_FS_E, self._callback)


class PoopyFSPublisher(multiprocessing.Process):

    def __init__(self, conn, conf, filepath, poopyFSpath, *args, **kwargs):
        super(PoopyFSPublisher, self).__init__(*args, **kwargs)
        self.conn = conn
        self.lconf = conf
        self.filepath = filepath
        self.poopyFSpath = poopyFSpath

    def _clean_poopyfs_filename(self, poopyFSpath):
        if not poopyFSpath.startswith(PREFIX_POOPY_FS):
            raise ValueError(
                "poopyFS path must start with '{}'".format(PREFIX_POOPY_FS)
            )
        return poopyFSpath.replace(PREFIX_POOPY_FS, "", 1)

    def run(self):
        logger.info(
            "Upoloading '{}' to '{}'".format(self.filepath, self.poopyFSpath)
        )
        to_path = self._clean_poopyfs_filename(self.poopyFSpath)
        with open(self.filepath, "rb") as fp:
            src = fp.read()
        body = serializer.dumps({"poopyFSpath": to_path, "src": src})

        conn = connection.PoopyConnection(self.conn)
        channel = conn.channel()
        channel.exchange_declare(exchange=POOPY_FS_E, type='fanout')
        channel.basic_publish(exchange=POOPY_FS_E, routing_key='', body=body)
