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
# CONSTANTS
#==============================================================================

PREFIX_POOPFS = "poopFS://"

POOPFS_E = "poopFS_exchange"


#==============================================================================
# CLASS
#==============================================================================

class ExcecutionCallbacks(object):

    def __init__(self, connection, conf, *args, **kwargs):
        super(ExcecutionCallbacks, self).__init__(*args, **kwargs)
        self.connection = connection
        self.lconf = conf
        if not os.path.isdir(lconf.POOP_FS):
            os.makedirs(lconf.POOP_FS)

    def _callback_poopfs(self, ch, method, properties, body):
        data = loads(body)
        fname = data["poopFS_path"]
        src = data["src"]
        fpath = os.path.join(self.lconf.POOP_FS, fname)
        dirname = os.path.dirname(fpath)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        with open(fpath, "w") as fp:
            fp.write(src)

    def run(self):
        self.connection.exchange_consume(POOPFS_E, self._callback_poopfs)


#==============================================================================
# FUNCTIONS
#==============================================================================

def loads(stream):
    return pickle.loads(stream.decode("base64"))


def dumps(data):
    return pickle.dumps(data).encode("base64")
