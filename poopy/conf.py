#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This module create and mantain the basic Poopy configuration

"""


#==============================================================================
# IMPORTS
#==============================================================================

import os
import uuid
import json
import logging
import collections

try:
    import cPickle as pickle
except ImportError:
    import pickle

from . import PRJ, STR_VERSION

#==============================================================================
# CONSTANTS
#==============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))

USER_HOME = os.path.expanduser("~")

POOPY_DIR = os.path.join(USER_HOME, ".poopy")

CONF_PATH = os.path.join(POOPY_DIR, "conf.json")

POOPY_FS = os.path.join(POOPY_DIR, "poopy_fs")

SCRIPTS = os.path.join(POOPY_DIR, "scripts")


#==============================================================================
# LOGGER
#==============================================================================

log_level = logging.DEBUG

def getLogger(name=PRJ):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = logging.Formatter(
        '[%(levelname)s|%(asctime)s] %(name)s > %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


#==============================================================================
# CONFS
#==============================================================================

def conf_from_file(conf_path=CONF_PATH, poopy_fs=POOPY_FS, scripts=SCRIPTS):
    data = None
    if not os.path.isdir(os.path.dirname(conf_path)):
        os.makedirs(os.path.dirname(conf_path))
    if not os.path.isdir(os.path.dirname(poopy_fs)):
        os.makedirs(os.path.dirname(poopy_fs))
    if os.path.exists(conf_path):
        with open(conf_path) as fp:
            data = json.load(fp)
    else:
        data = {"UUID": unicode(uuid.uuid4())}
        with open(conf_path, "w") as fp:
            json.dump(data, fp, indent=2)
    data["CONF_PATH"] = conf_path
    data["POOPY_FS"] = poopy_fs
    data["SCRIPTS"] = scripts
    return conf(**data)


def conf(**kwargs):
    data = {
        "PATH": PATH,
        "USER_HOME": USER_HOME,
        "CONF_PATH": None,
        "UUID": unicode(uuid.uuid4()),
        "TTL": 30,
        "SLEEP": 5,
        "POOPY_FS": None,
        "SCRIPTS": None
    }
    data.update(kwargs)
    cls = collections.namedtuple("Conf", data.keys())
    return cls(**data)


#==============================================================================
# MAIN
#==============================================================================

if __name__ == "__main__":
    print(__doc__)




