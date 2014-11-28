#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""This module create and mantain the basic AMPoopQ configuration

"""


#==============================================================================
# IMPORTS
#==============================================================================

import os
import uuid
import json
import collections

try:
    import cPickle as pickle
except ImportError:
    import pickle


#==============================================================================
# CONSTANTS
#==============================================================================

PATH = os.path.abspath(os.path.dirname(__file__))

USER_HOME = os.path.expanduser("~")

AMPOOPQ_DIR = os.path.join(USER_HOME, ".ampoopq")

CONF_PATH = os.path.join(AMPOOPQ_DIR, "conf.json")

POOP_FS = os.path.join(AMPOOPQ_DIR, "poop_fs")


#==============================================================================
# CONFS
#==============================================================================

def conf_from_file(conf_path=CONF_PATH, poop_fs=POOP_FS):
    data = None
    if not os.path.isdir(os.path.dirname(conf_path)):
        os.makedirs(os.path.dirname(conf_path))
    if not os.path.isdir(os.path.dirname(poop_fs)):
        os.makedirs(os.path.dirname(poop_fs))
    if os.path.exists(conf_path):
        with open(conf_path) as fp:
            data = json.load(fp)
    else:
        data = {"UUID": unicode(uuid.uuid4())}
        with open(conf_path, "w") as fp:
            json.dump(data, fp, indent=2)
    data["CONF_PATH"] = conf_path
    data["POOP_FS"] = poop_fs
    return conf(**data)


def conf(**kwargs):
    data = {
        "PATH": PATH,
        "USER_HOME": USER_HOME,
        "CONF_PATH": None,
        "UUID": unicode(uuid.uuid4()),
        "TTL": 30,
        "SLEEP": 5,
        "POOP_FS": None
    }
    data.update(kwargs)
    cls = collections.namedtuple("Conf", data.keys())
    return cls(**data)


#==============================================================================
# MAIN
#==============================================================================

if __name__ == "__main__":
    print(__doc__)




