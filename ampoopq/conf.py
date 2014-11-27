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

CONF_PATH = os.path.join(USER_HOME, ".ampoopq.json")


#==============================================================================
# CONFS
#==============================================================================

def conf_from_file(conf_path=CONF_PATH):
    data = None
    if os.path.exists(conf_path):
        with open(conf_path) as fp:
            data = json.load(fp)
    else:
        data = {"UUID": unicode(uuid.uuid4())}
        with open(conf_path, "w") as fp:
            json.dump(data, fp, indent=2)
    data["CONF_PATH"] = conf_path
    return conf(**data)


def conf(**kwargs):
    data = {
        "PATH": PATH,
        "USER_HOME": USER_HOME,
        "CONF_PATH": None,
        "UUID": unicode(uuid.uuid4()),
        "TTL": 30,
        "SLEEP": 10
    }
    data.update(kwargs)
    cls = collections.namedtuple("Conf", data.keys())
    return cls(**data)


def dumps(confobj):
    data = confobj._asdict()
    return pickle.dumps(data).encode("base64")


def loads(stream):
    data = pickle.loads(stream.decode("base64"))
    return conf(**data)







