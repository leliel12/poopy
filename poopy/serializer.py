#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Serializers of Poopy

"""


#==============================================================================
# IMPORTS
#==============================================================================

try:
    import cPickle as pickle
except ImportError:
    import pickle


#==============================================================================
# FUNCTIONS
#==============================================================================

def dump(data, fp):
    fp.write(dumps(data))


def dumps(data):
    return pickle.dumps(data).encode("base64")


def load(stream):
    return loads(stream.read())


def loads(stream):
    return pickle.loads(stream.decode("base64"))



#==============================================================================
# MAIN
#==============================================================================

if __name__ == "__main__":
    print(__doc__)




