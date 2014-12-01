#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Script model for Poopy

"""


#==============================================================================
# IMPORTS
#==============================================================================

import abc
import codecs

import unicodecsv as csv

import numpy as np
from scipy.io import arff


#==============================================================================
# BASE
#==============================================================================

class BaseReader(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load(self, fpath):
        raise NotImplementedError()

    @classmethod
    def subclasses(cls):
        subclss = set()
        def sub_of(actualcls):
            for sc in actualcls.__subclasses__():
                subclss.add(sc)
                sub_of(sc)
        sub_of(cls)
        return tuple(subclss)

    @classmethod
    def reader_by_name(cls, name):
        for sc in cls.subclasses():
            if sc.__name__ == name:
                return sc()
        raise ValueError("Reader whit name '{}' not found".format(name))


#==============================================================================
# ARFF
#==============================================================================

class ARFFReader(BaseReader):

    def load(self, fpath, **kwargs):
        with open(fpath) as fp:
            return arff.loadarff(fp, **kwargs)


class CSVReader(BaseReader):

    def load(self, fpath, **kwargs):
        with open(fpath) as fp:
            reader = csv.reader(fp, **kwargs)
            return iter(reader)


class NumericCsv(BaseReader):

    def load(self, fpath, **kwargs):
        with open(fpath) as fp:
            return np.loadtxt(fp, **kwargs)


class TxtReader(BaseReader):

    def load(self, fp, **kwargs):
        with codecs.open(fpath, **kwargs) as fp:
            return fp.readlines()



