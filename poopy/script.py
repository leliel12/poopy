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
import runpy
import inspect
import collections


#==============================================================================
# CONSTANTS
#==============================================================================

SCRIPT_TEMPLATE = """
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ampoopq import script


class Script(script.ScriptBase):

    def map(self, k, v, ctx):
        raise NotImplementedError()

    def reduce(self, k, v, ctx):
        raise NotImplementedError()

    def setup(self, ctx):
        raise NotImplementedError()

""".strip()


#==============================================================================
# SCRIPT CLASSES
#==============================================================================

class ScriptMeta(abc.ABCMeta):

    def __init__(self, *args, **kwargs):
        super(ScriptMeta, self).__init__(*args, **kwargs)

        # modify readers list
        readers_dict = {r.__name__: r for r in self.readers}
        ReadersClass = collections.namedtuple("Readers", readers_dict.keys())
        self._readers = self.readers
        self.readers = ReadersClass(**readers_dict)

    def __repr__(cls):
        return (
            getattr(cls, 'class_name', None) or
            super(ScriptMeta, cls).__repr__()
        )


class ScriptBase(object):

    class_name = None
    readers = [runpy]
    __metaclass__ = ScriptMeta

    @abc.abstractmethod
    def map(self, k, v, ctx):
        raise NotImplementedError()

    @abc.abstractmethod
    def reduce(self, k, v, ctx):
        raise NotImplementedError()

    @abc.abstractmethod
    def setup(self, ctx):
        raise NotImplementedError()

    def conf_for_node(self, uuid):
        return {}


#==============================================================================
# JOB
#==============================================================================

class Job(object):

    def __init__(self):
        self._name = "<NO-NAME>"
        self._global_vars = {}
        self._mappers = []
        self._reducers = []
        self._input_path = []
        self._output_path = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        self._name = unicode(v)

    @property
    def output_path(self):
        return self._output_path

    @output_path.setter
    def output_path(self, v):
        self._output_path = unicode(v)

    @property
    def input_path(self):
        return self._input_path

    @property
    def reducers(self):
        return self._reducers

    @property
    def mappers(self):
        return self._mappers

    @property
    def global_vars(self):
        return self._global_vars


#==============================================================================
# FUNCTIONS
#==============================================================================

def cls_from_path(path, clsname):
    Cls = runpy.run_path(path)[clsname]
    if not inspect.isclass(Cls) or not issubclass(Cls, ScriptBase):
        msg = "'{}' is not subclass of poopy.script.ScriptBase".format(Cls)
        raise TypeError(msg)
    Cls.class_name = "<class '{}:{}'>".format(path, clsname)
    return Cls








