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
import copy

from . import readers


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
        for vi in v:
            ctx.emit(k, vi)

    def setup(self, job):
        job.name = "NO-NAME"


""".strip()


#==============================================================================
# SCRIPT CLASSES
#==============================================================================

class ScriptMeta(abc.ABCMeta):

    def __init__(self, *args, **kwargs):
        super(ScriptMeta, self).__init__(*args, **kwargs)

        # modify readers list
        readers_dict = {r.__name__: r for r in readers.BaseReader.subclasses()}
        ReadersClass = collections.namedtuple("Readers", readers_dict.keys())
        self.readers = ReadersClass(**readers_dict)

    def __repr__(cls):
        return (
            getattr(cls, 'class_name', None) or
            super(ScriptMeta, cls).__repr__()
        )


class ScriptBase(object):

    class_name = None
    __metaclass__ = ScriptMeta

    @abc.abstractmethod
    def map(self, k, v, ctx):
        raise NotImplementedError()

    def reduce(self, k, v, ctx):
        for vi in v:
            ctx.emit(k, vi)

    @abc.abstractmethod
    def setup(self, ctx):
        raise NotImplementedError()


#==============================================================================
# JOB
#==============================================================================

class Job(object):

    def __init__(self, script, clsname, iname, uuid=None):
        self._script = script
        self._clsname = clsname
        self._name = "<NO-NAME>"
        self._global_vars = {}
        self._mappers = []
        self._reducers = []
        self._input_path = []
        self._output_path = "<NO-PATH>"
        self._iname = iname

        # populate this conf with the script
        self.script.setup(self)

    @property
    def script(self):
        return self._script

    @property
    def clsname(self):
        return self._clsname

    @property
    def iname(self):
        return self._iname

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

    @property
    def local_vars(self):
        return self._local_vars


#==============================================================================
# CONTEXTS
#==============================================================================

class Context(object):

    def __init__(self, emiter, job):
        self.job = job
        self._emiter = emiter

    def emit(self, k, v):
        self._emiter(k, v)


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








