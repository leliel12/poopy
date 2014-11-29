#!/usr/bin/env python
# -*- coding: utf-8 -*-

# "THE WISKEY-WARE LICENSE":
# <jbc.develop@gmail.com> wrote this file. As long as you retain this notice
# you can do whatever you want with this stuff. If we meet some day, and you
# think this stuff is worth it, you can buy me a WISKEY in return Juan BC


#==============================================================================
# DOCS
#==============================================================================

"""Script model for ampq

"""


#==============================================================================
# IMPORTS
#==============================================================================

import abc


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
# CLASS
#==============================================================================

class ScriptBase(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def map(self, k, v, ctx):
        raise NotImplementedError()

    @abc.abstractmethod
    def reduce(self, k, v, ctx):
        raise NotImplementedError()

    @abc.abstractmethod
    def setup(self, ctx):
        raise NotImplementedError()



