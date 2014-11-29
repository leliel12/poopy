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