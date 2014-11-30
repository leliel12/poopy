#!/usr/bin/env python
# -*- coding: utf-8 -*-

from poopy import script


class Script(script.ScriptBase):

    def map(self, k, v, ctx):
        raise NotImplementedError()

    def reduce(self, k, v, ctx):
        raise NotImplementedError()

    def setup(self, job):
        job.name = "Random Forest"
        job.mappers.append("map")
        job.reducers.append("reduce")
        job.input_path.append(("poopyFS:///", self.readers.runpy))
        job.output_path = "output"



