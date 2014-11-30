#!/usr/bin/env python
# -*- coding: utf-8 -*-

from poopy import script


class Script(script.ScriptBase):

    def map(self, k, v, ctx):
        raise NotImplementedError()

    def reduce(self, k, v, ctx):
        raise NotImplementedError()

    def setup(self, job):
        job.set_name("Random Forest")
        #job.setJarByClass(MyJ.class);
        job.set_mapper("map")
        #job.setCombinerClass(IntSumReducer.class);
        job.set_reducer("reduce");
        #~ job.setOutputKeyClass(Text.class);
        #~ job.setOutputValueClass(IntWritable.class);
        job.set_input_path("poopyFS:///", script.ARFFReader)
        job.output_path("output")



